from __future__ import annotations

import json
import math
import os
import tempfile
from collections import defaultdict, deque
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import fmean, pstdev
from typing import Any

BASELINE_SCHEMA_VERSION = 2


class BaselineStateError(RuntimeError):
    """The durable baseline state cannot be read or written safely."""


@dataclass(frozen=True, slots=True)
class BaselineObservation:
    symbol: str
    feature: str
    value: float
    observed_at: datetime
    available_at: datetime
    scope: str = "default"

    def __post_init__(self) -> None:
        if (
            not self.symbol.strip()
            or not self.feature.strip()
            or not self.scope.strip()
        ):
            raise ValueError("Baseline symbol and feature are required.")
        if isinstance(self.value, bool) or not math.isfinite(self.value):
            raise ValueError("Baseline observation must be finite.")
        observed_at = _utc(self.observed_at)
        available_at = _utc(self.available_at)
        if observed_at > available_at:
            raise ValueError("Baseline value cannot be available before observation.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(self, "feature", self.feature.strip())
        object.__setattr__(self, "scope", self.scope.strip())
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "available_at", available_at)


@dataclass(frozen=True, slots=True)
class BaselineSnapshot:
    symbol: str
    feature: str
    as_of: datetime
    sample_count: int
    minimum_samples: int
    cold_start: bool
    mean: float | None
    standard_deviation: float | None
    minimum: float | None
    maximum: float | None
    scope: str = "default"


class JsonBaselineStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._updates_path = path.with_name(f"{path.stem}.updates.jsonl")

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> tuple[BaselineObservation, ...]:
        if not self._path.exists():
            snapshot: tuple[BaselineObservation, ...] = ()
        else:
            try:
                payload: Any = json.loads(self._path.read_text(encoding="utf-8"))
                if (
                    not isinstance(payload, dict)
                    or payload.get("version") not in {1, BASELINE_SCHEMA_VERSION}
                    or not isinstance(payload.get("observations"), list)
                ):
                    raise ValueError
                snapshot = tuple(
                    _observation_from_json(item) for item in payload["observations"]
                )
            except (
                OSError,
                TypeError,
                ValueError,
                KeyError,
                json.JSONDecodeError,
            ) as error:
                raise BaselineStateError(
                    "Watchdog baseline state is invalid."
                ) from error
        return self._merge_updates(snapshot)

    def save(self, observations: tuple[BaselineObservation, ...]) -> None:
        payload = {
            "version": BASELINE_SCHEMA_VERSION,
            "observations": [_observation_to_json(item) for item in observations],
        }
        temporary: Path | None = None
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self._path.parent,
                prefix=f".{self._path.name}.",
                suffix=".tmp",
                delete=False,
            ) as stream:
                temporary = Path(stream.name)
                json.dump(payload, stream, sort_keys=True, separators=(",", ":"))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self._path)
            # The compact snapshot is durable before the replay log is reset.
            self._updates_path.unlink(missing_ok=True)
        except OSError as error:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
            raise BaselineStateError(
                "Watchdog baseline state cannot be saved."
            ) from error

    def append_many(self, observations: tuple[BaselineObservation, ...]) -> None:
        if not observations:
            return
        try:
            self._updates_path.parent.mkdir(parents=True, exist_ok=True)
            with self._updates_path.open("a", encoding="utf-8") as stream:
                for item in observations:
                    json.dump(
                        _observation_to_json(item),
                        stream,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
        except OSError as error:
            raise BaselineStateError(
                "Watchdog baseline update cannot be appended."
            ) from error

    @property
    def pending_update_count(self) -> int:
        if not self._updates_path.exists():
            return 0
        with self._updates_path.open("rb") as stream:
            return sum(1 for line in stream if line.endswith(b"\n"))

    def _merge_updates(
        self, snapshot: tuple[BaselineObservation, ...]
    ) -> tuple[BaselineObservation, ...]:
        if not self._updates_path.exists():
            return snapshot
        merged = list(snapshot)
        identities = {
            (item.symbol, item.scope, item.feature, item.available_at): item
            for item in snapshot
        }
        try:
            with self._updates_path.open("r", encoding="utf-8") as stream:
                for raw in stream:
                    if not raw.endswith("\n"):
                        continue
                    item = _observation_from_json(json.loads(raw))
                    key = (item.symbol, item.scope, item.feature, item.available_at)
                    existing = identities.get(key)
                    if existing is not None:
                        if existing != item:
                            raise ValueError
                        continue
                    identities[key] = item
                    merged.append(item)
        except (
            OSError,
            ValueError,
            TypeError,
            KeyError,
            json.JSONDecodeError,
        ) as error:
            raise BaselineStateError(
                "Watchdog baseline updates are invalid."
            ) from error
        return tuple(merged)


class RollingBaselineEngine:
    """Bounded persisted observations queried strictly by availability time."""

    def __init__(
        self,
        store: JsonBaselineStore,
        *,
        minimum_samples: int = 20,
        maximum_samples: int = 240,
    ) -> None:
        if minimum_samples <= 0 or maximum_samples < minimum_samples:
            raise ValueError("Baseline sample limits are inconsistent.")
        self._store = store
        self._minimum_samples = minimum_samples
        self._maximum_samples = maximum_samples
        self._rings: dict[
            tuple[str, str, str], deque[BaselineObservation]
        ] = {}
        for item in sorted(store.load(), key=lambda value: value.available_at):
            self._ring_for(item).append(item)
        self._pending_updates = store.pending_update_count
        if self._pending_updates >= maximum_samples * 4:
            store.save(self.observations)
            self._pending_updates = 0

    @property
    def observations(self) -> tuple[BaselineObservation, ...]:
        return tuple(
            item for key in sorted(self._rings) for item in self._rings[key]
        )

    @property
    def retained_counts(self) -> tuple[int, int, int]:
        """Key count, observation count and configured per-key bound."""
        return (
            len(self._rings),
            sum(len(items) for items in self._rings.values()),
            self._maximum_samples,
        )

    def observe(
        self,
        observation: BaselineObservation,
        *,
        detected_at: datetime,
    ) -> None:
        decision_time = _utc(detected_at)
        if observation.available_at > decision_time:
            raise ValueError("Future observation cannot enter a PIT baseline.")
        previous = self._ring_for(observation)
        if previous and observation.available_at <= previous[-1].available_at:
            raise ValueError(
                "Baseline observations must arrive in chronological order."
            )
        self._store.append_many((observation,))
        previous.append(observation)
        self._pending_updates += 1
        self._compact_if_due()

    def observe_many(
        self,
        observations: tuple[BaselineObservation, ...],
        *,
        detected_at: datetime,
    ) -> None:
        """Validate a PIT batch and persist it with one atomic replacement."""
        decision_time = _utc(detected_at)
        pending: list[BaselineObservation] = []
        last_by_key = {
            key: items[-1] for key, items in self._rings.items() if items
        }
        for observation in observations:
            if observation.available_at > decision_time:
                raise ValueError("Future observation cannot enter a PIT baseline.")
            key = self._key(observation)
            previous = last_by_key.get(key)
            if previous and observation.available_at == previous.available_at:
                if observation == previous:
                    continue
                raise ValueError("Conflicting baseline observation timestamp.")
            if previous and observation.available_at < previous.available_at:
                raise ValueError(
                    "Baseline observations must arrive in chronological order."
                )
            last_by_key[key] = observation
            pending.append(observation)
        self._store.append_many(tuple(pending))
        for observation in pending:
            self._ring_for(observation).append(observation)
        self._pending_updates += len(pending)
        self._compact_if_due()

    def snapshot(
        self,
        symbol: str,
        feature: str,
        *,
        detected_at: datetime,
        scope: str = "default",
    ) -> BaselineSnapshot:
        as_of = _utc(detected_at)
        normalized_symbol = symbol.strip().upper()
        normalized_feature = feature.strip()
        normalized_scope = scope.strip()
        if not normalized_symbol or not normalized_feature or not normalized_scope:
            raise ValueError("Baseline identity cannot be empty.")
        selected = tuple(
            item.value
            for item in self._rings.get(
                (normalized_symbol, normalized_scope, normalized_feature), ()
            )
            if item.available_at <= as_of
        )
        if not selected and normalized_scope != "default":
            selected = tuple(
                item.value
                for fallback_scope in ("default", "legacy")
                for item in self._rings.get(
                    (normalized_symbol, fallback_scope, normalized_feature), ()
                )
                if item.available_at <= as_of
            )[-self._maximum_samples :]
        cold_start = len(selected) < self._minimum_samples
        if cold_start:
            mean = deviation = minimum = maximum = None
        else:
            mean = fmean(selected)
            deviation = pstdev(selected)
            minimum = min(selected)
            maximum = max(selected)
        return BaselineSnapshot(
            symbol=normalized_symbol,
            feature=normalized_feature,
            as_of=as_of,
            sample_count=len(selected),
            minimum_samples=self._minimum_samples,
            cold_start=cold_start,
            mean=mean,
            standard_deviation=deviation,
            minimum=minimum,
            maximum=maximum,
            scope=normalized_scope,
        )

    def sample_counts(
        self,
        feature: str,
        *,
        detected_at: datetime,
    ) -> Mapping[str, int]:
        as_of = _utc(detected_at)
        normalized_feature = feature.strip()
        if not normalized_feature:
            raise ValueError("Baseline feature cannot be empty.")
        counts_by_scope: dict[tuple[str, str], int] = defaultdict(int)
        for (symbol, scope, item_feature), items in self._rings.items():
            if item_feature == normalized_feature:
                counts_by_scope[(symbol, scope)] = sum(
                    item.available_at <= as_of for item in items
                )
        counts: dict[str, int] = defaultdict(int)
        for (symbol, _scope), count in counts_by_scope.items():
            counts[symbol] = max(counts[symbol], count)
        return dict(counts)

    @staticmethod
    def _key(observation: BaselineObservation) -> tuple[str, str, str]:
        return observation.symbol, observation.scope, observation.feature

    def _ring_for(
        self, observation: BaselineObservation
    ) -> deque[BaselineObservation]:
        return self._rings.setdefault(
            self._key(observation), deque(maxlen=self._maximum_samples)
        )

    def _compact_if_due(self) -> None:
        if self._pending_updates >= self._maximum_samples * 4:
            self._store.save(self.observations)
            self._pending_updates = 0


def _observation_to_json(item: BaselineObservation) -> dict[str, object]:
    return {
        "symbol": item.symbol,
        "feature": item.feature,
        "value": item.value,
        "observed_at": item.observed_at.isoformat(),
        "available_at": item.available_at.isoformat(),
        "scope": item.scope,
    }


def _observation_from_json(value: object) -> BaselineObservation:
    if not isinstance(value, dict):
        raise ValueError
    return BaselineObservation(
        symbol=str(value["symbol"]),
        feature=str(value["feature"]),
        value=float(value["value"]),
        observed_at=datetime.fromisoformat(str(value["observed_at"])),
        available_at=datetime.fromisoformat(str(value["available_at"])),
        scope=str(value.get("scope", "legacy")),
    )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Watchdog time must be timezone-aware.")
    return value.astimezone(UTC)
