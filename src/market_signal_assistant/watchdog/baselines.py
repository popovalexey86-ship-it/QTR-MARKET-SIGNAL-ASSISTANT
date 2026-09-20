from __future__ import annotations

import json
import math
import os
import tempfile
from collections import defaultdict
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

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> tuple[BaselineObservation, ...]:
        if not self._path.exists():
            return ()
        try:
            payload: Any = json.loads(self._path.read_text(encoding="utf-8"))
            if (
                not isinstance(payload, dict)
                or payload.get("version") not in {1, BASELINE_SCHEMA_VERSION}
                or not isinstance(payload.get("observations"), list)
            ):
                raise ValueError
            return tuple(
                _observation_from_json(item) for item in payload["observations"]
            )
        except (
            OSError,
            TypeError,
            ValueError,
            KeyError,
            json.JSONDecodeError,
        ) as error:
            raise BaselineStateError("Watchdog baseline state is invalid.") from error

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
        except OSError as error:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
            raise BaselineStateError(
                "Watchdog baseline state cannot be saved."
            ) from error


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
        self._observations = self._bounded(store.load())

    @property
    def observations(self) -> tuple[BaselineObservation, ...]:
        return self._observations

    def observe(
        self,
        observation: BaselineObservation,
        *,
        detected_at: datetime,
    ) -> None:
        decision_time = _utc(detected_at)
        if observation.available_at > decision_time:
            raise ValueError("Future observation cannot enter a PIT baseline.")
        key = (observation.symbol, observation.scope, observation.feature)
        previous = tuple(
            item
            for item in self._observations
            if (item.symbol, item.scope, item.feature) == key
        )
        if previous and observation.available_at <= previous[-1].available_at:
            raise ValueError(
                "Baseline observations must arrive in chronological order."
            )
        self._observations = self._bounded((*self._observations, observation))
        self._store.save(self._observations)

    def observe_many(
        self,
        observations: tuple[BaselineObservation, ...],
        *,
        detected_at: datetime,
    ) -> None:
        """Validate a PIT batch and persist it with one atomic replacement."""
        decision_time = _utc(detected_at)
        candidate = self._observations
        for observation in observations:
            if observation.available_at > decision_time:
                raise ValueError("Future observation cannot enter a PIT baseline.")
            key = (observation.symbol, observation.scope, observation.feature)
            previous = tuple(
                item
                for item in candidate
                if (item.symbol, item.scope, item.feature) == key
            )
            if previous and observation.available_at == previous[-1].available_at:
                if observation == previous[-1]:
                    continue
                raise ValueError("Conflicting baseline observation timestamp.")
            if previous and observation.available_at < previous[-1].available_at:
                raise ValueError(
                    "Baseline observations must arrive in chronological order."
                )
            candidate = self._bounded((*candidate, observation))
        self._store.save(candidate)
        self._observations = candidate

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
            for item in self._observations
            if item.symbol == normalized_symbol
            and item.scope == normalized_scope
            and item.feature == normalized_feature
            and item.available_at <= as_of
        )[-self._maximum_samples :]
        if not selected and normalized_scope != "default":
            selected = tuple(
                item.value
                for item in self._observations
                if item.symbol == normalized_symbol
                and item.scope in {"default", "legacy"}
                and item.feature == normalized_feature
                and item.available_at <= as_of
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
        for item in self._observations:
            if item.feature == normalized_feature and item.available_at <= as_of:
                counts_by_scope[(item.symbol, item.scope)] += 1
        counts: dict[str, int] = defaultdict(int)
        for (symbol, _scope), count in counts_by_scope.items():
            counts[symbol] = max(counts[symbol], count)
        return dict(counts)

    def _bounded(
        self,
        observations: tuple[BaselineObservation, ...],
    ) -> tuple[BaselineObservation, ...]:
        grouped: dict[tuple[str, str, str], list[BaselineObservation]] = defaultdict(
            list
        )
        for item in observations:
            grouped[(item.symbol, item.scope, item.feature)].append(item)
        retained = tuple(
            item
            for key in sorted(grouped)
            for item in sorted(grouped[key], key=lambda value: value.available_at)[
                -self._maximum_samples :
            ]
        )
        return retained


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
