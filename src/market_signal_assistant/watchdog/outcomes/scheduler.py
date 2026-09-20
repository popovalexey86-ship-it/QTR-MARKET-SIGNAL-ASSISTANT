from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from market_signal_assistant.watchdog.events.journal import WatchdogEventJournal
from market_signal_assistant.watchdog.events.models import WatchdogEventEvidence
from market_signal_assistant.watchdog.models import AnomalyType
from market_signal_assistant.watchdog.outcomes.journal import WatchdogOutcomeJournal
from market_signal_assistant.watchdog.outcomes.models import (
    OUTCOME_HORIZONS_MINUTES,
    BreakoutSide,
    ForwardOutcome,
    OutcomeDataQuality,
    PriceObservation,
    outcome_id,
    target_time,
)
from market_signal_assistant.watchdog.outcomes.price_journal import (
    WatchdogPriceJournal,
)

CHECKPOINT_SCHEMA_VERSION = 1


class OutcomeCheckpointError(RuntimeError):
    """Outcome checkpoint cannot be recovered or persisted safely."""


@dataclass(frozen=True, slots=True)
class PendingHorizon:
    event_id: str
    symbol: str
    horizon_minutes: int
    target_time: datetime


class JsonOutcomeCheckpointStore:
    def __init__(self, path: Path) -> None:
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> dict[str, tuple[PriceObservation, ...]]:
        if not self._path.exists():
            return {}
        try:
            payload: Any = json.loads(self._path.read_text(encoding="utf-8"))
            if (
                not isinstance(payload, dict)
                or payload.get("version") != CHECKPOINT_SCHEMA_VERSION
                or not isinstance(payload.get("events"), list)
            ):
                raise ValueError
            result: dict[str, tuple[PriceObservation, ...]] = {}
            for raw_event in payload["events"]:
                if not isinstance(raw_event, dict):
                    raise ValueError
                event_id = str(raw_event["event_id"])
                raw_points = raw_event["points"]
                if not event_id or not isinstance(raw_points, list):
                    raise ValueError
                if event_id in result:
                    raise ValueError
                result[event_id] = tuple(_point_from_json(item) for item in raw_points)
            return result
        except (
            OSError,
            TypeError,
            ValueError,
            KeyError,
            json.JSONDecodeError,
        ) as error:
            raise OutcomeCheckpointError("Outcome checkpoint is invalid.") from error

    def save(self, points: dict[str, tuple[PriceObservation, ...]]) -> None:
        payload = {
            "version": CHECKPOINT_SCHEMA_VERSION,
            "events": [
                {
                    "event_id": event_id,
                    "points": [_point_to_json(item) for item in points[event_id]],
                }
                for event_id in sorted(points)
            ],
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
            raise OutcomeCheckpointError(
                "Outcome checkpoint cannot be saved."
            ) from error


class ForwardOutcomeScheduler:
    """Recoverable fixed-horizon scheduler driven only by injected prices."""

    def __init__(
        self,
        events: WatchdogEventJournal,
        outcomes: WatchdogOutcomeJournal,
        checkpoint: JsonOutcomeCheckpointStore,
        *,
        prices: WatchdogPriceJournal | None = None,
        expansion_threshold: float = 0.01,
        reversal_threshold: float = 0.005,
    ) -> None:
        if not 0 < expansion_threshold < 1 or not 0 < reversal_threshold < 1:
            raise ValueError("Outcome movement thresholds must be between 0 and 1.")
        self._events = events
        self._outcomes = outcomes
        self._checkpoint = checkpoint
        self._prices = prices or WatchdogPriceJournal(
            outcomes.path.with_name("price_observations.jsonl")
        )
        self._expansion_threshold = expansion_threshold
        self._reversal_threshold = reversal_threshold
        all_events = {item.event_id: item for item in events.records()}
        completed = {
            (item.event_id, item.horizon_minutes) for item in outcomes.records()
        }
        self._used_observations = {
            (item.event_id, item.observed_at)
            for item in outcomes.records()
            if item.data_quality is not OutcomeDataQuality.MISSING
        }
        self._event_index = {
            event_id: event
            for event_id, event in all_events.items()
            if not all(
                (event_id, horizon) in completed for horizon in OUTCOME_HORIZONS_MINUTES
            )
        }
        self._completed = {item for item in completed if item[0] in self._event_index}
        self._used_observations = {
            item for item in self._used_observations if item[0] in self._event_index
        }
        loaded = checkpoint.load()
        self._points = {
            event_id: points
            for event_id, points in loaded.items()
            if event_id in self._event_index and not self._event_complete(event_id)
        }
        for event_id in self._event_index:
            if not self._event_complete(event_id):
                self._points.setdefault(event_id, ())
        raw_points = self._prices.records()
        for event_id, event in self._event_index.items():
            if self._event_complete(event_id):
                continue
            merged = {item.observation_id: item for item in self._points[event_id]}
            for item in raw_points:
                if (
                    item.symbol == event.symbol
                    and item.observed_at >= event.detected_at
                ):
                    merged[item.observation_id] = item
            self._points[event_id] = tuple(
                sorted(merged.values(), key=lambda item: item.observed_at)
            )

    def register(self, event: WatchdogEventEvidence) -> None:
        persisted = next(
            (
                item
                for item in self._events.records()
                if item.event_id == event.event_id
            ),
            None,
        )
        if persisted != event:
            raise ValueError("Outcome scheduling requires persisted event evidence.")
        self._event_index[event.event_id] = event
        if not self._event_complete(event.event_id):
            self._points.setdefault(event.event_id, ())
        self._persist_checkpoint()

    def pending_horizons(self) -> tuple[PendingHorizon, ...]:
        return tuple(
            PendingHorizon(
                event.event_id,
                event.symbol,
                horizon,
                target_time(event.detected_at, horizon),
            )
            for event in sorted(
                self._event_index.values(), key=lambda item: item.event_id
            )
            for horizon in OUTCOME_HORIZONS_MINUTES
            if (event.event_id, horizon) not in self._completed
        )

    def observe(self, point: PriceObservation) -> tuple[ForwardOutcome, ...]:
        self._prices.append(point)
        created: list[ForwardOutcome] = []
        for event in sorted(self._event_index.values(), key=lambda item: item.event_id):
            if event.symbol != point.symbol or self._event_complete(event.event_id):
                continue
            if point.observed_at < event.detected_at:
                continue
            points = self._points.setdefault(event.event_id, ())
            if points and point.observed_at <= points[-1].observed_at:
                if point == points[-1]:
                    pass
                else:
                    # Tier changes can switch 1m/5m/15m feeds. A coarser
                    # candle may therefore arrive after a newer fine-grained
                    # observation. Preserve it in the raw journal, but never
                    # regress an event's chronological outcome path.
                    continue
            else:
                points = (*points, point)
                self._points[event.event_id] = points
            created.extend(self._recover_event_due(event, points))
            if self._event_complete(event.event_id):
                self._points.pop(event.event_id, None)
        self._persist_checkpoint()
        return tuple(created)

    def recover_pending(self) -> tuple[ForwardOutcome, ...]:
        """Rebuild due logical outcomes from immutable raw price evidence."""
        created: list[ForwardOutcome] = []
        for event in sorted(self._event_index.values(), key=lambda item: item.event_id):
            points = self._points.get(event.event_id, ())
            created.extend(self._recover_event_due(event, points))
            if self._event_complete(event.event_id):
                self._points.pop(event.event_id, None)
        self._persist_checkpoint()
        return tuple(created)

    def mark_missing(
        self,
        *,
        as_of: datetime,
        grace: timedelta,
    ) -> tuple[ForwardOutcome, ...]:
        now = _utc(as_of)
        if grace < timedelta(0):
            raise ValueError("Missing-outcome grace cannot be negative.")
        created: list[ForwardOutcome] = []
        for event in sorted(self._event_index.values(), key=lambda item: item.event_id):
            for horizon in OUTCOME_HORIZONS_MINUTES:
                key = (event.event_id, horizon)
                target = target_time(event.detected_at, horizon)
                if key in self._completed or now < target + grace:
                    continue
                outcome = ForwardOutcome(
                    outcome_id=outcome_id(event.event_id, horizon),
                    event_id=event.event_id,
                    symbol=event.symbol,
                    horizon_minutes=horizon,
                    target_time=target,
                    observed_at=now,
                    available_at=now,
                    reference_price=event.price_at_detection,
                    horizon_price=None,
                    signed_return=None,
                    abs_return=None,
                    mfe_up=None,
                    mfe_down=None,
                    max_abs_excursion=None,
                    realized_range_after_event=None,
                    time_to_mfe_up_seconds=None,
                    time_to_mfe_down_seconds=None,
                    lateness_seconds=(now - target).total_seconds(),
                    data_quality=OutcomeDataQuality.MISSING,
                )
                self._outcomes.append(outcome)
                self._completed.add(key)
                created.append(outcome)
            if self._event_complete(event.event_id):
                self._points.pop(event.event_id, None)
        self._persist_checkpoint()
        return tuple(created)

    def _calculate(
        self,
        event: WatchdogEventEvidence,
        horizon: int,
        points: tuple[PriceObservation, ...],
        selected: PriceObservation,
    ) -> ForwardOutcome:
        reference = event.price_at_detection
        highs = tuple((item.high or item.price) for item in points)
        lows = tuple((item.low or item.price) for item in points)
        up_moves = tuple(value / reference - 1.0 for value in highs)
        down_moves = tuple(1.0 - value / reference for value in lows)
        mfe_up = max(0.0, max(up_moves))
        mfe_down = max(0.0, max(down_moves))
        signed_return = selected.price / reference - 1.0
        high_index = up_moves.index(max(up_moves))
        low_index = down_moves.index(max(down_moves))
        expansion = _expansion(
            event,
            points,
            reference,
            self._expansion_threshold,
            signed_return,
        )
        signs = tuple(_sign(item.price / reference - 1.0) for item in points)
        final_sign = _sign(signed_return)
        persistence = (
            sum(1 for item in signs if item == final_sign) / len(signs)
            if final_sign != 0 and signs
            else 0.0
        )
        volatility_persistence = _volatility_persistence(event, points)
        target = target_time(event.detected_at, horizon)
        lateness = max(0.0, (selected.available_at - target).total_seconds())
        on_time_tolerance = _source_interval(selected.source) + timedelta(seconds=30)
        return ForwardOutcome(
            outcome_id=outcome_id(event.event_id, horizon),
            event_id=event.event_id,
            symbol=event.symbol,
            horizon_minutes=horizon,
            target_time=target,
            observed_at=selected.observed_at,
            available_at=selected.available_at,
            reference_price=reference,
            horizon_price=selected.price,
            signed_return=signed_return,
            abs_return=abs(signed_return),
            mfe_up=mfe_up,
            mfe_down=mfe_down,
            max_abs_excursion=max(mfe_up, mfe_down),
            realized_range_after_event=(
                max((*highs, reference)) - min((*lows, reference))
            )
            / reference,
            time_to_mfe_up_seconds=(
                points[high_index].observed_at - event.detected_at
            ).total_seconds()
            if mfe_up > 0
            else 0.0,
            time_to_mfe_down_seconds=(
                points[low_index].observed_at - event.detected_at
            ).total_seconds()
            if mfe_down > 0
            else 0.0,
            lateness_seconds=lateness,
            data_quality=(
                OutcomeDataQuality.ON_TIME
                if lateness <= on_time_tolerance.total_seconds()
                else OutcomeDataQuality.LATE
            ),
            did_expansion_occur=expansion[0],
            time_to_expansion_seconds=expansion[1],
            expansion_magnitude=expansion[2],
            breakout_side=expansion[3],
            false_breakout=expansion[4],
            subsequent_abs_move=abs(signed_return),
            persistence=persistence,
            reversal=(
                mfe_up >= self._reversal_threshold
                and mfe_down >= self._reversal_threshold
            ),
            volatility_persistence=volatility_persistence,
        )

    def _recover_event_due(
        self,
        event: WatchdogEventEvidence,
        points: tuple[PriceObservation, ...],
    ) -> tuple[ForwardOutcome, ...]:
        created: list[ForwardOutcome] = []
        for horizon in OUTCOME_HORIZONS_MINUTES:
            key = (event.event_id, horizon)
            if key in self._completed:
                continue
            target = target_time(event.detected_at, horizon)
            selected = next(
                (
                    item
                    for item in points
                    if item.observed_at >= target
                    and (event.event_id, item.observed_at)
                    not in self._used_observations
                ),
                None,
            )
            if selected is None:
                continue
            path = tuple(
                item for item in points if item.observed_at <= selected.observed_at
            )
            outcome = self._calculate(event, horizon, path, selected)
            self._outcomes.append(outcome)
            self._completed.add(key)
            self._used_observations.add((event.event_id, selected.observed_at))
            created.append(outcome)
        return tuple(created)

    def _event_complete(self, event_id: str) -> bool:
        return all(
            (event_id, horizon) in self._completed
            for horizon in OUTCOME_HORIZONS_MINUTES
        )

    def _persist_checkpoint(self) -> None:
        retained = {
            event_id: points
            for event_id, points in self._points.items()
            if not self._event_complete(event_id)
        }
        self._checkpoint.save(retained)


def _source_interval(source: str) -> timedelta:
    for suffix, duration in (
        ("-1m", timedelta(minutes=1)),
        ("-5m", timedelta(minutes=5)),
        ("-15m", timedelta(minutes=15)),
    ):
        if source.endswith(suffix):
            return duration
    return timedelta(minutes=1)


def _expansion(
    event: WatchdogEventEvidence,
    points: tuple[PriceObservation, ...],
    reference: float,
    threshold: float,
    final_return: float,
) -> tuple[bool | None, float | None, float | None, BreakoutSide | None, bool | None]:
    relevant = {AnomalyType.COMPRESSION, AnomalyType.RANGE_BUILDUP}
    if not relevant.intersection(event.anomaly_types):
        return None, None, None, None, None
    up = tuple((item.high or item.price) / reference - 1.0 for item in points)
    down = tuple(1.0 - (item.low or item.price) / reference for item in points)
    up_hit = any(item >= threshold for item in up)
    down_hit = any(item >= threshold for item in down)
    occurred = up_hit or down_hit
    side = (
        BreakoutSide.BOTH
        if up_hit and down_hit
        else BreakoutSide.UP
        if up_hit
        else BreakoutSide.DOWN
        if down_hit
        else BreakoutSide.NONE
    )
    hit_indexes = tuple(
        index
        for index, (up_move, down_move) in enumerate(zip(up, down, strict=True))
        if max(up_move, down_move) >= threshold
    )
    time_to_expansion = (
        (points[hit_indexes[0]].observed_at - event.detected_at).total_seconds()
        if hit_indexes
        else None
    )
    magnitude = max((*up, *down), default=0.0)
    false_breakout = (
        occurred
        and side is not BreakoutSide.BOTH
        and (
            abs(final_return) < threshold * 0.25
            or (side is BreakoutSide.UP and final_return < 0)
            or (side is BreakoutSide.DOWN and final_return > 0)
        )
    )
    return occurred, time_to_expansion, magnitude, side, false_breakout


def _volatility_persistence(
    event: WatchdogEventEvidence,
    points: tuple[PriceObservation, ...],
) -> float | None:
    if AnomalyType.VOLATILITY_EXPANSION not in event.anomaly_types or len(points) < 2:
        return None
    baseline = next(
        (
            item.baseline_value
            for item in event.features
            if item.name == "realized_volatility_5"
        ),
        None,
    )
    if baseline is None or baseline <= 0:
        return None
    moves = tuple(
        abs(current.price / previous.price - 1.0)
        for previous, current in zip(points, points[1:], strict=False)
    )
    return sum(1 for item in moves if item >= baseline) / len(moves)


def _point_to_json(item: PriceObservation) -> dict[str, object]:
    return {
        "symbol": item.symbol,
        "observed_at": item.observed_at.isoformat(),
        "available_at": item.available_at.isoformat(),
        "price": item.price,
        "high": item.high,
        "low": item.low,
        "source": item.source,
    }


def _point_from_json(value: object) -> PriceObservation:
    if not isinstance(value, dict):
        raise ValueError
    return PriceObservation(
        symbol=str(value["symbol"]),
        observed_at=datetime.fromisoformat(str(value["observed_at"])),
        available_at=datetime.fromisoformat(str(value["available_at"])),
        price=float(value["price"]),
        high=float(value["high"]),
        low=float(value["low"]),
        source=str(value["source"]),
    )


def _sign(value: float) -> int:
    return 1 if value > 0 else -1 if value < 0 else 0


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Outcome scheduler time must be timezone-aware.")
    return value.astimezone(UTC)
