from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime

from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
    ShadowOutcomeStatus,
    ShadowTradeStage,
)
from market_signal_assistant.qtr_micro_scalper.shadow_journal import (
    ShadowTradeJournal,
    ShadowTradeRecord,
)
from market_signal_assistant.qtr_micro_scalper.shadow_runtime import (
    ShadowRuntimeEventType,
)

UNKNOWN_MARKET_STATE = "UNKNOWN"
UNSPECIFIED_SETUP_TYPE = "UNSPECIFIED"
_SCORE_RANGES = (
    ("0-49", 0.0, 50.0),
    ("50-64", 50.0, 65.0),
    ("65-79", 65.0, 80.0),
    ("80-100", 80.0, 101.0),
)


@dataclass(frozen=True, slots=True)
class TradeMetricDimensions:
    market_state: str = UNKNOWN_MARKET_STATE
    setup_type: str = UNSPECIFIED_SETUP_TYPE

    def __post_init__(self) -> None:
        market_state = self.market_state.strip().upper()
        setup_type = self.setup_type.strip().upper()
        if not market_state or not setup_type:
            raise ValueError("Metric dimensions cannot be empty.")
        object.__setattr__(self, "market_state", market_state)
        object.__setattr__(self, "setup_type", setup_type)


@dataclass(frozen=True, slots=True)
class TradeMetricsSummary:
    targets: int
    snapshots: int
    scores: int
    shadow_entries: int
    closed_trades: int
    wins: int
    losses: int
    breakeven: int
    expired: int
    win_rate: float
    loss_rate: float
    average_r: float
    total_r: float
    average_mfe: float
    average_mae: float
    tp1_hit_rate: float
    tp2_hit_rate: float
    stop_rate: float
    expired_rate: float


@dataclass(frozen=True, slots=True)
class MetricsSlice:
    key: str
    metrics: TradeMetricsSummary

    def __post_init__(self) -> None:
        key = self.key.strip()
        if not key:
            raise ValueError("Metrics slice key cannot be empty.")
        object.__setattr__(self, "key", key)


@dataclass(frozen=True, slots=True)
class MetricsSnapshot:
    generated_at: datetime
    journal_records: int
    unique_trade_plans: int
    overall: TradeMetricsSummary
    by_symbol: tuple[MetricsSlice, ...]
    by_direction: tuple[MetricsSlice, ...]
    by_score_range: tuple[MetricsSlice, ...]
    by_market_state: tuple[MetricsSlice, ...]
    by_setup_type: tuple[MetricsSlice, ...]
    warnings: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "generated_at", _utc(self.generated_at))


@dataclass(frozen=True, slots=True)
class _TradeLifecycle:
    trade_id: str
    records: tuple[ShadowTradeRecord, ...]
    latest: ShadowTradeRecord
    entered: bool
    closed: bool
    tp1_hit: bool
    tp2_hit: bool
    stopped: bool
    expired: bool
    dimensions: TradeMetricDimensions


class ShadowMetricsAggregator:
    """Deterministic performance statistics over the durable shadow journal."""

    def __init__(self, journal: ShadowTradeJournal) -> None:
        self._journal = journal

    def snapshot(
        self,
        *,
        generated_at: datetime,
        dimensions: Mapping[str, TradeMetricDimensions] | None = None,
    ) -> MetricsSnapshot:
        return aggregate_shadow_metrics(
            self._journal.records(),
            generated_at=generated_at,
            dimensions=dimensions,
        )


def aggregate_shadow_metrics(
    records: Iterable[ShadowTradeRecord],
    *,
    generated_at: datetime,
    dimensions: Mapping[str, TradeMetricDimensions] | None = None,
) -> MetricsSnapshot:
    """Aggregate the latest lifecycle per trade, independent of input order."""

    normalized_at = _utc(generated_at)
    materialized = tuple(records)
    supplied_dimensions = dimensions or {}
    lifecycles = _lifecycles(materialized, supplied_dimensions)
    return MetricsSnapshot(
        generated_at=normalized_at,
        journal_records=len(materialized),
        unique_trade_plans=len(lifecycles),
        overall=_summary(lifecycles),
        by_symbol=_breakdown(lifecycles, lambda item: item.latest.symbol),
        by_direction=_breakdown(
            lifecycles,
            lambda item: item.latest.direction.value,
        ),
        by_score_range=_score_breakdown(lifecycles),
        by_market_state=_breakdown(
            lifecycles,
            lambda item: item.dimensions.market_state,
        ),
        by_setup_type=_breakdown(
            lifecycles,
            lambda item: item.dimensions.setup_type,
        ),
        warnings=(
            "Target, snapshot and score counts are journal-observable unique "
            "shadow trade plans; rejected pre-trade observations are not stored "
            "in ShadowTradeJournal.",
        ),
    )


def _lifecycles(
    records: tuple[ShadowTradeRecord, ...],
    dimensions: Mapping[str, TradeMetricDimensions],
) -> tuple[_TradeLifecycle, ...]:
    grouped: dict[str, list[ShadowTradeRecord]] = defaultdict(list)
    for record in records:
        grouped[record.trade_id].append(record)
    lifecycles: list[_TradeLifecycle] = []
    for trade_id in sorted(grouped):
        ordered = tuple(sorted(grouped[trade_id], key=_record_order))
        latest = ordered[-1]
        event_types = {
            event.event_type for record in ordered for event in record.events
        }
        entered = any(
            record.entry_time is not None
            or record.stage
            in {
                ShadowTradeStage.OPEN,
                ShadowTradeStage.TP1_HIT,
                ShadowTradeStage.CLOSED,
            }
            for record in ordered
        )
        expired = (
            latest.stage is ShadowTradeStage.EXPIRED
            or ShadowRuntimeEventType.EXPIRED in event_types
            or latest.outcome is ShadowOutcomeStatus.NOT_TRIGGERED
        )
        closed = entered and latest.stage in {
            ShadowTradeStage.CLOSED,
            ShadowTradeStage.EXPIRED,
        }
        lifecycles.append(
            _TradeLifecycle(
                trade_id=trade_id,
                records=ordered,
                latest=latest,
                entered=entered,
                closed=closed,
                tp1_hit=(
                    any(record.stage is ShadowTradeStage.TP1_HIT for record in ordered)
                    or ShadowRuntimeEventType.TP1_REACHED in event_types
                    or ShadowRuntimeEventType.TP2_REACHED in event_types
                ),
                tp2_hit=ShadowRuntimeEventType.TP2_REACHED in event_types,
                stopped=ShadowRuntimeEventType.STOPPED in event_types,
                expired=expired,
                dimensions=dimensions.get(trade_id, TradeMetricDimensions()),
            )
        )
    return tuple(lifecycles)


def _summary(lifecycles: tuple[_TradeLifecycle, ...]) -> TradeMetricsSummary:
    entries = tuple(item for item in lifecycles if item.entered)
    closed = tuple(item for item in lifecycles if item.closed)
    wins = sum(item.latest.outcome is ShadowOutcomeStatus.WIN for item in closed)
    losses = sum(item.latest.outcome is ShadowOutcomeStatus.LOSS for item in closed)
    breakeven = sum(
        item.latest.outcome is ShadowOutcomeStatus.BREAKEVEN for item in closed
    )
    expired = sum(item.expired for item in lifecycles)
    result_values = tuple(item.latest.result_r for item in closed)
    mfe_values = tuple(item.latest.mfe for item in closed)
    mae_values = tuple(item.latest.mae for item in closed)
    return TradeMetricsSummary(
        targets=len({item.latest.symbol for item in lifecycles}),
        snapshots=len(lifecycles),
        scores=len(lifecycles),
        shadow_entries=len(entries),
        closed_trades=len(closed),
        wins=wins,
        losses=losses,
        breakeven=breakeven,
        expired=expired,
        win_rate=_rate(wins, len(closed)),
        loss_rate=_rate(losses, len(closed)),
        average_r=_average(result_values),
        total_r=sum(result_values),
        average_mfe=_average(mfe_values),
        average_mae=_average(mae_values),
        tp1_hit_rate=_rate(sum(item.tp1_hit for item in entries), len(entries)),
        tp2_hit_rate=_rate(sum(item.tp2_hit for item in entries), len(entries)),
        stop_rate=_rate(sum(item.stopped for item in closed), len(closed)),
        expired_rate=_rate(expired, len(lifecycles)),
    )


def _breakdown(
    lifecycles: tuple[_TradeLifecycle, ...],
    key: Callable[[_TradeLifecycle], str],
) -> tuple[MetricsSlice, ...]:
    grouped: dict[str, list[_TradeLifecycle]] = defaultdict(list)
    for lifecycle in lifecycles:
        grouped[key(lifecycle)].append(lifecycle)
    return tuple(
        MetricsSlice(slice_key, _summary(tuple(grouped[slice_key])))
        for slice_key in sorted(grouped)
    )


def _score_breakdown(
    lifecycles: tuple[_TradeLifecycle, ...],
) -> tuple[MetricsSlice, ...]:
    grouped: dict[str, list[_TradeLifecycle]] = defaultdict(list)
    for lifecycle in lifecycles:
        grouped[_score_range(lifecycle.latest.score)].append(lifecycle)
    return tuple(
        MetricsSlice(label, _summary(tuple(grouped[label])))
        for label, _minimum, _maximum in _SCORE_RANGES
        if label in grouped
    )


def _score_range(score: float) -> str:
    return next(
        label for label, minimum, maximum in _SCORE_RANGES if minimum <= score < maximum
    )


def _record_order(record: ShadowTradeRecord) -> tuple[datetime, int, str]:
    stage_order = {
        ShadowTradeStage.WAITING_ENTRY: 0,
        ShadowTradeStage.OPEN: 1,
        ShadowTradeStage.TP1_HIT: 2,
        ShadowTradeStage.CLOSED: 3,
        ShadowTradeStage.EXPIRED: 3,
    }
    return record.recorded_at, stage_order[record.stage], record.record_id


def _rate(numerator: int, denominator: int) -> float:
    return 0.0 if denominator == 0 else numerator / denominator * 100.0


def _average(values: tuple[float, ...]) -> float:
    return 0.0 if not values else sum(values) / len(values)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Metrics timestamp must be timezone-aware.")
    return value.astimezone(UTC)
