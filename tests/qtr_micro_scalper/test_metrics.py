from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_signal_assistant.qtr_micro_scalper.metrics import (
    MetricsSlice,
    ShadowMetricsAggregator,
    TradeMetricDimensions,
    TradeMetricsSummary,
    aggregate_shadow_metrics,
)
from market_signal_assistant.qtr_micro_scalper.setup_context import ShadowDirection
from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
    ShadowOutcomeStatus,
    ShadowTradeStage,
)
from market_signal_assistant.qtr_micro_scalper.shadow_journal import (
    ShadowTradeJournal,
    ShadowTradeRecord,
)
from market_signal_assistant.qtr_micro_scalper.shadow_runtime import (
    ShadowRuntimeEvent,
    ShadowRuntimeEventType,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def runtime_event(
    trade_id: str,
    symbol: str,
    event_type: ShadowRuntimeEventType,
    *,
    seconds: int = 0,
) -> ShadowRuntimeEvent:
    return ShadowRuntimeEvent(
        event_id=f"{trade_id}-{event_type.value}-{seconds}",
        event_type=event_type,
        occurred_at=NOW + timedelta(seconds=seconds),
        trade_id=trade_id,
        symbol=symbol,
        stage=(
            ShadowTradeStage.EXPIRED
            if event_type is ShadowRuntimeEventType.EXPIRED
            else ShadowTradeStage.CLOSED
        ),
        price=None if event_type is ShadowRuntimeEventType.EXPIRED else 100.0,
        realized_r=0.0,
        message=f"Virtual {event_type.value}.",
    )


def record(
    trade_id: str,
    *,
    symbol: str = "BTCUSDT",
    direction: ShadowDirection = ShadowDirection.LONG,
    score: float = 85.0,
    stage: ShadowTradeStage = ShadowTradeStage.CLOSED,
    outcome: ShadowOutcomeStatus = ShadowOutcomeStatus.WIN,
    result_r: float = 2.0,
    mfe: float = 3.0,
    mae: float = 0.5,
    entered: bool = True,
    seconds: int = 10,
    events: tuple[ShadowRuntimeEvent, ...] = (),
) -> ShadowTradeRecord:
    return ShadowTradeRecord(
        recorded_at=NOW + timedelta(seconds=seconds),
        trade_id=trade_id,
        symbol=symbol,
        direction=direction,
        stage=stage,
        entry=100.0,
        stop=99.0,
        tp1=101.0,
        tp2=102.0,
        score=score,
        reasons=("Deterministic shadow record.",),
        warnings=(),
        entry_time=NOW + timedelta(seconds=1) if entered else None,
        exit_time=(
            NOW + timedelta(seconds=seconds)
            if stage in {ShadowTradeStage.CLOSED, ShadowTradeStage.EXPIRED}
            else None
        ),
        outcome=outcome,
        result_r=result_r,
        mfe=mfe,
        mae=mae,
        events=events,
    )


def sample_records() -> tuple[ShadowTradeRecord, ...]:
    tp1 = runtime_event("win", "BTCUSDT", ShadowRuntimeEventType.TP1_REACHED, seconds=5)
    tp2 = runtime_event(
        "win", "BTCUSDT", ShadowRuntimeEventType.TP2_REACHED, seconds=10
    )
    stopped = runtime_event(
        "loss", "ETHUSDT", ShadowRuntimeEventType.STOPPED, seconds=10
    )
    expired = runtime_event(
        "expired", "BTCUSDT", ShadowRuntimeEventType.EXPIRED, seconds=10
    )
    return (
        record("win", events=(tp1, tp2)),
        record(
            "loss",
            symbol="ETHUSDT",
            direction=ShadowDirection.SHORT,
            score=70.0,
            outcome=ShadowOutcomeStatus.LOSS,
            result_r=-1.0,
            mfe=0.2,
            mae=1.0,
            events=(stopped,),
        ),
        record(
            "breakeven",
            symbol="SOLUSDT",
            score=60.0,
            outcome=ShadowOutcomeStatus.BREAKEVEN,
            result_r=0.0,
            mfe=1.2,
            mae=0.3,
            events=(
                runtime_event(
                    "breakeven",
                    "SOLUSDT",
                    ShadowRuntimeEventType.TP1_REACHED,
                    seconds=5,
                ),
            ),
        ),
        record(
            "expired",
            score=45.0,
            stage=ShadowTradeStage.EXPIRED,
            outcome=ShadowOutcomeStatus.NOT_TRIGGERED,
            result_r=0.0,
            mfe=0.0,
            mae=0.0,
            entered=False,
            events=(expired,),
        ),
    )


def slices(values: tuple[MetricsSlice, ...]) -> dict[str, TradeMetricsSummary]:
    return {item.key: item.metrics for item in values}


def test_overall_shadow_performance_metrics() -> None:
    snapshot = aggregate_shadow_metrics(sample_records(), generated_at=NOW)
    metrics = snapshot.overall
    assert metrics.targets == 3
    assert metrics.snapshots == 4
    assert metrics.scores == 4
    assert metrics.shadow_entries == 3
    assert metrics.closed_trades == 3
    assert metrics.wins == 1
    assert metrics.losses == 1
    assert metrics.breakeven == 1
    assert metrics.expired == 1
    assert metrics.win_rate == pytest.approx(100 / 3)
    assert metrics.loss_rate == pytest.approx(100 / 3)
    assert metrics.total_r == pytest.approx(1.0)
    assert metrics.average_r == pytest.approx(1 / 3)
    assert metrics.average_mfe == pytest.approx((3.0 + 0.2 + 1.2) / 3)
    assert metrics.average_mae == pytest.approx((0.5 + 1.0 + 0.3) / 3)
    assert metrics.tp1_hit_rate == pytest.approx(200 / 3)
    assert metrics.tp2_hit_rate == pytest.approx(100 / 3)
    assert metrics.stop_rate == pytest.approx(100 / 3)
    assert metrics.expired_rate == 25.0


def test_lifecycle_records_are_deduplicated_by_trade_id() -> None:
    waiting = record(
        "win",
        stage=ShadowTradeStage.WAITING_ENTRY,
        outcome=ShadowOutcomeStatus.PENDING,
        result_r=0.0,
        mfe=0.0,
        mae=0.0,
        entered=False,
        seconds=0,
    )
    closed = sample_records()[0]
    snapshot = aggregate_shadow_metrics((closed, waiting), generated_at=NOW)
    assert snapshot.journal_records == 2
    assert snapshot.unique_trade_plans == 1
    assert snapshot.overall.shadow_entries == 1
    assert snapshot.overall.closed_trades == 1
    assert snapshot.overall.total_r == 2.0


def test_breakdown_by_symbol_and_direction() -> None:
    snapshot = aggregate_shadow_metrics(sample_records(), generated_at=NOW)
    by_symbol = slices(snapshot.by_symbol)
    by_direction = slices(snapshot.by_direction)
    assert tuple(by_symbol) == ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    assert by_symbol["BTCUSDT"].snapshots == 2
    assert by_symbol["ETHUSDT"].losses == 1
    assert by_direction["LONG"].shadow_entries == 2
    assert by_direction["SHORT"].losses == 1


def test_score_range_boundaries_and_ordering() -> None:
    records = (
        record("a", score=49.999),
        record("b", score=50.0),
        record("c", score=65.0),
        record("d", score=80.0),
        record("e", score=100.0),
    )
    snapshot = aggregate_shadow_metrics(records, generated_at=NOW)
    ranges = slices(snapshot.by_score_range)
    assert tuple(ranges) == ("0-49", "50-64", "65-79", "80-100")
    assert ranges["0-49"].scores == 1
    assert ranges["50-64"].scores == 1
    assert ranges["65-79"].scores == 1
    assert ranges["80-100"].scores == 2


def test_market_state_and_setup_type_dimensions() -> None:
    records = sample_records()
    dimensions = {
        "win": TradeMetricDimensions("BUY_PRESSURE", "BREAKOUT"),
        "loss": TradeMetricDimensions("SELL_PRESSURE", "RETEST"),
        "breakeven": TradeMetricDimensions("RANGE", "REVERSAL"),
        "expired": TradeMetricDimensions("LOW_ACTIVITY", "BREAKOUT"),
    }
    snapshot = aggregate_shadow_metrics(
        records,
        generated_at=NOW,
        dimensions=dimensions,
    )
    market = slices(snapshot.by_market_state)
    setups = slices(snapshot.by_setup_type)
    assert market["BUY_PRESSURE"].wins == 1
    assert market["SELL_PRESSURE"].losses == 1
    assert setups["BREAKOUT"].snapshots == 2
    assert setups["RETEST"].total_r == -1.0


def test_missing_dimensions_are_explicit_not_inferred() -> None:
    snapshot = aggregate_shadow_metrics((record("one"),), generated_at=NOW)
    assert tuple(slices(snapshot.by_market_state)) == ("UNKNOWN",)
    assert tuple(slices(snapshot.by_setup_type)) == ("UNSPECIFIED",)


def test_open_trade_counts_as_entry_but_not_closed_trade() -> None:
    opened = record(
        "open",
        stage=ShadowTradeStage.OPEN,
        outcome=ShadowOutcomeStatus.OPEN,
        result_r=0.0,
        seconds=5,
    )
    snapshot = aggregate_shadow_metrics((opened,), generated_at=NOW)
    assert snapshot.overall.shadow_entries == 1
    assert snapshot.overall.closed_trades == 0
    assert snapshot.overall.win_rate == 0.0
    assert snapshot.overall.average_r == 0.0


def test_empty_journal_returns_zero_metrics() -> None:
    snapshot = aggregate_shadow_metrics((), generated_at=NOW)
    assert snapshot.journal_records == 0
    assert snapshot.unique_trade_plans == 0
    assert snapshot.overall.closed_trades == 0
    assert snapshot.overall.total_r == 0.0
    assert snapshot.by_symbol == ()


def test_aggregation_is_deterministic_for_input_order() -> None:
    records = sample_records()
    first = aggregate_shadow_metrics(records, generated_at=NOW)
    second = aggregate_shadow_metrics(tuple(reversed(records)), generated_at=NOW)
    assert first == second


def test_aggregator_reads_recovered_journal(tmp_path: Path) -> None:
    path = tmp_path / "shadow.jsonl"
    journal = ShadowTradeJournal(path)
    assert all(journal.append(item) for item in sample_records())
    restarted = ShadowTradeJournal(path)
    snapshot = ShadowMetricsAggregator(restarted).snapshot(generated_at=NOW)
    assert snapshot.journal_records == 4
    assert snapshot.overall.closed_trades == 3
    assert snapshot.overall.expired == 1


def test_metrics_models_are_immutable_and_timestamp_is_utc() -> None:
    snapshot = aggregate_shadow_metrics((record("one"),), generated_at=NOW)
    assert snapshot.generated_at.tzinfo is UTC
    with pytest.raises(FrozenInstanceError):
        snapshot.journal_records = 0  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        snapshot.overall.total_r = 0.0  # type: ignore[misc]


def test_naive_generated_at_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        aggregate_shadow_metrics((), generated_at=datetime(2026, 8, 16, 12))
