from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from test_metrics import record as trade_record
from test_metrics import runtime_event

from market_signal_assistant.qtr_micro_scalper.analytics import (
    AnalyticsMetrics,
    AnalyticsSlice,
    IncrementalDecisionAnalytics,
    ShadowAnalyticsEngine,
    analyze_shadow_journals,
)
from market_signal_assistant.qtr_micro_scalper.decision_journal import (
    ShadowDecisionEventType,
    ShadowDecisionJournal,
    ShadowDecisionRecord,
)
from market_signal_assistant.qtr_micro_scalper.scoring import (
    ScalperComponentScores,
)
from market_signal_assistant.qtr_micro_scalper.setup_context import ShadowDirection
from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
    ShadowOutcomeStatus,
    ShadowTradeStage,
)
from market_signal_assistant.qtr_micro_scalper.shadow_journal import (
    ShadowTradeJournal,
)
from market_signal_assistant.qtr_micro_scalper.shadow_runtime import (
    ShadowRuntimeEventType,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def decision(
    event_type: ShadowDecisionEventType,
    *,
    symbol: str = "BTCUSDT",
    score: float = 85.0,
    market_state: str = "BUY_PRESSURE",
    setup_context: str = "BREAKOUT",
    reasons: tuple[str, ...] = ("Explainable shadow decision.",),
    warnings: tuple[str, ...] = (),
    seconds: int = 0,
) -> ShadowDecisionRecord:
    return ShadowDecisionRecord(
        timestamp=NOW + timedelta(seconds=seconds),
        symbol=symbol,
        event_type=event_type,
        score=score,
        score_components=ScalperComponentScores(
            liquidity_score=20.0,
            trade_flow_score=25.0,
            orderbook_score=15.0,
            market_state_score=12.0,
            setup_score=8.0,
            risk_score=-5.0,
        ),
        market_state=market_state,
        setup_context=setup_context,
        reasons=reasons,
        warnings=warnings,
    )


def slices(values: tuple[AnalyticsSlice, ...]) -> dict[str, AnalyticsMetrics]:
    return {item.key: item.metrics for item in values}


def test_overall_operational_analytics() -> None:
    decisions = (
        decision(ShadowDecisionEventType.SHADOW_ENTRY_CREATED),
        decision(
            ShadowDecisionEventType.SHADOW_ENTRY_CREATED,
            symbol="ETHUSDT",
            score=70.0,
            market_state="SELL_PRESSURE",
            setup_context="RETEST",
            seconds=1,
        ),
        decision(
            ShadowDecisionEventType.DECISION_BLOCKED,
            symbol="SOLUSDT",
            score=55.0,
            reasons=("Spread is too wide.",),
            seconds=2,
        ),
    )
    stopped = runtime_event(
        "loss",
        "ETHUSDT",
        ShadowRuntimeEventType.STOPPED,
        seconds=10,
    )
    trades = (
        trade_record("win"),
        trade_record(
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
        trade_record(
            "open",
            symbol="XRPUSDT",
            score=65.0,
            stage=ShadowTradeStage.OPEN,
            outcome=ShadowOutcomeStatus.OPEN,
            result_r=0.0,
            seconds=5,
        ),
    )
    snapshot = analyze_shadow_journals(decisions, trades, generated_at=NOW)
    assert snapshot.overall.total_decisions == 3
    assert snapshot.overall.blocked_decisions == 1
    assert snapshot.overall.shadow_entries == 2
    assert snapshot.overall.completed_trades == 2
    assert snapshot.overall.active_trades == 1
    assert snapshot.overall.wins == 1
    assert snapshot.overall.win_rate == 50.0
    assert snapshot.overall.average_r == 0.5
    assert snapshot.overall.total_r == 1.0


def test_score_bucket_boundaries_and_order() -> None:
    decisions = tuple(
        decision(
            ShadowDecisionEventType.DECISION_BLOCKED,
            symbol=f"S{index}USDT",
            score=score,
            seconds=index,
        )
        for index, score in enumerate(
            (0.0, 49.999, 50.0, 64.999, 65.0, 79.999, 80.0, 100.0)
        )
    )
    snapshot = analyze_shadow_journals(decisions, (), generated_at=NOW)
    buckets = slices(snapshot.by_score_bucket)
    assert tuple(buckets) == ("0-49", "50-64", "65-79", "80-100")
    assert [metrics.total_decisions for metrics in buckets.values()] == [2, 2, 2, 2]


def test_symbol_direction_market_state_and_setup_breakdowns() -> None:
    decisions = (
        decision(ShadowDecisionEventType.SHADOW_ENTRY_CREATED),
        decision(
            ShadowDecisionEventType.SHADOW_ENTRY_CREATED,
            symbol="ETHUSDT",
            score=70.0,
            market_state="SELL_PRESSURE",
            setup_context="RETEST",
            seconds=1,
        ),
        decision(
            ShadowDecisionEventType.DECISION_BLOCKED,
            symbol="SOLUSDT",
            score=55.0,
            market_state="BALANCED",
            setup_context="WATCH",
            seconds=2,
        ),
    )
    trades = (
        trade_record("btc"),
        trade_record(
            "eth",
            symbol="ETHUSDT",
            direction=ShadowDirection.SHORT,
            score=70.0,
            outcome=ShadowOutcomeStatus.LOSS,
            result_r=-1.0,
        ),
    )
    snapshot = analyze_shadow_journals(decisions, trades, generated_at=NOW)
    assert slices(snapshot.by_symbol)["BTCUSDT"].completed_trades == 1
    assert slices(snapshot.by_direction)["LONG"].shadow_entries == 1
    assert slices(snapshot.by_direction)["SHORT"].shadow_entries == 1
    assert slices(snapshot.by_direction)["UNKNOWN"].blocked_decisions == 1
    assert slices(snapshot.by_market_state)["BUY_PRESSURE"].wins == 1
    assert slices(snapshot.by_market_state)["SELL_PRESSURE"].total_r == -1.0
    assert slices(snapshot.by_setup_type)["BREAKOUT"].shadow_entries == 1
    assert slices(snapshot.by_setup_type)["WATCH"].blocked_decisions == 1


def test_top_blocked_reasons_are_classified_and_ranked() -> None:
    decisions = (
        decision(
            ShadowDecisionEventType.DECISION_BLOCKED,
            reasons=("Spread is too wide; risk is elevated.",),
        ),
        decision(
            ShadowDecisionEventType.DECISION_BLOCKED,
            symbol="ETHUSDT",
            reasons=("Spread exceeds limit.",),
            seconds=1,
        ),
        decision(
            ShadowDecisionEventType.DECISION_BLOCKED,
            symbol="SOLUSDT",
            reasons=("Opposing liquidity creates a liquidity conflict.",),
            warnings=("Trade data is stale.", "Low score below threshold."),
            seconds=2,
        ),
    )
    snapshot = analyze_shadow_journals(decisions, (), generated_at=NOW)
    assert [(item.reason, item.count) for item in snapshot.top_blocked_reasons] == [
        ("spread", 2),
        ("liquidity conflict", 1),
        ("low score", 1),
        ("risk", 1),
        ("stale data", 1),
    ]


def test_top_loss_reasons_cover_stop_expired_and_failed_setup() -> None:
    stopped = runtime_event(
        "stopped",
        "BTCUSDT",
        ShadowRuntimeEventType.STOPPED,
    )
    expired = runtime_event(
        "expired",
        "ETHUSDT",
        ShadowRuntimeEventType.EXPIRED,
    )
    trades = (
        trade_record(
            "stopped",
            outcome=ShadowOutcomeStatus.LOSS,
            result_r=-1.0,
            events=(stopped,),
        ),
        trade_record(
            "expired",
            symbol="ETHUSDT",
            stage=ShadowTradeStage.EXPIRED,
            outcome=ShadowOutcomeStatus.NOT_TRIGGERED,
            result_r=0.0,
            mfe=0.0,
            mae=0.0,
            entered=False,
            events=(expired,),
        ),
        trade_record(
            "failed",
            symbol="SOLUSDT",
            outcome=ShadowOutcomeStatus.LOSS,
            result_r=-0.4,
        ),
    )
    snapshot = analyze_shadow_journals((), trades, generated_at=NOW)
    assert snapshot.overall.completed_trades == 3
    assert snapshot.overall.win_rate == 0.0
    assert [(item.reason, item.count) for item in snapshot.top_loss_reasons] == [
        ("expired", 1),
        ("failed setup", 1),
        ("stop", 1),
    ]


def test_duplicate_records_are_counted_once_and_order_is_deterministic() -> None:
    blocked = decision(ShadowDecisionEventType.DECISION_BLOCKED)
    trade = trade_record("one")
    first = analyze_shadow_journals(
        (blocked, blocked),
        (trade, trade),
        generated_at=NOW,
    )
    second = analyze_shadow_journals(
        tuple(reversed((blocked, blocked))),
        tuple(reversed((trade, trade))),
        generated_at=NOW,
    )
    assert first == second
    assert first.overall.total_decisions == 1
    assert first.overall.completed_trades == 1


def test_engine_reads_both_recovered_journals(tmp_path: Path) -> None:
    decision_path = tmp_path / "decisions.jsonl"
    trade_path = tmp_path / "trades.jsonl"
    decisions = ShadowDecisionJournal(decision_path)
    trades = ShadowTradeJournal(trade_path)
    assert decisions.append(decision(ShadowDecisionEventType.SHADOW_ENTRY_CREATED))
    assert trades.append(trade_record("one"))
    engine = ShadowAnalyticsEngine(
        ShadowDecisionJournal(decision_path),
        ShadowTradeJournal(trade_path),
    )
    snapshot = engine.snapshot(generated_at=NOW)
    assert snapshot.decision_journal_records == 1
    assert snapshot.trade_journal_records == 1
    assert snapshot.overall.shadow_entries == 1
    assert snapshot.overall.completed_trades == 1


def test_empty_snapshot_is_immutable_and_utc_aware() -> None:
    snapshot = analyze_shadow_journals((), (), generated_at=NOW)
    assert snapshot.generated_at.tzinfo is UTC
    assert snapshot.overall == AnalyticsMetrics(0, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0)
    assert snapshot.by_score_bucket == ()
    assert snapshot.top_blocked_reasons == ()
    assert snapshot.top_loss_reasons == ()
    with pytest.raises(FrozenInstanceError):
        snapshot.overall.total_r = 1.0  # type: ignore[misc]


def test_naive_generated_at_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        analyze_shadow_journals(
            (),
            (),
            generated_at=datetime(2026, 8, 16, 12),
        )

def test_incremental_analytics_is_semantically_equivalent() -> None:
    decisions = (
        decision(ShadowDecisionEventType.SCORE_CREATED, seconds=0),
        decision(ShadowDecisionEventType.SHADOW_ENTRY_CREATED, seconds=1),
        decision(
            ShadowDecisionEventType.DECISION_BLOCKED,
            symbol="ETHUSDT",
            score=55.0,
            market_state="SELL_PRESSURE",
            setup_context="RETEST",
            reasons=("Spread is too wide.",),
            seconds=2,
        ),
    )
    trades = (trade_record("win"),)
    incremental = IncrementalDecisionAnalytics()
    for item in decisions:
        incremental.consume(item)

    expected = analyze_shadow_journals(decisions, trades, generated_at=NOW)
    actual = incremental.snapshot(trades, generated_at=NOW)

    assert actual == expected


def test_incremental_analytics_retains_only_entry_records() -> None:
    incremental = IncrementalDecisionAnalytics()
    for seconds in range(1_000):
        incremental.consume(
            decision(
                ShadowDecisionEventType.SCORE_CREATED,
                seconds=seconds,
            )
        )
    incremental.consume(
        decision(
            ShadowDecisionEventType.DECISION_BLOCKED,
            seconds=1_001,
            reasons=("Stale data.",),
        )
    )
    incremental.consume(
        decision(
            ShadowDecisionEventType.SHADOW_ENTRY_CREATED,
            seconds=1_002,
        )
    )

    metrics = incremental.metrics()

    assert metrics.records_processed == 1_002
    assert metrics.retained_entry_records == 1
    assert metrics.aggregate_state_size < 20
