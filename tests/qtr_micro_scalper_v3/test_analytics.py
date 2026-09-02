from __future__ import annotations

from datetime import timedelta

from market_signal_assistant.qtr_micro_scalper_v3.analytics import V3AnalyticsEngine
from market_signal_assistant.qtr_micro_scalper_v3.models import (
    ImpulseDirection,
    V3ExitReason,
    V3ForwardOutcome,
    V3TradeRecord,
)
from qtr_micro_scalper_v3.helpers import NOW


def trade(trade_id: str, direction: ImpulseDirection, net: float) -> V3TradeRecord:
    return V3TradeRecord(
        record_id=f"record-{trade_id}",
        recorded_at=NOW,
        trade_id=trade_id,
        symbol="BTCUSDT",
        impulse_id=f"impulse-{trade_id}",
        direction=direction,
        entry_at=NOW - timedelta(seconds=30),
        exit_at=NOW,
        entry_price=100.0,
        exit_price=100.0 * (1 + net / 100),
        exit_reason=V3ExitReason.CASH_TARGET if net > 0 else V3ExitReason.CASH_STOP,
        gross_return_pct=net + 0.1,
        transaction_cost_pct=0.1,
        net_return_pct=net,
        mfe_pct=max(net, 0.0),
        mae_pct=max(-net, 0.0),
    )


def outcome(
    entry_id: str,
    direction: ImpulseDirection,
    window: int,
) -> V3ForwardOutcome:
    return V3ForwardOutcome(
        record_id=f"outcome-{entry_id}-{window}",
        entry_id=entry_id,
        symbol="BTCUSDT",
        direction=direction,
        entry_at=NOW,
        measured_at=NOW + timedelta(seconds=window),
        window_seconds=window,
        mfe_pct=0.6,
        mae_pct=0.1,
        gross_hypothetical_pct=0.3,
        transaction_cost_pct=0.1,
        net_hypothetical_pct=0.2,
        reached_025=True,
        reached_050=window >= 180,
        time_to_025_seconds=40.0,
        time_to_050_seconds=120.0 if window >= 180 else None,
    )


def test_minimum_acceptance_report_and_direction_splits() -> None:
    trades = (
        trade("1", ImpulseDirection.LONG, 0.3),
        trade("2", ImpulseDirection.SHORT, -0.2),
    )
    outcomes = tuple(
        outcome(str(index), direction, window)
        for index, direction in (
            (1, ImpulseDirection.LONG),
            (2, ImpulseDirection.SHORT),
        )
        for window in (60, 180, 300, 600)
    )
    report = V3AnalyticsEngine().analyze(trades, outcomes)

    assert report.trade_count == 2
    assert report.long_count == 1
    assert report.short_count == 1
    assert report.gross_return_pct == 0.3
    assert report.transaction_cost_pct == 0.2
    assert report.net_return_pct == 0.1
    assert report.win_rate == 0.5
    assert report.profit_factor == 1.5
    assert report.reach_025_by_seconds[600] == 1.0
    assert report.reach_050_by_seconds[60] == 0.0
    assert report.reach_050_by_seconds[180] == 1.0
    assert report.by_direction[ImpulseDirection.LONG].trade_count == 1
    assert report.by_direction[ImpulseDirection.SHORT].net_return_pct == -0.2
