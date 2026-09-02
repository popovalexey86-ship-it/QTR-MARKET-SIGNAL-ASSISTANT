from __future__ import annotations

from collections import defaultdict
from statistics import median

from market_signal_assistant.qtr_micro_scalper_v3.models import (
    ImpulseDirection,
    V3AnalyticsSnapshot,
    V3DirectionStats,
    V3ForwardOutcome,
    V3TradeRecord,
)


class V3AnalyticsEngine:
    """Deterministic acceptance report over frozen V3 shadow records."""

    def analyze(
        self,
        trades: tuple[V3TradeRecord, ...],
        outcomes: tuple[V3ForwardOutcome, ...],
    ) -> V3AnalyticsSnapshot:
        terminal = tuple(record for record in trades if record.exit_at is not None)
        net = tuple(record.net_return_pct for record in terminal)
        gross = sum(record.gross_return_pct for record in terminal)
        costs = sum(record.transaction_cost_pct for record in terminal)
        wins = sum(value > 0 for value in net)
        positive = sum(value for value in net if value > 0)
        negative = abs(sum(value for value in net if value < 0))
        profit_factor = None if negative == 0 else positive / negative
        drawdown = _maximum_drawdown(net)
        reach_025, reach_050 = _reach_rates(outcomes)
        count = len(terminal)
        by_direction = {
            direction: _direction_stats(
                tuple(record for record in terminal if record.direction is direction)
            )
            for direction in (ImpulseDirection.LONG, ImpulseDirection.SHORT)
        }
        return V3AnalyticsSnapshot(
            trade_count=count,
            long_count=sum(
                record.direction is ImpulseDirection.LONG for record in terminal
            ),
            short_count=sum(
                record.direction is ImpulseDirection.SHORT for record in terminal
            ),
            gross_return_pct=_round(gross),
            transaction_cost_pct=_round(costs),
            net_return_pct=_round(sum(net)),
            mean_net_per_trade_pct=_round(sum(net) / count) if count else 0.0,
            median_net_per_trade_pct=_round(median(net)) if net else 0.0,
            win_rate=wins / count if count else 0.0,
            profit_factor=_round(profit_factor) if profit_factor is not None else None,
            max_drawdown_pct=_round(drawdown),
            reach_025_by_seconds=reach_025,
            reach_050_by_seconds=reach_050,
            by_direction=by_direction,
        )


def _direction_stats(records: tuple[V3TradeRecord, ...]) -> V3DirectionStats:
    net = tuple(record.net_return_pct for record in records)
    positive = sum(value for value in net if value > 0)
    negative = abs(sum(value for value in net if value < 0))
    count = len(records)
    return V3DirectionStats(
        trade_count=count,
        gross_return_pct=_round(sum(record.gross_return_pct for record in records)),
        transaction_cost_pct=_round(
            sum(record.transaction_cost_pct for record in records)
        ),
        net_return_pct=_round(sum(net)),
        mean_net_per_trade_pct=_round(sum(net) / count) if count else 0.0,
        median_net_per_trade_pct=_round(median(net)) if net else 0.0,
        win_rate=sum(value > 0 for value in net) / count if count else 0.0,
        profit_factor=(
            _round(positive / negative) if negative > 0 else None
        ),
        max_drawdown_pct=_round(_maximum_drawdown(net)),
    )


def _reach_rates(
    outcomes: tuple[V3ForwardOutcome, ...],
) -> tuple[dict[int, float], dict[int, float]]:
    grouped: dict[int, list[V3ForwardOutcome]] = defaultdict(list)
    for outcome in outcomes:
        if outcome.window_seconds in {60, 180, 300, 600}:
            grouped[outcome.window_seconds].append(outcome)
    rates_025: dict[int, float] = {}
    rates_050: dict[int, float] = {}
    for window in (60, 180, 300, 600):
        records = grouped[window]
        rates_025[window] = (
            sum(record.reached_025 for record in records) / len(records)
            if records
            else 0.0
        )
        rates_050[window] = (
            sum(record.reached_050 for record in records) / len(records)
            if records
            else 0.0
        )
    return rates_025, rates_050


def _maximum_drawdown(returns: tuple[float, ...]) -> float:
    equity = 0.0
    peak = 0.0
    maximum = 0.0
    for value in returns:
        equity += value
        peak = max(peak, equity)
        maximum = max(maximum, peak - equity)
    return maximum


def _round(value: float) -> float:
    return round(value, 12)
