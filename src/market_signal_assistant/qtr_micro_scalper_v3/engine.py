from __future__ import annotations

import hashlib
from collections import OrderedDict
from dataclasses import dataclass, replace

from market_signal_assistant.qtr_micro_scalper_v3.models import (
    CashCostEstimate,
    ImpulseDirection,
    ImpulseSnapshot,
    SweepDirection,
    V3EntryDecision,
    V3ExitReason,
    V3PriceObservation,
    V3ShadowTrade,
    V3TradeStage,
    V3TradeUpdate,
)


@dataclass(frozen=True, slots=True)
class CashScalperConfig:
    """Explicit V3 baseline hypotheses; none are calibrated production claims."""

    max_data_age_ms: float = 1_500.0
    max_spread_bps: float = 8.0
    min_depth_10bps: float = 25_000.0
    taker_fee_bps_per_side: float = 5.5
    slippage_bps_per_side: float = 1.0
    target_bps: float = 35.0
    stop_bps: float = 20.0
    runner_target_bps: float = 50.0
    runner_fraction: float = 0.0
    time_stop_seconds: float = 90.0
    min_net_potential_bps: float = 10.0
    min_potential_cost_ratio: float = 1.5
    min_flow_imbalance: float = 0.25
    min_price_response_bps: float = 3.0
    min_price_response_per_10k: float = 0.25
    min_flow_acceleration: float = 0.75
    max_impulse_age_seconds: float = 20.0
    max_impulse_displacement_bps: float = 20.0
    max_trigger_progress_atr: float = 0.75
    max_opposing_book_imbalance: float = 0.10

    def __post_init__(self) -> None:
        positive = (
            self.max_data_age_ms,
            self.max_spread_bps,
            self.min_depth_10bps,
            self.taker_fee_bps_per_side,
            self.target_bps,
            self.stop_bps,
            self.runner_target_bps,
            self.time_stop_seconds,
            self.min_net_potential_bps,
            self.min_potential_cost_ratio,
            self.min_flow_imbalance,
            self.min_price_response_bps,
            self.min_price_response_per_10k,
            self.min_flow_acceleration,
            self.max_impulse_age_seconds,
            self.max_impulse_displacement_bps,
            self.max_trigger_progress_atr,
            self.max_opposing_book_imbalance,
        )
        if any(value <= 0 for value in positive):
            raise ValueError("V3 baseline thresholds must be positive.")
        if self.slippage_bps_per_side < 0:
            raise ValueError("V3 slippage cannot be negative.")
        if not 0.0 <= self.runner_fraction < 1.0:
            raise ValueError("V3 runner fraction must be in [0, 1).")
        if self.runner_target_bps < self.target_bps:
            raise ValueError("Runner target cannot precede the primary target.")


class CashScalperEngine:
    """Independent V3 decision and cash-first shadow lifecycle engine."""

    def __init__(
        self,
        config: CashScalperConfig | None = None,
        *,
        max_remembered_symbols: int = 1_000,
    ) -> None:
        if max_remembered_symbols < 1:
            raise ValueError("Remembered symbol capacity must be positive.")
        self._config = config or CashScalperConfig()
        self._max_remembered_symbols = max_remembered_symbols
        self._terminal_impulses: OrderedDict[str, str] = OrderedDict()

    @property
    def config(self) -> CashScalperConfig:
        return self._config

    @property
    def remembered_symbol_count(self) -> int:
        return len(self._terminal_impulses)

    def estimate_cost(
        self,
        snapshot: ImpulseSnapshot,
        notional: float,
    ) -> CashCostEstimate:
        if notional <= 0:
            raise ValueError("V3 notional must be positive.")
        fee_bps = self._config.taker_fee_bps_per_side * 2.0
        slippage_bps = self._config.slippage_bps_per_side * 2.0
        total_bps = fee_bps + snapshot.spread_bps + slippage_bps
        total_pct = total_bps / 100.0
        return CashCostEstimate(
            fee_bps=fee_bps,
            spread_bps=snapshot.spread_bps,
            slippage_bps=slippage_bps,
            total_round_trip_bps=total_bps,
            total_round_trip_pct=total_pct,
            expected_cash=notional * total_bps / 10_000.0,
        )

    def evaluate(
        self,
        snapshot: ImpulseSnapshot,
        *,
        notional: float = 1_000.0,
    ) -> V3EntryDecision:
        cost = self.estimate_cost(snapshot, notional)
        blocking: list[str] = []
        reasons: list[str] = []
        warnings: list[str] = []
        direction = snapshot.direction

        if snapshot.source_age_ms > self._config.max_data_age_ms:
            blocking.append("stale_market_data")
        if snapshot.spread_bps > self._config.max_spread_bps:
            blocking.append("spread_too_wide")
        if snapshot.bid_depth_10bps < self._config.min_depth_10bps:
            blocking.append("insufficient_bid_liquidity")
        if snapshot.ask_depth_10bps < self._config.min_depth_10bps:
            blocking.append("insufficient_ask_liquidity")
        net_potential = snapshot.estimated_potential_bps - cost.total_round_trip_bps
        potential_ratio = snapshot.estimated_potential_bps / cost.total_round_trip_bps
        if (
            net_potential < self._config.min_net_potential_bps
            or potential_ratio < self._config.min_potential_cost_ratio
        ):
            blocking.append("insufficient_net_potential")
        if direction is ImpulseDirection.NONE:
            blocking.append("no_directional_impulse")
        else:
            sign = 1.0 if direction is ImpulseDirection.LONG else -1.0
            if snapshot.flow_imbalance_5s * sign < self._config.min_flow_imbalance:
                blocking.append("insufficient_aggressive_flow")
            if (
                snapshot.price_displacement_5s_bps * sign
                < self._config.min_price_response_bps
                or snapshot.price_response_bps_per_10k
                < self._config.min_price_response_per_10k
            ):
                blocking.append("flow_price_not_aligned")
            if (
                snapshot.orderbook_imbalance * sign
                < -self._config.max_opposing_book_imbalance
            ):
                blocking.append("opposing_orderbook_pressure")
            if _opposing_sweep(direction, snapshot.sweep_direction):
                blocking.append("opposing_liquidity_sweep")
        if snapshot.flow_acceleration < self._config.min_flow_acceleration:
            blocking.append("flow_not_accelerating")
        if snapshot.absorption_detected:
            blocking.append("absorption_or_exhaustion")
        if snapshot.impulse_age_seconds > self._config.max_impulse_age_seconds:
            blocking.append("impulse_too_old")
        if (
            snapshot.impulse_displacement_bps
            > self._config.max_impulse_displacement_bps
        ):
            blocking.append("impulse_already_extended")
        if (
            snapshot.trigger_progress_atr is not None
            and snapshot.trigger_progress_atr > self._config.max_trigger_progress_atr
        ):
            blocking.append("trigger_progress_too_far")
        if self._terminal_impulses.get(snapshot.symbol) == snapshot.impulse_id:
            blocking.append("impulse_already_traded")

        if not blocking:
            reasons.extend(
                (
                    "spread_and_depth_tradable",
                    "aggressive_flow_moves_price",
                    "fresh_impulse_not_extended",
                    "potential_materially_exceeds_cost",
                )
            )
            if snapshot.sweep_direction is not SweepDirection.NONE:
                reasons.append("directional_sweep")
        if snapshot.trigger_progress_atr is None:
            warnings.append("trigger_progress_unavailable")

        entry = (
            _entry_price(snapshot)
            if direction is not ImpulseDirection.NONE
            else None
        )
        target = _move(entry, direction, self._config.target_bps) if entry else None
        stop = _move(entry, direction, -self._config.stop_bps) if entry else None
        break_even = (
            _move(entry, direction, cost.total_round_trip_bps) if entry else None
        )
        return V3EntryDecision(
            accepted=not blocking,
            direction=direction,
            evaluated_at=snapshot.observed_at,
            impulse_id=snapshot.impulse_id,
            entry_price=entry,
            target_price=target,
            stop_price=stop,
            break_even_price=break_even,
            cost=cost,
            reasons=tuple(reasons),
            warnings=tuple(warnings),
            blocking_reasons=tuple(dict.fromkeys(blocking)),
            snapshot=snapshot,
        )

    def open_shadow_trade(self, decision: V3EntryDecision) -> V3ShadowTrade:
        if not decision.accepted:
            raise ValueError("Blocked V3 decision cannot create a shadow trade.")
        assert decision.entry_price is not None
        assert decision.target_price is not None
        assert decision.stop_price is not None
        assert decision.break_even_price is not None
        trade_id = _trade_id(decision)
        return V3ShadowTrade(
            trade_id=trade_id,
            symbol=decision.snapshot.symbol,
            impulse_id=decision.impulse_id,
            direction=decision.direction,
            stage=V3TradeStage.OPEN,
            entry_at=decision.evaluated_at,
            entry_price=decision.entry_price,
            stop_price=decision.stop_price,
            target_price=decision.target_price,
            runner_target_price=_move(
                decision.entry_price,
                decision.direction,
                self._config.runner_target_bps,
            ),
            runner_stop_price=decision.break_even_price,
            round_trip_cost_pct=decision.cost.total_round_trip_pct,
            runner_fraction=self._config.runner_fraction,
            remaining_fraction=1.0,
            last_observed_at=decision.evaluated_at,
            last_price=decision.entry_price,
        )

    def manage(
        self,
        trade: V3ShadowTrade,
        observation: V3PriceObservation,
    ) -> V3TradeUpdate:
        if trade.stage is V3TradeStage.CLOSED:
            return V3TradeUpdate(trade, False)
        if observation.symbol != trade.symbol:
            raise ValueError("V3 price observation belongs to another symbol.")
        if observation.observed_at <= trade.last_observed_at:
            return V3TradeUpdate(trade, False)

        favorable = _directional_return_pct(
            trade.direction, trade.entry_price, observation.price
        )
        mfe = max(trade.mfe_pct, favorable)
        mae = max(trade.mae_pct, -favorable)
        updated = replace(
            trade,
            last_observed_at=observation.observed_at,
            last_price=observation.price,
            mfe_pct=mfe,
            mae_pct=mae,
        )
        if observation.directional_failure:
            return V3TradeUpdate(
                _close(updated, observation, V3ExitReason.DIRECTIONAL_FAILURE), True
            )

        if trade.stage is V3TradeStage.OPEN:
            if _stop_reached(trade, observation.price):
                return V3TradeUpdate(
                    _close(updated, observation, V3ExitReason.CASH_STOP), True
                )
            if _target_reached(trade.direction, trade.target_price, observation.price):
                if trade.runner_fraction == 0.0:
                    return V3TradeUpdate(
                        _close(updated, observation, V3ExitReason.CASH_TARGET), True
                    )
                primary_fraction = 1.0 - trade.runner_fraction
                primary_gross = favorable * primary_fraction
                return V3TradeUpdate(
                    replace(
                        updated,
                        stage=V3TradeStage.RUNNER,
                        primary_exit_at=observation.observed_at,
                        remaining_fraction=trade.runner_fraction,
                        gross_return_pct=primary_gross,
                    ),
                    True,
                )
        else:
            if _target_reached(
                trade.direction, trade.runner_target_price, observation.price
            ):
                return V3TradeUpdate(
                    _close_runner(updated, observation, V3ExitReason.RUNNER_TARGET),
                    True,
                )
            if _runner_stop_reached(trade, observation.price):
                return V3TradeUpdate(
                    _close_runner(updated, observation, V3ExitReason.RUNNER_STOP),
                    True,
                )

        if (
            observation.observed_at - trade.entry_at
        ).total_seconds() >= self._config.time_stop_seconds:
            return V3TradeUpdate(
                _close(updated, observation, V3ExitReason.TIME_STOP), True
            )
        return V3TradeUpdate(updated, True)

    def remember_terminal(self, trade: V3ShadowTrade) -> None:
        if trade.stage is not V3TradeStage.CLOSED:
            raise ValueError("Only terminal V3 trades can close an impulse episode.")
        self._terminal_impulses.pop(trade.symbol, None)
        self._terminal_impulses[trade.symbol] = trade.impulse_id
        while len(self._terminal_impulses) > self._max_remembered_symbols:
            self._terminal_impulses.popitem(last=False)


def _entry_price(snapshot: ImpulseSnapshot) -> float:
    return (
        snapshot.best_ask
        if snapshot.direction is ImpulseDirection.LONG
        else snapshot.best_bid
    )


def _move(price: float, direction: ImpulseDirection, bps: float) -> float:
    sign = 1.0 if direction is ImpulseDirection.LONG else -1.0
    return price * (1.0 + sign * bps / 10_000.0)


def _directional_return_pct(
    direction: ImpulseDirection, entry: float, price: float
) -> float:
    sign = 1.0 if direction is ImpulseDirection.LONG else -1.0
    return sign * (price - entry) / entry * 100.0


def _target_reached(
    direction: ImpulseDirection, target: float, price: float
) -> bool:
    if direction is ImpulseDirection.LONG:
        return price >= target
    return price <= target


def _stop_reached(trade: V3ShadowTrade, price: float) -> bool:
    if trade.direction is ImpulseDirection.LONG:
        return price <= trade.stop_price
    return price >= trade.stop_price


def _runner_stop_reached(trade: V3ShadowTrade, price: float) -> bool:
    if trade.direction is ImpulseDirection.LONG:
        return price <= trade.runner_stop_price
    return price >= trade.runner_stop_price


def _close(
    trade: V3ShadowTrade,
    observation: V3PriceObservation,
    reason: V3ExitReason,
) -> V3ShadowTrade:
    gross = _directional_return_pct(
        trade.direction, trade.entry_price, observation.price
    )
    return replace(
        trade,
        stage=V3TradeStage.CLOSED,
        remaining_fraction=0.0,
        exit_at=observation.observed_at,
        exit_price=observation.price,
        exit_reason=reason,
        gross_return_pct=gross,
        transaction_cost_pct=trade.round_trip_cost_pct,
        net_return_pct=gross - trade.round_trip_cost_pct,
    )


def _close_runner(
    trade: V3ShadowTrade,
    observation: V3PriceObservation,
    reason: V3ExitReason,
) -> V3ShadowTrade:
    runner_gross = _directional_return_pct(
        trade.direction, trade.entry_price, observation.price
    ) * trade.remaining_fraction
    gross = trade.gross_return_pct + runner_gross
    return replace(
        trade,
        stage=V3TradeStage.CLOSED,
        remaining_fraction=0.0,
        exit_at=observation.observed_at,
        exit_price=observation.price,
        exit_reason=reason,
        gross_return_pct=gross,
        transaction_cost_pct=trade.round_trip_cost_pct,
        net_return_pct=gross - trade.round_trip_cost_pct,
    )


def _opposing_sweep(
    direction: ImpulseDirection, sweep: SweepDirection
) -> bool:
    return (direction is ImpulseDirection.LONG and sweep is SweepDirection.DOWN) or (
        direction is ImpulseDirection.SHORT and sweep is SweepDirection.UP
    )


def _trade_id(decision: V3EntryDecision) -> str:
    raw = "|".join(
        (
            decision.snapshot.symbol,
            decision.impulse_id,
            decision.direction.value,
            decision.evaluated_at.isoformat(),
        )
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
