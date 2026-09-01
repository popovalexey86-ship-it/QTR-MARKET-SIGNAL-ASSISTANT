from __future__ import annotations

from market_signal_assistant.inplay.early_discovery import MarketDirection
from market_signal_assistant.inplay.early_discovery_v2 import (
    EarlyDiscoveryV2Result,
    RetestState,
)
from market_signal_assistant.setup_engine.models import (
    SetupAnalysisInput,
    SetupDirection,
)

MINIMUM_CONFIRMATION_FACTOR = 0.3
MINIMUM_LIQUIDITY_FACTOR = 0.5
MINIMUM_COMPRESSION_FACTOR = 0.5


def input_from_early_discovery_v2(
    result: EarlyDiscoveryV2Result,
    *,
    invalidation_level: float | None = None,
) -> SetupAnalysisInput:
    """Adapt an already-computed V2 result without loading market data."""
    direction = _direction(result.market_direction)
    historical_failure = bool(
        result.breakout_failure is True or result.returned_inside_range is True
    )
    correct_side = result.is_correct_side_of_level
    hold_candles = result.breakout_hold_candles
    breakout_confirmed = bool(
        result.breakout_level is not None
        and result.current_price is not None
        and correct_side is True
        and (hold_candles or 0) >= 1
        and not (result.breakout_failure is True and correct_side is not True)
    )
    retest_detected = bool(
        result.returned_to_level
        or result.retest_state
        in {RetestState.RETEST_IN_PROGRESS, RetestState.RETEST_HELD}
    )
    retest_held = result.retest_state is RetestState.RETEST_HELD
    structure_recovered = bool(
        historical_failure and correct_side is True and retest_held
    )
    current_failure = bool(
        result.breakout_failure is True
        and correct_side is not True
        and not structure_recovered
    )
    volume_confirmation = _component_confirmation(
        result,
        ("volume_acceleration", "breakout_volume"),
        MINIMUM_CONFIRMATION_FACTOR,
    )
    volatility_confirmation = _component_confirmation(
        result, ("atr_expansion",), MINIMUM_CONFIRMATION_FACTOR
    )
    liquidity_ok = _component_confirmation(
        result, ("liquidity",), MINIMUM_LIQUIDITY_FACTOR
    )
    compression_detected = _component_confirmation(
        result, ("compression",), MINIMUM_COMPRESSION_FACTOR
    )
    continuation_detected = bool(
        breakout_confirmed
        and (result.breakout_age_bars or 0) >= 2
        and (hold_candles or 0) >= 2
        and not result.returned_to_level
    )
    reversal_detected = bool(
        result.direction_v1 in {MarketDirection.UP, MarketDirection.DOWN}
        and result.direction_v2 in {MarketDirection.UP, MarketDirection.DOWN}
        and result.direction_v1 is not result.direction_v2
    )
    conflicting_confirmations = _direction_conflict(result)
    structure_confirmation = _structure_confirmation(
        result,
        breakout_confirmed=breakout_confirmed,
        retest_held=retest_held,
        reversal_detected=reversal_detected,
    )
    missing = _adapter_missing_data(
        result,
        volume_confirmation=volume_confirmation,
        volatility_confirmation=volatility_confirmation,
        liquidity_ok=liquidity_ok,
        compression_detected=compression_detected,
        conflicting_confirmations=conflicting_confirmations,
    )
    return SetupAnalysisInput(
        snapshot_ids=(result.scan_id,),
        source="early_discovery_v2",
        symbol=result.symbol,
        analyzed_at=result.scanned_at,
        direction=direction,
        current_price=result.current_price,
        trigger_level=result.breakout_level,
        invalidation_level=(
            invalidation_level
            if invalidation_level is not None
            else _invalidation_level(result, direction)
        ),
        price_change_24h_pct=result.price_change_24h_pct,
        distance_to_trigger_pct=_distance_pct(
            result.current_price,
            result.breakout_level,
        ),
        distance_to_trigger_atr=result.absolute_distance_atr,
        breakout_age_bars=result.breakout_age_bars,
        hold_candles=hold_candles,
        breakout_confirmed=breakout_confirmed,
        correct_side_of_level=correct_side,
        returned_inside_range=result.returned_inside_range is True,
        retest_detected=retest_detected,
        retest_held=retest_held,
        breakout_failed=current_failure,
        current_breakout_failure=current_failure,
        historical_breakout_failure=historical_failure,
        structure_recovered=structure_recovered,
        volume_confirmation=volume_confirmation,
        volatility_confirmation=volatility_confirmation,
        structure_confirmation=structure_confirmation,
        liquidity_ok=liquidity_ok,
        spread_pct=result.spread_pct,
        compression_detected=compression_detected,
        continuation_detected=continuation_detected,
        reversal_detected=reversal_detected,
        conflicting_confirmations=conflicting_confirmations,
        completed_candles=max(hold_candles or 0, 1 if breakout_confirmed else 0),
        technical_data_complete=result.technical_error is None,
        extra_missing_data=missing,
        technical_gap=result.technical_error is not None,
    )


def _direction(value: MarketDirection) -> SetupDirection:
    return {
        MarketDirection.UP: SetupDirection.UP,
        MarketDirection.DOWN: SetupDirection.DOWN,
        MarketDirection.NEUTRAL: SetupDirection.NEUTRAL,
    }[value]


def _component_factors(
    result: EarlyDiscoveryV2Result, component_id: str
) -> tuple[float, ...]:
    return tuple(
        component.points / component.maximum_points
        for component in result.component_scores
        if component.component_id == component_id and component.maximum_points > 0
    )


def _component_confirmation(
    result: EarlyDiscoveryV2Result,
    component_ids: tuple[str, ...],
    threshold: float,
) -> bool | None:
    factors = tuple(_component_factors(result, item) for item in component_ids)
    if any(values and max(values) >= threshold for values in factors):
        return True
    if all(values for values in factors):
        return False
    return None


def _direction_conflict(result: EarlyDiscoveryV2Result) -> bool | None:
    first = result.direction_v1
    second = result.direction_v2
    if first is None or second is None:
        return None
    directional = {MarketDirection.UP, MarketDirection.DOWN}
    return bool(first in directional and second in directional and first is not second)


def _structure_confirmation(
    result: EarlyDiscoveryV2Result,
    *,
    breakout_confirmed: bool,
    retest_held: bool,
    reversal_detected: bool,
) -> bool | None:
    if result.technical_error is not None:
        return None
    if (
        result.breakout_level is None
        or result.current_price is None
        or result.is_correct_side_of_level is None
        or result.breakout_hold_candles is None
    ):
        return None
    return bool(
        retest_held
        or reversal_detected
        or (breakout_confirmed and result.breakout_hold_candles >= 2)
    )


def _adapter_missing_data(
    result: EarlyDiscoveryV2Result,
    *,
    volume_confirmation: bool | None,
    volatility_confirmation: bool | None,
    liquidity_ok: bool | None,
    compression_detected: bool | None,
    conflicting_confirmations: bool | None,
) -> tuple[str, ...]:
    missing: list[str] = []
    if result.technical_error is not None:
        missing.append("early_discovery_v2_technical_error")
    for name, value in (
        ("volume_confirmation", volume_confirmation),
        ("volatility_confirmation", volatility_confirmation),
        ("liquidity_ok", liquidity_ok),
        ("compression_detected", compression_detected),
        ("conflicting_confirmations", conflicting_confirmations),
        ("hold_candles", result.breakout_hold_candles),
    ):
        if value is None:
            missing.append(name)
    return tuple(missing)


def _distance_pct(current_price: float | None, level: float | None) -> float | None:
    if current_price is None or level is None or level <= 0:
        return None
    return abs(current_price - level) / level * 100.0


def _invalidation_level(
    result: EarlyDiscoveryV2Result,
    direction: SetupDirection,
) -> float | None:
    if (
        result.breakout_level is None
        or result.absolute_distance is None
        or result.absolute_distance_atr is None
        or result.absolute_distance_atr <= 0
        or direction is SetupDirection.NEUTRAL
    ):
        return None
    atr = result.absolute_distance / result.absolute_distance_atr
    if atr <= 0:
        return None
    if direction is SetupDirection.UP:
        return max(1e-12, result.breakout_level - atr)
    return result.breakout_level + atr
