from __future__ import annotations

from datetime import datetime, timedelta

from market_signal_assistant.qtr_micro.models import direction_from_setup
from market_signal_assistant.qtr_micro.settings import QtrMicroSettings
from market_signal_assistant.qtr_micro.strategy import StrategyDecision
from market_signal_assistant.qtr_setup_pilot.models import QtrSetupCandidate
from market_signal_assistant.setup_engine.models import SetupState, SetupType

_ALLOWED_SETUP_TYPES = frozenset(
    (
        SetupType.BREAKOUT,
        SetupType.RETEST,
        SetupType.CONTINUATION,
    )
)


class LegacyStrategy:
    """
    Strategy adapter reproducing the strategic entry gates that historically
    lived inside QtrMicroEntryEngine.prepare_entry().

    Risk, sizing, leverage, fee checks and execution remain in the core engine.
    """

    name = "legacy"

    def __init__(self, settings: QtrMicroSettings) -> None:
        self._settings = settings

    def evaluate(
        self,
        candidate: QtrSetupCandidate,
        *,
        now: datetime,
    ) -> StrategyDecision:
        result = candidate.result

        if result.setup_state is not SetupState.READY_TO_CONSIDER:
            return StrategyDecision.skip("setup_not_ready")

        if not result.trade_eligible:
            return StrategyDecision.skip("trade_not_eligible")

        if result.data_quality != "COMPLETE" or result.technical_gap:
            return StrategyDecision.skip("technical_data_invalid")

        if result.setup_type not in _ALLOWED_SETUP_TYPES:
            return StrategyDecision.skip("setup_type_not_allowed")

        direction = direction_from_setup(result.direction)
        if direction is None:
            return StrategyDecision.skip("invalid_direction")

        signal_age = now - result.analyzed_at
        if signal_age > timedelta(seconds=self._settings.max_signal_age_seconds):
            return StrategyDecision.skip("signal_stale")

        if result.current_breakout_failure:
            return StrategyDecision.skip("breakout_failure")

        if result.is_late:
            return StrategyDecision.skip("late_entry")

        if not result.spread_ok:
            return StrategyDecision.skip("spread_not_ok")

        if (
            result.distance_to_trigger_atr is None
            or result.distance_to_trigger_atr
            > self._settings.max_entry_distance_atr
        ):
            return StrategyDecision.skip("too_far_from_trigger")

        if result.trigger_level is None:
            return StrategyDecision.skip("trigger_missing")

        if result.invalidation_level is None:
            return StrategyDecision.skip("invalidation_missing")

        return StrategyDecision.trade(
            direction=direction,
            trigger_price=result.trigger_level,
            invalidation_price=result.invalidation_level,
            reason="legacy_setup_accepted",
        )
