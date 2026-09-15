from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, datetime

from market_signal_assistant.providers import PublicPriceQuote
from market_signal_assistant.qtr_entry_readiness.models import (
    ENTRY_READINESS_SCHEMA_VERSION,
    AgeBucket,
    DistanceBucket,
    EntryReadinessConfig,
    EntryReadinessEvaluation,
    InternalDisposition,
    InternalReason,
    RiskBucket,
    UserReadiness,
    WaitReason,
)
from market_signal_assistant.qtr_setup_pilot.models import QtrSetupCandidate
from market_signal_assistant.qtr_setup_pilot.notifications import (
    qtr_telegram_quality_components,
)
from market_signal_assistant.setup_engine.models import (
    SetupDirection,
    SetupState,
    SetupType,
    TradeEligibility,
)

_SUPPORTED_SETUPS = frozenset((SetupType.RETEST, SetupType.BREAKOUT))


class EntryReadinessEngine:
    """Pure shadow timing decision over an existing Setup Pilot candidate."""

    def __init__(self, config: EntryReadinessConfig | None = None) -> None:
        self._config = config or EntryReadinessConfig()

    def evaluate(
        self,
        candidate: QtrSetupCandidate,
        quote: PublicPriceQuote | None,
        evaluated_at: datetime,
    ) -> EntryReadinessEvaluation:
        now = _utc(evaluated_at)
        result = candidate.result
        source = candidate.source_input
        direction = result.direction
        setup_type = result.setup_type
        trigger = _positive(result.trigger_level)
        atr = _positive(candidate.atr_value)
        invalidation = _positive(result.invalidation_level)
        confirmation_time = _utc(result.analyzed_at)
        confirmation_age = (now - confirmation_time).total_seconds()
        signal_time = _episode_time(candidate.episode_id)
        signal_age = (
            (now - signal_time).total_seconds() if signal_time is not None else None
        )
        market_quote = _usable_quote(candidate, quote, now, self._config)
        fresh_price = market_quote.price if market_quote is not None else None
        fresh_price_time = (
            market_quote.observed_at if market_quote is not None else None
        )
        confirmation_ok = _confirmation_ok(candidate)
        geometry_ok = _geometry_ok(direction, trigger, invalidation)
        structure_ok = bool(result.structure_confirmation and geometry_ok)
        correct_side = _correct_side(direction, fresh_price, trigger)
        zone = _entry_zone(direction, trigger, atr, self._config)
        fresh_distance = _distance_atr(fresh_price, trigger, atr)
        signal_distance = (
            abs(result.distance_to_trigger_atr)
            if result.distance_to_trigger_atr is not None
            and math.isfinite(result.distance_to_trigger_atr)
            else None
        )
        protective = _protective_level(direction, invalidation, atr, self._config)
        risk_distance = (
            abs(fresh_price - protective)
            if fresh_price is not None and protective is not None
            else None
        )
        risk_distance_atr = (
            risk_distance / atr
            if risk_distance is not None and atr is not None
            else None
        )
        quality_components = qtr_telegram_quality_components(candidate)
        episode_key = _episode_key(candidate)
        disposition, internal_reason, readiness, wait_reason = self._decision(
            candidate=candidate,
            quote=market_quote,
            confirmation_age=confirmation_age,
            confirmation_ok=confirmation_ok,
            structure_ok=structure_ok,
            correct_side=correct_side,
            fresh_distance=fresh_distance,
            trigger=trigger,
            atr=atr,
            invalidation=invalidation,
        )
        evaluation_id = _evaluation_id(
            episode_key=episode_key,
            evaluated_at=now,
            quote=market_quote,
            readiness=readiness,
            wait_reason=wait_reason,
            internal_reason=internal_reason,
        )
        return EntryReadinessEvaluation(
            schema_version=ENTRY_READINESS_SCHEMA_VERSION,
            recorded_at=now,
            evaluation_id=evaluation_id,
            candidate_id=_candidate_id(candidate),
            setup_episode_id=candidate.episode_id,
            setup_episode_key=episode_key,
            symbol=result.symbol,
            direction=_direction_label(direction),
            setup_type=setup_type.value,
            quality_score=sum(quality_components.values()),
            quality_components=quality_components,
            user_readiness=readiness,
            wait_reason=wait_reason,
            internal_disposition=disposition,
            internal_reason=internal_reason,
            signal_time=signal_time,
            confirmation_time=confirmation_time,
            evaluation_time=now,
            fresh_price_time=fresh_price_time,
            signal_age_seconds=signal_age,
            confirmation_age_seconds=confirmation_age,
            signal_price=result.current_price,
            fresh_price=fresh_price,
            trigger=trigger,
            atr=atr,
            entry_zone_low=zone[0] if zone is not None else None,
            entry_zone_high=zone[1] if zone is not None else None,
            structural_invalidation=invalidation,
            protective_level=protective,
            signal_distance_atr=signal_distance,
            fresh_distance_atr=fresh_distance,
            distance_bucket=_distance_bucket(fresh_distance),
            risk_distance=risk_distance,
            risk_distance_atr=risk_distance_atr,
            risk_bucket=_risk_bucket(risk_distance_atr),
            age_bucket=_age_bucket(confirmation_age),
            structure_ok=structure_ok,
            confirmation_ok=confirmation_ok,
            retest_held=(
                result.retest_held if setup_type is SetupType.RETEST else None
            ),
            breakout_confirmed=(
                source.breakout_confirmed
                if setup_type is SetupType.BREAKOUT
                else None
            ),
            volume_confirmed=(
                result.volume_confirmation
                if setup_type is SetupType.BREAKOUT
                else None
            ),
            correct_side=correct_side,
            spread_ok=result.spread_ok,
            liquidity_ok=result.liquidity_ok,
            current_failure=result.current_breakout_failure,
            late=result.is_late,
            context_ids=source.snapshot_ids,
        )

    def _decision(
        self,
        *,
        candidate: QtrSetupCandidate,
        quote: PublicPriceQuote | None,
        confirmation_age: float,
        confirmation_ok: bool,
        structure_ok: bool,
        correct_side: bool | None,
        fresh_distance: float | None,
        trigger: float | None,
        atr: float | None,
        invalidation: float | None,
    ) -> tuple[
        InternalDisposition,
        InternalReason | None,
        UserReadiness | None,
        WaitReason | None,
    ]:
        result = candidate.result
        source = candidate.source_input
        if (
            result.technical_gap
            or result.missing_data
            or result.data_quality != "COMPLETE"
        ):
            return _suppressed(InternalReason.TECHNICAL_DATA_INCOMPLETE)
        if result.direction not in {SetupDirection.UP, SetupDirection.DOWN}:
            return _suppressed(InternalReason.INVALID_DIRECTION)
        if result.setup_type not in _SUPPORTED_SETUPS:
            return _suppressed(InternalReason.UNSUPPORTED_SETUP)
        if trigger is None:
            return _suppressed(InternalReason.TRIGGER_MISSING)
        if atr is None:
            return _suppressed(InternalReason.ATR_MISSING)
        if invalidation is None:
            return _suppressed(InternalReason.INVALIDATION_MISSING)
        if result.is_late or result.setup_state is SetupState.LATE:
            return _suppressed(InternalReason.LATE)
        if result.current_breakout_failure:
            return _suppressed(InternalReason.CURRENT_FAILURE)
        if result.setup_state is SetupState.CANCELLED:
            return _suppressed(InternalReason.STRUCTURE_INVALID)
        if not result.spread_ok:
            return _suppressed(InternalReason.SPREAD_BAD)
        if not result.liquidity_ok:
            return _suppressed(InternalReason.LIQUIDITY_BAD)
        if confirmation_age < 0 or (
            confirmation_age > self._config.max_confirmation_age_seconds
        ):
            return _suppressed(InternalReason.STALE_CONFIRMATION)
        if not confirmation_ok:
            return _evaluated(UserReadiness.WAIT, WaitReason.CONFIRMATION_PENDING)
        if quote is None:
            return _suppressed(InternalReason.FRESH_PRICE_MISSING)
        if not structure_ok:
            return _suppressed(InternalReason.STRUCTURE_INVALID)
        if (
            result.setup_state is not SetupState.READY_TO_CONSIDER
            or result.trade_eligibility is not TradeEligibility.READY_TO_CONSIDER
            or not result.trade_eligible
        ):
            return _suppressed(InternalReason.STRUCTURE_INVALID)
        if (
            source.correct_side_of_level is not True
            and result.current_price is not None
        ):
            return _suppressed(InternalReason.STRUCTURE_INVALID)
        if correct_side is not True:
            return _suppressed(InternalReason.WRONG_SIDE)
        if (
            fresh_distance is None
            or fresh_distance > self._config.max_entry_distance_atr
        ):
            return _evaluated(
                UserReadiness.WAIT,
                WaitReason.PRICE_NOT_IN_ENTRY_ZONE,
            )
        return _evaluated(UserReadiness.NOW, None)


def _suppressed(
    reason: InternalReason,
) -> tuple[InternalDisposition, InternalReason, None, None]:
    return InternalDisposition.SUPPRESSED, reason, None, None


def _evaluated(
    readiness: UserReadiness, reason: WaitReason | None
) -> tuple[InternalDisposition, None, UserReadiness, WaitReason | None]:
    return InternalDisposition.EVALUATED, None, readiness, reason


def _confirmation_ok(candidate: QtrSetupCandidate) -> bool:
    result = candidate.result
    if result.setup_type is SetupType.RETEST:
        return result.retest_held
    if result.setup_type is SetupType.BREAKOUT:
        return bool(
            candidate.source_input.breakout_confirmed
            and result.volume_confirmation
        )
    return False


def _positive(value: float | None) -> float | None:
    if value is None or not math.isfinite(value) or value <= 0:
        return None
    return value


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Entry-readiness timestamp must be timezone-aware.")
    return value.astimezone(UTC)


def _usable_quote(
    candidate: QtrSetupCandidate,
    quote: PublicPriceQuote | None,
    evaluated_at: datetime,
    config: EntryReadinessConfig,
) -> PublicPriceQuote | None:
    if quote is None or quote.symbol != candidate.result.symbol:
        return None
    age = (evaluated_at - quote.observed_at).total_seconds()
    if age < 0 or age > config.max_confirmation_age_seconds:
        return None
    return quote


def _correct_side(
    direction: SetupDirection,
    price: float | None,
    trigger: float | None,
) -> bool | None:
    if price is None or trigger is None:
        return None
    if direction is SetupDirection.UP:
        return price >= trigger
    if direction is SetupDirection.DOWN:
        return price <= trigger
    return None


def _direction_label(direction: SetupDirection) -> str:
    if direction is SetupDirection.UP:
        return "LONG"
    if direction is SetupDirection.DOWN:
        return "SHORT"
    return direction.value


def _geometry_ok(
    direction: SetupDirection,
    trigger: float | None,
    invalidation: float | None,
) -> bool:
    if trigger is None or invalidation is None:
        return False
    if direction is SetupDirection.UP:
        return invalidation < trigger
    if direction is SetupDirection.DOWN:
        return invalidation > trigger
    return False


def _entry_zone(
    direction: SetupDirection,
    trigger: float | None,
    atr: float | None,
    config: EntryReadinessConfig,
) -> tuple[float, float] | None:
    if trigger is None or atr is None:
        return None
    width = config.max_entry_distance_atr * atr
    if direction is SetupDirection.UP:
        return trigger, trigger + width
    if direction is SetupDirection.DOWN:
        return trigger - width, trigger
    return None


def _protective_level(
    direction: SetupDirection,
    invalidation: float | None,
    atr: float | None,
    config: EntryReadinessConfig,
) -> float | None:
    if invalidation is None or atr is None:
        return None
    value = (
        invalidation - config.protective_buffer_atr * atr
        if direction is SetupDirection.UP
        else invalidation + config.protective_buffer_atr * atr
        if direction is SetupDirection.DOWN
        else None
    )
    return value if value is not None and value > 0 else None


def _distance_atr(
    price: float | None, trigger: float | None, atr: float | None
) -> float | None:
    if price is None or trigger is None or atr is None:
        return None
    return abs(price - trigger) / atr


def _distance_bucket(value: float | None) -> DistanceBucket | None:
    if value is None:
        return None
    if value <= 0.15:
        return DistanceBucket.ZERO_TO_015
    if value <= 0.25:
        return DistanceBucket.FROM_015_TO_025
    if value <= 0.40:
        return DistanceBucket.FROM_025_TO_040
    if value <= 0.60:
        return DistanceBucket.FROM_040_TO_060
    if value <= 0.80:
        return DistanceBucket.FROM_060_TO_080
    if value <= 1.20:
        return DistanceBucket.FROM_080_TO_120
    return DistanceBucket.ABOVE_120


def _risk_bucket(value: float | None) -> RiskBucket | None:
    if value is None:
        return None
    if value <= 0.50:
        return RiskBucket.ZERO_TO_050
    if value <= 1.00:
        return RiskBucket.FROM_050_TO_100
    if value <= 1.50:
        return RiskBucket.FROM_100_TO_150
    if value <= 2.00:
        return RiskBucket.FROM_150_TO_200
    return RiskBucket.ABOVE_200


def _age_bucket(value: float) -> AgeBucket:
    if value <= 30:
        return AgeBucket.ZERO_TO_30
    if value <= 60:
        return AgeBucket.FROM_30_TO_60
    if value <= 120:
        return AgeBucket.FROM_60_TO_120
    if value <= 300:
        return AgeBucket.FROM_120_TO_300
    return AgeBucket.ABOVE_300


def _episode_time(value: str) -> datetime | None:
    try:
        return _utc(datetime.fromisoformat(value))
    except ValueError:
        return None


def _episode_key(candidate: QtrSetupCandidate) -> str:
    episode = candidate.episode_id
    if episode == "unassigned":
        episode = hashlib.sha256(
            "|".join(candidate.source_input.snapshot_ids).encode("utf-8")
        ).hexdigest()[:20]
    return "::".join(
        (
            candidate.result.symbol,
            candidate.result.direction.value,
            candidate.result.setup_type.value,
            episode,
        )
    )


def _candidate_id(candidate: QtrSetupCandidate) -> str:
    payload = {
        "episode": candidate.episode_id,
        "snapshots": candidate.source_input.snapshot_ids,
        "symbol": candidate.result.symbol,
        "analyzed_at": candidate.result.analyzed_at.isoformat(),
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode("utf-8")
    ).hexdigest()[:24]
    return f"QTRC-{digest}"


def _evaluation_id(
    *,
    episode_key: str,
    evaluated_at: datetime,
    quote: PublicPriceQuote | None,
    readiness: UserReadiness | None,
    wait_reason: WaitReason | None,
    internal_reason: InternalReason | None,
) -> str:
    payload = {
        "episode_key": episode_key,
        "evaluated_at": evaluated_at.isoformat(),
        "fresh_price": quote.price if quote is not None else None,
        "fresh_price_time": quote.observed_at.isoformat() if quote else None,
        "user_readiness": readiness.value if readiness else None,
        "wait_reason": wait_reason.value if wait_reason else None,
        "internal_reason": internal_reason.value if internal_reason else None,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return f"QTRER-{digest[:24]}"
