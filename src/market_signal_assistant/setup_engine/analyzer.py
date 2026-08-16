from __future__ import annotations

from market_signal_assistant.setup_engine.models import (
    SETUP_CLASSIFICATION_PRIORITY,
    SetupAnalysisInput,
    SetupAnalysisResult,
    SetupDirection,
    SetupState,
    SetupType,
    TradeEligibility,
)

MAXIMUM_READY_SPREAD_PCT = 0.2
MAXIMUM_READY_DISTANCE_ATR = 2.0
LATE_PRICE_CHANGE_PCT = 15.0
EXTREME_PRICE_CHANGE_PCT = 30.0
MINIMUM_READY_HOLD_CANDLES = 2
MINIMUM_COMPLETED_CANDLES = 2
MAXIMUM_FRESH_BREAKOUT_AGE_BARS = 6


def analyze_setup(data: SetupAnalysisInput) -> SetupAnalysisResult:
    """Classify a supplied snapshot without I/O, clocks, state, or network."""
    missing_data = _missing_data(data)
    spread_ok = (
        data.spread_pct is not None and data.spread_pct <= MAXIMUM_READY_SPREAD_PCT
    )
    liquidity_ok = data.liquidity_ok is True
    structure_ok = data.structure_confirmation is True
    volume_ok = data.volume_confirmation is True
    volatility_ok = data.volatility_confirmation is True
    correct_side = data.correct_side_of_level is True
    hold_candles = data.hold_candles or 0
    historical_failure = _historical_breakout_failure(data)
    structure_recovered = _structure_recovered(data, historical_failure)
    current_failure = _current_breakout_failure(data, structure_recovered)
    freshness_ok = bool(
        data.breakout_age_bars is not None
        and data.breakout_age_bars <= MAXIMUM_FRESH_BREAKOUT_AGE_BARS
    )
    distance_atr = (
        abs(data.distance_to_trigger_atr)
        if data.distance_to_trigger_atr is not None
        else None
    )
    absolute_change = abs(data.price_change_24h_pct or 0.0)
    is_late = bool(
        absolute_change >= LATE_PRICE_CHANGE_PCT
        or (distance_atr is not None and distance_atr > MAXIMUM_READY_DISTANCE_ATR)
    )
    candidates = classification_candidates(data, current_failure=current_failure)
    setup_type = candidates[0]
    ready = _is_ready(
        data,
        setup_type,
        missing_data,
        spread_ok=spread_ok,
        liquidity_ok=liquidity_ok,
        structure_ok=structure_ok,
        volume_ok=volume_ok,
        volatility_ok=volatility_ok,
        correct_side=correct_side,
        hold_candles=hold_candles,
        freshness_ok=freshness_ok,
        is_late=is_late,
        current_failure=current_failure,
    )
    setup_state = _setup_state(
        data,
        setup_type,
        missing_data,
        ready=ready,
        structure_ok=structure_ok,
        correct_side=correct_side,
        hold_candles=hold_candles,
        is_late=is_late,
        current_failure=current_failure,
    )
    no_trade_reasons = _no_trade_reasons(
        data,
        missing_data,
        current_failure=current_failure,
        structure_ok=structure_ok,
        correct_side=correct_side,
        spread_ok=spread_ok,
        liquidity_ok=liquidity_ok,
        freshness_ok=freshness_ok,
        hold_candles=hold_candles,
        is_late=is_late,
    )
    trade_eligibility = _trade_eligibility(
        data,
        setup_type,
        setup_state,
        ready=ready,
        current_failure=current_failure,
        missing_data=missing_data,
        is_late=is_late,
        spread_ok=spread_ok,
        liquidity_ok=liquidity_ok,
        correct_side=correct_side,
        freshness_ok=freshness_ok,
    )
    reasons = _reasons(
        data,
        setup_type,
        setup_state,
        structure_ok=structure_ok,
        volume_ok=volume_ok,
        volatility_ok=volatility_ok,
        liquidity_ok=liquidity_ok,
        spread_ok=spread_ok,
        correct_side=correct_side,
        hold_candles=hold_candles,
        freshness_ok=freshness_ok,
    )
    warnings = _warnings(
        data,
        missing_data,
        spread_ok=spread_ok,
        liquidity_ok=liquidity_ok,
        distance_atr=distance_atr,
        absolute_change=absolute_change,
        current_failure=current_failure,
    )
    confidence = _confidence(
        data,
        structure_ok=structure_ok,
        volume_ok=volume_ok,
        volatility_ok=volatility_ok,
        liquidity_ok=liquidity_ok,
        spread_ok=spread_ok,
        correct_side=correct_side,
        hold_candles=hold_candles,
        freshness_ok=freshness_ok,
    )
    return SetupAnalysisResult(
        symbol=data.symbol,
        analyzed_at=data.analyzed_at,
        direction=data.direction,
        setup_type=setup_type,
        setup_state=setup_state,
        confidence=confidence,
        trigger_level=data.trigger_level,
        invalidation_level=data.invalidation_level,
        current_price=data.current_price,
        distance_to_trigger_pct=(
            abs(data.distance_to_trigger_pct)
            if data.distance_to_trigger_pct is not None
            else None
        ),
        distance_to_trigger_atr=distance_atr,
        breakout_age_bars=data.breakout_age_bars,
        hold_candles=data.hold_candles,
        retest_detected=data.retest_detected,
        retest_held=data.retest_held,
        breakout_failed=current_failure,
        volume_confirmation=volume_ok,
        volatility_confirmation=volatility_ok,
        structure_confirmation=structure_ok,
        freshness_confirmation=freshness_ok,
        liquidity_ok=liquidity_ok,
        spread_ok=spread_ok,
        is_late=is_late,
        reasons=reasons,
        warnings=warnings,
        missing_data=missing_data,
        current_breakout_failure=current_failure,
        historical_breakout_failure=historical_failure,
        structure_recovered=structure_recovered,
        trade_eligible=ready,
        trade_eligibility=trade_eligibility,
        no_trade_reasons=no_trade_reasons,
        data_quality=_data_quality(data, missing_data),
        technical_gap=data.technical_gap or not data.technical_data_complete,
        classification_candidates=candidates,
        classification_winner_reason=_winner_reason(setup_type, current_failure),
    )


def classification_candidates(
    data: SetupAnalysisInput,
    *,
    current_failure: bool | None = None,
) -> tuple[SetupType, ...]:
    """Return current structural candidates before deterministic priority."""
    if current_failure is None:
        recovered = _structure_recovered(data, _historical_breakout_failure(data))
        current_failure = _current_breakout_failure(data, recovered)
    candidates = {
        SetupType.FALSE_BREAKOUT: current_failure,
        SetupType.REVERSAL: data.reversal_detected,
        SetupType.RETEST: data.retest_detected,
        SetupType.BREAKOUT: data.breakout_confirmed,
        SetupType.CONTINUATION: data.continuation_detected,
        SetupType.IMPULSE: bool(
            data.volume_confirmation is True and data.volatility_confirmation is True
        ),
        SetupType.COMPRESSION: bool(
            data.compression_detected
            and not (
                data.volume_confirmation is True
                and data.volatility_confirmation is True
            )
        ),
    }
    selected = tuple(
        item
        for item in SETUP_CLASSIFICATION_PRIORITY
        if item is not SetupType.NO_TRADE and candidates[item]
    )
    return selected or (SetupType.NO_TRADE,)


def _historical_breakout_failure(data: SetupAnalysisInput) -> bool:
    if data.historical_breakout_failure is not None:
        return data.historical_breakout_failure
    return bool(data.breakout_failed or data.returned_inside_range)


def _structure_recovered(
    data: SetupAnalysisInput,
    historical_failure: bool,
) -> bool:
    if data.structure_recovered is not None:
        return data.structure_recovered
    return bool(
        historical_failure and data.correct_side_of_level is True and data.retest_held
    )


def _current_breakout_failure(
    data: SetupAnalysisInput,
    structure_recovered: bool,
) -> bool:
    observed_failure = (
        data.current_breakout_failure
        if data.current_breakout_failure is not None
        else data.breakout_failed
    )
    return bool(
        observed_failure
        and data.correct_side_of_level is not True
        and not structure_recovered
    )


def _no_trade_reasons(
    data: SetupAnalysisInput,
    missing_data: tuple[str, ...],
    *,
    current_failure: bool,
    structure_ok: bool,
    correct_side: bool,
    spread_ok: bool,
    liquidity_ok: bool,
    freshness_ok: bool,
    hold_candles: int,
    is_late: bool,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if data.direction is SetupDirection.NEUTRAL:
        reasons.append("направление не подтверждено")
    if not structure_ok:
        reasons.append("структура недостаточна")
    if missing_data:
        reasons.append("технические данные неполные")
    if data.conflicting_confirmations is True:
        reasons.append("подтверждения конфликтуют")
    if current_failure:
        reasons.append("текущий пробой провален")
    distance = abs(data.distance_to_trigger_atr or 0.0)
    if distance > MAXIMUM_READY_DISTANCE_ATR:
        reasons.append("цена слишком далеко")
    if abs(data.price_change_24h_pct or 0.0) >= LATE_PRICE_CHANGE_PCT:
        reasons.append("движение уже состоялось")
    if not spread_ok:
        reasons.append("широкий спред")
    if not liquidity_ok:
        reasons.append("слабая ликвидность")
    if not correct_side:
        reasons.append("цена не удерживает правильную сторону уровня")
    if hold_candles < MINIMUM_READY_HOLD_CANDLES and not data.retest_held:
        reasons.append("удержание ещё не подтверждено")
    if not freshness_ok:
        reasons.append("структура потеряла свежесть")
    if is_late and not any(
        reason in reasons
        for reason in ("цена слишком далеко", "движение уже состоялось")
    ):
        reasons.append("рассматривать вход поздно")
    return tuple(dict.fromkeys(reasons))


def _trade_eligibility(
    data: SetupAnalysisInput,
    setup_type: SetupType,
    setup_state: SetupState,
    *,
    ready: bool,
    current_failure: bool,
    missing_data: tuple[str, ...],
    is_late: bool,
    spread_ok: bool,
    liquidity_ok: bool,
    correct_side: bool,
    freshness_ok: bool,
) -> TradeEligibility:
    if current_failure or setup_state is SetupState.CANCELLED:
        return TradeEligibility.CANCELLED
    if is_late or setup_state is SetupState.LATE:
        return TradeEligibility.LATE
    if ready:
        return TradeEligibility.READY_TO_CONSIDER
    if (
        setup_type is SetupType.NO_TRADE
        or data.direction is SetupDirection.NEUTRAL
        or missing_data
        or data.conflicting_confirmations is True
        or not spread_ok
        or not liquidity_ok
        or not correct_side
        or not freshness_ok
    ):
        return TradeEligibility.NO_TRADE
    if setup_state is SetupState.CONFIRMING:
        return TradeEligibility.CONFIRMING
    if setup_state is SetupState.FORMING:
        return TradeEligibility.FORMING
    return TradeEligibility.STRUCTURE_EXISTS


def _data_quality(
    data: SetupAnalysisInput,
    missing_data: tuple[str, ...],
) -> str:
    if data.technical_gap or not data.technical_data_complete:
        return "TECHNICAL_ERROR"
    if missing_data:
        return "INCOMPLETE"
    return "COMPLETE"


def _winner_reason(setup_type: SetupType, current_failure: bool) -> str:
    if current_failure:
        return "Текущий подтверждённый провал имеет приоритет."
    if setup_type is SetupType.NO_TRADE:
        return "Текущая рыночная конструкция не классифицирована."
    return f"Выбран первый актуальный кандидат по приоритету: {setup_type.name_ru}."


def _is_ready(
    data: SetupAnalysisInput,
    setup_type: SetupType,
    missing_data: tuple[str, ...],
    *,
    spread_ok: bool,
    liquidity_ok: bool,
    structure_ok: bool,
    volume_ok: bool,
    volatility_ok: bool,
    correct_side: bool,
    hold_candles: int,
    freshness_ok: bool,
    is_late: bool,
    current_failure: bool,
) -> bool:
    if setup_type in {
        SetupType.NO_TRADE,
        SetupType.FALSE_BREAKOUT,
        SetupType.COMPRESSION,
    }:
        return False
    if (
        missing_data
        or data.direction is SetupDirection.NEUTRAL
        or current_failure
        or data.conflicting_confirmations is True
        or is_late
        or not spread_ok
        or not liquidity_ok
        or not structure_ok
        or not volume_ok
        or not volatility_ok
        or not correct_side
        or not freshness_ok
        or hold_candles < MINIMUM_READY_HOLD_CANDLES
        or data.completed_candles < MINIMUM_COMPLETED_CANDLES
    ):
        return False
    if setup_type is SetupType.RETEST:
        return data.retest_held
    if setup_type is SetupType.BREAKOUT:
        return data.breakout_confirmed
    return True


def _setup_state(
    data: SetupAnalysisInput,
    setup_type: SetupType,
    missing_data: tuple[str, ...],
    *,
    ready: bool,
    structure_ok: bool,
    correct_side: bool,
    hold_candles: int,
    is_late: bool,
    current_failure: bool,
) -> SetupState:
    if setup_type is SetupType.NO_TRADE:
        return SetupState.WATCHING
    if setup_type is SetupType.FALSE_BREAKOUT or current_failure:
        return SetupState.CANCELLED
    if is_late:
        return SetupState.LATE
    if ready:
        return SetupState.READY_TO_CONSIDER
    if (
        structure_ok
        and correct_side
        and (hold_candles >= 1 or data.retest_held)
        and not current_failure
    ):
        return SetupState.CONFIRMING
    if data.direction is not SetupDirection.NEUTRAL:
        return SetupState.FORMING
    return SetupState.WATCHING


def _missing_data(data: SetupAnalysisInput) -> tuple[str, ...]:
    values = list(data.extra_missing_data)
    if not data.technical_data_complete:
        values.append("technical_data")
    for name, value in (
        ("current_price", data.current_price),
        ("trigger_level", data.trigger_level),
        ("distance_to_trigger_atr", data.distance_to_trigger_atr),
        ("breakout_age_bars", data.breakout_age_bars),
        ("spread_pct", data.spread_pct),
        ("correct_side_of_level", data.correct_side_of_level),
        ("volume_confirmation", data.volume_confirmation),
        ("volatility_confirmation", data.volatility_confirmation),
        ("structure_confirmation", data.structure_confirmation),
        ("liquidity_ok", data.liquidity_ok),
        ("hold_candles", data.hold_candles),
        ("conflicting_confirmations", data.conflicting_confirmations),
    ):
        if value is None:
            values.append(name)
    return tuple(dict.fromkeys(values))


def _confidence(
    data: SetupAnalysisInput,
    *,
    structure_ok: bool,
    volume_ok: bool,
    volatility_ok: bool,
    liquidity_ok: bool,
    spread_ok: bool,
    correct_side: bool,
    hold_candles: int,
    freshness_ok: bool,
) -> float:
    score = 10.0 if data.direction is not SetupDirection.NEUTRAL else 0.0
    score += 20.0 if structure_ok else 0.0
    score += 10.0 if correct_side else 0.0
    score += min(10.0, hold_candles * 5.0)
    score += 5.0 if freshness_ok else 0.0
    score += 15.0 if volume_ok else 0.0
    score += 15.0 if volatility_ok else 0.0
    score += 10.0 if data.retest_held else 0.0
    score += 5.0 if liquidity_ok else 0.0
    score += 5.0 if spread_ok else 0.0
    return min(100.0, score)


def _reasons(
    data: SetupAnalysisInput,
    setup_type: SetupType,
    setup_state: SetupState,
    *,
    structure_ok: bool,
    volume_ok: bool,
    volatility_ok: bool,
    liquidity_ok: bool,
    spread_ok: bool,
    correct_side: bool,
    hold_candles: int,
    freshness_ok: bool,
) -> tuple[str, ...]:
    classification = {
        SetupType.FALSE_BREAKOUT: "Завершённая свеча вернулась внутрь диапазона.",
        SetupType.REVERSAL: "Подтверждается структура противоположного направления.",
        SetupType.RETEST: "Цена вернулась к уровню после пробоя.",
        SetupType.BREAKOUT: "Цена закрылась за подтверждённой границей диапазона.",
        SetupType.CONTINUATION: "После коррекции подтверждается продолжение движения.",
        SetupType.IMPULSE: "Объём и волатильность ускоряются одновременно.",
        SetupType.COMPRESSION: "Диапазон сжимается возле значимой границы.",
        SetupType.NO_TRADE: "Достаточная торговая конструкция не подтверждена.",
    }
    reasons = [classification[setup_type]]
    if data.direction is SetupDirection.NEUTRAL:
        reasons.append("Направление не подтверждено.")
    if data.conflicting_confirmations:
        reasons.append("Подтверждения противоречат друг другу.")
    reasons.extend(
        (
            "Структура подтверждена."
            if structure_ok
            else "Структура пока не подтверждена.",
            "Объём подтверждает движение."
            if volume_ok
            else "Объём не подтверждает движение.",
            "Волатильность подтверждает движение."
            if volatility_ok
            else "Волатильность не подтверждает движение.",
            "Цена находится на правильной стороне уровня."
            if correct_side
            else "Правильная сторона уровня не подтверждена.",
            f"Уровень удержан завершёнными свечами: {hold_candles}.",
            "Движение остаётся свежим."
            if freshness_ok
            else "Свежесть движения не подтверждена.",
            "Ликвидность достаточна."
            if liquidity_ok
            else "Ликвидность недостаточна или неизвестна.",
            "Спред допустим."
            if spread_ok
            else "Спред превышает допустимое значение или неизвестен.",
            f"Состояние: {setup_state.name_ru}.",
        )
    )
    return tuple(reasons)


def _warnings(
    data: SetupAnalysisInput,
    missing_data: tuple[str, ...],
    *,
    spread_ok: bool,
    liquidity_ok: bool,
    distance_atr: float | None,
    absolute_change: float,
    current_failure: bool,
) -> tuple[str, ...]:
    warnings: list[str] = []
    if absolute_change >= EXTREME_PRICE_CHANGE_PCT:
        warnings.append("Движение за 24 часа достигло 30%: рассматривать поздно.")
    elif absolute_change >= LATE_PRICE_CHANGE_PCT:
        warnings.append("Движение за 24 часа достигло 15%: рассматривать поздно.")
    if distance_atr is not None and distance_atr > MAXIMUM_READY_DISTANCE_ATR:
        warnings.append("Цена находится дальше 2 ATR от уровня.")
    if not spread_ok:
        warnings.append("Спред выше 0,2% или не определён.")
    if not liquidity_ok:
        warnings.append("Ликвидность недостаточна или не определена.")
    if current_failure:
        warnings.append("Пробой провален.")
    if data.completed_candles < MINIMUM_COMPLETED_CANDLES:
        warnings.append("Недостаточно завершённых свечей.")
    if (
        data.breakout_age_bars is not None
        and data.breakout_age_bars > MAXIMUM_FRESH_BREAKOUT_AGE_BARS
    ):
        warnings.append("Движение старше 6 завершённых баров.")
    if missing_data:
        warnings.append("Технические данные неполные.")
    return tuple(warnings)
