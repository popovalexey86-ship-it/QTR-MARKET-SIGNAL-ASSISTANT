from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum


class SetupDirection(StrEnum):
    UP = "UP"
    DOWN = "DOWN"
    NEUTRAL = "NEUTRAL"

    @property
    def name_ru(self) -> str:
        return DIRECTION_NAMES_RU[self]


class SetupType(StrEnum):
    BREAKOUT = "BREAKOUT"
    RETEST = "RETEST"
    IMPULSE = "IMPULSE"
    COMPRESSION = "COMPRESSION"
    CONTINUATION = "CONTINUATION"
    FALSE_BREAKOUT = "FALSE_BREAKOUT"
    REVERSAL = "REVERSAL"
    NO_TRADE = "NO_TRADE"

    @property
    def name_ru(self) -> str:
        return SETUP_TYPE_NAMES_RU[self]


class SetupState(StrEnum):
    WATCHING = "WATCHING"
    FORMING = "FORMING"
    CONFIRMING = "CONFIRMING"
    READY_TO_CONSIDER = "READY_TO_CONSIDER"
    LATE = "LATE"
    CANCELLED = "CANCELLED"

    @property
    def name_ru(self) -> str:
        return SETUP_STATE_NAMES_RU[self]


class TradeEligibility(StrEnum):
    STRUCTURE_EXISTS = "STRUCTURE_EXISTS"
    NO_TRADE = "NO_TRADE"
    FORMING = "FORMING"
    CONFIRMING = "CONFIRMING"
    READY_TO_CONSIDER = "READY_TO_CONSIDER"
    CANCELLED = "CANCELLED"
    LATE = "LATE"

    @property
    def name_ru(self) -> str:
        return TRADE_ELIGIBILITY_NAMES_RU[self]


DIRECTION_NAMES_RU = {
    SetupDirection.UP: "ВВЕРХ",
    SetupDirection.DOWN: "ВНИЗ",
    SetupDirection.NEUTRAL: "НЕЙТРАЛЬНО",
}

SETUP_TYPE_NAMES_RU = {
    SetupType.BREAKOUT: "ПРОБОЙ",
    SetupType.RETEST: "РЕТЕСТ",
    SetupType.IMPULSE: "ИМПУЛЬС",
    SetupType.COMPRESSION: "СЖАТИЕ",
    SetupType.CONTINUATION: "ПРОДОЛЖЕНИЕ",
    SetupType.FALSE_BREAKOUT: "ЛОЖНЫЙ ПРОБОЙ",
    SetupType.REVERSAL: "РАЗВОРОТ",
    SetupType.NO_TRADE: "НЕТ СДЕЛКИ",
}

SETUP_STATE_NAMES_RU = {
    SetupState.WATCHING: "НАБЛЮДАЕМ",
    SetupState.FORMING: "ФОРМИРУЕТСЯ",
    SetupState.CONFIRMING: "ПОДТВЕРЖДАЕТСЯ",
    SetupState.READY_TO_CONSIDER: "ГОТОВО К РАССМОТРЕНИЮ",
    SetupState.LATE: "ПОЗДНО",
    SetupState.CANCELLED: "ОТМЕНЕНО",
}

TRADE_ELIGIBILITY_NAMES_RU = {
    TradeEligibility.STRUCTURE_EXISTS: "ЕСТЬ КОНСТРУКЦИЯ",
    TradeEligibility.NO_TRADE: "НЕТ СДЕЛКИ",
    TradeEligibility.FORMING: "ФОРМИРУЕТСЯ",
    TradeEligibility.CONFIRMING: "ПОДТВЕРЖДАЕТСЯ",
    TradeEligibility.READY_TO_CONSIDER: "ГОТОВО К РАССМОТРЕНИЮ",
    TradeEligibility.CANCELLED: "ОТМЕНЕНО",
    TradeEligibility.LATE: "ПОЗДНО",
}

SETUP_CLASSIFICATION_PRIORITY = (
    SetupType.FALSE_BREAKOUT,
    SetupType.REVERSAL,
    SetupType.RETEST,
    SetupType.BREAKOUT,
    SetupType.CONTINUATION,
    SetupType.IMPULSE,
    SetupType.COMPRESSION,
    SetupType.NO_TRADE,
)


@dataclass(frozen=True, slots=True)
class SetupAnalysisInput:
    snapshot_ids: tuple[str, ...]
    source: str
    symbol: str
    analyzed_at: datetime
    direction: SetupDirection
    current_price: float | None
    trigger_level: float | None
    invalidation_level: float | None = None
    price_change_24h_pct: float | None = None
    distance_to_trigger_pct: float | None = None
    distance_to_trigger_atr: float | None = None
    breakout_age_bars: int | None = None
    hold_candles: int | None = None
    breakout_confirmed: bool = False
    correct_side_of_level: bool | None = None
    returned_inside_range: bool = False
    retest_detected: bool = False
    retest_held: bool = False
    breakout_failed: bool = False
    volume_confirmation: bool | None = None
    volatility_confirmation: bool | None = None
    structure_confirmation: bool | None = None
    liquidity_ok: bool | None = None
    spread_pct: float | None = None
    compression_detected: bool | None = None
    continuation_detected: bool = False
    reversal_detected: bool = False
    conflicting_confirmations: bool | None = None
    current_breakout_failure: bool | None = None
    historical_breakout_failure: bool | None = None
    structure_recovered: bool | None = None
    technical_gap: bool = False
    completed_candles: int = 0
    technical_data_complete: bool = True
    extra_missing_data: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        symbol = self.symbol.strip().upper()
        source = self.source.strip()
        snapshot_ids = tuple(item.strip() for item in self.snapshot_ids if item.strip())
        if not symbol:
            raise ValueError("Символ Setup Engine не может быть пустым.")
        if not source:
            raise ValueError("Источник Setup Engine не может быть пустым.")
        if not snapshot_ids:
            raise ValueError("Нужен хотя бы один идентификатор снимка.")
        if self.analyzed_at.tzinfo is None or self.analyzed_at.utcoffset() is None:
            raise ValueError("Время анализа должно содержать часовой пояс.")
        for name, value in (
            ("current_price", self.current_price),
            ("trigger_level", self.trigger_level),
            ("invalidation_level", self.invalidation_level),
        ):
            if value is not None and (not math.isfinite(value) or value <= 0):
                raise ValueError(f"{name} должно быть положительным конечным числом.")
        for name, value in (
            ("price_change_24h_pct", self.price_change_24h_pct),
            ("distance_to_trigger_pct", self.distance_to_trigger_pct),
            ("distance_to_trigger_atr", self.distance_to_trigger_atr),
            ("spread_pct", self.spread_pct),
        ):
            if value is not None and not math.isfinite(value):
                raise ValueError(f"{name} должно быть конечным числом.")
        if self.spread_pct is not None and self.spread_pct < 0:
            raise ValueError("spread_pct не может быть отрицательным.")
        if self.breakout_age_bars is not None and self.breakout_age_bars < 0:
            raise ValueError("Возраст пробоя не может быть отрицательным.")
        if self.hold_candles is not None and self.hold_candles < 0:
            raise ValueError("Число свечей удержания не может быть отрицательным.")
        if self.completed_candles < 0:
            raise ValueError("Число завершённых свечей не может быть отрицательным.")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "snapshot_ids", snapshot_ids)
        object.__setattr__(self, "analyzed_at", self.analyzed_at.astimezone(UTC))
        object.__setattr__(
            self,
            "extra_missing_data",
            tuple(dict.fromkeys(self.extra_missing_data)),
        )


@dataclass(frozen=True, slots=True)
class SetupAnalysisResult:
    symbol: str
    analyzed_at: datetime
    direction: SetupDirection
    setup_type: SetupType
    setup_state: SetupState
    confidence: float
    trigger_level: float | None
    invalidation_level: float | None
    current_price: float | None
    distance_to_trigger_pct: float | None
    distance_to_trigger_atr: float | None
    breakout_age_bars: int | None
    hold_candles: int | None
    retest_detected: bool
    retest_held: bool
    breakout_failed: bool
    volume_confirmation: bool
    volatility_confirmation: bool
    structure_confirmation: bool
    freshness_confirmation: bool
    liquidity_ok: bool
    spread_ok: bool
    is_late: bool
    reasons: tuple[str, ...]
    warnings: tuple[str, ...]
    missing_data: tuple[str, ...]
    current_breakout_failure: bool
    historical_breakout_failure: bool
    structure_recovered: bool
    trade_eligible: bool
    trade_eligibility: TradeEligibility
    no_trade_reasons: tuple[str, ...]
    data_quality: str
    technical_gap: bool
    classification_candidates: tuple[SetupType, ...]
    classification_winner_reason: str

    def __post_init__(self) -> None:
        if not 0.0 <= self.confidence <= 100.0:
            raise ValueError("Confidence должен быть от 0 до 100.")

    @property
    def direction_ru(self) -> str:
        return self.direction.name_ru

    @property
    def setup_type_ru(self) -> str:
        return self.setup_type.name_ru

    @property
    def setup_state_ru(self) -> str:
        return self.setup_state.name_ru

    @property
    def trade_eligibility_ru(self) -> str:
        return self.trade_eligibility.name_ru
