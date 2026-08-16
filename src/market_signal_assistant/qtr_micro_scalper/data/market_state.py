from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

from market_signal_assistant.qtr_micro_scalper.data.liquidity import (
    FlowSide,
    LiquidityBookFrame,
    LiquidityIntelligence,
    LiquidityWall,
    PressureDirection,
    SweepDirection,
)
from market_signal_assistant.qtr_micro_scalper.data.trades import TradeFlowMetrics


class MarketState(StrEnum):
    NOT_READY = "NOT_READY"
    BALANCED = "BALANCED"
    BUY_PRESSURE = "BUY_PRESSURE"
    SELL_PRESSURE = "SELL_PRESSURE"
    UPWARD_SWEEP = "UPWARD_SWEEP"
    DOWNWARD_SWEEP = "DOWNWARD_SWEEP"
    BUY_FLOW_ABSORBED = "BUY_FLOW_ABSORBED"
    SELL_FLOW_ABSORBED = "SELL_FLOW_ABSORBED"
    TWO_SIDED_LIQUIDITY = "TWO_SIDED_LIQUIDITY"


class MarketBias(StrEnum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class MarketStateEngineConfig:
    max_book_age_ms: float = 1_000.0
    max_trade_age_ms: float = 750.0
    pressure_state_threshold: float = 0.15
    two_sided_wall_min_ratio: float = 3.0
    opposing_wall_warning_ratio: float = 3.0

    def __post_init__(self) -> None:
        for name, value in (
            ("max book age", self.max_book_age_ms),
            ("max trade age", self.max_trade_age_ms),
            ("two-sided wall ratio", self.two_sided_wall_min_ratio),
            ("opposing wall ratio", self.opposing_wall_warning_ratio),
        ):
            if not _is_positive(value):
                raise ValueError(f"Market state {name} must be positive.")
        if (
            not _is_finite(self.pressure_state_threshold)
            or not 0.0 <= self.pressure_state_threshold <= 1.0
        ):
            raise ValueError("Market state pressure threshold must be between 0 and 1.")


@dataclass(frozen=True, slots=True)
class CombinedMarketMetrics:
    spread_bps: float | None
    book_age_ms: float | None
    trade_age_ms: float | None
    delta_1s: float
    delta_5s: float
    delta_15s: float
    delta_60s: float
    cvd_process: float
    cvd_utc_day: float
    cvd_episode: float | None
    imbalance_l1: float | None
    imbalance_l5: float | None
    imbalance_l10: float | None
    bid_depth_10bps: float | None
    ask_depth_10bps: float | None
    book_pressure: float | None
    trade_pressure: float
    depth_pressure: float | None
    combined_pressure: float
    sweep_score: float
    absorption_score: float
    strongest_bid_wall_ratio: float | None
    strongest_ask_wall_ratio: float | None


@dataclass(frozen=True, slots=True)
class MarketStateAssessment:
    symbol: str
    assessed_at: datetime
    state: MarketState
    bias: MarketBias
    directional_score: float
    confidence: float
    ready: bool
    metrics: CombinedMarketMetrics
    reasons: tuple[str, ...]
    confirmations: tuple[str, ...]
    warnings: tuple[str, ...]

    def __post_init__(self) -> None:
        if not -1.0 <= self.directional_score <= 1.0:
            raise ValueError("Market state directional score must be between -1 and 1.")
        if not 0.0 <= self.confidence <= 100.0:
            raise ValueError("Market state confidence must be between 0 and 100.")
        if not self.reasons:
            raise ValueError("Market state assessment requires reasons.")
        if self.assessed_at.tzinfo is None or self.assessed_at.utcoffset() is None:
            raise ValueError("Market state assessed_at must be timezone-aware.")
        object.__setattr__(self, "assessed_at", self.assessed_at.astimezone(UTC))


class MarketStateEngine:
    """Classify normalized microstructure without producing an entry decision."""

    def __init__(self, config: MarketStateEngineConfig | None = None) -> None:
        self._config = config or MarketStateEngineConfig()

    def assess(
        self,
        current: LiquidityBookFrame,
        trade_flow: TradeFlowMetrics,
        liquidity: LiquidityIntelligence,
        *,
        assessed_at: datetime,
    ) -> MarketStateAssessment:
        normalized_at = _normalize_timestamp(assessed_at)
        _validate_sources(current, trade_flow, liquidity, normalized_at)
        combined = _combine_metrics(current, trade_flow, liquidity, normalized_at)
        blocking = _readiness_reasons(current, trade_flow, combined, self._config)
        if blocking:
            return _assessment(
                symbol=current.metrics.symbol,
                assessed_at=normalized_at,
                state=MarketState.NOT_READY,
                bias=MarketBias.UNKNOWN,
                directional_score=0.0,
                confidence=0.0,
                ready=False,
                metrics=combined,
                reasons=blocking,
            )

        classification = _classify(liquidity, self._config)
        warnings = _warnings(classification.state, liquidity, self._config)
        return _assessment(
            symbol=current.metrics.symbol,
            assessed_at=normalized_at,
            state=classification.state,
            bias=classification.bias,
            directional_score=classification.directional_score,
            confidence=classification.confidence,
            ready=True,
            metrics=combined,
            reasons=classification.reasons,
            confirmations=classification.confirmations,
            warnings=warnings,
        )


@dataclass(frozen=True, slots=True)
class _Classification:
    state: MarketState
    bias: MarketBias
    directional_score: float
    confidence: float
    reasons: tuple[str, ...]
    confirmations: tuple[str, ...]


def simulate_market_state(
    current: LiquidityBookFrame,
    trade_flow: TradeFlowMetrics,
    liquidity: LiquidityIntelligence,
    *,
    assessed_at: datetime,
    config: MarketStateEngineConfig | None = None,
) -> MarketStateAssessment:
    """Offline entry point using exactly the same classification path."""

    return MarketStateEngine(config).assess(
        current,
        trade_flow,
        liquidity,
        assessed_at=assessed_at,
    )


def _classify(
    liquidity: LiquidityIntelligence,
    config: MarketStateEngineConfig,
) -> _Classification:
    sweep = liquidity.sweep
    if sweep.detected and sweep.direction is SweepDirection.UP:
        return _Classification(
            MarketState.UPWARD_SWEEP,
            MarketBias.BULLISH,
            sweep.score / 100,
            _event_confidence(sweep.score, liquidity.pressure.confidence),
            ("Зафиксирован восходящий liquidity sweep.",),
            sweep.reasons,
        )
    if sweep.detected and sweep.direction is SweepDirection.DOWN:
        return _Classification(
            MarketState.DOWNWARD_SWEEP,
            MarketBias.BEARISH,
            -sweep.score / 100,
            _event_confidence(sweep.score, liquidity.pressure.confidence),
            ("Зафиксирован нисходящий liquidity sweep.",),
            sweep.reasons,
        )

    absorption = liquidity.absorption
    if absorption.detected and absorption.aggressive_side is FlowSide.BUY:
        return _Classification(
            MarketState.BUY_FLOW_ABSORBED,
            MarketBias.BEARISH,
            -absorption.score / 100,
            absorption.score,
            ("Агрессивные покупки поглощаются встречной ликвидностью.",),
            absorption.reasons,
        )
    if absorption.detected and absorption.aggressive_side is FlowSide.SELL:
        return _Classification(
            MarketState.SELL_FLOW_ABSORBED,
            MarketBias.BULLISH,
            absorption.score / 100,
            absorption.score,
            ("Агрессивные продажи поглощаются встречной ликвидностью.",),
            absorption.reasons,
        )

    pressure = liquidity.pressure
    if (
        pressure.direction is PressureDirection.BUY
        and pressure.combined_pressure >= config.pressure_state_threshold
    ):
        return _Classification(
            MarketState.BUY_PRESSURE,
            MarketBias.BULLISH,
            pressure.combined_pressure,
            pressure.confidence,
            (
                "Совокупные trade и orderbook метрики указывают "
                "на давление покупателей.",
            ),
            pressure.reasons,
        )
    if (
        pressure.direction is PressureDirection.SELL
        and pressure.combined_pressure <= -config.pressure_state_threshold
    ):
        return _Classification(
            MarketState.SELL_PRESSURE,
            MarketBias.BEARISH,
            pressure.combined_pressure,
            pressure.confidence,
            ("Совокупные trade и orderbook метрики указывают на давление продавцов.",),
            pressure.reasons,
        )

    strongest_bid = _strongest_wall(liquidity.bid_walls)
    strongest_ask = _strongest_wall(liquidity.ask_walls)
    if (
        strongest_bid is not None
        and strongest_ask is not None
        and strongest_bid >= config.two_sided_wall_min_ratio
        and strongest_ask >= config.two_sided_wall_min_ratio
    ):
        confidence = min(
            100.0,
            min(strongest_bid, strongest_ask)
            / config.two_sided_wall_min_ratio
            * 50,
        )
        return _Classification(
            MarketState.TWO_SIDED_LIQUIDITY,
            MarketBias.NEUTRAL,
            0.0,
            confidence,
            ("Сильные bid и ask walls удерживают рынок с обеих сторон.",),
            ("Обнаружена двухсторонняя концентрация ликвидности.",),
        )

    return _Classification(
        MarketState.BALANCED,
        MarketBias.NEUTRAL,
        pressure.combined_pressure,
        max(0.0, 100 - pressure.confidence),
        ("Направленное микроструктурное преимущество не подтверждено.",),
        pressure.reasons,
    )


def _combine_metrics(
    current: LiquidityBookFrame,
    trade_flow: TradeFlowMetrics,
    liquidity: LiquidityIntelligence,
    assessed_at: datetime,
) -> CombinedMarketMetrics:
    book = current.metrics
    pressure = liquidity.pressure
    return CombinedMarketMetrics(
        spread_bps=book.spread_bps,
        book_age_ms=_source_age_ms(assessed_at, book.book_exchange_at),
        trade_age_ms=_source_age_ms(assessed_at, trade_flow.last_trade_at),
        delta_1s=trade_flow.delta_1s,
        delta_5s=trade_flow.delta_5s,
        delta_15s=trade_flow.delta_15s,
        delta_60s=trade_flow.delta_60s,
        cvd_process=trade_flow.cvd_process,
        cvd_utc_day=trade_flow.cvd_utc_day,
        cvd_episode=trade_flow.cvd_episode,
        imbalance_l1=book.imbalance_l1,
        imbalance_l5=book.imbalance_l5,
        imbalance_l10=book.imbalance_l10,
        bid_depth_10bps=book.bid_depth_10bps,
        ask_depth_10bps=book.ask_depth_10bps,
        book_pressure=pressure.book_pressure,
        trade_pressure=pressure.trade_pressure,
        depth_pressure=pressure.depth_pressure,
        combined_pressure=pressure.combined_pressure,
        sweep_score=liquidity.sweep.score,
        absorption_score=liquidity.absorption.score,
        strongest_bid_wall_ratio=_strongest_wall(liquidity.bid_walls),
        strongest_ask_wall_ratio=_strongest_wall(liquidity.ask_walls),
    )


def _readiness_reasons(
    current: LiquidityBookFrame,
    trade_flow: TradeFlowMetrics,
    metrics: CombinedMarketMetrics,
    config: MarketStateEngineConfig,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if not current.metrics.ready:
        details = ", ".join(current.metrics.health_reasons)
        reasons.append("Order book не готов: " + details)
    if metrics.book_age_ms is None:
        reasons.append("Отсутствует timestamp order book.")
    elif metrics.book_age_ms > config.max_book_age_ms:
        reasons.append("Order book устарел.")
    if trade_flow.last_trade_at is None:
        reasons.append("Отсутствует история public trades.")
    elif (
        metrics.trade_age_ms is not None
        and metrics.trade_age_ms > config.max_trade_age_ms
    ):
        reasons.append("Public trades устарели.")
    return tuple(reasons)


def _warnings(
    state: MarketState,
    liquidity: LiquidityIntelligence,
    config: MarketStateEngineConfig,
) -> tuple[str, ...]:
    warnings: list[str] = []
    ask_wall = _strongest_wall(liquidity.ask_walls)
    bid_wall = _strongest_wall(liquidity.bid_walls)
    bullish_states = {
        MarketState.UPWARD_SWEEP,
        MarketState.BUY_PRESSURE,
        MarketState.SELL_FLOW_ABSORBED,
    }
    bearish_states = {
        MarketState.DOWNWARD_SWEEP,
        MarketState.SELL_PRESSURE,
        MarketState.BUY_FLOW_ABSORBED,
    }
    if (
        state in bullish_states
        and ask_wall is not None
        and ask_wall >= config.opposing_wall_warning_ratio
    ):
        warnings.append("Сильная ask wall ограничивает bullish-сценарий.")
    if (
        state in bearish_states
        and bid_wall is not None
        and bid_wall >= config.opposing_wall_warning_ratio
    ):
        warnings.append("Сильная bid wall ограничивает bearish-сценарий.")
    return tuple(warnings)


def _assessment(
    *,
    symbol: str,
    assessed_at: datetime,
    state: MarketState,
    bias: MarketBias,
    directional_score: float,
    confidence: float,
    ready: bool,
    metrics: CombinedMarketMetrics,
    reasons: tuple[str, ...],
    confirmations: tuple[str, ...] = (),
    warnings: tuple[str, ...] = (),
) -> MarketStateAssessment:
    return MarketStateAssessment(
        symbol=symbol,
        assessed_at=assessed_at,
        state=state,
        bias=bias,
        directional_score=max(-1.0, min(1.0, directional_score)),
        confidence=max(0.0, min(100.0, confidence)),
        ready=ready,
        metrics=metrics,
        reasons=_unique(reasons),
        confirmations=_unique(confirmations),
        warnings=_unique(warnings),
    )


def _validate_sources(
    current: LiquidityBookFrame,
    trade_flow: TradeFlowMetrics,
    liquidity: LiquidityIntelligence,
    assessed_at: datetime,
) -> None:
    if len({current.metrics.symbol, trade_flow.symbol, liquidity.symbol}) != 1:
        raise ValueError("Market state sources must use the same symbol.")
    if current.metrics.as_of > assessed_at or trade_flow.as_of > assessed_at:
        raise ValueError("Market state source cannot be newer than assessed_at.")


def _source_age_ms(as_of: datetime, source_at: datetime | None) -> float | None:
    if source_at is None:
        return None
    age = (as_of - source_at).total_seconds() * 1_000
    if age < 0:
        raise ValueError("Market state source timestamp cannot be in the future.")
    return age


def _strongest_wall(walls: tuple[LiquidityWall, ...]) -> float | None:
    strengths = tuple(wall.strength_ratio for wall in walls)
    return max(strengths, default=None)


def _event_confidence(event_score: float, pressure_confidence: float) -> float:
    return min(100.0, 0.7 * event_score + 0.3 * pressure_confidence)


def _unique(values: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(value for value in values if value.strip()))


def _normalize_timestamp(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("Market state assessed_at must be timezone-aware.")
    if value.utcoffset() is None:
        raise ValueError("Market state assessed_at must be timezone-aware.")
    return value.astimezone(UTC)


def _is_finite(value: float) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(value)
    )


def _is_positive(value: float) -> bool:
    return _is_finite(value) and value > 0
