from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from statistics import median

from market_signal_assistant.qtr_micro_scalper.data.models import OrderBookLevel
from market_signal_assistant.qtr_micro_scalper.data.orderbook import (
    OrderBookMetrics,
    OrderBookState,
)
from market_signal_assistant.qtr_micro_scalper.data.trades import TradeFlowMetrics


class BookSide(StrEnum):
    BID = "BID"
    ASK = "ASK"


class FlowSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"
    NONE = "NONE"


class SweepDirection(StrEnum):
    UP = "UP"
    DOWN = "DOWN"
    NONE = "NONE"


class PressureDirection(StrEnum):
    BUY = "BUY"
    SELL = "SELL"
    NEUTRAL = "NEUTRAL"


@dataclass(frozen=True, slots=True)
class LiquidityIntelligenceConfig:
    wall_strength_ratio: float = 3.0
    wall_max_distance_bps: float = 25.0
    wall_min_reference_levels: int = 3
    absorption_flow_ratio: float = 1.5
    absorption_max_move_bps: float = 2.0
    absorption_min_depth_retention: float = 0.8
    sweep_flow_ratio: float = 1.5
    sweep_min_levels: int = 2
    sweep_min_displacement_bps: float = 2.0
    pressure_direction_threshold: float = 0.15

    def __post_init__(self) -> None:
        positive_values = (
            self.wall_strength_ratio,
            self.wall_max_distance_bps,
            self.absorption_flow_ratio,
            self.absorption_max_move_bps,
            self.sweep_flow_ratio,
            self.sweep_min_displacement_bps,
        )
        if any(not _is_positive(value) for value in positive_values):
            raise ValueError("Liquidity intelligence thresholds must be positive.")
        if (
            isinstance(self.wall_min_reference_levels, bool)
            or self.wall_min_reference_levels < 1
        ):
            raise ValueError("Wall reference level count must be positive.")
        if isinstance(self.sweep_min_levels, bool) or self.sweep_min_levels < 1:
            raise ValueError("Sweep level count must be positive.")
        for name, value in (
            ("absorption depth retention", self.absorption_min_depth_retention),
            ("pressure direction threshold", self.pressure_direction_threshold),
        ):
            if not _is_finite(value) or not 0.0 <= value <= 1.0:
                raise ValueError(f"Liquidity {name} must be between 0 and 1.")


@dataclass(frozen=True, slots=True)
class LiquidityBookFrame:
    metrics: OrderBookMetrics
    bids: tuple[OrderBookLevel, ...]
    asks: tuple[OrderBookLevel, ...]

    def __post_init__(self) -> None:
        bids = tuple(self.bids)
        asks = tuple(self.asks)
        if any(not isinstance(level, OrderBookLevel) for level in (*bids, *asks)):
            raise ValueError("Liquidity frame requires OrderBookLevel values.")
        object.__setattr__(self, "bids", tuple(sorted(bids, reverse=True, key=_price)))
        object.__setattr__(self, "asks", tuple(sorted(asks, key=_price)))

    @classmethod
    def from_state(
        cls,
        state: OrderBookState,
        *,
        as_of: datetime,
    ) -> LiquidityBookFrame:
        if not isinstance(as_of, datetime):
            raise ValueError("Liquidity frame as_of must be a datetime.")
        bids, asks = state.levels()
        return cls(metrics=state.metrics(as_of=as_of), bids=bids, asks=asks)


@dataclass(frozen=True, slots=True)
class LiquidityWall:
    side: BookSide
    price: float
    quote_notional: float
    strength_ratio: float
    distance_bps: float


@dataclass(frozen=True, slots=True)
class AbsorptionDetection:
    detected: bool
    aggressive_side: FlowSide
    score: float
    aggressive_notional: float
    aggressive_flow_ratio: float
    favorable_price_move_bps: float | None
    opposing_depth_retention: float | None
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SweepDetection:
    detected: bool
    direction: SweepDirection
    score: float
    aggressive_notional: float
    aggressive_flow_ratio: float
    levels_consumed: int
    swept_notional: float
    price_displacement_bps: float | None
    depth_depletion: float | None
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PressureMetrics:
    book_pressure: float | None
    trade_pressure: float
    depth_pressure: float | None
    combined_pressure: float
    direction: PressureDirection
    confidence: float
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class LiquidityIntelligence:
    symbol: str
    bid_walls: tuple[LiquidityWall, ...]
    ask_walls: tuple[LiquidityWall, ...]
    absorption: AbsorptionDetection
    sweep: SweepDetection
    pressure: PressureMetrics


class LiquidityIntelligenceLayer:
    """Pure explainable analysis over normalized offline/live-equivalent inputs."""

    def __init__(
        self,
        config: LiquidityIntelligenceConfig | None = None,
    ) -> None:
        self._config = _resolved_config(config)

    def analyze(
        self,
        previous: LiquidityBookFrame,
        current: LiquidityBookFrame,
        trade_flow: TradeFlowMetrics,
        *,
        aggressive_notional_baseline: float,
    ) -> LiquidityIntelligence:
        _validate_inputs(previous, current, trade_flow)
        baseline = _positive_baseline(aggressive_notional_baseline)
        walls = detect_liquidity_walls(current, config=self._config)
        return LiquidityIntelligence(
            symbol=current.metrics.symbol,
            bid_walls=tuple(wall for wall in walls if wall.side is BookSide.BID),
            ask_walls=tuple(wall for wall in walls if wall.side is BookSide.ASK),
            absorption=detect_absorption(
                previous,
                current,
                trade_flow,
                aggressive_notional_baseline=baseline,
                config=self._config,
            ),
            sweep=detect_sweep(
                previous,
                current,
                trade_flow,
                aggressive_notional_baseline=baseline,
                config=self._config,
            ),
            pressure=calculate_pressure(
                current,
                trade_flow,
                aggressive_notional_baseline=baseline,
                config=self._config,
            ),
        )


def detect_liquidity_walls(
    frame: LiquidityBookFrame,
    *,
    config: LiquidityIntelligenceConfig | None = None,
) -> tuple[LiquidityWall, ...]:
    config = _resolved_config(config)
    mid = frame.metrics.mid_price
    if mid is None or mid <= 0:
        return ()
    walls = (
        *_walls_for_side(frame.bids, BookSide.BID, mid, config),
        *_walls_for_side(frame.asks, BookSide.ASK, mid, config),
    )
    return tuple(sorted(walls, key=lambda wall: (-wall.strength_ratio, wall.price)))


def detect_absorption(
    previous: LiquidityBookFrame,
    current: LiquidityBookFrame,
    trade_flow: TradeFlowMetrics,
    *,
    aggressive_notional_baseline: float,
    config: LiquidityIntelligenceConfig | None = None,
) -> AbsorptionDetection:
    config = _resolved_config(config)
    _validate_inputs(previous, current, trade_flow)
    baseline = _positive_baseline(aggressive_notional_baseline)
    delta = trade_flow.delta_5s
    side = FlowSide.BUY if delta > 0 else FlowSide.SELL if delta < 0 else FlowSide.NONE
    aggressive_notional = abs(delta)
    flow_ratio = aggressive_notional / baseline
    move = _price_move_bps(previous.metrics.mid_price, current.metrics.mid_price)
    favorable_move = _favorable_move(move, side)
    retention = _opposing_depth_retention(previous, current, side)

    reasons: list[str] = []
    if side is FlowSide.NONE:
        reasons.append("Нет направленного агрессивного потока.")
    if flow_ratio < config.absorption_flow_ratio:
        reasons.append("Агрессивный поток ниже адаптивного порога.")
    if favorable_move is None:
        reasons.append("Недоступно изменение mid price.")
    elif favorable_move > config.absorption_max_move_bps:
        reasons.append("Цена сдвинулась вслед за агрессивным потоком.")
    if retention is None:
        reasons.append("Недоступна встречная глубина книги.")
    elif retention < config.absorption_min_depth_retention:
        reasons.append("Встречная ликвидность не удержалась.")

    detected = (
        side is not FlowSide.NONE
        and flow_ratio >= config.absorption_flow_ratio
        and favorable_move is not None
        and favorable_move <= config.absorption_max_move_bps
        and retention is not None
        and retention >= config.absorption_min_depth_retention
    )
    if detected:
        reasons = [
            "Крупный агрессивный поток не дал соразмерного движения цены.",
            "Встречная ликвидность сохранилась или восстановилась.",
        ]
    score = _absorption_score(flow_ratio, favorable_move, retention, config)
    return AbsorptionDetection(
        detected=detected,
        aggressive_side=side,
        score=score,
        aggressive_notional=aggressive_notional,
        aggressive_flow_ratio=flow_ratio,
        favorable_price_move_bps=favorable_move,
        opposing_depth_retention=retention,
        reasons=tuple(reasons),
    )


def detect_sweep(
    previous: LiquidityBookFrame,
    current: LiquidityBookFrame,
    trade_flow: TradeFlowMetrics,
    *,
    aggressive_notional_baseline: float,
    config: LiquidityIntelligenceConfig | None = None,
) -> SweepDetection:
    config = _resolved_config(config)
    _validate_inputs(previous, current, trade_flow)
    baseline = _positive_baseline(aggressive_notional_baseline)
    delta = trade_flow.delta_5s
    candidate = SweepDirection.UP if delta > 0 else (
        SweepDirection.DOWN if delta < 0 else SweepDirection.NONE
    )
    aggressive_notional = abs(delta)
    flow_ratio = aggressive_notional / baseline
    move = _price_move_bps(previous.metrics.mid_price, current.metrics.mid_price)
    displacement = _directional_displacement(move, candidate)
    levels, swept_notional = _consumed_levels(previous, current, candidate)
    depletion = _depth_depletion(previous, current, candidate)
    acceleration = _delta_acceleration(trade_flow, baseline)

    normalized_flow = _normalized_ratio(flow_ratio, config.sweep_flow_ratio)
    normalized_levels = min(levels / config.sweep_min_levels, 1.0)
    normalized_move = _normalized_optional_ratio(
        displacement,
        config.sweep_min_displacement_bps,
    )
    normalized_depletion = max(0.0, min(depletion or 0.0, 1.0))
    score = 100 * (
        0.30 * normalized_flow
        + 0.20 * normalized_levels
        + 0.20 * normalized_move
        + 0.15 * normalized_depletion
        + 0.15 * acceleration
    )

    reasons: list[str] = []
    if candidate is SweepDirection.NONE:
        reasons.append("Нет направленного trade delta.")
    if flow_ratio < config.sweep_flow_ratio:
        reasons.append("Агрессивный поток ниже адаптивного порога.")
    if levels < config.sweep_min_levels:
        reasons.append("Недостаточно поглощённых уровней книги.")
    if displacement is None:
        reasons.append("Недоступно смещение mid price.")
    elif displacement < config.sweep_min_displacement_bps:
        reasons.append("Смещение цены ниже порога sweep.")
    if depletion is None or depletion <= 0:
        reasons.append("Не подтверждено истощение встречной ликвидности.")

    detected = (
        candidate is not SweepDirection.NONE
        and flow_ratio >= config.sweep_flow_ratio
        and levels >= config.sweep_min_levels
        and displacement is not None
        and displacement >= config.sweep_min_displacement_bps
        and depletion is not None
        and depletion > 0
    )
    if detected:
        reasons = [
            "Агрессивный поток поглотил несколько встречных уровней.",
            "Цена сместилась в направлении потока.",
            "Встречная глубина книги сократилась.",
        ]
    return SweepDetection(
        detected=detected,
        direction=candidate if detected else SweepDirection.NONE,
        score=min(100.0, max(0.0, score)),
        aggressive_notional=aggressive_notional,
        aggressive_flow_ratio=flow_ratio,
        levels_consumed=levels,
        swept_notional=swept_notional,
        price_displacement_bps=displacement,
        depth_depletion=depletion,
        reasons=tuple(reasons),
    )


def calculate_pressure(
    current: LiquidityBookFrame,
    trade_flow: TradeFlowMetrics,
    *,
    aggressive_notional_baseline: float,
    config: LiquidityIntelligenceConfig | None = None,
) -> PressureMetrics:
    config = _resolved_config(config)
    if current.metrics.symbol != trade_flow.symbol:
        raise ValueError("Pressure inputs must use the same symbol.")
    baseline = _positive_baseline(aggressive_notional_baseline)
    book = _weighted_book_pressure(current.metrics)
    trade = math.tanh(trade_flow.delta_5s / baseline)
    depth = _depth_pressure(current.metrics)

    available: list[tuple[float, float]] = [(trade, 0.45)]
    if book is not None:
        available.append((book, 0.35))
    if depth is not None:
        available.append((depth, 0.20))
    combined = sum(value * weight for value, weight in available) / sum(
        weight for _, weight in available
    )
    if combined >= config.pressure_direction_threshold:
        direction = PressureDirection.BUY
    elif combined <= -config.pressure_direction_threshold:
        direction = PressureDirection.SELL
    else:
        direction = PressureDirection.NEUTRAL
    reasons = _pressure_reasons(book, trade, depth, direction)
    return PressureMetrics(
        book_pressure=book,
        trade_pressure=trade,
        depth_pressure=depth,
        combined_pressure=combined,
        direction=direction,
        confidence=min(100.0, abs(combined) * 100),
        reasons=reasons,
    )


def simulate_liquidity_intelligence(
    previous: LiquidityBookFrame,
    current: LiquidityBookFrame,
    trade_flow: TradeFlowMetrics,
    *,
    aggressive_notional_baseline: float,
    config: LiquidityIntelligenceConfig | None = None,
) -> LiquidityIntelligence:
    """Run the same deterministic layer used by future shadow consumers."""

    return LiquidityIntelligenceLayer(config).analyze(
        previous,
        current,
        trade_flow,
        aggressive_notional_baseline=aggressive_notional_baseline,
    )


def _walls_for_side(
    levels: tuple[OrderBookLevel, ...],
    side: BookSide,
    mid: float,
    config: LiquidityIntelligenceConfig,
) -> tuple[LiquidityWall, ...]:
    eligible = tuple(
        level
        for level in levels
        if _distance_bps(level.price, mid) <= config.wall_max_distance_bps
    )
    if len(eligible) < config.wall_min_reference_levels:
        return ()
    notionals = tuple(level.price * level.quantity for level in eligible)
    baseline = median(notionals)
    if baseline <= 0:
        return ()
    return tuple(
        LiquidityWall(
            side=side,
            price=level.price,
            quote_notional=notional,
            strength_ratio=notional / baseline,
            distance_bps=_distance_bps(level.price, mid),
        )
        for level, notional in zip(eligible, notionals, strict=True)
        if notional / baseline >= config.wall_strength_ratio
    )


def _validate_inputs(
    previous: LiquidityBookFrame,
    current: LiquidityBookFrame,
    trade_flow: TradeFlowMetrics,
) -> None:
    symbols = {
        previous.metrics.symbol,
        current.metrics.symbol,
        trade_flow.symbol,
    }
    if len(symbols) != 1:
        raise ValueError("Liquidity intelligence inputs must use the same symbol.")
    if previous.metrics.as_of > current.metrics.as_of:
        raise ValueError("Previous liquidity frame cannot follow current frame.")


def _positive_baseline(value: float) -> float:
    if not _is_positive(value):
        raise ValueError("Aggressive notional baseline must be positive.")
    return value


def _price_move_bps(previous: float | None, current: float | None) -> float | None:
    if previous is None or current is None or previous <= 0:
        return None
    return (current - previous) / previous * 10_000


def _favorable_move(move: float | None, side: FlowSide) -> float | None:
    if move is None or side is FlowSide.NONE:
        return None
    return max(move, 0.0) if side is FlowSide.BUY else max(-move, 0.0)


def _opposing_depth_retention(
    previous: LiquidityBookFrame,
    current: LiquidityBookFrame,
    side: FlowSide,
) -> float | None:
    if side is FlowSide.BUY:
        before = previous.metrics.ask_depth_10bps
        after = current.metrics.ask_depth_10bps
    elif side is FlowSide.SELL:
        before = previous.metrics.bid_depth_10bps
        after = current.metrics.bid_depth_10bps
    else:
        return None
    if before is None or after is None or before <= 0:
        return None
    return after / before


def _absorption_score(
    flow_ratio: float,
    favorable_move: float | None,
    retention: float | None,
    config: LiquidityIntelligenceConfig,
) -> float:
    flow_score = _normalized_ratio(flow_ratio, config.absorption_flow_ratio)
    move_score = 0.0 if favorable_move is None else max(
        0.0,
        1 - favorable_move / config.absorption_max_move_bps,
    )
    retention_score = 0.0 if retention is None else min(retention, 1.0)
    weighted = 0.45 * flow_score + 0.30 * move_score + 0.25 * retention_score
    return min(100.0, 100 * weighted)


def _directional_displacement(
    move: float | None,
    direction: SweepDirection,
) -> float | None:
    if move is None or direction is SweepDirection.NONE:
        return None
    return move if direction is SweepDirection.UP else -move


def _consumed_levels(
    previous: LiquidityBookFrame,
    current: LiquidityBookFrame,
    direction: SweepDirection,
) -> tuple[int, float]:
    if direction is SweepDirection.UP and current.metrics.best_ask is not None:
        consumed = tuple(
            level
            for level in previous.asks
            if level.price < current.metrics.best_ask
        )
    elif direction is SweepDirection.DOWN and current.metrics.best_bid is not None:
        consumed = tuple(
            level
            for level in previous.bids
            if level.price > current.metrics.best_bid
        )
    else:
        consumed = ()
    return len(consumed), sum(level.price * level.quantity for level in consumed)


def _depth_depletion(
    previous: LiquidityBookFrame,
    current: LiquidityBookFrame,
    direction: SweepDirection,
) -> float | None:
    if direction is SweepDirection.UP:
        before = previous.metrics.ask_depth_10bps
        after = current.metrics.ask_depth_10bps
    elif direction is SweepDirection.DOWN:
        before = previous.metrics.bid_depth_10bps
        after = current.metrics.bid_depth_10bps
    else:
        return None
    if before is None or after is None or before <= 0:
        return None
    return (before - after) / before


def _delta_acceleration(trade_flow: TradeFlowMetrics, baseline: float) -> float:
    expected_one_second = max(abs(trade_flow.delta_5s) / 5, baseline / 5)
    return min(abs(trade_flow.delta_1s) / expected_one_second, 1.0)


def _weighted_book_pressure(metrics: OrderBookMetrics) -> float | None:
    components = (
        (metrics.imbalance_l1, 0.50),
        (metrics.imbalance_l5, 0.30),
        (metrics.imbalance_l10, 0.20),
    )
    available = tuple(
        (value, weight) for value, weight in components if value is not None
    )
    if not available:
        return None
    return sum(value * weight for value, weight in available) / sum(
        weight for _, weight in available
    )


def _depth_pressure(metrics: OrderBookMetrics) -> float | None:
    bid = metrics.bid_depth_10bps
    ask = metrics.ask_depth_10bps
    if bid is None or ask is None or bid + ask == 0:
        return None
    return (bid - ask) / (bid + ask)


def _pressure_reasons(
    book: float | None,
    trade: float,
    depth: float | None,
    direction: PressureDirection,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if abs(trade) >= 0.25:
        reasons.append("Trade delta создаёт направленное давление.")
    if book is not None and abs(book) >= 0.15:
        reasons.append("Многоуровневый imbalance подтверждает давление.")
    if depth is not None and abs(depth) >= 0.15:
        reasons.append("Глубина книги асимметрична.")
    if direction is PressureDirection.NEUTRAL:
        reasons.append("Совокупное давление остаётся нейтральным.")
    return tuple(reasons)


def _normalized_ratio(value: float, threshold: float) -> float:
    return min(value / threshold, 1.0)


def _normalized_optional_ratio(value: float | None, threshold: float) -> float:
    if value is None:
        return 0.0
    return max(0.0, min(value / threshold, 1.0))


def _distance_bps(price: float, mid: float) -> float:
    return abs(price - mid) / mid * 10_000


def _price(level: OrderBookLevel) -> float:
    return level.price


def _is_finite(value: float) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(value)
    )


def _is_positive(value: float) -> bool:
    return _is_finite(value) and value > 0


def _resolved_config(
    value: LiquidityIntelligenceConfig | None,
) -> LiquidityIntelligenceConfig:
    return value if value is not None else LiquidityIntelligenceConfig()
