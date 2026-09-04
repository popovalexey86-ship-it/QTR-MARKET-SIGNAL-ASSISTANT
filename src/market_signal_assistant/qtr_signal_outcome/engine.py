from __future__ import annotations

from datetime import UTC, datetime, timedelta

from market_signal_assistant.qtr_signal_outcome.models import (
    BarrierHit,
    BarrierOrder,
    BarrierPairOutcome,
    Direction,
    HorizonOutcome,
    MarketCandle,
    OutcomeStatus,
    SignalOutcome,
    SignalSnapshot,
)

CHECKPOINTS = (5, 15, 30, 60, 120, 240)
FAVORABLE_THRESHOLDS = (0.5, 1.0, 1.5, 2.0, 3.0)
ADVERSE_THRESHOLDS = (0.5, 1.0, 1.5)
BARRIER_PAIRS = (
    (0.5, 0.5),
    (1.0, 1.0),
    (1.5, 1.0),
    (2.0, 1.0),
    (3.0, 1.0),
)


class OutcomeEngine:
    def __init__(self, maximum_horizon_minutes: int = 240) -> None:
        if maximum_horizon_minutes < 5 or maximum_horizon_minutes > 240:
            raise ValueError("Outcome horizon must be within 5..240 minutes.")
        self._maximum_horizon = maximum_horizon_minutes

    @property
    def maximum_horizon_minutes(self) -> int:
        return self._maximum_horizon

    def analyze(
        self,
        signal: SignalSnapshot,
        candles: tuple[MarketCandle, ...],
        *,
        analyzed_at: datetime | None = None,
    ) -> SignalOutcome:
        ordered = _causal_candles(signal, candles, self._maximum_horizon)
        through = ordered[-1].closed_at if ordered else None
        deadline = _full_candle_cutoff(signal.signal_timestamp, self._maximum_horizon)
        status = (
            OutcomeStatus.COMPLETE
            if through is not None and through >= deadline
            else OutcomeStatus.PARTIAL
        )
        checkpoints = tuple(
            _horizon(signal, ordered, minutes)
            for minutes in CHECKPOINTS
            if minutes <= self._maximum_horizon
        )
        favorable = tuple(
            _barrier_hit(signal, ordered, threshold, favorable=True)
            for threshold in FAVORABLE_THRESHOLDS
        )
        adverse = tuple(
            _barrier_hit(signal, ordered, threshold, favorable=False)
            for threshold in ADVERSE_THRESHOLDS
        )
        pairs = tuple(
            BarrierPairOutcome(fav, -adv, _barrier_order(signal, ordered, fav, adv))
            for fav, adv in BARRIER_PAIRS
        )
        invalidation = _invalidation(signal, ordered)
        return SignalOutcome(
            signal=signal,
            status=status,
            analyzed_at=analyzed_at or datetime.now(UTC),
            analyzed_through=through,
            maximum_horizon_minutes=self._maximum_horizon,
            horizons=checkpoints,
            favorable_barriers=favorable,
            adverse_barriers=adverse,
            barrier_orders=pairs,
            invalidation_hit=invalidation is not None,
            invalidation_first_hit_timestamp=(
                invalidation.closed_at if invalidation is not None else None
            ),
            invalidation_minutes=(
                _minutes(signal, invalidation) if invalidation is not None else None
            ),
        )


def failed_market_data_outcome(
    signal: SignalSnapshot,
    message: str,
    maximum_horizon_minutes: int,
    *,
    analyzed_at: datetime | None = None,
) -> SignalOutcome:
    return SignalOutcome(
        signal=signal,
        status=OutcomeStatus.FAILED_MARKET_DATA,
        analyzed_at=analyzed_at or datetime.now(UTC),
        analyzed_through=None,
        maximum_horizon_minutes=maximum_horizon_minutes,
        horizons=(),
        favorable_barriers=(),
        adverse_barriers=(),
        barrier_orders=(),
        invalidation_hit=False,
        invalidation_first_hit_timestamp=None,
        invalidation_minutes=None,
        market_data_error=message,
    )


def _causal_candles(
    signal: SignalSnapshot,
    candles: tuple[MarketCandle, ...],
    maximum_horizon: int,
) -> tuple[MarketCandle, ...]:
    ordered = tuple(sorted(candles, key=lambda item: item.opened_at))
    previous: datetime | None = None
    deadline = signal.signal_timestamp + timedelta(minutes=maximum_horizon)
    accepted: list[MarketCandle] = []
    for item in ordered:
        if item.symbol != signal.symbol:
            raise ValueError("Candle and signal symbols must match.")
        if previous is not None and item.opened_at <= previous:
            raise ValueError("Candle timestamps must be strictly increasing.")
        previous = item.opened_at
        # A candle already open at signal time may contain pre-signal extremes.
        if item.opened_at < signal.signal_timestamp:
            continue
        if item.closed_at > deadline:
            continue
        accepted.append(item)
    return tuple(accepted)


def _horizon(
    signal: SignalSnapshot,
    candles: tuple[MarketCandle, ...],
    minutes: int,
) -> HorizonOutcome:
    deadline = _full_candle_cutoff(signal.signal_timestamp, minutes)
    relevant = tuple(item for item in candles if item.closed_at <= deadline)
    complete = bool(relevant) and relevant[-1].closed_at >= deadline
    if not complete:
        return HorizonOutcome(minutes, None, None, None, None, None, None, None)
    close = relevant[-1].close
    sign = 1.0 if signal.direction is Direction.LONG else -1.0
    favorable = max(0.0, *(_favorable(signal, item) for item in relevant))
    adverse = max(0.0, *(_adverse(signal, item) for item in relevant))
    return HorizonOutcome(
        horizon_minutes=minutes,
        close_price=close,
        directional_close_return_pct=(
            (close - signal.signal_price) * sign / signal.signal_price * 100.0
        ),
        directional_close_return_atr=(close - signal.signal_price) * sign / signal.atr,
        mfe_price=favorable,
        mae_price=adverse,
        mfe_atr=favorable / signal.atr,
        mae_atr=adverse / signal.atr,
    )


def _favorable(signal: SignalSnapshot, candle: MarketCandle) -> float:
    if signal.direction is Direction.LONG:
        return candle.high - signal.signal_price
    return signal.signal_price - candle.low


def _adverse(signal: SignalSnapshot, candle: MarketCandle) -> float:
    if signal.direction is Direction.LONG:
        return signal.signal_price - candle.low
    return candle.high - signal.signal_price


def _barrier_hit(
    signal: SignalSnapshot,
    candles: tuple[MarketCandle, ...],
    threshold: float,
    *,
    favorable: bool,
) -> BarrierHit:
    found = _first_barrier_candle(signal, candles, threshold, favorable=favorable)
    return BarrierHit(
        threshold_atr=threshold if favorable else -threshold,
        hit=found is not None,
        first_hit_timestamp=found.closed_at if found is not None else None,
        first_hit_minutes_from_signal=(
            _minutes(signal, found) if found is not None else None
        ),
    )


def _first_barrier_candle(
    signal: SignalSnapshot,
    candles: tuple[MarketCandle, ...],
    threshold: float,
    *,
    favorable: bool,
) -> MarketCandle | None:
    distance = signal.atr * threshold
    for item in candles:
        excursion = _favorable(signal, item) if favorable else _adverse(signal, item)
        if excursion >= distance:
            return item
    return None


def _barrier_order(
    signal: SignalSnapshot,
    candles: tuple[MarketCandle, ...],
    favorable: float,
    adverse: float,
) -> BarrierOrder:
    favorable_candle = _first_barrier_candle(signal, candles, favorable, favorable=True)
    adverse_candle = _first_barrier_candle(signal, candles, adverse, favorable=False)
    if favorable_candle is None and adverse_candle is None:
        return BarrierOrder.NEITHER
    if adverse_candle is None:
        return BarrierOrder.FAVORABLE_FIRST
    if favorable_candle is None:
        return BarrierOrder.ADVERSE_FIRST
    if favorable_candle.opened_at == adverse_candle.opened_at:
        return BarrierOrder.AMBIGUOUS_SAME_CANDLE
    if favorable_candle.opened_at < adverse_candle.opened_at:
        return BarrierOrder.FAVORABLE_FIRST
    return BarrierOrder.ADVERSE_FIRST


def _invalidation(
    signal: SignalSnapshot,
    candles: tuple[MarketCandle, ...],
) -> MarketCandle | None:
    level = signal.invalidation_price
    if level is None:
        return None
    for item in candles:
        if signal.direction is Direction.LONG and item.low <= level:
            return item
        if signal.direction is Direction.SHORT and item.high >= level:
            return item
    return None


def _minutes(signal: SignalSnapshot, candle: MarketCandle) -> float:
    return (candle.closed_at - signal.signal_timestamp).total_seconds() / 60.0


def _full_candle_cutoff(signal_time: datetime, minutes: int) -> datetime:
    """Use only complete minute candles wholly observable after the signal."""
    return signal_time.replace(second=0, microsecond=0) + timedelta(minutes=minutes)
