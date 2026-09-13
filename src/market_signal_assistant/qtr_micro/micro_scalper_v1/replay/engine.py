from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from market_signal_assistant.qtr_micro.micro_scalper_v1.confirmation import (
    BuyerConfirmationGate,
)
from market_signal_assistant.qtr_micro.micro_scalper_v1.reclaim import (
    LongReclaimTrigger,
    ReclaimSnapshot,
)
from market_signal_assistant.qtr_micro.micro_scalper_v1.replay.stream import (
    ReplayEvent,
    ReplayEventType,
)
from market_signal_assistant.qtr_micro.micro_scalper_v1.replay.trade_flow import (
    ReplayTradeFlowAccumulator,
)
from market_signal_assistant.qtr_micro_scalper.data.liquidity import (
    FlowSide,
    LiquidityBookFrame,
    LiquidityIntelligenceLayer,
    SweepDirection,
)
from market_signal_assistant.qtr_micro_scalper.data.orderbook import (
    OrderBookState,
)


class ReplayClock:
    def __init__(self) -> None:
        self._now: datetime | None = None

    def set(self, value: datetime) -> None:
        self._now = value

    def __call__(self) -> datetime:
        if self._now is None:
            raise RuntimeError("Replay clock has not been initialized.")
        return self._now


class ReplaySetupStage(StrEnum):
    WAIT_ABSORPTION = "wait_absorption"
    WAIT_CONFIRMATION = "wait_confirmation"
    WAIT_RECLAIM = "wait_reclaim"


@dataclass(slots=True)
class PendingLongSetup:
    stage: ReplaySetupStage
    stage_started_at: datetime
    reclaimed_level: float
    sweep_price: float


@dataclass(frozen=True, slots=True)
class ReplayConfig:
    aggressive_notional_baseline: float = 1_000.0
    analysis_interval_ms: int = 1_000
    sequence_window_seconds: float = 5.0
    orderbook_depth: int = 200


@dataclass(frozen=True, slots=True)
class ReplayStats:
    total_events: int
    trade_events: int
    orderbook_events: int
    analyses: int
    down_sweeps: int
    sell_absorptions: int
    sweep_then_absorption: int
    causal_sweep_absorptions: int
    buyer_confirmations: int
    confirmed_reclaims: int
    long_candidates: int
    expired_setups: int


class HistoricalReplayEngine:
    def __init__(
        self,
        *,
        symbol: str,
        config: ReplayConfig | None = None,
    ) -> None:
        self._symbol = symbol
        self._config = config or ReplayConfig()

        self._clock = ReplayClock()
        self._trade_flow = ReplayTradeFlowAccumulator(
            clock=self._clock,
        )
        self._book = OrderBookState(
            symbol,
            depth=self._config.orderbook_depth,
            require_contiguous_update_ids=True,
        )
        self._liquidity = LiquidityIntelligenceLayer()
        self._confirmation = BuyerConfirmationGate()
        self._reclaim = LongReclaimTrigger()

    def run(self, events: object) -> ReplayStats:
        total_events = 0
        trade_events = 0
        orderbook_events = 0
        analyses = 0
        down_sweeps = 0
        sell_absorptions = 0
        sweep_then_absorption = 0
        causal_sweep_absorptions = 0
        buyer_confirmations = 0
        confirmed_reclaims = 0
        long_candidates = 0
        expired_setups = 0

        previous_frame: LiquidityBookFrame | None = None
        last_analysis_at: datetime | None = None
        pending_down_sweep_at: datetime | None = None
        pending_setup: PendingLongSetup | None = None

        analysis_interval = timedelta(
            milliseconds=self._config.analysis_interval_ms
        )
        sequence_window = timedelta(
            seconds=self._config.sequence_window_seconds
        )

        for event in events:
            if not isinstance(event, ReplayEvent):
                raise TypeError(
                    "Replay stream must contain ReplayEvent objects."
                )

            total_events += 1
            self._clock.set(event.exchange_at)

            if event.event_type is ReplayEventType.TRADE:
                trade_events += 1
                self._trade_flow.ingest(event.payload)
                continue

            if event.event_type is not ReplayEventType.ORDERBOOK:
                continue

            orderbook_events += 1
            self._book.process(event.payload)

            if (
                last_analysis_at is not None
                and event.exchange_at - last_analysis_at
                < analysis_interval
            ):
                continue

            current_frame = LiquidityBookFrame.from_state(
                self._book,
                as_of=event.exchange_at,
            )

            if not current_frame.metrics.ready:
                continue

            if previous_frame is None:
                previous_frame = current_frame
                last_analysis_at = event.exchange_at
                continue

            trade_flow = self._trade_flow.metrics(
                self._symbol,
                as_of=event.exchange_at,
            )

            liquidity = self._liquidity.analyze(
                previous_frame,
                current_frame,
                trade_flow,
                aggressive_notional_baseline=(
                    self._config.aggressive_notional_baseline
                ),
            )

            analyses += 1

            is_down_sweep = (
                liquidity.sweep.detected
                and liquidity.sweep.direction is SweepDirection.DOWN
            )

            if is_down_sweep:
                down_sweeps += 1
                pending_down_sweep_at = event.exchange_at

            is_sell_absorption = (
                liquidity.absorption.detected
                and liquidity.absorption.aggressive_side is FlowSide.SELL
            )

            if is_sell_absorption:
                sell_absorptions += 1

                if pending_down_sweep_at is not None:
                    age = event.exchange_at - pending_down_sweep_at
                    if timedelta(0) <= age <= sequence_window:
                        sweep_then_absorption += 1
                        pending_down_sweep_at = None

            if (
                pending_down_sweep_at is not None
                and event.exchange_at - pending_down_sweep_at
                > sequence_window
            ):
                pending_down_sweep_at = None

            if (
                pending_setup is not None
                and event.exchange_at - pending_setup.stage_started_at
                > sequence_window
            ):
                pending_setup = None
                expired_setups += 1

            setup_created_now = False

            if is_down_sweep and pending_setup is None:
                reclaimed_level = previous_frame.metrics.best_bid
                sweep_price = current_frame.metrics.best_bid

                if (
                    reclaimed_level is not None
                    and sweep_price is not None
                    and reclaimed_level > 0
                    and sweep_price > 0
                    and sweep_price < reclaimed_level
                ):
                    pending_setup = PendingLongSetup(
                        stage=ReplaySetupStage.WAIT_ABSORPTION,
                        stage_started_at=event.exchange_at,
                        reclaimed_level=reclaimed_level,
                        sweep_price=sweep_price,
                    )
                    setup_created_now = True

            if (
                pending_setup is not None
                and not setup_created_now
                and pending_setup.stage
                is ReplaySetupStage.WAIT_ABSORPTION
                and event.exchange_at
                > pending_setup.stage_started_at
                and is_sell_absorption
            ):
                pending_setup.stage = (
                    ReplaySetupStage.WAIT_CONFIRMATION
                )
                pending_setup.stage_started_at = event.exchange_at
                causal_sweep_absorptions += 1
                continue_setup = False
            else:
                continue_setup = True

            if (
                continue_setup
                and pending_setup is not None
                and pending_setup.stage
                is ReplaySetupStage.WAIT_CONFIRMATION
                and event.exchange_at
                > pending_setup.stage_started_at
            ):
                confirmation = self._confirmation.evaluate(
                    trade_flow,
                    current_frame.metrics,
                )
                if confirmation.accepted:
                    pending_setup.stage = (
                        ReplaySetupStage.WAIT_RECLAIM
                    )
                    pending_setup.stage_started_at = event.exchange_at
                    buyer_confirmations += 1
                    continue_setup = False

            if (
                continue_setup
                and pending_setup is not None
                and pending_setup.stage
                is ReplaySetupStage.WAIT_RECLAIM
                and event.exchange_at
                > pending_setup.stage_started_at
            ):
                current_price = current_frame.metrics.best_bid

                if current_price is not None:
                    reclaim = self._reclaim.evaluate(
                        ReclaimSnapshot(
                            reclaimed_level=(
                                pending_setup.reclaimed_level
                            ),
                            sweep_price=pending_setup.sweep_price,
                            current_price=current_price,
                        )
                    )

                    if reclaim.accepted:
                        confirmed_reclaims += 1
                        long_candidates += 1
                        pending_setup = None

            previous_frame = current_frame
            last_analysis_at = event.exchange_at

        return ReplayStats(
            total_events=total_events,
            trade_events=trade_events,
            orderbook_events=orderbook_events,
            analyses=analyses,
            down_sweeps=down_sweeps,
            sell_absorptions=sell_absorptions,
            sweep_then_absorption=sweep_then_absorption,
            causal_sweep_absorptions=causal_sweep_absorptions,
            buyer_confirmations=buyer_confirmations,
            confirmed_reclaims=confirmed_reclaims,
            long_candidates=long_candidates,
            expired_setups=expired_setups,
        )
