from __future__ import annotations

import argparse
import asyncio
import os
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from market_signal_assistant.qtr_micro_scalper.data.models import (
    OrderBookEvent,
    PublicTradeEvent,
)
from market_signal_assistant.qtr_micro_scalper.data.trades import TradeFlowAccumulator
from market_signal_assistant.qtr_micro_scalper.live.collector import (
    UnifiedMarketDataCollector,
    WebSocketFactory,
    default_websocket_factory,
)
from market_signal_assistant.qtr_micro_scalper.live.orderbook_ws import (
    OrderBookCollector,
)
from market_signal_assistant.qtr_micro_scalper.live.trades_ws import (
    PublicTradeCollector,
)
from market_signal_assistant.qtr_micro_scalper_v3.engine import (
    CashScalperConfig,
    CashScalperEngine,
)
from market_signal_assistant.qtr_micro_scalper_v3.runtime import (
    V3FeatureBuilder,
    V3ShadowRuntime,
)
from market_signal_assistant.qtr_micro_scalper_v3.telemetry import (
    DEFAULT_ENTRY_TELEMETRY_PATH,
    DEFAULT_FORWARD_OUTCOME_PATH,
    DEFAULT_TRADE_JOURNAL_PATH,
    JsonlTelemetryJournal,
)

MarketEvent = PublicTradeEvent | OrderBookEvent


@dataclass(frozen=True, slots=True)
class V3ServiceSettings:
    enabled: bool = False
    shadow_mode: bool = True
    symbols: tuple[str, ...] = ("BTCUSDT",)
    notional: float = 1_000.0
    entry_telemetry_path: Path = DEFAULT_ENTRY_TELEMETRY_PATH
    trade_journal_path: Path = DEFAULT_TRADE_JOURNAL_PATH
    forward_outcome_path: Path = DEFAULT_FORWARD_OUTCOME_PATH

    def __post_init__(self) -> None:
        normalized = tuple(
            dict.fromkeys(item.strip().upper() for item in self.symbols if item.strip())
        )
        if not normalized:
            raise ValueError("V3 shadow service requires at least one symbol.")
        if self.notional <= 0:
            raise ValueError("V3 shadow notional must be positive.")
        object.__setattr__(self, "symbols", normalized)

    @classmethod
    def from_environment(cls) -> V3ServiceSettings:
        symbols = tuple(
            item.strip()
            for item in os.getenv("QTR_SCALPER_V3_SYMBOLS", "BTCUSDT").split(",")
            if item.strip()
        )
        return cls(
            enabled=_env_bool("QTR_SCALPER_V3_ENABLED", False),
            shadow_mode=_env_bool("QTR_SCALPER_V3_SHADOW_MODE", True),
            symbols=symbols,
            notional=float(os.getenv("QTR_SCALPER_V3_SHADOW_NOTIONAL", "1000")),
            entry_telemetry_path=Path(
                os.getenv(
                    "QTR_SCALPER_V3_ENTRY_TELEMETRY_PATH",
                    str(DEFAULT_ENTRY_TELEMETRY_PATH),
                )
            ),
            trade_journal_path=Path(
                os.getenv(
                    "QTR_SCALPER_V3_TRADE_JOURNAL_PATH",
                    str(DEFAULT_TRADE_JOURNAL_PATH),
                )
            ),
            forward_outcome_path=Path(
                os.getenv(
                    "QTR_SCALPER_V3_FORWARD_OUTCOME_PATH",
                    str(DEFAULT_FORWARD_OUTCOME_PATH),
                )
            ),
        )


class V3ShadowService:
    """Lazy public-data service. It has no private API or order capability."""

    def __init__(
        self,
        *,
        settings: V3ServiceSettings,
        collector: UnifiedMarketDataCollector,
        feature_builder: V3FeatureBuilder,
        runtime: V3ShadowRuntime,
        queue: asyncio.Queue[MarketEvent],
    ) -> None:
        self._settings = settings
        self._collector = collector
        self._features = feature_builder
        self._runtime = runtime
        self._queue = queue
        self._worker: asyncio.Task[None] | None = None
        self._running = False

    @property
    def running(self) -> bool:
        return self._running

    async def start(self) -> None:
        if self._running:
            return
        if not self._settings.enabled:
            raise RuntimeError("QTR Scalper V3 is disabled.")
        if not self._settings.shadow_mode:
            raise RuntimeError("QTR Scalper V3 is restricted to shadow mode.")
        self._worker = asyncio.create_task(self._run())
        await self._collector.start()
        self._running = True

    async def stop(self) -> None:
        if not self._running and self._worker is None:
            return
        await self._collector.stop()
        worker = self._worker
        self._worker = None
        if worker is not None:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        self._running = False

    async def wait(self) -> None:
        worker = self._worker
        if worker is not None:
            await worker

    async def _run(self) -> None:
        while True:
            event = await self._queue.get()
            try:
                if not isinstance(event, PublicTradeEvent):
                    continue
                snapshot = self._features.observe_trade(event)
                active = self._runtime.active_trade(event.symbol)
                directional_failure = False
                if snapshot is not None and active is not None:
                    directional_failure = (
                        snapshot.direction is not active.direction
                        and abs(snapshot.flow_imbalance_5s) >= 0.25
                        and abs(snapshot.price_displacement_5s_bps) >= 3.0
                    )
                self._runtime.process_price(
                    event.symbol,
                    event.received_at,
                    event.price,
                    directional_failure=directional_failure,
                )
                if snapshot is not None:
                    self._runtime.process_snapshot(snapshot)
            finally:
                self._queue.task_done()


def build_v3_shadow_service(
    settings: V3ServiceSettings | None = None,
    *,
    websocket_factory: WebSocketFactory = default_websocket_factory,
    config: CashScalperConfig | None = None,
) -> V3ShadowService:
    """Compose V3 without opening sockets; network starts only in start()."""

    resolved = settings or V3ServiceSettings.from_environment()
    queue: asyncio.Queue[MarketEvent] = asyncio.Queue(maxsize=10_000)

    def sink(event: MarketEvent) -> None:
        try:
            queue.put_nowait(event)
        except asyncio.QueueFull:
            return

    trade_flow = TradeFlowAccumulator()
    books = OrderBookCollector(
        resolved.symbols,
        websocket_factory=websocket_factory,
        event_sink=sink,
    )
    trades = PublicTradeCollector(
        resolved.symbols,
        trade_flow,
        websocket_factory=websocket_factory,
        event_sink=sink,
    )
    collector = UnifiedMarketDataCollector((trades, books))
    engine = CashScalperEngine(config)
    runtime = V3ShadowRuntime(
        engine=engine,
        entry_journal=JsonlTelemetryJournal(resolved.entry_telemetry_path),
        trade_journal=JsonlTelemetryJournal(resolved.trade_journal_path),
        outcome_journal=JsonlTelemetryJournal(resolved.forward_outcome_path),
        notional=resolved.notional,
    )
    return V3ShadowService(
        settings=resolved,
        collector=collector,
        feature_builder=V3FeatureBuilder(trade_flow, books.state),
        runtime=runtime,
        queue=queue,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="qtr-scalper-v3-shadow",
        description="QTR Micro Scalper V3 — изолированный shadow-only процесс.",
    )
    parser.add_argument(
        "--symbols",
        help="Список USDT-инструментов через запятую; иначе QTR_SCALPER_V3_SYMBOLS.",
    )
    return parser.parse_args(argv)


async def _run(args: argparse.Namespace) -> int:
    settings = V3ServiceSettings.from_environment()
    if args.symbols:
        settings = V3ServiceSettings(
            enabled=settings.enabled,
            shadow_mode=settings.shadow_mode,
            symbols=tuple(args.symbols.split(",")),
            notional=settings.notional,
            entry_telemetry_path=settings.entry_telemetry_path,
            trade_journal_path=settings.trade_journal_path,
            forward_outcome_path=settings.forward_outcome_path,
        )
    service = build_v3_shadow_service(settings)
    await service.start()
    try:
        await service.wait()
    finally:
        await service.stop()
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:
        return 0


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value.")


if __name__ == "__main__":
    raise SystemExit(main())
