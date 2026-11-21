"""Opt-in FAST WATCH V2 shadow WebSocket runner; never sends orders or alerts."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import shutil
import signal
import subprocess
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from market_signal_assistant.providers import BybitPublicProvider
from market_signal_assistant.watchdog.fast_watch_v2 import (
    MAX_FRAME_BYTES,
    FastWatchConfig,
    FastWatchIntegrityError,
    FastWatchShadow,
)
from market_signal_assistant.watchdog.runtime.retry import ApiRequestBudget
from market_signal_assistant.watchdog.universe import DynamicUniverse

WS_URL = "wss://stream.bybit.com/v5/public/linear"
REFRESH_SECONDS = 900
STALE_SECONDS = 150
MAX_RECONNECTS = 3


class FastWatchSocket(Protocol):
    async def send(self, message: str) -> None: ...

    async def recv(self) -> str | bytes: ...


def eligible_symbols(provider: BybitPublicProvider, *, at: datetime) -> frozenset[str]:
    snapshot = DynamicUniverse().build(
        provider.list_instruments(), observed_at=at, baseline_samples={}
    )
    return frozenset(item.instrument.symbol for item in snapshot.eligible)


async def consume_socket(
    pilot: FastWatchShadow,
    socket: FastWatchSocket,
    *,
    stop: asyncio.Event,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    stale_seconds: float = STALE_SECONDS,
) -> None:
    """One sequential consumer; library receive queue supplies bounded backpressure."""
    pilot.begin_subscription(now())
    await socket.send(
        json.dumps(
            {"op": "subscribe", "args": [f"kline.1.{s}" for s in pilot.config.symbols]},
            separators=(",", ":"),
        )
    )
    last_frame = asyncio.get_running_loop().time()
    while not stop.is_set():
        try:
            frame = await asyncio.wait_for(socket.recv(), timeout=5.0)
        except TimeoutError:
            pilot.assert_progress(now())
            if asyncio.get_running_loop().time() - last_frame > stale_seconds:
                raise ConnectionError("Bybit FAST WATCH feed is stale.") from None
            continue
        last_frame = asyncio.get_running_loop().time()
        pilot.ingest(frame, received_at=now())
        pilot.assert_progress(now())


async def run_stream(
    pilot: FastWatchShadow,
    *,
    stop: asyncio.Event,
    eligibility: Callable[[], frozenset[str]],
) -> None:
    """Reconnect with a finite budget; a missing confirmed minute fails closed."""
    try:
        from websockets.asyncio.client import connect
        from websockets.exceptions import ConnectionClosed
    except ImportError as error:
        raise RuntimeError(
            "FAST WATCH requires the optional websocket extra."
        ) from error
    if not set(pilot.config.symbols) <= await asyncio.to_thread(eligibility):
        raise FastWatchIntegrityError("Requested symbols are not eligible.")
    reconnects = 0
    last_refresh = asyncio.get_running_loop().time()
    while not stop.is_set():
        try:
            async with connect(
                WS_URL,
                max_size=MAX_FRAME_BYTES,
                max_queue=16,
                ping_interval=20,
                ping_timeout=20,
                open_timeout=10,
            ) as socket:
                pilot.begin_subscription(datetime.now(UTC))
                await socket.send(
                    json.dumps(
                        {
                            "op": "subscribe",
                            "args": [f"kline.1.{s}" for s in pilot.config.symbols],
                        },
                        separators=(",", ":"),
                    )
                )
                last_frame = asyncio.get_running_loop().time()
                while not stop.is_set():
                    try:
                        frame = await asyncio.wait_for(socket.recv(), timeout=5.0)
                    except TimeoutError:
                        frame = None
                    if frame is not None:
                        last_frame = asyncio.get_running_loop().time()
                        before = pilot.confirmed_count
                        pilot.ingest(frame, received_at=datetime.now(UTC))
                        if pilot.confirmed_count > before:
                            reconnects = 0
                    pilot.assert_progress(datetime.now(UTC))
                    current = asyncio.get_running_loop().time()
                    if current - last_frame > STALE_SECONDS:
                        raise ConnectionError("Bybit FAST WATCH feed is stale.")
                    if current - last_refresh >= REFRESH_SECONDS:
                        if not set(pilot.config.symbols) <= await asyncio.to_thread(
                            eligibility
                        ):
                            raise FastWatchIntegrityError(
                                "Selected symbol lost eligibility."
                            )
                        last_refresh = current
        except FastWatchIntegrityError:
            raise
        except (ConnectionClosed, ConnectionError, OSError, TimeoutError) as error:
            if stop.is_set():
                break
            pilot.record_transport_failure(type(error).__name__, at=datetime.now(UTC))
            reconnects += 1
            if reconnects > MAX_RECONNECTS:
                raise FastWatchIntegrityError(
                    "WebSocket reconnect budget exhausted."
                ) from None
            await asyncio.sleep(min(2**reconnects, 8))


def _verify_checkout(checkout: Path, expected: str) -> None:
    for arguments, wanted in (
        (["rev-parse", "HEAD"], expected),
        (["status", "--porcelain"], ""),
    ):
        result = subprocess.run(
            ["git", "-C", str(checkout), *arguments],
            capture_output=True,
            text=True,
            check=True,
        )
        if result.stdout.strip() != wanted:
            raise FastWatchIntegrityError("Checkout SHA or cleanliness differs.")


def _resource_gate(root: Path) -> None:
    existing = root if root.exists() else root.parent
    if shutil.disk_usage(existing).free < 4 * 1024**3:
        raise FastWatchIntegrityError("Less than 4 GiB free for shadow dataset.")
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        available = next(
            (
                int(line.split()[1]) * 1024
                for line in meminfo.read_text().splitlines()
                if line.startswith("MemAvailable:")
            ),
            0,
        )
        if available < 1024**3:
            raise FastWatchIntegrityError("Less than 1 GiB MemAvailable.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Isolated 1m FAST WATCH shadow pilot")
    parser.add_argument("--checkout", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--symbols", nargs="+", required=True)
    args = parser.parse_args(argv)
    config = FastWatchConfig(tuple(args.symbols), args.expected_commit)
    _verify_checkout(args.checkout.resolve(), config.commit)
    _resource_gate(args.data_root.resolve())
    budget = ApiRequestBudget(12)
    provider = BybitPublicProvider(request_gate=budget.acquire)

    def eligibility() -> frozenset[str]:
        return eligible_symbols(provider, at=datetime.now(UTC))

    # Eligibility and resources are checked before creating any dataset files.
    if not set(config.symbols) <= eligibility():
        raise FastWatchIntegrityError("Requested symbols are not currently eligible.")
    pilot = FastWatchShadow(args.data_root, config)
    pilot.start()
    stop = asyncio.Event()
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            for signum in (signal.SIGINT, signal.SIGTERM):
                with contextlib.suppress(NotImplementedError):
                    loop.add_signal_handler(signum, stop.set)
            loop.run_until_complete(
                run_stream(pilot, stop=stop, eligibility=eligibility)
            )
        finally:
            loop.close()
            asyncio.set_event_loop(None)
    finally:
        pilot.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
