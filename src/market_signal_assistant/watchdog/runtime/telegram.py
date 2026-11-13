"""Standalone Watchdog Telegram projector; never starts the Watchdog runtime."""

from __future__ import annotations

import argparse
import os
import signal
import threading
from pathlib import Path

from market_signal_assistant.watchdog.telegram_adapter import (
    DeliveryError,
    JournalChangedError,
    TelegramHttpSender,
    WatchdogTelegramAdapter,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-journal", type=Path, required=True)
    parser.add_argument("--state-db", type=Path, required=True)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--minimum-spacing-seconds", type=float, default=1.0)
    args = parser.parse_args(argv)
    if args.poll_seconds <= 0 or args.minimum_spacing_seconds <= 0:
        parser.error("poll and spacing must be positive")
    token = os.environ.get("QTR_WATCHDOG_TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("QTR_WATCHDOG_TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        parser.error("Watchdog Telegram token/chat ID are required in environment")
    stop = threading.Event()

    def request_stop(_signum: int, _frame: object) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    adapter = WatchdogTelegramAdapter(
        args.event_journal,
        args.state_db,
        sender=TelegramHttpSender(),
        token=token,
        chat_id=chat_id,
        minimum_spacing_seconds=args.minimum_spacing_seconds,
    )
    try:
        while not stop.is_set():
            try:
                adapter.run_once()
            except (DeliveryError, JournalChangedError) as error:
                # Error types/messages contain no token, chat ID, or API payload.
                print(f"watchdog telegram stopped: {type(error).__name__}: {error}")
                return 2
            stop.wait(args.poll_seconds)
    finally:
        adapter.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
