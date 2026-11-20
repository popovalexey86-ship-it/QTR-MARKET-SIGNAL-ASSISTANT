"""Standalone Watchdog Telegram projector; never starts the Watchdog runtime."""

from __future__ import annotations

import argparse
import os
import signal
import sqlite3
import threading
from pathlib import Path

from market_signal_assistant.watchdog.runtime.control_snapshot import (
    ControlSnapshotError,
)
from market_signal_assistant.watchdog.telegram_adapter import (
    DeliveryError,
    JournalChangedError,
    TelegramHttpSender,
    WatchdogTelegramAdapter,
)
from market_signal_assistant.watchdog.telegram_control import (
    TelegramControlHttp,
    WatchdogControlPanel,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-journal", type=Path, required=True)
    parser.add_argument("--state-db", type=Path, required=True)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--minimum-spacing-seconds", type=float, default=1.0)
    parser.add_argument("--panel-root", type=Path)
    parser.add_argument("--alert-policy-v1", action="store_true")
    args = parser.parse_args(argv)
    if args.poll_seconds <= 0 or args.minimum_spacing_seconds <= 0:
        parser.error("poll and spacing must be positive")
    token = os.environ.get("QTR_WATCHDOG_TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("QTR_WATCHDOG_TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        parser.error("Watchdog Telegram token/chat ID are required in environment")
    allowed_users: frozenset[int] = frozenset()
    if args.panel_root is not None:
        raw_ids = os.environ.get("QTR_WATCHDOG_TELEGRAM_ALLOWED_USER_IDS", "")
        try:
            allowed_users = frozenset(
                int(item.strip()) for item in raw_ids.split(",") if item.strip()
            )
        except ValueError:
            parser.error("Watchdog panel user allowlist is invalid")
        if not allowed_users:
            parser.error("Watchdog panel requires allowed user IDs")
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
        panel_enabled=args.panel_root is not None,
        alert_policy_enabled=args.alert_policy_v1,
    )
    panel = (
        WatchdogControlPanel(
            adapter.db,
            args.panel_root,
            transport=TelegramControlHttp(),
            token=token,
            chat_id=chat_id,
            allowed_user_ids=allowed_users,
            minimum_spacing_seconds=args.minimum_spacing_seconds,
        )
        if args.panel_root is not None
        else None
    )
    try:
        while not stop.is_set():
            try:
                adapter.run_once()
                if panel is not None:
                    try:
                        panel.run_once()
                    except (
                        DeliveryError,
                        ControlSnapshotError,
                        sqlite3.Error,
                        OSError,
                        ValueError,
                        TypeError,
                    ) as error:
                        # Panel read/API failures cannot suppress anomaly alerts.
                        print(f"watchdog panel retry: {type(error).__name__}")
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
