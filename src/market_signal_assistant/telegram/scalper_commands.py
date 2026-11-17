"""Adapter called only after the existing bot allow-list check."""

from __future__ import annotations

from datetime import datetime

from market_signal_assistant.qtr_micro_scalper.control_store import ControlSnapshotStore
from market_signal_assistant.telegram.scalper_formatting import format_scalper_command

SCALPER_COMMANDS = frozenset(
    {
        "scalper_status",
        "scalper_active",
        "scalper_stats",
        "scalper_blocks",
        "scalper_experiments",
        "scalper_health",
        "scalper_version",
        "scalper_recent",
    }
)


class ScalperCommands:
    def __init__(self, store: ControlSnapshotStore | None = None) -> None:
        self.store = store or ControlSnapshotStore()

    def execute(self, command: str, *, now: datetime | None = None) -> tuple[str, ...]:
        if command not in SCALPER_COMMANDS:
            raise ValueError("Команда Scalper не поддерживается.")
        return format_scalper_command(command, self.store.read(now=now))
