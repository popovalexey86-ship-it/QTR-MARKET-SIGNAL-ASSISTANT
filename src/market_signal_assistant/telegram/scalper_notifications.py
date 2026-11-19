"""Bounded snapshot-only observer with durable at-most-once send claims.

A Telegram send and a local file replace cannot be one transaction. Persist the
claim before attempting delivery: uncertain sends are never automatically replayed.
This prevents duplicate storms at the cost of loss in a crash/send-failure window.
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import re
import stat
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

from market_signal_assistant.qtr_micro_scalper.control_store import (
    ControlRead,
    ControlSnapshotStore,
    SnapshotState,
)
from market_signal_assistant.telegram.scalper_formatting import format_recent_event

MAX_NOTIFY_CHATS = 10
MAX_CURSOR_BYTES = 16 * 1024
MAX_BATCH = 21 * MAX_NOTIFY_CHATS
_LOGGER = logging.getLogger(__name__)
HEALTH_STATES = frozenset(
    {
        "HEALTHY",
        "DEGRADED",
        "STOPPED",
        "STOPPING",
        "STARTING",
        "DISABLED",
        "BLOCKED",
        "MISSING",
        "MALFORMED",
        "UNSUPPORTED",
        "STALE",
        "FUTURE",
        "LIVE_FLAG",
        "NOT_SHADOW",
        "LIVE_UNKNOWN",
    }
)


@dataclass(frozen=True, slots=True)
class ScalperNotifyConfig:
    chat_ids: tuple[int, ...] = ()
    interval_seconds: float = 10.0
    cursor_path: Path | None = None

    def __post_init__(self) -> None:
        if not 5 <= self.interval_seconds <= 60:
            raise ValueError("Notification interval must be 5..60 seconds")
        if len(self.chat_ids) > MAX_NOTIFY_CHATS or len(set(self.chat_ids)) != len(
            self.chat_ids
        ):
            raise ValueError("Invalid notification recipients")
        if any(
            type(chat) is not int or not 0 < abs(chat) < 2**63 for chat in self.chat_ids
        ):
            raise ValueError("Invalid notification chat")

    @classmethod
    def from_environment(cls) -> ScalperNotifyConfig:
        raw = os.getenv("QTR_SCALPER_V2_NOTIFY_CHAT_IDS", "").strip()
        chats = (
            tuple(sorted(set(int(item.strip()) for item in raw.split(","))))
            if raw
            else ()
        )
        configured = os.getenv("QTR_SCALPER_V2_NOTIFY_CURSOR_PATH", "").strip()
        path = (
            Path(configured)
            if configured
            else Path(__file__).resolve().parents[3]
            / "data"
            / "qtr_micro_scalper_notify_cursor.json"
        )
        return cls(
            chats,
            float(os.getenv("QTR_SCALPER_V2_NOTIFY_INTERVAL_SECONDS", "10")),
            path,
        )


@dataclass(frozen=True, slots=True)
class ChatCursor:
    chat_id: int
    events_seeded: bool
    watermark: str
    health: str


class ScalperCursorStore:
    def __init__(self, path: Path, snapshot_path: Path) -> None:
        self.path = path.absolute()
        self.snapshot_path = str(snapshot_path.resolve())

    def _validate(self) -> None:
        ControlSnapshotStore(self.path)._validate_path()
        if self.path.resolve() == Path(self.snapshot_path):
            raise ValueError("Cursor must not replace control snapshot")

    @contextmanager
    def exclusive(self) -> Iterator[bool]:
        self._validate()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Zero-byte fixed lock file, no growing lock history. Nonblocking so a
        # duplicate observer cannot hang the bot or race the durable claim.
        with self.path.with_suffix(self.path.suffix + ".lock").open("a+b") as stream:
            try:
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                yield False
                return
            try:
                yield True
            finally:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)

    def load(self) -> dict[int, ChatCursor] | None:
        try:
            self._validate()
            if not stat.S_ISREG(self.path.stat().st_mode):
                return None
            with self.path.open("rb") as stream:
                raw = stream.read(MAX_CURSOR_BYTES + 1)
            if len(raw) > MAX_CURSOR_BYTES:
                return None
            data = json.loads(raw)
            if (
                not isinstance(data, dict)
                or set(data) != {"schema_version", "snapshot_path", "chats"}
                or type(data["schema_version"]) is not int
                or data["schema_version"] != 1
                or data["snapshot_path"] != self.snapshot_path
                or not isinstance(data["chats"], list)
                or len(data["chats"]) > MAX_NOTIFY_CHATS
            ):
                return None
            result = {}
            for item in data["chats"]:
                if not isinstance(item, dict) or set(item) != {
                    "chat_id",
                    "events_seeded",
                    "watermark",
                    "health",
                }:
                    return None
                chat = item["chat_id"]
                key = item["watermark"]
                if (
                    type(chat) is not int
                    or not 0 < abs(chat) < 2**63
                    or chat in result
                    or type(item["events_seeded"]) is not bool
                    or item["health"] not in HEALTH_STATES
                    or not isinstance(key, str)
                    or len(key) > 160
                ):
                    return None
                if key:
                    pieces = key.split("|")
                    if (
                        len(pieces) != 3
                        or pieces[1] not in {"0", "1", "2"}
                        or not re.fullmatch(r"operator-[0-9a-f]{64}", pieces[2])
                    ):
                        return None
                    stamp = datetime.fromisoformat(pieces[0])
                    if stamp.tzinfo is None or stamp.utcoffset() is None:
                        return None
                result[chat] = ChatCursor(
                    chat, item["events_seeded"], key, item["health"]
                )
            return result
        except (
            OSError,
            ValueError,
            TypeError,
            KeyError,
            RecursionError,
            OverflowError,
        ):
            return None

    def save(self, cursors: dict[int, ChatCursor]) -> None:
        self._validate()
        if len(cursors) > MAX_NOTIFY_CHATS:
            raise ValueError("Too many cursors")
        data = json.dumps(
            {
                "schema_version": 1,
                "snapshot_path": self.snapshot_path,
                "chats": [asdict(cursors[key]) for key in sorted(cursors)],
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        if len(data) > MAX_CURSOR_BYTES:
            raise ValueError("Cursor too large")
        # Fixed reserved temp name under the cursor flock: a SIGKILL cannot
        # accumulate a new UUID-named orphan on every bot restart.
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        for key, value in os.environ.items():
            if (
                key.startswith("QTR_")
                and (key.endswith("JOURNAL_PATH") or key.endswith("AUDIT_PATH"))
                and Path(value).absolute() == temporary
            ):
                raise ValueError("Cursor temp aliases evidence")
        try:
            # Remove only this reserved stale directory entry, never follow an
            # old symlink/hardlink to write through it. O_EXCL closes the race.
            temporary.unlink(missing_ok=True)
            descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
        finally:
            temporary.unlink(missing_ok=True)


def _health(read: ControlRead) -> str:
    if read.state is not SnapshotState.FRESH or read.snapshot is None:
        return str(read.state)
    runtime = read.snapshot.runtime
    if runtime.live_enabled is True:
        return "LIVE_FLAG"
    if runtime.live_enabled is None:
        return "LIVE_UNKNOWN"
    if not runtime.shadow_mode:
        return "NOT_SHADOW"
    if runtime.status == "RUNNING":
        return "HEALTHY" if runtime.ready else "DEGRADED"
    return runtime.status


def _health_message(state: str, read: ControlRead) -> str:
    if state == "HEALTHY":
        return "✅ Scalper восстановлен · снимок свежий · runtime готов"
    labels = {
        "STALE": "снимок устарел",
        "MISSING": "снимок отсутствует",
        "MALFORMED": "снимок повреждён",
        "UNSUPPORTED": "схема не поддерживается",
        "FUTURE": "время снимка в будущем",
        "LIVE_FLAG": "обнаружен LIVE-флаг; trade-уведомления заблокированы",
        "LIVE_UNKNOWN": "LIVE-флаг неизвестен",
        "NOT_SHADOW": "Shadow-режим не подтверждён",
    }
    detail = labels.get(state, state)
    suffix = ""
    if state == "STALE":
        suffix = f" · возраст {int(read.age_seconds or 0)} с"
    elif state == "DEGRADED" and read.snapshot is not None:
        suffix = f" · ошибок pipeline {read.snapshot.runtime.pipeline_errors}"
    return f"⚠️ Scalper {detail}{suffix}"


class ScalperNotifier:
    def __init__(
        self,
        snapshots: ControlSnapshotStore,
        cursors: ScalperCursorStore,
        config: ScalperNotifyConfig,
        send: Callable[[int, str], Awaitable[None]],
    ) -> None:
        self.snapshots, self.cursors, self.config, self.send = (
            snapshots,
            cursors,
            config,
            send,
        )
        self._poll_lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self.send_failures = 0
        self.state_failures = 0
        self.polls = 0

    async def poll_once(self, *, now: datetime | None = None) -> int:
        if not self.config.chat_ids:
            return 0
        async with self._poll_lock:
            self.polls += 1
            messages: list[tuple[int, str]] = []
            try:
                with self.cursors.exclusive() as owned:
                    if not owned:
                        return 0
                    read = self.snapshots.read(now=now)
                    old = self.cursors.load() or {}
                    updated = {}
                    health = _health(read)
                    eligible = (
                        read.state is SnapshotState.FRESH
                        and read.snapshot is not None
                        and read.snapshot.schema_version == 2
                        and read.snapshot.runtime.shadow_mode
                        and read.snapshot.runtime.live_enabled is False
                    )
                    events = (
                        read.snapshot.recent_events
                        if eligible and read.snapshot is not None
                        else ()
                    )
                    for chat in self.config.chat_ids:
                        previous = old.get(chat)
                        seeded = previous.events_seeded if previous else False
                        watermark = previous.watermark if previous else ""
                        if previous is not None and previous.health != health:
                            messages.append((chat, _health_message(health, read)))
                        if eligible:
                            if seeded:
                                for event in events:
                                    if (
                                        event.order_key > watermark
                                        and event.event_type
                                        in {
                                            "OPEN",
                                            "CLOSED",
                                            "EXPIRED",
                                            "NOT_TRIGGERED",
                                        }
                                    ):
                                        messages.append(
                                            (chat, format_recent_event(event))
                                        )
                            watermark = max(
                                watermark, events[-1].order_key if events else ""
                            )
                            seeded = True
                        updated[chat] = ChatCursor(chat, seeded, watermark, health)
                    if len(messages) > MAX_BATCH:
                        raise ValueError("Notification batch exceeded bounds")
                    # Persist all claims before the first send. Never send if the
                    # durable cursor cannot be published. Per-chat progress is
                    # independent and survives changed recipient configuration.
                    if old != updated:
                        self.cursors.save(updated)
            except (OSError, ValueError, TypeError, RuntimeError):
                self.state_failures += 1
                if self.state_failures == 1:
                    _LOGGER.warning(
                        "Scalper cursor unavailable; notifications suppressed"
                    )
                return 0
            sent = 0
            for chat, text in messages:
                try:
                    await asyncio.wait_for(self.send(chat, text), timeout=10)
                    sent += 1
                except (OSError, RuntimeError, ValueError, TimeoutError):
                    self.send_failures += 1
                    if self.send_failures == 1:
                        _LOGGER.warning(
                            "Scalper notification delivery uncertain; claim retained"
                        )
                except Exception:
                    # SDK errors are optional dependencies; do not import a
                    # concrete transport merely to catch its exception type.
                    self.send_failures += 1
                    if self.send_failures == 1:
                        _LOGGER.warning(
                            "Scalper notification delivery uncertain; claim retained"
                        )
            return sent

    def start(self) -> None:
        if self.config.chat_ids and (self._task is None or self._task.done()):
            self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        while True:
            try:
                await self.poll_once()
            except Exception:
                self.state_failures += 1
                _LOGGER.warning("Scalper operator poll failed; runtime unaffected")
            await asyncio.sleep(self.config.interval_seconds)

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
