"""Independent, read-only Watchdog event-journal to Telegram projector.

The SQLite cursor and pending outbox are committed together. Delivery is
at-least-once: Telegram has no idempotency key for sendMessage.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import urllib.error
import urllib.request
from collections import Counter
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, cast

from market_signal_assistant.watchdog.events.journal import _event_from_payload
from market_signal_assistant.watchdog.events.models import WatchdogEventEvidence
from market_signal_assistant.watchdog.jsonl_io import (
    JsonlLineTooLong,
    read_bounded_line,
)
from market_signal_assistant.watchdog.runtime.control_snapshot import (
    ControlSnapshotError,
    load_control_snapshot,
)
from market_signal_assistant.watchdog.telegram_alert_policy import (
    DIGEST_SECONDS,
    WINDOW_SECONDS,
    decide_alert,
)
from market_signal_assistant.watchdog.telegram_presentation import (
    alert_keyboard,
    compact_alert,
    detail_text,
    detail_token,
    digest_keyboard,
    display_timezone,
)

_LABELS = {
    "COMPRESSION": "Сжатие диапазона",
    "RANGE_BUILDUP": "Формирование диапазона",
    "VOLUME_SHOCK": "Всплеск объёма",
    "VOLUME_ACCELERATION": "Ускорение объёма",
    "PRICE_ACCELERATION": "Ускорение цены",
    "VOLATILITY_EXPANSION": "Рост волатильности",
    "OI_SHOCK": "Аномалия открытого интереса",
    "FUNDING_ANOMALY": "Аномалия funding",
}


class JournalChangedError(RuntimeError):
    """The immutable source changed identity, shrank, or contains bad evidence."""


class DeliveryError(Exception):
    def __init__(self, kind: str, *, retry_after: float | None = None) -> None:
        super().__init__(kind)
        self.kind = kind
        self.retry_after = retry_after


class TelegramSender(Protocol):
    def send(
        self,
        token: str,
        chat_id: str,
        text: str,
        keyboard: list[list[dict[str, str]]] | None = None,
    ) -> None: ...


class TelegramHttpSender:
    """Only sendMessage; API errors never include URL, token, or response text."""

    def send(
        self,
        token: str,
        chat_id: str,
        text: str,
        keyboard: list[list[dict[str, str]]] | None = None,
    ) -> None:
        payload: dict[str, object] = {"chat_id": chat_id, "text": text}
        if keyboard:
            payload["reply_markup"] = {"inline_keyboard": keyboard}
        body = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                try:
                    payload = json.loads(response.read(65536))
                except ValueError:
                    raise DeliveryError("API_RESPONSE", retry_after=30) from None
                if not isinstance(payload, dict) or payload.get("ok") is not True:
                    code = (
                        payload.get("error_code") if isinstance(payload, dict) else None
                    )
                    if code == 429:
                        raise DeliveryError(
                            "RATE_LIMIT", retry_after=_retry_after(payload)
                        )
                    if isinstance(code, int) and 400 <= code < 500:
                        raise DeliveryError("CLIENT_ERROR")
                    raise DeliveryError("API_RESPONSE", retry_after=30)
        except urllib.error.HTTPError as error:
            if error.code == 429:
                try:
                    parsed: object = json.loads(error.read(65536))
                except (ValueError, OSError):
                    parsed = None
                raise DeliveryError(
                    "RATE_LIMIT", retry_after=_retry_after(parsed)
                ) from None
            if error.code >= 500:
                raise DeliveryError("SERVER_ERROR", retry_after=30) from None
            raise DeliveryError("CLIENT_ERROR") from None
        except (urllib.error.URLError, TimeoutError, OSError):
            raise DeliveryError("NETWORK_ERROR", retry_after=15) from None


def _retry_after(payload: object) -> float:
    if isinstance(payload, dict):
        parameters = payload.get("parameters")
        if isinstance(parameters, dict):
            value = parameters.get("retry_after")
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return min(300.0, max(1.0, float(value)))
    return 30.0


def format_event(event: WatchdogEventEvidence) -> str:
    """Plain text avoids Markdown escaping and keeps untrusted data inert."""
    detected = event.detected_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    types = ", ".join(_LABELS[item.value] for item in event.anomaly_types)
    lines = [
        "🚨 QTR Watchdog — рыночная аномалия",
        f"Символ: {event.symbol}",
        f"Тип: {types}",
        f"Время: {detected}",
        f"Score: {event.anomaly_score:.1f}/100",
        f"Состояние: {event.state_before.value} → {event.state_after.value}",
    ]
    if event.features:
        lines.append("📊 Доступные метрики:")
        for feature in event.features[:12]:
            unit = f" {feature.unit}" if feature.unit else ""
            lines.append(f"• {feature.name}: {feature.value:.6g}{unit}")
        if len(event.features) > 12:
            lines.append(f"• … ещё {len(event.features) - 12}")
    if event.missing_data or event.stale_data:
        lines.append("⚠️ Часть данных отсутствует или устарела")
    return "\n".join(lines)[:3900] + f"\nID: {event.event_id}"


class WatchdogTelegramAdapter:
    def __init__(
        self,
        journal: Path,
        state_db: Path,
        *,
        sender: TelegramSender,
        token: str,
        chat_id: str,
        minimum_spacing_seconds: float = 1.0,
        clock: Callable[[], float] = time.time,
        panel_enabled: bool = False,
        alert_policy_enabled: bool = False,
        display_timezone_name: str = "Europe/Moscow",
        panel_root: Path | None = None,
    ) -> None:
        if not token or not chat_id or minimum_spacing_seconds <= 0:
            raise ValueError("Telegram credentials and positive spacing are required")
        if not journal.is_file():
            raise JournalChangedError("event journal missing at bootstrap")
        self.journal = journal
        self.sender = sender
        self.token = token
        self.chat_id = chat_id
        self.spacing = minimum_spacing_seconds
        self.clock = clock
        self.panel_enabled = panel_enabled
        self.alert_policy_enabled = alert_policy_enabled
        display_timezone(display_timezone_name)
        if panel_root is not None and journal.resolve() != (
            panel_root / "events/events.jsonl"
        ).resolve():
            raise ValueError("Control snapshot root does not match event journal")
        self.display_timezone_name = display_timezone_name
        self.panel_root = panel_root
        state_db.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(state_db, isolation_level=None, timeout=10)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS cursor (id INTEGER PRIMARY KEY CHECK(id=1), "
            "device INTEGER, inode INTEGER, offset INTEGER NOT NULL, "
            "skip_partial INTEGER NOT NULL DEFAULT 0, destination_hash TEXT NOT NULL, "
            "next_send REAL NOT NULL DEFAULT 0)"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS outbox (event_id TEXT PRIMARY KEY, "
            "payload_hash TEXT NOT NULL, text TEXT, status TEXT NOT NULL, "
            "attempts INTEGER NOT NULL DEFAULT 0, "
            "next_attempt REAL NOT NULL DEFAULT 0, "
            "last_error TEXT, enqueued_at REAL NOT NULL DEFAULT 0)"
        )
        if alert_policy_enabled:
            outbox_columns = {
                row[1] for row in self.db.execute("PRAGMA table_info(outbox)")
            }
            if "priority" not in outbox_columns:
                self.db.execute(
                    "ALTER TABLE outbox ADD COLUMN priority INTEGER NOT NULL DEFAULT 0"
                )
            if "expires_at" not in outbox_columns:
                self.db.execute("ALTER TABLE outbox ADD COLUMN expires_at REAL")
            if "keyboard" not in outbox_columns:
                self.db.execute("ALTER TABLE outbox ADD COLUMN keyboard TEXT")
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS alert_policy_last ("
                "symbol TEXT PRIMARY KEY, detected_ts REAL NOT NULL, "
                "score REAL NOT NULL)"
            )
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS alert_policy_sent ("
                "event_id TEXT PRIMARY KEY, detected_ts REAL NOT NULL)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS alert_policy_sent_time "
                "ON alert_policy_sent(detected_ts)"
            )
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS alert_policy_digest ("
                "period INTEGER PRIMARY KEY, suppressed INTEGER NOT NULL, "
                "top_symbol TEXT NOT NULL, top_score REAL NOT NULL, "
                "emitted INTEGER NOT NULL DEFAULT 0)"
            )
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS alert_policy_stats ("
                "reason TEXT PRIMARY KEY, count INTEGER NOT NULL)"
            )
        if panel_enabled:
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS panel_events ("
                "event_id TEXT PRIMARY KEY, detected_ts REAL NOT NULL, "
                "symbol TEXT NOT NULL, score REAL NOT NULL, "
                "anomaly_types TEXT NOT NULL, details TEXT, detail_token TEXT)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS panel_events_time "
                "ON panel_events(detected_ts)"
            )
            panel_columns = {
                row[1] for row in self.db.execute("PRAGMA table_info(panel_events)")
            }
            if "details" not in panel_columns:
                self.db.execute("ALTER TABLE panel_events ADD COLUMN details TEXT")
            if "detail_token" not in panel_columns:
                self.db.execute("ALTER TABLE panel_events ADD COLUMN detail_token TEXT")
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS panel_events_detail "
                "ON panel_events(detail_token)"
            )
        # Existing pilot state remains readable. An old pending row has unknown
        # age (zero) and must be reviewed, never silently treated as fresh.
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(outbox)")}
        if "enqueued_at" not in columns:
            self.db.execute(
                "ALTER TABLE outbox ADD COLUMN enqueued_at REAL NOT NULL DEFAULT 0"
            )
        destination_hash = hashlib.sha256(chat_id.encode("utf-8")).hexdigest()
        existing_cursor = self.db.execute(
            "SELECT destination_hash FROM cursor WHERE id=1"
        ).fetchone()
        if existing_cursor is None:
            # First installation begins at the current EOF: historical events
            # are not replayed. A partially written historical line is skipped.
            stat = journal.stat()
            skip_partial = 0
            if stat.st_size:
                with journal.open("rb") as stream:
                    stream.seek(-1, 2)
                    skip_partial = int(stream.read(1) != b"\n")
            self.db.execute(
                "INSERT INTO cursor(id,device,inode,offset,skip_partial,"
                "destination_hash) VALUES(1,?,?,?,?,?)",
                (
                    stat.st_dev,
                    stat.st_ino,
                    stat.st_size,
                    skip_partial,
                    destination_hash,
                ),
            )
        elif existing_cursor[0] != destination_hash:
            self.db.close()
            raise ValueError("Telegram destination changed for existing outbox")

    def close(self) -> None:
        self.db.close()

    def collect(self, *, limit: int = 100) -> int:
        row = self.db.execute(
            "SELECT device,inode,offset,skip_partial FROM cursor WHERE id=1"
        ).fetchone()
        assert row is not None
        device, inode, offset, skip_partial = row
        if not self.journal.exists():
            if device is not None:
                raise JournalChangedError("event journal disappeared")
            return 0
        stat = self.journal.stat()
        if device is not None and (stat.st_dev, stat.st_ino) != (device, inode):
            raise JournalChangedError("event journal rotated")
        if stat.st_size < offset:
            raise JournalChangedError("event journal truncated")
        count = 0
        with self.journal.open("rb") as stream:
            stream.seek(offset)
            while count < limit:
                try:
                    line = read_bounded_line(stream)
                except JsonlLineTooLong as error:
                    raise JournalChangedError("event line exceeds limit") from error
                if not line:
                    break
                if not line.endswith(b"\n"):
                    break  # writer is still completing this line
                if skip_partial:
                    new_offset = stream.tell()
                    self.db.execute("BEGIN IMMEDIATE")
                    try:
                        updated = self.db.execute(
                            "UPDATE cursor SET device=?,inode=?,offset=?,"
                            "skip_partial=0 WHERE id=1 AND offset=?",
                            (stat.st_dev, stat.st_ino, new_offset, offset),
                        )
                        if updated.rowcount != 1:
                            raise JournalChangedError(
                                "cursor advanced by another reader"
                            )
                        self.db.execute("COMMIT")
                    except Exception:
                        self.db.execute("ROLLBACK")
                        raise
                    offset = new_offset
                    skip_partial = 0
                    continue
                try:
                    raw: object = json.loads(line)
                    if not isinstance(raw, dict):
                        raise ValueError("event must be an object")
                    payload = cast(Mapping[str, object], raw)
                    event = _event_from_payload(payload)
                except (UnicodeDecodeError, ValueError, TypeError, KeyError) as error:
                    raise JournalChangedError("invalid event JSONL line") from error
                digest = hashlib.sha256(line.rstrip(b"\n")).hexdigest()
                new_offset = stream.tell()
                self.db.execute("BEGIN IMMEDIATE")
                try:
                    existing = self.db.execute(
                        "SELECT payload_hash FROM outbox WHERE event_id=?",
                        (event.event_id,),
                    ).fetchone()
                    if existing is not None and existing[0] != digest:
                        raise JournalChangedError("conflicting event_id")
                    selected_for_panel = False
                    if existing is None:
                        should_send = True
                        if self.alert_policy_enabled:
                            now = self.clock()
                            previous = self.db.execute(
                                "SELECT detected_ts,score FROM alert_policy_last "
                                "WHERE symbol=?",
                                (event.symbol,),
                            ).fetchone()
                            sent_last_day = self.db.execute(
                                "SELECT count(*) FROM alert_policy_sent "
                                "WHERE detected_ts>?",
                                (event.detected_at.timestamp() - WINDOW_SECONDS,),
                            ).fetchone()[0]
                            decision = decide_alert(
                                event,
                                now=now,
                                previous=previous,
                                sent_last_day=sent_last_day,
                            )
                            should_send = decision.send
                            selected_for_panel = should_send
                            self.db.execute(
                                "INSERT INTO alert_policy_stats(reason,count) "
                                "VALUES(?,1) ON CONFLICT(reason) DO UPDATE "
                                "SET count=count+1",
                                (decision.reason,),
                            )
                            if should_send:
                                self.db.execute(
                                    "INSERT INTO alert_policy_sent VALUES(?,?)",
                                    (event.event_id, event.detected_at.timestamp()),
                                )
                                self.db.execute(
                                    "INSERT INTO alert_policy_last VALUES(?,?,?) "
                                    "ON CONFLICT(symbol) DO UPDATE SET "
                                    "detected_ts=excluded.detected_ts,score=excluded.score",
                                    (
                                        event.symbol,
                                        event.detected_at.timestamp(),
                                        event.anomaly_score,
                                    ),
                                )
                            elif decision.reason != "EXPIRED_OR_FUTURE":
                                period = (
                                    int(event.detected_at.timestamp()) // DIGEST_SECONDS
                                )
                                self.db.execute(
                                    "INSERT INTO alert_policy_digest("
                                    "period,suppressed,top_symbol,top_score) "
                                    "VALUES(?,1,?,?) ON CONFLICT(period) DO UPDATE SET "
                                    "suppressed=suppressed+1,"
                                    "top_symbol=CASE WHEN excluded.top_score>top_score "
                                    "THEN excluded.top_symbol ELSE top_symbol END,"
                                    "top_score=max(top_score,excluded.top_score)",
                                    (period, event.symbol, event.anomaly_score),
                                )
                        if should_send:
                            if self.alert_policy_enabled:
                                self.db.execute(
                                    "INSERT INTO outbox("
                                    "event_id,payload_hash,text,status,"
                                    "enqueued_at,priority,expires_at,keyboard) "
                                    "VALUES(?,?,?,'PENDING',?,?,?,?)",
                                    (
                                        event.event_id,
                                        digest,
                                        compact_alert(
                                            event,
                                            reason=decision.reason,
                                            now=now,
                                            timezone_name=self.display_timezone_name,
                                        ),
                                        self.clock(),
                                        2 if event.anomaly_score >= 60 else 1,
                                        event.detected_at.timestamp() + 600,
                                        json.dumps(alert_keyboard(event)),
                                    ),
                                )
                            else:
                                self.db.execute(
                                    "INSERT INTO outbox("
                                    "event_id,payload_hash,text,status,enqueued_at) "
                                    "VALUES(?,?,?,'PENDING',?)",
                                    (
                                        event.event_id,
                                        digest,
                                        format_event(event),
                                        self.clock(),
                                    ),
                                )
                        else:
                            # Keep the event ID/hash durable so a later duplicate
                            # or conflicting journal record cannot evade dedup.
                            self.db.execute(
                                "INSERT INTO outbox("
                                "event_id,payload_hash,text,status,enqueued_at) "
                                "VALUES(?, ?, NULL, 'SUPPRESSED', ?)",
                                (event.event_id, digest, self.clock()),
                            )
                    if self.panel_enabled:
                        self.db.execute(
                            "INSERT OR IGNORE INTO panel_events("
                            "event_id,detected_ts,symbol,score,anomaly_types,"
                            "details,detail_token) VALUES(?,?,?,?,?,?,?)",
                            (
                                event.event_id,
                                event.detected_at.timestamp(),
                                event.symbol,
                                event.anomaly_score,
                                json.dumps(
                                    [item.value for item in event.anomaly_types]
                                ),
                                detail_text(event) if selected_for_panel else None,
                                detail_token(event.event_id)
                                if selected_for_panel
                                else None,
                            ),
                        )
                    updated = self.db.execute(
                        "UPDATE cursor SET device=?,inode=?,offset=? "
                        "WHERE id=1 AND offset=?",
                        (stat.st_dev, stat.st_ino, new_offset, offset),
                    )
                    if updated.rowcount != 1:
                        raise JournalChangedError("cursor advanced by another reader")
                    self.db.execute("COMMIT")
                except Exception:
                    self.db.execute("ROLLBACK")
                    raise
                offset = new_offset
                count += 1
        return count

    def deliver_one(self) -> bool:
        now = self.clock()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            next_send = self.db.execute(
                "SELECT next_send FROM cursor WHERE id=1"
            ).fetchone()[0]
            if now < next_send:
                self.db.execute("COMMIT")
                return False
            ordering = "priority DESC,rowid" if self.alert_policy_enabled else "rowid"
            expiry_column = "expires_at" if self.alert_policy_enabled else "NULL"
            keyboard_column = "keyboard" if self.alert_policy_enabled else "NULL"
            row = self.db.execute(
                f"SELECT event_id,text,attempts,{expiry_column},"
                f"{keyboard_column} FROM outbox WHERE "
                "(status='PENDING' OR (status='INFLIGHT' AND next_attempt<=?)) "
                f"AND next_attempt<=? ORDER BY {ordering} LIMIT 1",
                (now, now),
            ).fetchone()
            if row is None:
                self.db.execute("COMMIT")
                return False
            event_id, message, attempts, expires_at, keyboard_json = row
            if expires_at is not None and now > float(expires_at):
                self.db.execute(
                    "UPDATE outbox SET status='SUPPRESSED',text=NULL,"
                    "last_error='EXPIRED_BEFORE_SEND' WHERE event_id=?",
                    (event_id,),
                )
                self.db.execute("COMMIT")
                return False
            self.db.execute(
                "UPDATE outbox SET status='INFLIGHT',attempts=?,next_attempt=? "
                "WHERE event_id=?",
                (attempts + 1, now + 60, event_id),
            )
            self.db.execute(
                "UPDATE cursor SET next_send=? WHERE id=1", (now + self.spacing,)
            )
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise
        try:
            if keyboard_json is not None:
                self.sender.send(
                    self.token,
                    self.chat_id,
                    str(message),
                    json.loads(keyboard_json),
                )
            else:
                self.sender.send(self.token, self.chat_id, str(message))
        except DeliveryError as error:
            if error.kind == "CLIENT_ERROR":
                # Keep the pending evidence; operator must repair config.
                self.db.execute(
                    "UPDATE outbox SET status='BLOCKED',last_error=? WHERE event_id=?",
                    (error.kind, event_id),
                )
                raise
            delay = error.retry_after if error.retry_after is not None else 30.0
            delay = min(300.0, max(1.0, delay * min(8, 2 ** min(attempts, 3))))
            self.db.execute(
                "UPDATE outbox SET status='PENDING',next_attempt=?,last_error=? "
                "WHERE event_id=?",
                (now + delay, error.kind, event_id),
            )
            self.db.execute(
                "UPDATE cursor SET next_send=max(next_send,?) WHERE id=1",
                (now + delay,),
            )
            return False
        self.db.execute(
            "UPDATE outbox SET status='SENT',text=NULL,last_error=NULL "
            "WHERE event_id=?",
            (event_id,),
        )
        return True

    def run_once(self) -> tuple[int, bool]:
        collected = self.collect()
        if self.alert_policy_enabled:
            self._emit_digest()
        return collected, self.deliver_one()

    def _emit_digest(self) -> None:
        now = self.clock()
        self.db.execute("BEGIN IMMEDIATE")
        try:
            rows = self.db.execute(
                "SELECT period,suppressed,top_symbol,top_score "
                "FROM alert_policy_digest WHERE emitted=0 AND ? >= "
                "(period+1)*? ORDER BY period",
                (now, DIGEST_SECONDS),
            ).fetchall()
            for period, count, symbol, score in rows:
                end = (period + 1) * DIGEST_SECONDS
                if now - end <= 3600:
                    message = self._digest_text(period, count, symbol, score, now)
                    digest_id = f"watchdog:digest:{period}"
                    self.db.execute(
                        "INSERT OR IGNORE INTO outbox("
                        "event_id,payload_hash,text,status,enqueued_at,"
                        "expires_at,keyboard) VALUES(?,?,?,'PENDING',?,?,?)",
                        (
                            digest_id,
                            hashlib.sha256(message.encode()).hexdigest(),
                            message,
                            now,
                            now + 600,
                            json.dumps(digest_keyboard()),
                        ),
                    )
                self.db.execute(
                    "UPDATE alert_policy_digest SET emitted=1 WHERE period=?",
                    (period,),
                )
            self.db.execute(
                "DELETE FROM alert_policy_digest WHERE emitted=1 AND period<?",
                (int((now - 48 * 3600) // DIGEST_SECONDS),),
            )
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    def _digest_text(
        self,
        period: int,
        suppressed: int,
        top_symbol: str,
        top_score: float,
        now: float,
    ) -> str:
        start = period * DIGEST_SECONDS
        end = (period + 1) * DIGEST_SECONDS
        total = suppressed
        type_counts: Counter[str] = Counter()
        historical: list[str] = []
        if self.panel_enabled:
            total = int(
                self.db.execute(
                    "SELECT count(*) FROM panel_events WHERE detected_ts>=? "
                    "AND detected_ts<?",
                    (start, end),
                ).fetchone()[0]
            )
            for (types_json,) in self.db.execute(
                "SELECT anomaly_types FROM panel_events WHERE detected_ts>=? "
                "AND detected_ts<?",
                (start, end),
            ):
                type_counts.update(json.loads(types_json))
            historical = [
                str(row[0])
                for row in self.db.execute(
                    "SELECT symbol FROM panel_events WHERE detected_ts>=? "
                    "AND detected_ts<? GROUP BY symbol "
                    "ORDER BY max(score) DESC LIMIT 5",
                    (start, end),
                )
            ]
        current: list[str] | None = None
        if self.panel_root is not None:
            try:
                snapshot = load_control_snapshot(
                    self.panel_root / "state/control-panel.json"
                )
                published = (
                    datetime.fromisoformat(snapshot["published_at"]).timestamp()
                    if snapshot is not None
                    else None
                )
                if published is not None and 0 <= now - published <= 120:
                    current_rows: list[dict[str, Any]] = []
                    assert snapshot is not None
                    for row in snapshot.get("symbols", []):
                        interval = str(row.get("interval"))
                        maximum = {"1m": 180, "5m": 420, "15m": 1020}.get(interval)
                        detected = datetime.fromisoformat(
                            str(row.get("detected_at"))
                        ).timestamp()
                        if (
                            maximum is not None
                            and 0 <= now - detected <= maximum
                            and row.get("state")
                            in {"WATCH", "IN_PLAY", "HIGH_ATTENTION"}
                            and row.get("anomaly_types")
                        ):
                            current_rows.append(row)
                    current = [
                        str(row.get("symbol"))
                        for row in sorted(
                            current_rows,
                            key=lambda row: -float(row.get("score", 0)),
                        )[:5]
                    ]
            except (ControlSnapshotError, KeyError, TypeError, ValueError):
                current = None
        current_text = (
            "Нет свежих данных"
            if current is None
            else ", ".join(current)
            if current
            else "активных аномалий нет"
        )
        types_text = (
            ", ".join(
                f"{_LABELS.get(kind, kind)}: {amount}"
                for kind, amount in type_counts.most_common(3)
            )
            or "нет данных"
        )
        historical_text = ", ".join(historical) or f"{top_symbol} ({top_score:.1f})"
        return (
            "🐺 Watchdog — сводка за 6 часов\n"
            f"Обнаружено событий: {total}; без отдельного уведомления: {suppressed}\n"
            f"Актуальные инструменты: {current_text}\n"
            f"Типы событий: {types_text}\n"
            f"Исторические лидеры окна: {historical_text}\n"
            "Текущее состояние основано только на свежем снимке."
        )[:3800]
