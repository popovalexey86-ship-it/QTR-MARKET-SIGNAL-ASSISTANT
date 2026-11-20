"""Read-only Watchdog panel sharing the existing Telegram adapter process."""

from __future__ import annotations

import json
import math
import sqlite3
import time
import urllib.error
import urllib.request
from collections import Counter
from collections.abc import Callable, Mapping
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from market_signal_assistant.watchdog.runtime.control_snapshot import (
    ControlSnapshotError,
    load_control_snapshot,
)
from market_signal_assistant.watchdog.telegram_adapter import (
    DeliveryError,
    _retry_after,
)

MAX_REPLY = 3800
MAX_UPDATES = 20
MAX_EVENTS = 100_000
MAX_PENDING_REPLIES = 1_000
EVENT_WINDOW = 86_400.0
COMMAND_COOLDOWN = 2.0
SCAN_COOLDOWN = 30.0
FRESH_SNAPSHOT_SECONDS = 120.0

MENU = (
    (("📡 Статус", "cmd:status"), ("🌍 Рынок", "cmd:market")),
    (("🔥 IN-PLAY", "cmd:inplay"), ("🏆 ТОП-10", "cmd:top")),
    (("🔎 Скан", "cmd:scan"), ("🚨 События", "cmd:events")),
    (("📊 Статистика", "cmd:stats"), ("⚙️ Настройки", "cmd:settings")),
    (("❓ Справка", "cmd:help"),),
)

_STATUS_LABELS = {
    "HEALTHY": "работает",
    "WARM-UP": "прогрев данных",
    "DEGRADED": "работает с ограничениями",
    "STALE": "данные устарели",
    "OFFLINE": "не работает",
}
_SNAPSHOT_LABELS = {
    "FRESH": "свежий",
    "UNKNOWN": "отсутствует",
    "INVALID": "повреждён",
    "STALE": "устарел",
}
_STATE_LABELS = {
    "NORMAL": "обычное наблюдение",
    "WATCH": "под наблюдением",
    "IN_PLAY": "активный инструмент",
    "HIGH_ATTENTION": "повышенное внимание",
    "COOLDOWN": "пауза после события",
}
_TYPE_LABELS = {
    "COMPRESSION": "сжатие волатильности",
    "RANGE_BUILDUP": "формирование диапазона",
    "VOLUME_SHOCK": "аномальный объём",
    "VOLUME_ACCELERATION": "ускорение объёма",
    "PRICE_ACCELERATION": "ценовой импульс",
    "VOLATILITY_EXPANSION": "расширение волатильности",
    "OI_SHOCK": "аномалия открытого интереса",
    "FUNDING_ANOMALY": "аномальная ставка финансирования",
}


def _types_ru(values: object) -> str:
    if not isinstance(values, list):
        return "нет"
    return (
        ", ".join(_TYPE_LABELS.get(str(value), str(value)) for value in values)
        or "нет"
    )


class ControlTransport(Protocol):
    def get_updates(self, token: str, offset: int) -> list[dict[str, Any]]: ...

    def send(
        self,
        token: str,
        chat_id: str,
        text: str,
        keyboard: tuple[tuple[tuple[str, str], ...], ...],
    ) -> None: ...

    def answer_callback(self, token: str, callback_id: str, text: str) -> None: ...


class TelegramControlHttp:
    """Telegram calls with bounded responses and sanitized failure reasons."""

    @staticmethod
    def _request(token: str, method: str, body: dict[str, object]) -> object:
        request = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/{method}",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=3) as response:
                raw = response.read(65_537)
            if len(raw) > 65_536:
                raise DeliveryError("API_RESPONSE")
            payload = json.loads(raw)
        except urllib.error.HTTPError as error:
            if error.code == 429:
                try:
                    parsed: object = json.loads(error.read(65_536))
                except (OSError, ValueError):
                    parsed = None
                raise DeliveryError(
                    "RATE_LIMIT", retry_after=_retry_after(parsed)
                ) from None
            if error.code >= 500:
                raise DeliveryError("SERVER_ERROR", retry_after=30) from None
            raise DeliveryError("CLIENT_ERROR") from None
        except (urllib.error.URLError, TimeoutError, OSError):
            raise DeliveryError("NETWORK_ERROR", retry_after=15) from None
        except ValueError:
            raise DeliveryError("API_RESPONSE", retry_after=30) from None
        if not isinstance(payload, dict) or payload.get("ok") is not True:
            code = payload.get("error_code") if isinstance(payload, dict) else None
            if code == 429:
                raise DeliveryError("RATE_LIMIT", retry_after=30)
            if isinstance(code, int) and 400 <= code < 500:
                raise DeliveryError("CLIENT_ERROR")
            raise DeliveryError("API_RESPONSE", retry_after=30)
        return payload.get("result")

    def get_updates(self, token: str, offset: int) -> list[dict[str, Any]]:
        result = self._request(
            token,
            "getUpdates",
            {
                "offset": offset,
                "limit": MAX_UPDATES,
                "timeout": 0,
                "allowed_updates": ["message", "callback_query"],
            },
        )
        if not isinstance(result, list) or len(result) > MAX_UPDATES:
            raise DeliveryError("API_RESPONSE")
        if any(
            not isinstance(item, dict) or not isinstance(item.get("update_id"), int)
            for item in result
        ):
            raise DeliveryError("API_RESPONSE")
        return result

    def send(
        self,
        token: str,
        chat_id: str,
        text: str,
        keyboard: tuple[tuple[tuple[str, str], ...], ...],
    ) -> None:
        rows = [
            [{"text": label, "callback_data": data} for label, data in row]
            for row in keyboard
        ]
        self._request(
            token,
            "sendMessage",
            {
                "chat_id": chat_id,
                "text": text[:MAX_REPLY],
                "reply_markup": {"inline_keyboard": rows},
            },
        )

    def answer_callback(self, token: str, callback_id: str, text: str) -> None:
        self._request(
            token,
            "answerCallbackQuery",
            {"callback_query_id": callback_id, "text": text[:100]},
        )


def _read_json(path: Path, limit: int) -> dict[str, Any] | None:
    if not path.exists():
        return None
    if path.stat().st_size > limit:
        raise ControlSnapshotError("Panel input exceeds byte limit")
    try:
        with path.open("rb") as stream:
            raw = stream.read(limit + 1)
        if len(raw) > limit:
            raise ValueError
        value = json.loads(raw)
    except (OSError, ValueError):
        raise ControlSnapshotError("Panel input is invalid") from None
    if not isinstance(value, dict):
        raise ControlSnapshotError("Panel input is invalid")
    return value


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _age(now: datetime, value: object) -> float | None:
    parsed = _timestamp(value)
    if parsed is None:
        return None
    age = (now - parsed).total_seconds()
    return age if age >= -5 else None


def _age_text(age: float | None) -> str:
    if age is None:
        return "неизвестен"
    if age < 60:
        return f"{max(0, int(age))} с"
    return f"{int(age // 60)} мин"


def _row_fresh(row: Mapping[str, Any], now: datetime) -> bool:
    age = _age(now, row.get("detected_at"))
    maximum = {"1m": 180, "5m": 420, "15m": 1020}.get(str(row.get("interval")))
    return age is not None and maximum is not None and age <= maximum


def _active(row: Mapping[str, Any], now: datetime) -> bool:
    return (
        _row_fresh(row, now)
        and row.get("state") in {"WATCH", "IN_PLAY", "HIGH_ATTENTION"}
        and isinstance(row.get("anomaly_types"), list)
        and bool(row["anomaly_types"])
    )


def _metric_lines(row: Mapping[str, Any]) -> list[str]:
    features = row.get("features")
    values = features if isinstance(features, dict) else {}
    lines: list[str] = []
    for key, label, unit in (
        ("price_change_1", "Цена", "%"),
        ("volume_acceleration_3", "Ускорение объёма", "×"),
        ("open_interest_change", "Открытый интерес", "%"),
        ("funding_rate", "Ставка финансирования", "%"),
    ):
        value = values.get(key)
        if (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(value)
        ):
            shown = value * 100 if unit == "%" else value
            period = {"1m": "1 мин", "5m": "5 мин", "15m": "15 мин"}.get(
                str(row.get("interval")), "период не указан"
            )
            interval = f" за {period}" if key == "price_change_1" else ""
            lines.append(f"{label}{interval}: {shown:+.2f}{unit}")
    return lines


class WatchdogControlPanel:
    """One update consumer; SQLite cursor and bounded reply outbox survive restart."""

    def __init__(
        self,
        db: sqlite3.Connection,
        data_root: Path,
        *,
        transport: ControlTransport,
        token: str,
        chat_id: str,
        allowed_user_ids: frozenset[int],
        minimum_spacing_seconds: float = 1.0,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if (
            not token
            or not chat_id
            or not allowed_user_ids
            or minimum_spacing_seconds <= 0
        ):
            raise ValueError("Panel requires token, chat and user allowlist")
        self.db = db
        self.data_root = data_root
        self.transport = transport
        self.token = token
        self.chat_id = chat_id
        self.allowed_user_ids = allowed_user_ids
        self.spacing = minimum_spacing_seconds
        self.clock = clock
        self._next_poll_at = 0.0
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS panel_cursor("
            "id INTEGER PRIMARY KEY CHECK(id=1), offset INTEGER NOT NULL)"
        )
        self.db.execute("INSERT OR IGNORE INTO panel_cursor(id,offset) VALUES(1,-1)")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS panel_outbox("
            "update_id INTEGER PRIMARY KEY, text TEXT NOT NULL, "
            "keyboard TEXT NOT NULL, status TEXT NOT NULL, "
            "attempts INTEGER NOT NULL DEFAULT 0, "
            "next_attempt REAL NOT NULL DEFAULT 0, created_at REAL NOT NULL)"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS panel_rate("
            "user_id INTEGER PRIMARY KEY, last_at REAL NOT NULL, "
            "last_scan_at REAL NOT NULL DEFAULT 0)"
        )

    def _inputs(
        self,
    ) -> tuple[
        dict[str, Any] | None,
        dict[str, Any] | None,
        dict[str, Any] | None,
        bool,
    ]:
        invalid_snapshot = False
        try:
            snapshot = load_control_snapshot(
                self.data_root / "state/control-panel.json"
            )
        except ControlSnapshotError:
            snapshot = None
            invalid_snapshot = True
        try:
            health = _read_json(self.data_root / "state/health.json", 1_048_576)
            supervisor = _read_json(self.data_root / "state/observer.json", 65_536)
        except ControlSnapshotError:
            health = supervisor = None
        return snapshot, health, supervisor, invalid_snapshot

    def _status(
        self,
        health: dict[str, Any] | None,
        supervisor: dict[str, Any] | None,
        now: datetime,
    ) -> str:
        if (
            health is None
            or supervisor is None
            or supervisor.get("status") != "running"
        ):
            return "OFFLINE"
        if health.get("degraded"):
            return "DEGRADED"
        loop_age = _age(now, health.get("last_loop_at"))
        due = _timestamp(health.get("next_market_update_due_at"))
        if (
            loop_age is None
            or loop_age > FRESH_SNAPSHOT_SECONDS
            or (due is not None and now > due and (now - due).total_seconds() > 120)
        ):
            return "STALE"
        if not health.get("baseline_ready_symbols"):
            return "WARM-UP"
        return "HEALTHY"

    def _events(self, now: float) -> list[tuple[float, str, float, list[str]]]:
        rows = self.db.execute(
            "SELECT detected_ts,symbol,score,anomaly_types FROM panel_events "
            "WHERE detected_ts>=? ORDER BY detected_ts DESC LIMIT 10",
            (now - EVENT_WINDOW,),
        )
        return [
            (float(ts), str(symbol), float(score), json.loads(types))
            for ts, symbol, score, types in rows
        ]

    def _counts(self) -> dict[str, int]:
        return dict(
            self.db.execute("SELECT status,count(*) FROM outbox GROUP BY status")
        )

    def _render(
        self, command: str
    ) -> tuple[str, tuple[tuple[tuple[str, str], ...], ...]]:
        now_ts = self.clock()
        now = datetime.fromtimestamp(now_ts, UTC)
        snapshot, health, supervisor, invalid_snapshot = self._inputs()
        state = self._status(health, supervisor, now)
        snapshot_age = _age(now, snapshot.get("published_at")) if snapshot else None
        if invalid_snapshot:
            snapshot_state = "INVALID"
        elif snapshot is None:
            snapshot_state = "UNKNOWN"
        elif snapshot_age is None or snapshot_age > FRESH_SNAPSHOT_SECONDS:
            snapshot_state = "STALE"
        else:
            snapshot_state = "FRESH"
        current_available = snapshot_state == "FRESH" and state not in {
            "STALE",
            "OFFLINE",
        }
        rows = snapshot.get("symbols", []) if snapshot else []
        fresh = [
            row
            for row in rows
            if isinstance(row, dict) and _row_fresh(row, now) and current_available
        ]
        active = [row for row in fresh if _active(row, now)]
        ranked = sorted(
            fresh, key=lambda row: (-float(row.get("score", 0)), str(row.get("symbol")))
        )
        eligible = snapshot.get("eligible_count") if snapshot else None
        ready = health.get("baseline_ready_symbols") if health else None
        market_age = (
            _age(now, health.get("last_successful_market_update")) if health else None
        )
        if command == "/start":
            return (
                "🐺 QTR Watchdog\nВыберите раздел. Данные только для наблюдения.",
                MENU,
            )
        if command == "/help":
            return (
                "❓ Команды: /status /market /inplay /top /symbol SYMBOL "
                "/scan /events /stats /settings.\n"
                "/scan показывает последний снимок; новых API-запросов нет.",
                MENU,
            )
        if command.startswith("/detail "):
            token = command[8:].strip().lower()
            if len(token) != 24 or any(ch not in "0123456789abcdef" for ch in token):
                return "⚠️ Неверный идентификатор подробностей.", MENU
            row = self.db.execute(
                "SELECT details FROM panel_events WHERE detail_token=? "
                "AND detected_ts>=? LIMIT 1",
                (token, now_ts - EVENT_WINDOW),
            ).fetchone()
            if row is None or row[0] is None:
                return "Подробности недоступны; исходный журнал сохранён.", MENU
            return str(row[0])[:MAX_REPLY], MENU
        if command == "/status":
            counts = self._counts()
            uptime = _age(now, health.get("started_at")) if health else None
            observer_status = supervisor.get("status") if supervisor else "нет данных"
            observer_text = (
                "работает" if observer_status == "running" else "не работает"
                if observer_status != "нет данных" else "нет данных"
            )
            processed = (
                health.get("symbols_processed", "нет данных")
                if health
                else "нет данных"
            )
            rss_bytes = health.get("process_rss_bytes") if health else None
            rss = (
                round(rss_bytes / 1048576, 1)
                if isinstance(rss_bytes, int)
                else "нет данных"
            )
            api_errors = (
                health.get("provider_errors", "нет данных") if health else "нет данных"
            )
            limits = (
                health.get("rate_limit_events", "нет данных")
                if health
                else "нет данных"
            )
            events_today = (
                health.get("events_today", "нет данных") if health else "нет данных"
            )
            deliveries = (
                f"{counts.get('SENT', 0)}/{counts.get('PENDING', 0)}/"
                f"{counts.get('BLOCKED', 0)}"
            )
            text = (
                f"📡 Наблюдатель: {_STATUS_LABELS[state]}\n"
                f"Процесс наблюдения: {observer_text}\n"
                f"Снимок рынка: {_SNAPSHOT_LABELS[snapshot_state]}\n"
                "Telegram-бот: работает\n"
                f"Время работы: {_age_text(uptime)}\n"
                f"Обновление рынка: {_age_text(market_age)} назад\n"
                "Доступных инструментов: "
                f"{eligible if eligible is not None else 'нет данных'}; "
                f"с прогретой базой: {ready if ready is not None else 'нет данных'}\n"
                f"Обработано: {processed}\n"
                f"Память процесса: {rss} МиБ\n"
                f"Ошибки API: {api_errors}; лимиты API: {limits}\n"
                f"Событий сегодня: {events_today}\n"
                f"Уведомления доставлено/ожидает/ошибка: {deliveries}"
            )
            return text, MENU
        if command in {"/market", "/scan"}:
            active_types: Counter[str] = Counter()
            for row in active:
                active_types.update(map(str, row.get("anomaly_types", [])))
            top = (
                ", ".join(str(row.get("symbol")) for row in ranked[:5]) or "нет данных"
            )
            prefix = (
                "🔎 Последний доступный снимок" if command == "/scan" else "🌍 Рынок"
            )
            published = snapshot.get("published_at") if snapshot else "нет данных"
            type_counts = (
                ", ".join(
                    f"{_TYPE_LABELS.get(key, key)} {value}"
                    for key, value in sorted(active_types.items())
                )
                or "нет"
            )
            active_count: str = f"{len(active)} среди {len(fresh)} свежих инструментов"
            if not current_available or (not fresh and eligible != 0):
                active_count = type_counts = top = "Нет свежих данных"
            text = (
                f"{prefix} — {_STATUS_LABELS[state]}\n"
                f"Снимок рынка: {_SNAPSHOT_LABELS[snapshot_state]}; "
                f"время: {published}\n"
                "Доступных инструментов: "
                f"{eligible if eligible is not None else 'нет данных'}; "
                f"с прогретой базой: {ready if ready is not None else 'нет данных'}\n"
                f"Текущие аномалии: {active_count}\n"
                f"По типам: {type_counts}\n"
                f"Последнее обновление рынка: {_age_text(market_age)} назад\n"
                f"Наиболее активные: {top}"
            )
            return text, MENU
        if command in {"/inplay", "/top"}:
            selected = (
                sorted(active, key=lambda row: -float(row.get("score", 0)))[:10]
                if command == "/inplay"
                else ranked[:10]
            )
            title = (
                "🔥 Актуальные аномалии"
                if command == "/inplay"
                else "🏆 ТОП-10 по текущей силе аномалии"
            )
            lines = [f"{title} — {_STATUS_LABELS[state]}"]
            if not selected:
                if current_available and (fresh or eligible == 0):
                    lines.append(
                        "Подтверждено: активных аномалий нет "
                        f"среди {len(fresh)} свежих инструментов."
                    )
                else:
                    lines.append("Нет свежих данных для этого раздела.")
            for row in selected:
                age = _age(now, row.get("detected_at"))
                row_types = _types_ru(row.get("anomaly_types"))
                lines.append(
                    f"{row.get('symbol')} · {float(row.get('score', 0)):.1f} · "
                    f"{row_types} · {_age_text(age)}"
                )
                lines.extend(_metric_lines(row)[:4])
            return "\n".join(lines), MENU
        if command.startswith("/symbol "):
            symbol = command[8:].strip().upper()
            if not symbol.isalnum() or len(symbol) > 25:
                return "⚠️ Укажите символ буквами и цифрами.", MENU
            if snapshot_state != "FRESH" or not current_available:
                return "⚠️ Нет свежих данных об инструменте.", MENU
            assert snapshot is not None
            eligible_symbols = snapshot.get("eligible_symbols", [])
            if symbol not in eligible_symbols:
                if snapshot.get("eligible_truncated", 0):
                    return (
                        f"🔎 {symbol}: полный список инструментов не помещается "
                        "в снимок; статус не подтверждён.",
                        MENU,
                    )
                rejection: list[str] = next(
                    (
                        item.get("reasons", [])
                        for item in snapshot.get("rejected", [])
                        if item.get("symbol") == symbol
                    ),
                    [],
                )
                detail = (
                    ", ".join(map(str, rejection))
                    or "нет подтверждённой причины в снимке"
                )
                return f"🔎 {symbol}: вне доступного списка. Причина: {detail}.", MENU
            symbol_row: dict[str, Any] | None = next(
                (item for item in rows if item.get("symbol") == symbol), None
            )
            if symbol_row is None:
                return f"🔎 {symbol}: доступен, оценка ещё не опубликована.", MENU
            freshness = (
                "АКТУАЛЬНО"
                if _row_fresh(symbol_row, now) and state not in {"STALE", "OFFLINE"}
                else "УСТАРЕЛО"
            )
            symbol_types = _types_ru(symbol_row.get("anomaly_types"))
            symbol_score = float(symbol_row.get("score", 0))
            baseline = "готов" if symbol_row.get("baseline_ready") else "прогрев"
            detected = symbol_row.get("detected_at")
            lines = [
                f"🔎 {symbol} — {freshness}",
                "Состояние: "
                f"{_STATE_LABELS.get(str(symbol_row.get('state')), 'нет данных')}; "
                f"сила аномалии: {symbol_score:.1f}/100",
                f"Категория инструмента: {symbol_row.get('tier')}; "
                f"базовая история: {baseline}",
                f"Аномалии последней оценки: {symbol_types}",
                f"Время: {detected}; возраст: {_age_text(_age(now, detected))}",
            ]
            if isinstance(symbol_row.get("close"), (int, float)):
                lines.append(
                    f"Последняя цена закрытия: {float(symbol_row['close']):.6g}"
                )
            lines.extend(_metric_lines(symbol_row))
            history = self.db.execute(
                "SELECT detected_ts,score FROM panel_events WHERE symbol=? "
                "ORDER BY detected_ts DESC LIMIT 3",
                (symbol,),
            ).fetchall()
            lines.append(f"Исторические события (до 3): {len(history)}")
            return "\n".join(lines), MENU
        if command == "/events":
            records = self._events(now_ts)
            lines = ["🚨 События с включения панели (история, до 24 ч)"]
            for ts, symbol, score, types in records:
                when = datetime.fromtimestamp(ts, UTC).strftime("%m-%d %H:%M UTC")
                lines.append(f"{when} · {symbol} · {score:.1f} · {_types_ru(types)}")
            if not records:
                lines.append("Событий пока нет.")
            return "\n".join(lines), MENU
        if command == "/stats":
            totals = {1: 0, 4: 0, 24: 0}
            event_types: Counter[str] = Counter()
            symbols: set[str] = set()
            buckets: Counter[str] = Counter()
            for ts, symbol, score, raw_types in self.db.execute(
                "SELECT detected_ts,symbol,score,anomaly_types FROM panel_events "
                "WHERE detected_ts>=?",
                (now_ts - EVENT_WINDOW,),
            ):
                age = now_ts - float(ts)
                for hours in totals:
                    if age <= hours * 3600:
                        totals[hours] += 1
                symbols.add(str(symbol))
                event_types.update(map(str, json.loads(raw_types)))
                bucket = min(4, max(0, int(float(score) // 20)))
                buckets[f"{bucket * 20}-{bucket * 20 + 19}"] += 1
            counts = self._counts()
            type_counts = (
                ", ".join(
                    f"{_TYPE_LABELS.get(key, key)} {value}"
                    for key, value in sorted(event_types.items())
                )
                or "нет"
            )
            score_counts = (
                ", ".join(f"{key}: {value}" for key, value in sorted(buckets.items()))
                or "нет"
            )
            deliveries = (
                f"{counts.get('SENT', 0)}/{counts.get('PENDING', 0)}/"
                f"{counts.get('BLOCKED', 0)}"
            )
            return (
                "📊 События с включения панели, за 1/4/24 ч: "
                f"{totals[1]}/{totals[4]}/{totals[24]}\n"
                f"Уникальных инструментов (24 ч): {len(symbols)}\n"
                f"Типы: {type_counts}\n"
                f"Распределение силы: {score_counts}\n"
                f"Уведомления доставлено/ожидает/ошибка: {deliveries}",
                MENU,
            )
        if command == "/settings":
            return (
                "⚙️ Автоматические уведомления: включены.\n"
                "Настройки текущего пилота доступны только для просмотра. "
                "Отключение уведомлений не поддерживается, чтобы не терять события.",
                MENU,
            )
        return "Неизвестная команда. Используйте /help.", MENU

    def _command(
        self, update: Mapping[str, Any]
    ) -> tuple[int, int, str, str | None] | None:
        callback = update.get("callback_query")
        source = callback if isinstance(callback, dict) else update.get("message")
        if not isinstance(source, dict):
            return None
        user = source.get("from")
        message = source.get("message") if callback else source
        chat = message.get("chat") if isinstance(message, dict) else None
        if not isinstance(user, dict) or not isinstance(chat, dict):
            return None
        user_id, chat_id = user.get("id"), chat.get("id")
        if not isinstance(user_id, int) or not isinstance(chat_id, int):
            return None
        callback_id = callback.get("id") if isinstance(callback, dict) else None
        raw = source.get("data") if callback else source.get("text")
        if not isinstance(raw, str) or len(raw) > 100:
            return None
        command = raw.replace("cmd:", "/", 1) if raw.startswith("cmd:") else raw
        command = (
            command.replace("symbol:", "/symbol ", 1)
            if command.startswith("symbol:")
            else command
        )
        command = (
            command.replace("detail:", "/detail ", 1)
            if command.startswith("detail:")
            else command
        )
        command = (
            command.split("@", 1)[0]
            if command.startswith("/") and " " not in command
            else command
        )
        return (
            user_id,
            chat_id,
            command.strip(),
            str(callback_id) if callback_id else None,
        )

    def _accept(self, update: Mapping[str, Any], offset: int) -> None:
        update_id = update.get("update_id")
        if not isinstance(update_id, int) or update_id < offset:
            return
        parsed = self._command(update)
        reply: tuple[str, tuple[tuple[tuple[str, str], ...], ...]] | None = None
        callback_id: str | None = None
        allowed = False
        rate_update: tuple[int, float, float] | None = None
        if parsed is not None:
            user_id, chat_id, command, callback_id = parsed
            allowed = user_id in self.allowed_user_ids and str(chat_id) == self.chat_id
            if allowed:
                row = self.db.execute(
                    "SELECT last_at,last_scan_at FROM panel_rate WHERE user_id=?",
                    (user_id,),
                ).fetchone()
                now = self.clock()
                last_at, last_scan = row if row is not None else (0.0, 0.0)
                cooldown = now - last_at >= COMMAND_COOLDOWN
                scan_ready = command != "/scan" or now - last_scan >= SCAN_COOLDOWN
                if cooldown and scan_ready:
                    reply = self._render(command)
                    rate_update = (
                        user_id,
                        now,
                        now if command == "/scan" else last_scan,
                    )
        self.db.execute("BEGIN IMMEDIATE")
        try:
            if rate_update is not None:
                self.db.execute(
                    "INSERT INTO panel_rate(user_id,last_at,last_scan_at) "
                    "VALUES(?,?,?) ON CONFLICT(user_id) DO UPDATE SET "
                    "last_at=excluded.last_at,last_scan_at=excluded.last_scan_at",
                    rate_update,
                )
            if reply is not None:
                text, keyboard = reply
                self.db.execute(
                    "INSERT OR IGNORE INTO panel_outbox("
                    "update_id,text,keyboard,status,created_at) "
                    "VALUES(?,?,?,'PENDING',?)",
                    (update_id, text[:MAX_REPLY], json.dumps(keyboard), self.clock()),
                )
            self.db.execute(
                "UPDATE panel_cursor SET offset=? WHERE id=1", (update_id + 1,)
            )
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise
        if callback_id:
            with suppress(DeliveryError):
                self.transport.answer_callback(
                    self.token,
                    callback_id,
                    "Принято" if allowed else "Доступ запрещён",
                )

    def poll(self) -> int:
        pending = self.db.execute(
            "SELECT count(*) FROM panel_outbox WHERE status IN ('PENDING','INFLIGHT')"
        ).fetchone()
        if pending is not None and int(pending[0]) >= MAX_PENDING_REPLIES:
            # Keep updates at Telegram until the local reply backlog drains.
            return 0
        offset = int(
            self.db.execute("SELECT offset FROM panel_cursor WHERE id=1").fetchone()[0]
        )
        if offset == -1:
            # Telegram's documented negative offset selects the last queued
            # update and forgets older updates. Never replay old bot commands.
            updates = self.transport.get_updates(self.token, -1)
            next_offset = max(
                (int(item["update_id"]) + 1 for item in updates), default=0
            )
            self.db.execute(
                "UPDATE panel_cursor SET offset=? WHERE id=1", (next_offset,)
            )
            return 0
        updates = self.transport.get_updates(self.token, offset)
        processed = 0
        for update in sorted(updates, key=lambda item: int(item.get("update_id", -1))):
            update_id = update.get("update_id")
            if isinstance(update_id, int) and update_id >= offset:
                self._accept(update, offset)
                offset = update_id + 1
                processed += 1
        return processed

    def deliver_one(self) -> bool:
        now = self.clock()
        next_send = self.db.execute(
            "SELECT next_send FROM cursor WHERE id=1"
        ).fetchone()
        if next_send is None or now < float(next_send[0]):
            return False
        row = self.db.execute(
            "SELECT update_id,text,keyboard,attempts FROM panel_outbox "
            "WHERE status IN ('PENDING','INFLIGHT') AND next_attempt<=? "
            "ORDER BY update_id LIMIT 1",
            (now,),
        ).fetchone()
        if row is None:
            return False
        update_id, text, keyboard_json, attempts = row
        self.db.execute(
            "UPDATE panel_outbox SET status='INFLIGHT',attempts=?,next_attempt=? "
            "WHERE update_id=?",
            (attempts + 1, now + 60, update_id),
        )
        try:
            keyboard = tuple(
                tuple((str(label), str(data)) for label, data in group)
                for group in json.loads(keyboard_json)
            )
            self.transport.send(self.token, self.chat_id, str(text), keyboard)
        except DeliveryError as error:
            delay = error.retry_after if error.retry_after is not None else 30.0
            self.db.execute(
                "UPDATE panel_outbox SET status='PENDING',next_attempt=? "
                "WHERE update_id=?",
                (now + min(300.0, max(1.0, delay)), update_id),
            )
            return False
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute(
                "UPDATE panel_outbox SET status='SENT',text='',keyboard='' "
                "WHERE update_id=?",
                (update_id,),
            )
            self.db.execute(
                "UPDATE cursor SET next_send=? WHERE id=1",
                (now + self.spacing,),
            )
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise
        return True

    def run_once(self) -> tuple[int, bool]:
        now = self.clock()
        self.db.execute(
            "DELETE FROM panel_events WHERE detected_ts<?", (now - EVENT_WINDOW,)
        )
        count = int(self.db.execute("SELECT count(*) FROM panel_events").fetchone()[0])
        if count > MAX_EVENTS:
            self.db.execute(
                "DELETE FROM panel_events WHERE event_id IN "
                "(SELECT event_id FROM panel_events ORDER BY detected_ts "
                "LIMIT ?)",
                (count - MAX_EVENTS,),
            )
        self.db.execute(
            "DELETE FROM panel_outbox WHERE status='SENT' AND created_at<?",
            (now - EVENT_WINDOW,),
        )
        polled = 0
        if now >= self._next_poll_at:
            try:
                polled = self.poll()
            except DeliveryError as error:
                self._next_poll_at = now + (
                    error.retry_after if error.retry_after is not None else 30.0
                )
        return polled, self.deliver_one()
