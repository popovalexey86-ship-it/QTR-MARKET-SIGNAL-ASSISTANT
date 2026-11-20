"""Offline contract tests for the optional Watchdog Telegram control panel."""

from __future__ import annotations

import io
import json
import urllib.error
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from watchdog_evidence_helpers import evidence

from market_signal_assistant.inplay.models import CatalogInstrument
from market_signal_assistant.watchdog.aggregation import (
    AggregationResult,
    SequenceContext,
)
from market_signal_assistant.watchdog.engine import WatchdogDetectionResult
from market_signal_assistant.watchdog.events.journal import _event_payload
from market_signal_assistant.watchdog.features import WatchdogFeatureSnapshot
from market_signal_assistant.watchdog.models import WatchdogState
from market_signal_assistant.watchdog.runtime.control_snapshot import (
    MAX_SNAPSHOT_BYTES,
    MAX_SYMBOLS,
    ControlSnapshotError,
    ControlSnapshotPublisher,
    load_control_snapshot,
)
from market_signal_assistant.watchdog.state_machine import WatchdogSymbolState
from market_signal_assistant.watchdog.telegram_adapter import (
    DeliveryError,
    WatchdogTelegramAdapter,
)
from market_signal_assistant.watchdog.telegram_control import (
    TelegramControlHttp,
    WatchdogControlPanel,
)
from market_signal_assistant.watchdog.telegram_presentation import detail_token
from market_signal_assistant.watchdog.universe import (
    UniverseEntry,
    UniverseSnapshot,
    UniverseTier,
)

NOW = datetime(2026, 11, 19, 12, tzinfo=UTC)


class AlertSender:
    def __init__(self) -> None:
        self.sent: list[str] = []
        self.keyboards: list[list[list[dict[str, str]]] | None] = []

    def send(
        self,
        token: str,
        chat_id: str,
        text: str,
        keyboard: list[list[dict[str, str]]] | None = None,
    ) -> None:
        self.sent.append(text)
        self.keyboards.append(keyboard)


class ControlTransport:
    def __init__(self) -> None:
        self.updates: list[dict[str, Any]] = []
        self.sent: list[str] = []
        self.callbacks: list[str] = []
        self.failure: DeliveryError | None = None

    def get_updates(self, token: str, offset: int) -> list[dict[str, Any]]:
        if self.failure:
            error, self.failure = self.failure, None
            raise error
        if offset == -1:
            last = self.updates[-1:]
            self.updates.clear()
            return last
        return [item for item in self.updates if item["update_id"] >= offset]

    def send(self, token: str, chat_id: str, text: str, keyboard: object) -> None:
        if self.failure:
            error, self.failure = self.failure, None
            raise error
        self.sent.append(text)

    def answer_callback(self, token: str, callback_id: str, text: str) -> None:
        self.callbacks.append(text)


def _message(
    update_id: int, command: str, *, user: int = 7, chat: int = 123
) -> dict[str, Any]:
    return {
        "update_id": update_id,
        "message": {"from": {"id": user}, "chat": {"id": chat}, "text": command},
    }


def _callback(update_id: int, command: str) -> dict[str, Any]:
    return {
        "update_id": update_id,
        "callback_query": {
            "id": f"cb-{update_id}",
            "from": {"id": 7},
            "data": command,
            "message": {"chat": {"id": 123}},
        },
    }


def _snapshot(root: Path, *, seconds_old: int = 0, ready: int = 1) -> None:
    stamp = (NOW - timedelta(seconds=seconds_old)).isoformat()
    rows = [
        {
            "symbol": symbol,
            "tier": "ACTIVE",
            "state": "IN_PLAY" if symbol == "AAAUSDT" else "NORMAL",
            "score": score,
            "anomaly_types": ["VOLUME_SHOCK"] if symbol == "AAAUSDT" else [],
            "event_id": "evt-a" if symbol == "AAAUSDT" else None,
            "detected_at": stamp,
            "observed_at": stamp,
            "available_at": stamp,
            "interval": "1m",
            "baseline_ready": True,
            "baseline_samples": 25,
            "close": 1.25,
            "features": {"price_change_1": 0.02},
        }
        for symbol, score in (("AAAUSDT", 72.0), ("BBBUSDT", 91.0))
    ]
    state = root / "state"
    state.mkdir(parents=True, exist_ok=True)
    (state / "control-panel.json").write_text(
        json.dumps(
            {
                "version": 1,
                "published_at": stamp,
                "universe_observed_at": stamp,
                "eligible_count": 2,
                "eligible_symbols": ["AAAUSDT", "BBBUSDT"],
                "eligible_truncated": 0,
                "rejected": [{"symbol": "BADUSDT", "reasons": ["spread"]}],
                "rejected_truncated": 0,
                "symbols": rows,
            }
        ),
        encoding="utf-8",
    )
    (state / "health.json").write_text(
        json.dumps(
            {
                "last_loop_at": stamp,
                "last_successful_market_update": stamp,
                "started_at": (NOW - timedelta(hours=1)).isoformat(),
                "next_market_update_due_at": (NOW + timedelta(minutes=1)).isoformat(),
                "baseline_ready_symbols": ready,
                "symbols_processed": 42,
                "process_rss_bytes": 80 * 1024 * 1024,
                "provider_errors": 0,
                "rate_limit_events": 0,
                "events_today": 2,
                "degraded": False,
            }
        ),
        encoding="utf-8",
    )
    (state / "observer.json").write_text(
        json.dumps({"status": "running"}), encoding="utf-8"
    )


def _panel(
    root: Path, now: list[float]
) -> tuple[
    WatchdogTelegramAdapter, WatchdogControlPanel, ControlTransport, AlertSender
]:
    journal = root / "events/events.jsonl"
    journal.parent.mkdir(parents=True, exist_ok=True)
    journal.touch()
    sender = AlertSender()
    adapter = WatchdogTelegramAdapter(
        journal,
        root / "delivery.sqlite3",
        sender=sender,
        token="private-test-token",
        chat_id="123",
        panel_enabled=True,
        clock=lambda: now[0],
    )
    transport = ControlTransport()
    panel = WatchdogControlPanel(
        adapter.db,
        root,
        transport=transport,
        token="private-test-token",
        chat_id="123",
        allowed_user_ids=frozenset({7}),
        clock=lambda: now[0],
    )
    return adapter, panel, transport, sender


def test_routing_russian_menu_current_vs_history_and_status(tmp_path: Path) -> None:
    now = [NOW.timestamp()]
    _snapshot(tmp_path)
    adapter, panel, transport, _ = _panel(tmp_path, now)
    assert panel.poll() == 0
    commands = [
        ("/start", "🐺"),
        ("/status", "Наблюдатель: работает"),
        ("/market", "Текущие аномалии: 1 среди 2 свежих инструментов"),
        ("/inplay", "AAAUSDT"),
        ("/top", "BBBUSDT"),
        ("/symbol AAAUSDT", "Цена за 1 мин: +2.00%"),
        ("/symbol BADUSDT", "широкий спред"),
        ("/symbol MISSINGUSDT", "вне доступного списка"),
        ("/scan", "Последний доступный снимок"),
        ("/events", "история"),
        ("/stats", "за 1/4/24 ч"),
        ("/settings", "Автоматические уведомления"),
        ("/help", "новых API-запросов нет"),
    ]
    for index, (command, expected) in enumerate(commands, start=1):
        now[0] += 3
        transport.updates.append(_message(index, command))
        assert panel.run_once() == (1, True)
        assert expected in transport.sent[-1]
        assert len(transport.sent[-1]) <= 3800
    assert transport.sent[4].index("BBBUSDT") < transport.sent[4].index("AAAUSDT")
    adapter.close()


def test_unified_alert_detail_callback_and_manual_commands_ignore_alert_cap(
    tmp_path: Path,
) -> None:
    now = [NOW.timestamp() + 20]
    _snapshot(tmp_path)
    journal = tmp_path / "events/events.jsonl"
    journal.parent.mkdir(parents=True, exist_ok=True)
    journal.touch()
    sender = AlertSender()
    adapter = WatchdogTelegramAdapter(
        journal,
        tmp_path / "delivery.sqlite3",
        sender=sender,
        token="private-test-token",
        chat_id="123",
        panel_enabled=True,
        alert_policy_enabled=True,
        panel_root=tmp_path,
        clock=lambda: now[0],
    )
    transport = ControlTransport()
    panel = WatchdogControlPanel(
        adapter.db,
        tmp_path,
        transport=transport,
        token="private-test-token",
        chat_id="123",
        allowed_user_ids=frozenset({7}),
        clock=lambda: now[0],
    )
    panel.poll()
    event = evidence(
        "watchdog:ABCUSDT:20261119T120000.000000Z",
        score=65,
        detected_at=NOW,
    )
    with journal.open("ab") as stream:
        stream.write((json.dumps(_event_payload(event)) + "\n").encode())
    assert adapter.run_once() == (1, True)
    assert "🐺 ABCUSDT" in sender.sent[0]
    assert "Почему сейчас" in sender.sent[0]
    assert sender.keyboards[0] is not None
    assert sender.keyboards[0][0][0]["url"].startswith(
        "https://www.tradingview.com/symbols/ABCUSDT.P/"
    )
    token = detail_token(event.event_id)
    transport.updates.append(_callback(1, f"detail:{token}"))
    now[0] += 3
    assert panel.run_once() == (1, True)
    assert "ID события" in transport.sent[-1]
    assert event.event_id in transport.sent[-1]
    adapter.db.executemany(
        "INSERT INTO alert_policy_sent VALUES(?,?)",
        [(f"cap-{i}", NOW.timestamp()) for i in range(36)],
    )
    transport.updates.append(_message(2, "/status"))
    now[0] += 3
    assert panel.run_once() == (1, True)
    assert "Наблюдатель: работает" in transport.sent[-1]
    adapter.close()


def test_digest_separates_current_snapshot_from_historical_events(
    tmp_path: Path,
) -> None:
    now = [NOW.timestamp() + 20]
    _snapshot(tmp_path)
    journal = tmp_path / "events/events.jsonl"
    journal.parent.mkdir(parents=True, exist_ok=True)
    journal.touch()
    adapter = WatchdogTelegramAdapter(
        journal,
        tmp_path / "delivery.sqlite3",
        sender=AlertSender(),
        token="private-test-token",
        chat_id="123",
        panel_enabled=True,
        alert_policy_enabled=True,
        panel_root=tmp_path,
        clock=lambda: now[0],
    )
    event = evidence(
        "watchdog:ABCUSDT:20261119T120000.000000Z",
        score=65,
        detected_at=NOW,
    )
    with journal.open("ab") as stream:
        stream.write((json.dumps(_event_payload(event)) + "\n").encode())
    assert adapter.collect() == 1
    period = int(now[0] // (6 * 3600))
    text = adapter._digest_text(period, 0, "ABCUSDT", 65, now[0])
    assert "Обнаружено событий: 1" in text
    assert "Актуальные инструменты: AAAUSDT" in text
    assert "Исторические лидеры окна: ABCUSDT" in text
    (tmp_path / "state/control-panel.json").unlink()
    text = adapter._digest_text(period, 0, "ABCUSDT", 65, now[0])
    assert "Актуальные инструменты: Нет свежих данных" in text
    assert "Исторические лидеры окна: ABCUSDT" in text
    adapter.close()


def test_unified_adapter_rejects_mismatched_snapshot_and_journal_roots(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "events/events.jsonl"
    journal.parent.mkdir(parents=True)
    journal.touch()
    state = tmp_path / "delivery.sqlite3"
    with pytest.raises(ValueError, match="does not match event journal"):
        WatchdogTelegramAdapter(
            journal,
            state,
            sender=AlertSender(),
            token="private-test-token",
            chat_id="123",
            panel_enabled=True,
            alert_policy_enabled=True,
            panel_root=tmp_path / "other-root",
        )
    assert not state.exists()


def test_authorization_callbacks_duplicates_cooldown_and_restart(
    tmp_path: Path,
) -> None:
    now = [NOW.timestamp()]
    _snapshot(tmp_path)
    adapter, panel, transport, _ = _panel(tmp_path, now)
    panel.poll()
    transport.updates.append(_message(1, "/status", user=99))
    assert panel.run_once() == (1, False)
    assert transport.sent == []
    now[0] += 3
    transport.updates.append(_callback(2, "cmd:status"))
    assert panel.run_once() == (1, True)
    assert transport.callbacks == ["Принято"]
    assert "Наблюдатель: работает" in transport.sent[0]
    assert panel.run_once() == (0, False)
    now[0] += 3
    transport.updates.append(_message(3, "/scan"))
    assert panel.run_once() == (1, True)
    transport.updates.append(_message(4, "/scan"))
    assert panel.run_once() == (1, False)
    assert len(transport.sent) == 2
    adapter.close()
    restarted, new_panel, _, _ = _panel(tmp_path, now)
    assert new_panel.db.execute("SELECT offset FROM panel_cursor").fetchone() == (5,)
    assert new_panel.db.execute("SELECT count(*) FROM panel_outbox").fetchone() == (2,)
    restarted.close()


def test_first_panel_start_discards_queued_historical_commands(tmp_path: Path) -> None:
    now = [NOW.timestamp()]
    _snapshot(tmp_path)
    adapter, panel, transport, _ = _panel(tmp_path, now)
    transport.updates.extend((_message(41, "/status"), _message(42, "/events")))
    assert panel.run_once() == (0, False)
    assert transport.sent == []
    assert adapter.db.execute("SELECT offset FROM panel_cursor").fetchone() == (43,)
    transport.updates.append(_message(43, "/status"))
    assert panel.run_once() == (1, True)
    assert "Наблюдатель: работает" in transport.sent[0]
    adapter.close()


def test_snapshot_unknown_stale_invalid_and_warmup_are_distinct(tmp_path: Path) -> None:
    now = [NOW.timestamp()]
    _snapshot(tmp_path, ready=0)
    adapter, panel, _, _ = _panel(tmp_path, now)
    status = panel._render("/status")[0]
    assert "Наблюдатель: прогрев данных" in status
    assert "Снимок рынка: свежий" in status
    (tmp_path / "state/control-panel.json").unlink()
    status = panel._render("/status")[0]
    assert "Наблюдатель: прогрев данных" in status
    assert "Процесс наблюдения: работает" in status
    assert "Снимок рынка: отсутствует" in status
    assert "Текущие аномалии: Нет свежих данных" in panel._render("/market")[0]
    _snapshot(tmp_path, seconds_old=600)
    status = panel._render("/status")[0]
    assert "Наблюдатель: данные устарели" in status
    assert "Снимок рынка: устарел" in status
    assert "AAAUSDT" not in panel._render("/inplay")[0]
    assert "Текущие аномалии: Нет свежих данных" in panel._render("/market")[0]
    assert "Нет свежих данных" in panel._render("/symbol AAAUSDT")[0]
    _snapshot(tmp_path, ready=0)
    (tmp_path / "state/control-panel.json").write_text("{broken")
    status = panel._render("/status")[0]
    assert "Наблюдатель: прогрев данных" in status
    assert "Снимок рынка: повреждён" in status
    assert "Процесс наблюдения: работает" in status
    assert "Текущие аномалии: Нет свежих данных" in panel._render("/market")[0]
    adapter.close()


def test_valid_empty_snapshot_confirms_zero_without_claiming_missing_data(
    tmp_path: Path,
) -> None:
    now = [NOW.timestamp()]
    _snapshot(tmp_path, ready=0)
    path = tmp_path / "state/control-panel.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    for row in payload["symbols"]:
        row["anomaly_types"] = []
        row["state"] = "NORMAL"
    path.write_text(json.dumps(payload), encoding="utf-8")
    adapter, panel, _, _ = _panel(tmp_path, now)
    status = panel._render("/status")[0]
    assert "Снимок рынка: свежий" in status
    assert "Наблюдатель: прогрев данных" in status
    market = panel._render("/market")[0]
    assert "Текущие аномалии: 0 среди 2 свежих инструментов" in market
    assert "Нет свежих данных" not in market
    assert "Подтверждено: активных аномалий нет" in panel._render("/inplay")[0]
    payload["symbols"] = []
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert "Текущие аномалии: Нет свежих данных" in panel._render("/market")[0]
    payload["eligible_count"] = 0
    payload["eligible_symbols"] = []
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert (
        "Текущие аномалии: 0 среди 0 свежих инструментов" in panel._render("/market")[0]
    )
    adapter.close()


def test_missing_snapshot_does_not_hide_healthy_observer(tmp_path: Path) -> None:
    now = [NOW.timestamp()]
    _snapshot(tmp_path, ready=1)
    (tmp_path / "state/control-panel.json").unlink()
    adapter, panel, _, _ = _panel(tmp_path, now)
    status = panel._render("/status")[0]
    assert "Наблюдатель: работает" in status
    assert "Снимок рынка: отсутствует" in status
    assert "Текущие аномалии: Нет свежих данных" in panel._render("/market")[0]
    adapter.close()


def test_api_error_retry_and_alert_compatibility(tmp_path: Path) -> None:
    now = [NOW.timestamp()]
    _snapshot(tmp_path)
    adapter, panel, transport, alert_sender = _panel(tmp_path, now)
    panel.poll()
    transport.updates.append(_message(1, "/status"))
    transport.failure = DeliveryError("RATE_LIMIT", retry_after=5)
    assert panel.run_once() == (0, False)
    # Alert collection and delivery remains independent of command API errors.
    event = evidence("watchdog:ABCUSDT:20261119T120000.000000Z", detected_at=NOW)
    with (tmp_path / "events/events.jsonl").open("ab") as stream:
        stream.write((json.dumps(_event_payload(event)) + "\n").encode())
    assert adapter.run_once() == (1, True)
    assert len(alert_sender.sent) == 1
    assert adapter.db.execute("SELECT count(*) FROM panel_events").fetchone() == (1,)
    stats = panel._render("/stats")[0]
    assert "1/1/1" in stats and "ABCUSDT" not in stats
    assert "Уникальных инструментов (24 ч): 1" in stats
    now[0] += 5
    assert panel.run_once() == (1, True)
    assert "Наблюдатель: работает" in transport.sent[0]
    adapter.close()


def test_command_reply_respects_alert_send_spacing(tmp_path: Path) -> None:
    now = [NOW.timestamp()]
    _snapshot(tmp_path)
    adapter, panel, transport, _ = _panel(tmp_path, now)
    panel.poll()
    event = evidence("watchdog:ABCUSDT:20261119T120000.000000Z", detected_at=NOW)
    with (tmp_path / "events/events.jsonl").open("ab") as stream:
        stream.write((json.dumps(_event_payload(event)) + "\n").encode())
    assert adapter.run_once() == (1, True)
    transport.updates.append(_message(1, "/status"))
    assert panel.run_once() == (1, False)
    assert transport.sent == []
    now[0] += 1
    assert panel.run_once() == (0, True)
    assert adapter.db.execute("SELECT next_send FROM cursor").fetchone() == (
        now[0] + 1,
    )
    adapter.close()


def test_pending_reply_cap_stops_new_update_polling(tmp_path: Path) -> None:
    now = [NOW.timestamp()]
    _snapshot(tmp_path)
    adapter, panel, transport, _ = _panel(tmp_path, now)
    panel.poll()
    adapter.db.executemany(
        "INSERT INTO panel_outbox(update_id,text,keyboard,status,created_at) "
        "VALUES(?,'x','[]','PENDING',?)",
        ((index, now[0]) for index in range(1000)),
    )
    transport.updates.append(_message(1001, "/status"))
    assert panel.poll() == 0
    assert adapter.db.execute("SELECT offset FROM panel_cursor").fetchone() == (0,)
    assert adapter.db.execute("SELECT count(*) FROM panel_outbox").fetchone() == (1000,)
    adapter.close()


def test_bounded_snapshot_input_and_bad_row(tmp_path: Path) -> None:
    path = tmp_path / "control-panel.json"
    path.write_bytes(b"x" * (MAX_SNAPSHOT_BYTES + 1))
    with pytest.raises(ControlSnapshotError):
        load_control_snapshot(path)
    _snapshot(tmp_path)
    path = tmp_path / "state/control-panel.json"
    value = json.loads(path.read_text(encoding="utf-8"))
    value["symbols"][0]["score"] = "not-a-number"
    path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(ControlSnapshotError):
        load_control_snapshot(path)


def test_control_api_errors_are_sanitized_and_429_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def rate_limited(*_args: object, **_kwargs: object) -> object:
        raise urllib.error.HTTPError(
            "secret-token-url",
            429,
            "rate",
            {},
            io.BytesIO(b'{"parameters":{"retry_after":7}}'),
        )

    monkeypatch.setattr("urllib.request.urlopen", rate_limited)
    with pytest.raises(DeliveryError) as caught:
        TelegramControlHttp().get_updates("private-test-token", 0)
    assert caught.value.kind == "RATE_LIMIT"
    assert caught.value.retry_after == 7
    assert "private-test-token" not in str(caught.value)

    def server_error(*_args: object, **_kwargs: object) -> object:
        raise urllib.error.HTTPError("secret-token-url", 503, "server", {}, None)

    monkeypatch.setattr("urllib.request.urlopen", server_error)
    with pytest.raises(DeliveryError) as caught:
        TelegramControlHttp().get_updates("private-test-token", 0)
    assert caught.value.kind == "SERVER_ERROR"


def test_publisher_retention_and_atomic_snapshot(tmp_path: Path) -> None:
    publisher = ControlSnapshotPublisher(tmp_path / "state/control-panel.json")
    result = WatchdogDetectionResult(
        snapshot=WatchdogFeatureSnapshot(
            symbol="ABCUSDT",
            interval="1m",
            observed_at=NOW,
            available_at=NOW,
            detected_at=NOW,
            completed_candle_count=1,
            features=(),
            baselines=(),
            missing_data=(),
            stale_data=(),
        ),
        aggregation=AggregationResult(0.0, (), SequenceContext(), (), ()),
        state=WatchdogSymbolState("ABCUSDT", WatchdogState.NORMAL, NOW, NOW, 0),
        transition=None,
        event=None,
        candidate=None,
    )
    entries = []
    for index in range(MAX_SYMBOLS + 50):
        symbol = f"S{index:04d}USDT"
        entry = UniverseEntry(
            instrument=CatalogInstrument(
                symbol=symbol,
                quote_coin="USDT",
                status="Trading",
                turnover_24h=10_000_000,
                bid=1.0,
                ask=1.01,
                base_coin="S",
                settle_coin="USDT",
                contract_type="LinearPerpetual",
                symbol_type="linear",
                is_pre_listing=False,
            ),
            tier=UniverseTier.STANDARD,
            baseline_samples=20,
            is_new_market=False,
            reasons=(),
        )
        entries.append(entry)
        publisher.capture(entry, result, close=1.0)
    assert publisher.retained_count == MAX_SYMBOLS
    publisher.publish(UniverseSnapshot(NOW, tuple(entries), ()), now=NOW)
    payload = load_control_snapshot(tmp_path / "state/control-panel.json")
    assert payload is not None
    assert payload["eligible_count"] == MAX_SYMBOLS + 50
    assert payload["eligible_truncated"] == 50
    assert len(payload["symbols"]) == MAX_SYMBOLS
    assert (tmp_path / "state/control-panel.json").stat().st_size < MAX_SNAPSHOT_BYTES
