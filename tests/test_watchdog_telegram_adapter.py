from __future__ import annotations

import io
import json
import urllib.error
from datetime import timedelta
from pathlib import Path

import pytest
from watchdog_evidence_helpers import NOW, evidence

from market_signal_assistant.watchdog.events.journal import _event_payload
from market_signal_assistant.watchdog.models import AnomalyType
from market_signal_assistant.watchdog.telegram_adapter import (
    DeliveryError,
    JournalChangedError,
    TelegramHttpSender,
    WatchdogTelegramAdapter,
    format_event,
)


class FakeSender:
    def __init__(self) -> None:
        self.sent: list[str] = []
        self.failures: list[DeliveryError] = []

    def send(self, token: str, chat_id: str, text: str) -> None:
        assert token == "private-test-token"
        assert chat_id == "123"
        if self.failures:
            raise self.failures.pop(0)
        self.sent.append(text)


def _append(path: Path, minute: int) -> str:
    at = NOW + timedelta(minutes=minute)
    event = evidence(
        f"watchdog:ABCUSDT:20260918T{at:%H%M%S}.000000Z", detected_at=at
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("ab") as stream:
        stream.write((json.dumps(_event_payload(event)) + "\n").encode("utf-8"))
    return event.event_id


def _adapter(
    journal: Path, db: Path, sender: FakeSender, *, now: list[float]
) -> WatchdogTelegramAdapter:
    journal.parent.mkdir(parents=True, exist_ok=True)
    journal.touch(exist_ok=True)
    return WatchdogTelegramAdapter(
        journal,
        db,
        sender=sender,
        token="private-test-token",
        chat_id="123",
        clock=lambda: now[0],
        minimum_spacing_seconds=2,
    )


def test_missing_journal_and_changed_destination_fail_closed(tmp_path: Path) -> None:
    journal = tmp_path / "events.jsonl"
    db = tmp_path / "delivery.sqlite3"
    with pytest.raises(JournalChangedError, match="missing at bootstrap"):
        WatchdogTelegramAdapter(
            journal, db, sender=FakeSender(), token="private-test-token",
            chat_id="123",
        )
    adapter = _adapter(journal, db, FakeSender(), now=[100.0])
    adapter.close()
    with pytest.raises(ValueError, match="destination changed"):
        WatchdogTelegramAdapter(
            journal, db, sender=FakeSender(), token="private-test-token",
            chat_id="456",
        )


def test_russian_message_has_context_metrics_and_missing_warning() -> None:
    event = evidence(
        anomaly_types=(AnomalyType.COMPRESSION, AnomalyType.VOLUME_SHOCK)
    )
    text = format_event(event)
    for fragment in (
        "🚨", "ABCUSDT", "Сжатие диапазона", "Всплеск объёма",
        "2026-09-18 08:00:00 UTC", "65.0/100", "normalized_range_5",
        "⚠️", event.event_id,
    ):
        assert fragment in text


def test_first_launch_skips_old_events_and_restart_keeps_cursor(tmp_path: Path) -> None:
    journal = tmp_path / "events.jsonl"
    db = tmp_path / "state" / "delivery.sqlite3"
    _append(journal, 0)
    sender = FakeSender()
    now = [100.0]
    adapter = _adapter(journal, db, sender, now=now)
    assert adapter.run_once() == (0, False)
    new_id = _append(journal, 1)
    assert adapter.run_once() == (1, True)
    assert len(sender.sent) == 1
    assert adapter.db.execute(
        "SELECT status FROM outbox WHERE event_id=?", (new_id,)
    ).fetchone() == ("SENT",)
    assert adapter.db.execute(
        "SELECT enqueued_at FROM outbox WHERE event_id=?", (new_id,)
    ).fetchone() == (100.0,)
    adapter.close()

    restarted = _adapter(journal, db, sender, now=now)
    assert restarted.run_once() == (0, False)
    assert len(sender.sent) == 1
    restarted.close()


def test_pending_survives_restart_and_duplicate_id_is_not_resent(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "events.jsonl"
    db = tmp_path / "delivery.sqlite3"
    sender = FakeSender()
    now = [100.0]
    adapter = _adapter(journal, db, sender, now=now)
    _append(journal, 1)
    assert adapter.collect() == 1
    adapter.close()
    restarted = _adapter(journal, db, sender, now=now)
    assert restarted.deliver_one() is True
    # Repeated identical immutable record has one durable event_id entry.
    original = journal.read_bytes()
    with journal.open("ab") as stream:
        stream.write(original)
    assert restarted.collect() == 1
    now[0] = 103.0
    assert restarted.deliver_one() is False
    assert len(sender.sent) == 1
    assert restarted.db.execute("SELECT COUNT(*) FROM outbox").fetchone() == (1,)
    restarted.close()


def test_rate_limit_and_server_error_backoff_without_loss(tmp_path: Path) -> None:
    journal = tmp_path / "events.jsonl"
    db = tmp_path / "delivery.sqlite3"
    sender = FakeSender()
    now = [100.0]
    adapter = _adapter(journal, db, sender, now=now)
    _append(journal, 1)
    adapter.collect()
    sender.failures = [
        DeliveryError("RATE_LIMIT", retry_after=5),
        DeliveryError("SERVER_ERROR", retry_after=3),
    ]
    assert adapter.deliver_one() is False
    now[0] = 104.0
    assert adapter.deliver_one() is False
    now[0] = 105.0
    assert adapter.deliver_one() is False
    now[0] = 111.0
    assert adapter.deliver_one() is True
    assert len(sender.sent) == 1
    assert adapter.db.execute(
        "SELECT attempts,status FROM outbox"
    ).fetchone() == (3, "SENT")
    adapter.close()


def test_global_spacing_is_persisted_between_starts(tmp_path: Path) -> None:
    journal = tmp_path / "events.jsonl"
    db = tmp_path / "delivery.sqlite3"
    sender = FakeSender()
    now = [100.0]
    adapter = _adapter(journal, db, sender, now=now)
    _append(journal, 1)
    _append(journal, 2)
    assert adapter.collect() == 2
    assert adapter.deliver_one() is True
    adapter.close()
    restarted = _adapter(journal, db, sender, now=now)
    assert restarted.deliver_one() is False
    now[0] = 102.0
    assert restarted.deliver_one() is True
    assert len(sender.sent) == 2
    restarted.close()


def test_partial_line_waits_and_corrupt_complete_line_fails_closed(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "events.jsonl"
    sender = FakeSender()
    adapter = _adapter(journal, tmp_path / "delivery.sqlite3", sender, now=[100.0])
    with journal.open("ab") as stream:
        stream.write(b"{broken")
    assert adapter.collect() == 0
    with journal.open("ab") as stream:
        stream.write(b"\n")
    with pytest.raises(JournalChangedError, match="invalid event"):
        adapter.collect()
    assert adapter.db.execute("SELECT offset FROM cursor").fetchone() == (0,)
    adapter.close()


def test_bootstrap_skips_historical_partial_tail_then_reads_new_event(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "events.jsonl"
    journal.write_bytes(b"{old incomplete")
    sender = FakeSender()
    adapter = _adapter(journal, tmp_path / "delivery.sqlite3", sender, now=[100.0])
    with journal.open("ab") as stream:
        stream.write(b" tail}\n")
    _append(journal, 1)
    assert adapter.run_once() == (1, True)
    assert len(sender.sent) == 1
    adapter.close()


def test_conflicting_event_id_fails_closed(tmp_path: Path) -> None:
    journal = tmp_path / "events.jsonl"
    adapter = _adapter(
        journal, tmp_path / "delivery.sqlite3", FakeSender(), now=[100.0]
    )
    _append(journal, 1)
    assert adapter.collect() == 1
    event = evidence(
        "watchdog:ABCUSDT:20260918T080100.000000Z",
        detected_at=NOW + timedelta(minutes=1),
        score=82.0,
    )
    with journal.open("ab") as stream:
        stream.write((json.dumps(_event_payload(event)) + "\n").encode("utf-8"))
    with pytest.raises(JournalChangedError, match="conflicting event_id"):
        adapter.collect()
    assert adapter.db.execute("SELECT COUNT(*) FROM outbox").fetchone() == (1,)
    adapter.close()


@pytest.mark.parametrize("change", ["truncate", "rotate"])
def test_journal_truncation_or_rotation_fails_closed(
    tmp_path: Path, change: str
) -> None:
    journal = tmp_path / "events.jsonl"
    _append(journal, 0)
    adapter = _adapter(
        journal, tmp_path / "delivery.sqlite3", FakeSender(), now=[100.0]
    )
    if change == "truncate":
        journal.write_bytes(b"")
    else:
        journal.rename(tmp_path / "old.jsonl")
        _append(journal, 1)
    with pytest.raises(JournalChangedError):
        adapter.collect()
    adapter.close()


def test_inflight_lease_recovers_after_crash_with_documented_duplicate_risk(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "events.jsonl"
    db = tmp_path / "delivery.sqlite3"
    sender = FakeSender()
    now = [100.0]
    adapter = _adapter(journal, db, sender, now=now)
    _append(journal, 1)
    adapter.collect()
    # Simulate a crash after a successful Telegram response but before SENT commit.
    sender.sent.append("first successful HTTP response")
    adapter.db.execute(
        "UPDATE outbox SET status='INFLIGHT',attempts=1,next_attempt=160"
    )
    adapter.close()
    restarted = _adapter(journal, db, sender, now=now)
    assert restarted.deliver_one() is False
    now[0] = 160.0
    assert restarted.deliver_one() is True
    assert len(sender.sent) == 2  # at-least-once, not exactly-once
    restarted.close()


@pytest.mark.parametrize(
    ("error", "kind"),
    [
        (urllib.error.HTTPError("secret-url", 429, "rate", {}, io.BytesIO(
            b'{"parameters":{"retry_after":7}}'
        )), "RATE_LIMIT"),
        (urllib.error.HTTPError("secret-url", 503, "server", {}, io.BytesIO(b"")),
         "SERVER_ERROR"),
        (urllib.error.URLError("secret-url timeout"), "NETWORK_ERROR"),
        (TimeoutError("timeout"), "NETWORK_ERROR"),
    ],
)
def test_http_errors_are_sanitized_and_retryable(
    monkeypatch: pytest.MonkeyPatch, error: Exception, kind: str
) -> None:
    def fail(*_args: object, **_kwargs: object) -> object:
        raise error

    monkeypatch.setattr("urllib.request.urlopen", fail)
    with pytest.raises(DeliveryError) as caught:
        TelegramHttpSender().send("private-token", "123", "test")
    assert caught.value.kind == kind
    assert "private-token" not in str(caught.value)
    assert "secret-url" not in str(caught.value)
