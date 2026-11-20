from __future__ import annotations

import json
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

from watchdog_evidence_helpers import NOW, evidence

from market_signal_assistant.watchdog.events.journal import _event_payload
from market_signal_assistant.watchdog.events.models import WatchdogEventEvidence
from market_signal_assistant.watchdog.models import AnomalyType, WatchdogState
from market_signal_assistant.watchdog.telegram_adapter import WatchdogTelegramAdapter
from market_signal_assistant.watchdog.telegram_alert_policy import decide_alert


class Sender:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def send(self, token: str, chat_id: str, text: str) -> None:
        assert (token, chat_id) == ("test-token", "123")
        self.messages.append(text)


def _event(minute: int, score: float = 65.0) -> WatchdogEventEvidence:
    at = NOW + timedelta(minutes=minute)
    event_id = f"watchdog:ABCUSDT:{at:%Y%m%dT%H%M%S}.000000Z"
    return evidence(event_id, detected_at=at, score=score)


def _append(path: Path, event: WatchdogEventEvidence) -> None:
    with path.open("ab") as stream:
        stream.write((json.dumps(_event_payload(event)) + "\n").encode())


def test_policy_distinguishes_secondary_cooldown_transition_escalation_and_ttl() -> (
    None
):
    event = _event(0)
    now = event.detected_at.timestamp()
    assert decide_alert(event, now=now, previous=None, sent_last_day=0).send
    assert not decide_alert(
        replace(event, anomaly_score=45), now=now, previous=None, sent_last_day=0
    ).send
    transition = replace(
        event,
        anomaly_score=50,
        anomaly_types=(AnomalyType.COMPRESSION, AnomalyType.RANGE_BUILDUP),
    )
    assert decide_alert(transition, now=now, previous=None, sent_last_day=0).send
    assert not decide_alert(event, now=now + 601, previous=None, sent_last_day=0).send
    assert not decide_alert(event, now=now, previous=None, sent_last_day=36).send
    later = _event(31, 85)
    assert (
        decide_alert(
            later,
            now=later.detected_at.timestamp(),
            previous=(now, 65),
            sent_last_day=1,
        ).reason
        == "MATERIAL_ESCALATION"
    )
    assert not decide_alert(
        _event(1, 70), now=now + 60, previous=(now, 65), sent_last_day=1
    ).send
    assert not decide_alert(
        replace(transition, state_after=WatchdogState.NORMAL),
        now=now,
        previous=None,
        sent_last_day=0,
    ).send


def test_policy_cursor_suppression_digest_and_restart_are_durable(
    tmp_path: Path,
) -> None:
    journal = tmp_path / "events.jsonl"
    journal.touch()
    db = tmp_path / "delivery.sqlite3"
    sender = Sender()
    now = [NOW.timestamp() + 30]

    def open_adapter() -> WatchdogTelegramAdapter:
        return WatchdogTelegramAdapter(
            journal,
            db,
            sender=sender,
            token="test-token",
            chat_id="123",
            clock=lambda: now[0],
            alert_policy_enabled=True,
        )

    adapter = open_adapter()
    _append(journal, _event(0, 65))
    assert adapter.run_once() == (1, True)
    _append(journal, _event(1, 45))
    now[0] = NOW.timestamp() + 90
    assert adapter.run_once() == (1, False)
    assert adapter.db.execute(
        "SELECT count(*) FROM outbox WHERE status='SUPPRESSED'"
    ).fetchone() == (1,)
    adapter.close()
    adapter = open_adapter()
    assert adapter.run_once() == (0, False)
    assert len(sender.messages) == 1
    now[0] = NOW.timestamp() + 4 * 3600 + 10 * 60
    assert adapter.run_once() == (0, True)
    assert "сводка за 6 часов" in sender.messages[-1]
    assert len(sender.messages) == 2
    adapter.close()


def test_expired_backlog_is_recorded_but_never_delivered(tmp_path: Path) -> None:
    journal = tmp_path / "events.jsonl"
    journal.touch()
    now = [NOW.timestamp() + 24 * 3600]
    sender = Sender()
    adapter = WatchdogTelegramAdapter(
        journal,
        tmp_path / "delivery.sqlite3",
        sender=sender,
        token="test-token",
        chat_id="123",
        clock=lambda: now[0],
        alert_policy_enabled=True,
    )
    _append(journal, _event(0, 90))
    assert adapter.run_once() == (1, False)
    assert adapter.db.execute(
        "SELECT count(*) FROM outbox WHERE status='SUPPRESSED'"
    ).fetchone() == (1,)
    assert sender.messages == []
    adapter.close()


def test_priority_sends_strongest_pending_alert_first(tmp_path: Path) -> None:
    journal = tmp_path / "events.jsonl"
    journal.touch()
    sender = Sender()
    adapter = WatchdogTelegramAdapter(
        journal,
        tmp_path / "delivery.sqlite3",
        sender=sender,
        token="test-token",
        chat_id="123",
        clock=lambda: NOW.timestamp() + 30,
        alert_policy_enabled=True,
    )
    weaker = _event(0, 55)
    stronger = replace(
        _event(0, 80),
        event_id="watchdog:DEFUSDT:20260918T080000.000000Z",
        symbol="DEFUSDT",
    )
    _append(journal, weaker)
    _append(journal, stronger)
    assert adapter.run_once() == (2, True)
    assert "DEFUSDT" in sender.messages[0]
    adapter.close()
