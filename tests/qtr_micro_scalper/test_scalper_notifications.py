from __future__ import annotations

import asyncio
import hashlib
from dataclasses import replace
from datetime import timedelta

import pytest
from test_control_plane import NOW, plane, snapshot

from market_signal_assistant.qtr_micro_scalper.control_models import (
    RecentEvent,
)
from market_signal_assistant.telegram.formatting import SAFE_MESSAGE_LIMIT
from market_signal_assistant.telegram.scalper_commands import ScalperCommands
from market_signal_assistant.telegram.scalper_notifications import (
    MAX_CURSOR_BYTES,
    ScalperCursorStore,
    ScalperNotifier,
    ScalperNotifyConfig,
)


def event(i, kind="OPEN", status=None):
    return RecentEvent(
        "operator-" + hashlib.sha256(str(i).encode()).hexdigest(),
        kind,
        NOW + timedelta(seconds=i),
        "ZECUSDT",
        f"trade-{i}",
        "LONG",
        123.45,
        124.0 if kind == "CLOSED" else None,
        0.42 if kind == "CLOSED" else None,
        "TIME_EXIT" if kind == "CLOSED" else None,
        status,
    )


class Harness:
    def __init__(self, path, chats=(7,)):
        self.path = path
        self.plane = plane(path)
        self.sent = []
        self.config = ScalperNotifyConfig(chats, 5, path / "cursor.json")
        self.store = ScalperCursorStore(path / "cursor.json", self.plane.store.path)
        self.worker = self.restart()

    async def send(self, chat, text):
        self.sent.append((chat, text))

    def restart(self):
        return ScalperNotifier(self.plane.store, self.store, self.config, self.send)

    def publish(self, events=(), *, status="RUNNING", ready=True, at=NOW):
        s = snapshot(self.path)
        s = replace(
            s,
            generated_at=at,
            observed_at=at,
            recent_events=tuple(events),
            runtime=replace(s.runtime, status=status, ready=ready),
        )
        assert self.plane.publish(s)

    async def poll(self, now=NOW):
        return await self.worker.poll_once(now=now)


def test_first_boot_new_event_once_and_restart_cursor(tmp_path):
    async def scenario():
        h = Harness(tmp_path)
        h.publish([event(0)])
        assert await h.poll() == 0
        h.publish([event(0), event(1)])
        assert await h.poll() == 1
        assert await h.poll() == 0
        h.worker = h.restart()
        assert await h.poll() == 0
        assert len(h.sent) == 1
        assert h.store.path.stat().st_size < MAX_CURSOR_BYTES

    asyncio.run(scenario())


def test_multiple_events_and_recipients_are_deterministic(tmp_path):
    async def scenario():
        h = Harness(tmp_path, (7, 8))
        h.publish()
        await h.poll()
        h.publish(
            [event(1), event(2, "CLOSED", "WIN"), event(3, "EXPIRED", "NOT_TRIGGERED")]
        )
        assert await h.poll() == 6
        assert [x[0] for x in h.sent] == [7, 7, 7, 8, 8, 8]
        assert [x[1].split()[1] for x in h.sent[:3]] == ["OPEN", "CLOSED", "EXPIRED"]
        h.worker = h.restart()
        assert await h.poll() == 0

    asyncio.run(scenario())


def test_corrupt_cursor_reseeds_without_replaying(tmp_path):
    async def scenario():
        h = Harness(tmp_path)
        h.publish([event(1)])
        h.store.path.write_bytes(b"corrupt")
        assert await h.poll() == 0
        assert h.store.load() is not None
        h.publish([event(1), event(2)])
        assert await h.poll() == 1

    asyncio.run(scenario())


def test_missing_destination_never_reads_or_sends(tmp_path, monkeypatch):
    async def scenario():
        h = Harness(tmp_path, ())

        def forbidden(*a, **k):
            raise AssertionError("disabled observer IO")

        monkeypatch.setattr(h.plane.store, "read", forbidden)
        assert await h.poll() == 0
        assert not h.store.path.exists()

    asyncio.run(scenario())


def test_send_failure_is_claimed_and_not_replayed(tmp_path):
    async def scenario():
        h = Harness(tmp_path)
        h.publish()
        await h.poll()
        attempts = []

        async def failed(chat, text):
            attempts.append(chat)
            raise RuntimeError("SDK failed")

        h.worker.send = failed
        h.publish([event(1)])
        assert await h.poll() == 0
        assert h.worker.send_failures == 1
        assert await h.poll() == 0 and attempts == [7]
        h.worker = h.restart()
        assert await h.poll() == 0

    asyncio.run(scenario())


def test_persistence_failure_sends_nothing(tmp_path, monkeypatch):
    async def scenario():
        h = Harness(tmp_path)
        h.publish()
        await h.poll()
        h.publish([event(1)])
        original = h.store.save

        def failure(*a):
            raise OSError("disk unavailable")

        monkeypatch.setattr(h.store, "save", failure)
        assert await h.poll() == 0 and not h.sent
        monkeypatch.setattr(h.store, "save", original)
        assert await h.poll() == 1

    asyncio.run(scenario())


def test_stale_never_sends_trades_and_health_edges_survive_restart(tmp_path):
    async def scenario():
        h = Harness(tmp_path)
        h.publish()
        await h.poll()
        h.publish([event(1)])
        assert await h.poll(NOW + timedelta(seconds=91)) == 1
        assert "устарел" in h.sent[-1][1] and "OPEN" not in h.sent[-1][1]
        assert await h.poll(NOW + timedelta(seconds=92)) == 0
        h.worker = h.restart()
        assert await h.poll(NOW + timedelta(seconds=93)) == 0
        h.publish([event(1)], at=NOW + timedelta(seconds=94))
        assert await h.poll(NOW + timedelta(seconds=94)) == 2
        assert "восстановлен" in h.sent[-2][1]
        assert await h.poll(NOW + timedelta(seconds=95)) == 0

    asyncio.run(scenario())


def test_degraded_recovery_edges_once(tmp_path):
    async def scenario():
        h = Harness(tmp_path)
        h.publish()
        await h.poll()
        h.publish(status="DEGRADED", ready=False)
        assert await h.poll() == 1
        assert "DEGRADED" in h.sent[-1][1]
        assert await h.poll() == 0
        h.worker = h.restart()
        assert await h.poll() == 0
        h.publish()
        assert await h.poll() == 1
        assert "восстановлен" in h.sent[-1][1]
        assert await h.poll() == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("damage", ["missing", "malformed", "unsupported", "future"])
def test_invalid_snapshot_health_only_and_no_claims(tmp_path, damage):
    async def scenario():
        h = Harness(tmp_path)
        h.publish()
        await h.poll()
        if damage == "missing":
            h.plane.store.path.unlink()
        elif damage == "malformed":
            h.plane.store.path.write_bytes(b"broken")
        elif damage == "unsupported":
            h.plane.store.path.write_bytes(b'{"schema_version":999}')
        else:
            h.publish([event(1)], at=NOW + timedelta(seconds=100))
        assert await h.poll() == 1
        assert "OPEN" not in h.sent[-1][1]
        assert await h.poll() == 0

    asyncio.run(scenario())


def test_initial_missing_then_fresh_seeds_current_buffer(tmp_path):
    async def scenario():
        h = Harness(tmp_path)
        assert await h.poll() == 0
        h.publish([event(1)])
        assert await h.poll() == 1
        assert "восстановлен" in h.sent[0][1]
        assert len(h.sent) == 1

    asyncio.run(scenario())


def test_same_timestamp_stable_tie_order_and_duplicate_workers(tmp_path):
    async def scenario():
        h = Harness(tmp_path)
        h.publish()
        await h.poll()
        values = sorted(
            [replace(event(i), occurred_at=NOW) for i in range(3)],
            key=lambda e: e.order_key,
        )
        h.publish(values)
        second = h.restart()
        await asyncio.gather(h.poll(), second.poll_once(now=NOW))
        assert len(h.sent) == 3
        assert await h.poll() == 0

    asyncio.run(scenario())


def test_new_recipient_seeds_instead_of_replaying_and_removed_pruned(tmp_path):
    async def scenario():
        h = Harness(tmp_path)
        h.publish([event(1)])
        await h.poll()
        config = ScalperNotifyConfig((7, 8), 5, h.store.path)
        worker = ScalperNotifier(h.plane.store, h.store, config, h.send)
        assert await worker.poll_once(now=NOW) == 0
        h.publish([event(1), event(2)])
        assert await worker.poll_once(now=NOW) == 2
        assert await h.poll() == 0
        assert set(h.store.load()) == {7}

    asyncio.run(scenario())


def test_cursor_alias_and_bad_config_are_fail_closed(tmp_path):
    with pytest.raises(ValueError):
        ScalperNotifyConfig((7,), 1)
    with pytest.raises(ValueError):
        ScalperNotifyConfig(tuple(range(1, 12)), 10)
    p = plane(tmp_path)
    with (
        pytest.raises(ValueError),
        ScalperCursorStore(p.store.path, p.store.path).exclusive(),
    ):
        pass


def test_start_stop_idempotent_and_cancel_during_send_keeps_claim(tmp_path):
    async def scenario():
        h = Harness(tmp_path)
        h.publish()
        await h.poll()
        entered = asyncio.Event()

        async def waiting(chat, text):
            entered.set()
            await asyncio.Event().wait()

        h.worker.send = waiting
        h.publish([event(1)])
        h.worker.start()
        first = h.worker._task
        h.worker.start()
        assert first is h.worker._task
        await asyncio.wait_for(entered.wait(), 1)
        await h.worker.stop()
        await h.worker.stop()
        h.worker = h.restart()
        assert await h.poll() == 0

    asyncio.run(scenario())


def test_recent_view_newest_ten_no_missing_data_labels(tmp_path):
    h = Harness(tmp_path)
    h.publish([event(i, "CLOSED", None) for i in range(20)])
    messages = ScalperCommands(h.plane.store).execute("scalper_recent", now=NOW)
    text = "\n".join(messages)
    assert text.count("CLOSED") == 10
    assert all(len(m) <= SAFE_MESSAGE_LIMIT for m in messages)
    assert "WIN" not in text and "LOSS" not in text
    assert (
        event(19).occurred_at.isoformat() in text
        and event(0).occurred_at.isoformat() not in text
    )


@pytest.mark.parametrize(
    "damage,word",
    [
        ("stale", "устарел"),
        ("malformed", "повреждён"),
        ("unsupported", "не поддерживается"),
        ("future", "будущем"),
    ],
)
def test_recent_invalid_snapshots_fail_closed(tmp_path, damage, word):
    h = Harness(tmp_path)
    h.publish([event(1)])
    when = NOW
    if damage == "stale":
        when = NOW + timedelta(seconds=91)
    if damage == "malformed":
        h.plane.store.path.write_bytes(b"broken")
    if damage == "unsupported":
        h.plane.store.path.write_bytes(b'{"schema_version":999}')
    if damage == "future":
        h.publish([event(1)], at=NOW + timedelta(seconds=100))
    text = " ".join(ScalperCommands(h.plane.store).execute("scalper_recent", now=when))
    assert word in text and "OPEN" not in text


def test_cursor_corruption_size_and_no_snapshot_overwrite(tmp_path):
    async def scenario():
        h = Harness(tmp_path)
        h.publish([event(1)])
        original = h.plane.store.path.read_bytes()
        h.store.path.write_bytes(b"x" * (MAX_CURSOR_BYTES + 1))
        assert await h.poll() == 0
        assert h.store.path.stat().st_size < MAX_CURSOR_BYTES
        assert h.plane.store.path.read_bytes() == original
        assert not list(tmp_path.glob(".scalper-notify-*"))

    asyncio.run(scenario())


def test_partial_multichat_send_failure_does_not_replay_success_or_failure(tmp_path):
    async def scenario():
        h = Harness(tmp_path, (7, 8))
        h.publish()
        await h.poll()
        attempts = []

        async def send(chat, text):
            attempts.append(chat)
            if chat == 8:
                raise OSError("delivery uncertain")

        h.worker.send = send
        h.publish([event(1)])
        assert await h.poll() == 1
        h.worker = h.restart()
        assert await h.poll() == 0
        assert attempts == [7, 8]

    asyncio.run(scenario())


def test_late_event_behind_durable_watermark_never_replays_old_timeline(tmp_path):
    async def scenario():
        h = Harness(tmp_path)
        h.publish([event(10)])
        await h.poll()
        h.publish([event(1), event(10)])
        assert await h.poll() == 0
        h.publish([event(10), event(11)])
        assert await h.poll() == 1

    asyncio.run(scenario())


def test_crash_temp_is_reused_without_unbounded_orphan_growth(tmp_path):
    async def scenario():
        h = Harness(tmp_path)
        h.publish()
        await h.poll()
        temporary = h.store.path.with_suffix(".json.tmp")
        for i in range(1, 11):
            temporary.write_bytes(b"simulated crash orphan")
            h.publish([event(i)])
            assert await h.poll() == 1
            assert not temporary.exists()
        assert (
            len(list(tmp_path.glob("cursor.json*"))) == 2
        )  # state + fixed zero-byte lock

    asyncio.run(scenario())
