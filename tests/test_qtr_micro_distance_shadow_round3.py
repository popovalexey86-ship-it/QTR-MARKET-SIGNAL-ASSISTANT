from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from test_qtr_micro_distance_shadow import NOW, PriceReaderSpy, _context, _snapshot
from test_qtr_micro_distance_shadow_fallback import _observer, _run
from test_qtr_micro_distance_shadow_review_regressions import _plan

from market_signal_assistant.qtr_micro import distance_shadow_accounting as accounting
from market_signal_assistant.qtr_micro.distance_shadow import (
    DistanceShadowDispatcher,
    summarize_distance_shadow,
)
from market_signal_assistant.qtr_setup_pilot.notifications import (
    QtrSetupNotificationPlan,
)


def test_existing_owner_cannot_reset_invalid_counters(tmp_path: Path) -> None:
    path = tmp_path / "evidence.accounting.jsonl"
    first = accounting.ShadowAccounting(path, "run")
    first.change(queue_dropped_count=1, queue_overflow_count=1)
    asyncio.run(first.wait())
    before = path.read_bytes()
    with pytest.raises(ValueError, match="Existing"):
        accounting.ShadowAccounting(path, "run")
    assert path.read_bytes() == before
    assert first.snapshot()["invalid_observations"] == 1


@pytest.mark.parametrize("invocation", ["", " ", None, "2" * 32])
def test_existing_run_cannot_acquire_another_process_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, invocation: Any
) -> None:
    path = tmp_path / "evidence.accounting.jsonl"
    accounting.ShadowAccounting(path, "run")
    monkeypatch.setattr(accounting, "_current_invocation_id", lambda: invocation)
    with pytest.raises(ValueError):
        accounting.ShadowAccounting(path, "run")
    if invocation and invocation.strip():
        fresh = accounting.ShadowAccounting(tmp_path / "fresh.accounting.jsonl", "new")
        assert fresh.invocation_id == invocation


@pytest.mark.parametrize("counter", [*accounting.COUNTERS, "invalid_observations"])
def test_reader_rejects_every_cumulative_counter_regression(
    tmp_path: Path, counter: str
) -> None:
    path = tmp_path / "evidence.accounting.jsonl"
    writer = accounting.ShadowAccounting(path, "run")
    first = {
        **writer.snapshot(),
        **dict.fromkeys(accounting.COUNTERS, 1),
        "accepted_count": 3,
        "invalid_observations": 4,
        "revision": 1,
    }
    last = {**first, counter: 0, "revision": 2}
    path.write_text(json.dumps(first) + "\n" + json.dumps(last) + "\n")
    with pytest.raises(ValueError, match="regression"):
        accounting.read_shadow_accounting(path, run_id="run", require_sealed=False)


@pytest.mark.parametrize("missing", ["invocation_id", "owner_id"])
def test_reader_rejects_missing_persisted_ownership(
    tmp_path: Path, missing: str
) -> None:
    path = tmp_path / "evidence.accounting.jsonl"
    writer = accounting.ShadowAccounting(path, "run")
    asyncio.run(writer.wait())
    owner_path = path.with_suffix(".owner.json")
    owner = json.loads(owner_path.read_bytes())
    owner.pop(missing)
    owner_path.write_text(json.dumps(owner))
    with pytest.raises(ValueError):
        accounting.read_shadow_accounting(path, run_id="run", require_sealed=False)


def test_accounted_duplicates_cannot_satisfy_snapshot_gate(tmp_path: Path) -> None:
    path = tmp_path / "evidence.jsonl"
    observer = _observer(path, PriceReaderSpy(101.0))
    dispatcher = DistanceShadowDispatcher(
        observer, fallback_provider=lambda c: _context(), clock=lambda: NOW
    )
    decisions = tuple(
        _snapshot(episode=str(i % 30), distance=0.2).decision for i in range(100)
    )

    async def exercise() -> None:
        cycle = dispatcher.enqueue(QtrSetupNotificationPlan(NOW, decisions, {}), NOW)
        dispatcher.production_completed(cycle, NOW)
        await dispatcher.wait()
        assert dispatcher.accounting.snapshot()["completed_count"] == 100
        with pytest.raises(ValueError, match="Duplicate"):
            await dispatcher.finalize()

    asyncio.run(exercise())
    summary = summarize_distance_shadow(path)
    assert summary["raw_valid_records"] == 100
    assert summary["unique_observations"] == 30
    assert summary["duplicate_records"] == 70
    assert summary["unique_episodes"] == 30
    assert not summary["sample_gate"]["valid_snapshots_met"]
    assert summary["category"] == "EXPERIMENT_INVALID"


def test_same_candidate_in_later_cycles_is_distinct(tmp_path: Path) -> None:
    path = tmp_path / "evidence.jsonl"
    observer = _observer(path, PriceReaderSpy(101.0))
    dispatcher = DistanceShadowDispatcher(
        observer, fallback_provider=lambda c: _context(), clock=lambda: NOW
    )

    async def exercise() -> None:
        await _run(dispatcher)
        plan = QtrSetupNotificationPlan(NOW, (_snapshot(distance=0.2).decision,), {})
        dispatcher._clock = lambda: NOW + timedelta(microseconds=1)
        cycle = dispatcher.enqueue(plan, NOW + timedelta(microseconds=1))
        dispatcher.production_completed(cycle, NOW + timedelta(microseconds=1))
        # Maintain truthful acquisition/completion ordering for the later cycle.
        dispatcher._clock = lambda: NOW + timedelta(microseconds=2)
        await dispatcher.wait()
        await dispatcher.finalize()

    asyncio.run(exercise())
    records = [json.loads(line) for line in path.read_bytes().splitlines()]
    assert records[0]["candidate_id"] == records[1]["candidate_id"]
    assert records[0]["production_cycle_id"] != records[1]["production_cycle_id"]
    assert summarize_distance_shadow(path)["unique_observations"] == 2


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_cutoff",
        "bad_finalized",
        "naive_finalized",
        "reverse_order",
        "unknown_seal_schema",
        "unknown_accounting_schema",
        "unknown_experiment_schema",
        "changed_cutoff",
        "changed_finalized",
        "unique_count",
        "missing_identity",
    ],
)
def test_seal_metadata_mutations_fail_closed(tmp_path: Path, mutation: str) -> None:
    path = tmp_path / "evidence.jsonl"
    observer = _observer(path, PriceReaderSpy(101.0))
    dispatcher = DistanceShadowDispatcher(
        observer, fallback_provider=lambda c: _context(), clock=lambda: NOW
    )

    async def exercise() -> None:
        await _run(dispatcher)
        await dispatcher.finalize()

    asyncio.run(exercise())
    assert summarize_distance_shadow(path)["finalized"]
    seal_path = observer.accounting_path.with_suffix(".seal.json")
    seal = json.loads(seal_path.read_bytes())
    if mutation == "missing_cutoff":
        seal.pop("admission_closed_at")
    elif mutation == "bad_finalized":
        seal["finalized_at"] = "NOT-A-TIMESTAMP"
    elif mutation == "naive_finalized":
        seal["finalized_at"] = "2026-10-27T12:00:00"
    elif mutation == "reverse_order":
        seal["admission_closed_at"] = "2099-01-01T00:00:00+00:00"
    elif mutation.startswith("unknown_"):
        seal[mutation.removeprefix("unknown_") + "_version"] = 999
    elif mutation == "changed_cutoff":
        seal["admission_closed_at"] = (NOW + timedelta(seconds=1)).isoformat()
    elif mutation == "changed_finalized":
        seal["finalized_at"] = NOW.isoformat()
    elif mutation == "unique_count":
        seal["unique_observation_count"] = 0
    else:
        seal["invocation_id"] = ""
    seal_path.write_text(json.dumps(seal))
    summary = summarize_distance_shadow(path)
    assert not summary["finalized"]
    assert summary["category"] == "EXPERIMENT_INVALID"


@pytest.mark.parametrize(
    "stage", ["before_change", "after_change", "schedule", "second_put"]
)
def test_admission_failure_has_no_orphans_or_double_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    dispatcher = DistanceShadowDispatcher(observer, clock=lambda: NOW)
    original_change = dispatcher.accounting.change
    original_put = dispatcher._queue.put_nowait
    puts = 0

    def change(**increments: int) -> None:
        if stage == "before_change":
            raise RuntimeError("before accounting change")
        original_change(**increments)
        if stage == "after_change":
            raise RuntimeError("after accounting change")

    def schedule() -> None:
        raise RuntimeError("accounting scheduling failure")

    def put(item: Any) -> None:
        nonlocal puts
        puts += 1
        if puts == 2:
            raise RuntimeError("after first queue insertion")
        original_put(item)

    async def exercise() -> None:
        with monkeypatch.context() as local:
            if stage == "schedule":
                local.setattr(dispatcher.accounting, "_schedule", schedule)
            elif stage == "second_put":
                local.setattr(dispatcher._queue, "put_nowait", put)
            else:
                local.setattr(dispatcher.accounting, "change", change)
            with pytest.raises(RuntimeError):
                dispatcher.enqueue(_plan(2), NOW)
        await asyncio.wait_for(dispatcher.wait(), 2)
        assert dispatcher._queue.empty() and not dispatcher._cycles
        state = dispatcher.accounting.snapshot()
        assert state["accepted_count"] == (0 if stage == "before_change" else 2)
        assert state["failed_invalid_count"] == state["accepted_count"]
        assert state["pending_count"] == state["completed_count"] == 0
        assert state["enqueue_dropped_count"] == state["invalid_observations"] == 2
        if stage != "before_change":
            with pytest.raises(RuntimeError, match="unavailable"):
                await dispatcher.finalize()

    asyncio.run(exercise())


def test_final_metadata_is_in_hashed_checkpoint(tmp_path: Path) -> None:
    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    dispatcher = DistanceShadowDispatcher(
        observer, fallback_provider=lambda c: _context(), clock=lambda: NOW
    )

    async def exercise() -> None:
        await _run(dispatcher)
        seal = await dispatcher.finalize()
        raw = observer.accounting_path.read_bytes()
        last = json.loads(raw.splitlines()[-1])
        assert seal["accounting_sha256"] == hashlib.sha256(raw).hexdigest()
        for key in (
            "admission_closed_at",
            "finalized_at",
            "seal_schema_version",
            "unique_observation_count",
            "evidence_sha256",
        ):
            assert last[key] == seal[key]

    asyncio.run(exercise())


def test_rollback_preserves_other_cycle_completion_ownership(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    dispatcher = DistanceShadowDispatcher(
        observer, fallback_provider=lambda c: _context(), clock=lambda: NOW
    )

    async def exercise() -> None:
        first = dispatcher.enqueue(_plan(), NOW)
        original = dispatcher._queue.put_nowait
        calls = 0

        def put(item: Any) -> None:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("second insertion")
            original(item)

        with monkeypatch.context() as local:
            local.setattr(dispatcher._queue, "put_nowait", put)
            with pytest.raises(RuntimeError):
                dispatcher.enqueue(_plan(2), NOW)
        assert dispatcher._queue.qsize() == 1
        assert set(dispatcher._cycles) == {first}
        dispatcher.production_completed(first, NOW)
        await asyncio.wait_for(dispatcher.wait(), 2)
        state = dispatcher.accounting.snapshot()
        assert state["accepted_count"] == 3
        assert state["completed_count"] == 1
        assert state["failed_invalid_count"] == state["invalid_observations"] == 2
        assert state["pending_count"] == 0

    asyncio.run(exercise())


def test_overflow_accounting_failure_does_not_double_count_drops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    dispatcher = DistanceShadowDispatcher(observer, capacity=1, clock=lambda: NOW)

    def fail() -> None:
        raise RuntimeError("accounting scheduling failure")

    with monkeypatch.context() as local:
        local.setattr(dispatcher.accounting, "_schedule", fail)
        with pytest.raises(RuntimeError):
            dispatcher.enqueue(_plan(2), NOW)
    asyncio.run(dispatcher.wait())
    state = dispatcher.accounting.snapshot()
    assert state["queue_dropped_count"] == state["invalid_observations"] == 2
    assert state["enqueue_dropped_count"] == state["accepted_count"] == 0
    assert state["pending_count"] == 0


def test_async_persistence_failure_keeps_returned_cycle_reachable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    dispatcher = DistanceShadowDispatcher(
        observer, fallback_provider=lambda c: _context(), clock=lambda: NOW
    )

    def fail(snapshot: Any) -> None:
        raise OSError("accounting write failed")

    monkeypatch.setattr(dispatcher.accounting, "_append", fail)

    async def exercise() -> None:
        cycle = dispatcher.enqueue(_plan(), NOW)
        assert cycle in dispatcher._cycles
        await asyncio.sleep(0)
        dispatcher.production_completed(cycle, NOW)
        await asyncio.wait_for(dispatcher.wait(), 2)
        state = dispatcher.accounting.snapshot()
        assert state["completed_count"] == 1
        assert state["pending_count"] == 0
        assert not state["authoritative"]
        with pytest.raises(RuntimeError, match="unavailable"):
            await dispatcher.finalize()

    asyncio.run(exercise())
