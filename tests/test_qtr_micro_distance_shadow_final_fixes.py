from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path
from typing import Any

import pytest
from test_qtr_micro_distance_shadow import NOW, PriceReaderSpy, _context
from test_qtr_micro_distance_shadow_fallback import _observer, _run
from test_qtr_micro_distance_shadow_review_regressions import _plan

from market_signal_assistant.qtr_micro.distance_shadow import (
    DistanceShadowDispatcher,
    summarize_distance_shadow,
)
from market_signal_assistant.qtr_micro.distance_shadow_accounting import (
    SEAL_SCHEMA_VERSION,
    read_shadow_accounting,
)


async def _finish_without_external_drain(
    dispatcher: DistanceShadowDispatcher, finalizer: asyncio.Task[dict[str, Any]]
) -> dict[str, Any]:
    try:
        return await asyncio.wait_for(asyncio.shield(finalizer), 2)
    except TimeoutError:
        # Clean up the historical deadlock, then fail instead of hanging pytest.
        await dispatcher.wait()
        await finalizer
        pytest.fail("Finalization required an external second drain.")


@pytest.mark.parametrize("count,cancel_at", [(2, 1), (5, 3)])
def test_finalization_recovers_from_mid_drain_worker_cancellation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, count: int, cancel_at: int
) -> None:
    entered, release = threading.Event(), threading.Event()
    calls = 0

    def provider(candidate: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == cancel_at:
            entered.set()
            assert release.wait(3)
        return _context()

    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    dispatcher = DistanceShadowDispatcher(
        observer, fallback_provider=provider, clock=lambda: NOW
    )
    joined = asyncio.Event()
    original_join = dispatcher._queue.join

    async def joining() -> None:
        joined.set()
        await original_join()

    monkeypatch.setattr(dispatcher._queue, "join", joining)

    async def exercise() -> None:
        cycle = dispatcher.enqueue(_plan(count), NOW)
        dispatcher.production_completed(cycle, NOW)
        assert await asyncio.to_thread(entered.wait, 2)
        finalizer = asyncio.create_task(dispatcher.finalize())
        await joined.wait()
        assert dispatcher._admission_closed_at is not None
        assert dispatcher.accounting.snapshot()["pending_count"] > 0
        assert not observer.accounting_path.with_suffix(".seal.json").exists()
        worker = dispatcher._worker
        assert worker is not None
        worker.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await worker
        seal = await _finish_without_external_drain(dispatcher, finalizer)
        assert seal["pending_count"] == 0
        assert seal["accepted_count"] == count
        assert seal["cancelled_invalid_count"] == seal["invalid_observations"] == 1
        assert seal["completed_count"] == seal["evidence_record_count"] == count - 1
        assert dispatcher._queue.empty() and not dispatcher._cycles
        assert dispatcher.enqueue(_plan(), NOW) is None

    asyncio.run(exercise())
    records = [
        json.loads(line) for line in observer._evidence.path.read_bytes().splitlines()
    ]
    assert len({r["candidate_id"] for r in records}) == count - 1
    assert (
        summarize_distance_shadow(observer._evidence.path)["category"]
        == "EXPERIMENT_INVALID"
    )


@pytest.mark.parametrize("completed_before_cancel", [0, 1])
def test_termination_before_pickup_or_immediately_after_task_done(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, completed_before_cancel: int
) -> None:
    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    dispatcher = DistanceShadowDispatcher(
        observer, fallback_provider=lambda c: _context(), clock=lambda: NOW
    )
    entered = asyncio.Event()
    block = asyncio.Event()
    original_get = dispatcher._queue.get
    get_calls = 0

    async def pickup() -> Any:
        nonlocal get_calls
        get_calls += 1
        if get_calls == completed_before_cancel + 1:
            entered.set()
            await block.wait()  # cancellation occurs before removing an item
        return await original_get()

    monkeypatch.setattr(dispatcher._queue, "get", pickup)

    async def exercise() -> None:
        cycle = dispatcher.enqueue(_plan(3), NOW)
        dispatcher.production_completed(cycle, NOW)
        await entered.wait()
        assert (
            dispatcher.accounting.snapshot()["completed_count"]
            == completed_before_cancel
        )
        finalizer = asyncio.create_task(dispatcher.finalize())
        await asyncio.sleep(0)
        worker = dispatcher._worker
        assert worker is not None
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker
        seal = await _finish_without_external_drain(dispatcher, finalizer)
        assert seal["accepted_count"] == seal["completed_count"] == 3
        assert seal["pending_count"] == seal["cancelled_invalid_count"] == 0
        assert seal["invalid_observations"] == 0
        assert dispatcher._queue.empty()

    asyncio.run(exercise())


def test_concurrent_drainers_share_exactly_one_replacement_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered, release = threading.Event(), threading.Event()
    calls = 0

    def provider(candidate: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 1:
            entered.set()
            assert release.wait(3)
        return _context()

    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    dispatcher = DistanceShadowDispatcher(
        observer, fallback_provider=provider, clock=lambda: NOW
    )
    original_run = dispatcher._run
    active = peak = starts = 0

    async def tracked_worker() -> None:
        nonlocal active, peak, starts
        starts += 1
        active += 1
        peak = max(peak, active)
        try:
            await original_run()
        finally:
            active -= 1

    monkeypatch.setattr(dispatcher, "_run", tracked_worker)

    async def exercise() -> None:
        cycle = dispatcher.enqueue(_plan(5), NOW)
        dispatcher.production_completed(cycle, NOW)
        assert await asyncio.to_thread(entered.wait, 2)
        finalizer = asyncio.create_task(dispatcher.finalize())
        drainers = [asyncio.create_task(dispatcher.wait()) for _ in range(4)]
        await asyncio.sleep(0.01)
        worker = dispatcher._worker
        assert worker is not None
        worker.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await worker
        seal = await _finish_without_external_drain(dispatcher, finalizer)
        await asyncio.gather(*drainers)
        assert starts == 2 and peak == 1
        assert seal["completed_count"] == 4
        assert seal["cancelled_invalid_count"] == 1 and seal["pending_count"] == 0
        records = [
            json.loads(line)
            for line in observer._evidence.path.read_bytes().splitlines()
        ]
        assert len(records) == len({r["candidate_id"] for r in records}) == 4

    asyncio.run(exercise())


@pytest.mark.parametrize("version", [None, "1", True, {}, 0, 2, 999])
def test_owner_seal_schema_mutation_cannot_certify(
    tmp_path: Path, version: Any
) -> None:
    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    dispatcher = DistanceShadowDispatcher(
        observer, fallback_provider=lambda c: _context(), clock=lambda: NOW
    )

    async def exercise() -> None:
        await _run(dispatcher)
        await dispatcher.finalize()

    asyncio.run(exercise())
    path = observer.accounting_path
    owner_path = path.with_suffix(".owner.json")
    owner = json.loads(owner_path.read_bytes())
    assert owner["seal_schema_version"] == SEAL_SCHEMA_VERSION
    assert (
        read_shadow_accounting(path, run_id=observer.run_id)["seal_schema_version"]
        == SEAL_SCHEMA_VERSION
    )
    if version is None:
        owner.pop("seal_schema_version")
    else:
        owner["seal_schema_version"] = version
    owner_path.write_text(json.dumps(owner), encoding="utf8")
    with pytest.raises(ValueError):
        read_shadow_accounting(path, run_id=observer.run_id)
    assert not summarize_distance_shadow(observer._evidence.path)["finalized"]
