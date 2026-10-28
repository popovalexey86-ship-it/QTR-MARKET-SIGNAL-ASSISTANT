from __future__ import annotations

import ast
import asyncio
import hashlib
import threading
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from test_qtr_micro_distance_shadow import NOW, PriceReaderSpy, _context, _snapshot
from test_qtr_micro_distance_shadow_fallback import _observer, _run

from market_signal_assistant.qtr_micro.distance_shadow import (
    DistanceShadowDispatcher,
    JsonlDistanceShadowEvidence,
    summarize_distance_shadow,
)
from market_signal_assistant.qtr_micro.distance_shadow_accounting import (
    read_shadow_accounting,
)
from market_signal_assistant.qtr_setup_pilot.notifications import (
    QtrNotificationReason,
    QtrSetupDecision,
    QtrSetupNotificationPlan,
)
from market_signal_assistant.telegram import bot


def _plan(count: int = 1) -> QtrSetupNotificationPlan:
    decisions = []
    for i in range(count):
        original = _snapshot(episode=str(i % 30), distance=0.2).decision
        candidate = replace(
            original.candidate,
            source_input=replace(
                original.candidate.source_input,
                snapshot_ids=(f"scan-{i}",),
            ),
        )
        decisions.append(replace(original, candidate=candidate))
    return QtrSetupNotificationPlan(NOW, tuple(decisions), {})


def test_healthy_checkpoint_then_overflow_and_failed_persistence_cannot_certify(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    d = DistanceShadowDispatcher(
        observer, fallback_provider=lambda c: _context(), clock=lambda: NOW
    )

    async def exercise() -> None:
        cycle = d.enqueue(_plan(100), NOW)
        d.production_completed(cycle, NOW)
        await d.wait()
        previous = observer.accounting_path.read_bytes()
        healthy = read_shadow_accounting(
            observer.accounting_path, run_id=observer.run_id, require_sealed=False
        )
        assert healthy["completed_count"] == 100
        assert healthy["invalid_observations"] == 0

        def fail(snapshot: Any) -> None:
            raise OSError("accounting unavailable")

        monkeypatch.setattr(d.accounting, "_append", fail)
        assert d.enqueue(_plan(129), NOW) is None
        await d.wait()
        assert observer.accounting_path.read_bytes() == previous
        assert d.invalid_observations == 129
        with pytest.raises(RuntimeError, match="unavailable"):
            await d.finalize()

    asyncio.run(exercise())
    for override in (None, 0):
        report = summarize_distance_shadow(
            tmp_path / "evidence.jsonl", invalid_observations=override
        )
        assert report["valid_records"] == 100
        assert report["unique_episodes"] == 30
        assert not report["finalized"]
        assert report["invalid_observations"] is None
        assert report["category"] == "EXPERIMENT_INVALID"


def test_only_sealed_run_can_produce_final_verdict_and_binds_evidence(
    tmp_path: Path,
) -> None:
    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    d = DistanceShadowDispatcher(
        observer, fallback_provider=lambda c: _context(), clock=lambda: NOW
    )

    async def exercise() -> None:
        await _run(d)
        provisional = summarize_distance_shadow(
            tmp_path / "evidence.jsonl", invalid_observations=0
        )
        assert provisional["category"] == "EXPERIMENT_INVALID"
        assert not provisional["finalized"]
        seal = await d.finalize()
        checkpoint = read_shadow_accounting(
            observer.accounting_path, run_id=observer.run_id
        )
        assert checkpoint == seal
        assert seal["accepted_count"] == seal["completed_count"] == 1
        assert seal["evidence_record_count"] == 1
        assert (
            seal["evidence_sha256"]
            == hashlib.sha256((tmp_path / "evidence.jsonl").read_bytes()).hexdigest()
        )
        assert seal["finalized_at"] and seal["admission_closed_at"]
        assert seal["revision"] == d.accounting.snapshot()["revision"]
        before = observer.accounting_path.read_bytes()
        assert d.enqueue(_plan(), NOW) is None
        assert observer.accounting_path.read_bytes() == before
        with pytest.raises(RuntimeError, match="closed"):
            d.accounting.change(queue_dropped_count=1)
        assert (await d.finalize()) == seal  # idempotent, no second publication

    asyncio.run(exercise())
    summary = summarize_distance_shadow(
        tmp_path / "evidence.jsonl",
        minimum_conclusive_snapshots=1,
        minimum_conclusive_episodes=1,
    )
    assert summary["finalized"]
    assert summary["category"] == "NO_MATERIAL_FUNNEL_CHANGE"
    with pytest.raises(ValueError, match="sealed"):
        JsonlDistanceShadowEvidence(tmp_path / "evidence.jsonl", run_id=observer.run_id)
    with (tmp_path / "evidence.jsonl").open("ab") as stream:
        stream.write(b"\n")
    assert (
        summarize_distance_shadow(tmp_path / "evidence.jsonl")["category"]
        == "EXPERIMENT_INVALID"
    )


@pytest.mark.parametrize("failure", ["seal_fsync", "seal_publish", "final_checkpoint"])
def test_failed_finalization_never_publishes_usable_seal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    d = DistanceShadowDispatcher(
        observer, fallback_provider=lambda c: _context(), clock=lambda: NOW
    )

    async def exercise() -> None:
        await _run(d)
        if failure == "final_checkpoint":

            def fail_append(snapshot: Any) -> None:
                raise OSError("checkpoint fsync failure")

            monkeypatch.setattr(d.accounting, "_append", fail_append)
        elif failure == "seal_publish":

            def fail_replace(*args: Any, **kwargs: Any) -> None:
                raise OSError("seal publication failure")

            monkeypatch.setattr(Path, "replace", fail_replace)
        else:
            publish = d.accounting._publish_seal

            def fail_seal(seal: Any) -> None:
                with monkeypatch.context() as local:

                    def fail_fsync(fd: int) -> None:
                        raise OSError("seal fsync failure")

                    local.setattr("os.fsync", fail_fsync)
                    publish(seal)

            monkeypatch.setattr(d.accounting, "_publish_seal", fail_seal)
        with pytest.raises((RuntimeError, OSError)):
            await d.finalize()

    asyncio.run(exercise())
    assert not observer.accounting_path.with_suffix(".seal.json").exists()
    report = summarize_distance_shadow(
        tmp_path / "evidence.jsonl", invalid_observations=0
    )
    assert report["category"] == "EXPERIMENT_INVALID"
    assert not report["finalized"]


@pytest.mark.parametrize("failure", ["constructor", "wiring"])
def test_actual_main_shadow_setup_failure_does_not_prevent_production_continuation(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    failure: str,
) -> None:
    import market_signal_assistant.qtr_micro.distance_shadow_context as context_module

    def fail(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("secret failure detail must not leak")

    if failure == "constructor":
        monkeypatch.setattr(context_module, "ReadOnlyShadowContextProvider", fail)
    source = Path(bot.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    wiring = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.If)
        and node.body
        and isinstance(node.body[0], ast.Assign)
        and isinstance(node.body[0].value, ast.Call)
        and isinstance(node.body[0].value.func, ast.Name)
        and node.body[0].value.func.id == "_wire_distance_shadow_fallback"
    )
    continued = []
    namespace = {
        "distance_shadow_dispatcher": SimpleNamespace(
            set_fallback_provider=fail if failure == "wiring" else lambda p: None
        ),
        "shadow_quote_client": SimpleNamespace(
            wallet_equity=lambda: 1, instrument_rules=lambda s: None
        ),
        "qtr_micro_runtime": SimpleNamespace(shadow_preflight_snapshot=lambda: None),
        "_wire_distance_shadow_fallback": bot._wire_distance_shadow_fallback,
    }
    exec(
        compile(ast.Module(body=[wiring], type_ignores=[]), "<main-wiring>", "exec"),
        namespace,
    )
    continued.append("production startup continuation")
    assert continued and namespace["distance_shadow_dispatcher"] is None
    assert "Shadow unavailable" in caplog.text
    assert "secret failure detail" not in caplog.text


def test_cancellation_awaiting_production_has_explicit_invalid_terminal(
    tmp_path: Path,
) -> None:
    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    d = DistanceShadowDispatcher(observer, clock=lambda: NOW)

    async def exercise() -> None:
        d.enqueue(_plan(), NOW)
        await asyncio.sleep(0)
        assert d._worker is not None
        d._worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await d._worker
        await d.wait()
        live = d.accounting.snapshot()
        assert live["completed_count"] == 0
        assert live["cancelled_invalid_count"] == 1
        assert live["invalid_observations"] == 1
        assert live["pending_count"] == 0
        seal = await d.finalize()
        assert seal["evidence_record_count"] == 0
        assert seal["invalid_observations"] == 1

    asyncio.run(exercise())
    assert not (tmp_path / "evidence.jsonl").exists()


@pytest.mark.parametrize("cancel_worker", [False, True])
def test_finalization_drains_outstanding_fallback_before_publishing_seal(
    tmp_path: Path,
    cancel_worker: bool,
) -> None:
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()

    def acquire(candidate: Any) -> Any:
        entered.set()
        assert release.wait(5)
        finished.set()
        return _context()

    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    d = DistanceShadowDispatcher(observer, fallback_provider=acquire, clock=lambda: NOW)

    async def exercise() -> None:
        cycle = d.enqueue(_plan(), NOW)
        d.production_completed(cycle, NOW)
        assert await asyncio.to_thread(entered.wait, 2)
        assert d._worker is not None
        worker = d._worker
        if cancel_worker:
            worker.cancel()
        finalization = asyncio.create_task(d.finalize())
        await asyncio.sleep(0.02)
        assert not finalization.done()
        assert not observer.accounting_path.with_suffix(".seal.json").exists()
        assert d.accounting.snapshot()["pending_count"] == 1
        release.set()
        if cancel_worker:
            with pytest.raises(asyncio.CancelledError):
                await worker
        seal = await finalization
        assert finished.is_set()
        assert seal["completed_count"] == (0 if cancel_worker else 1)
        assert seal["cancelled_invalid_count"] == int(cancel_worker)
        assert seal["invalid_observations"] == int(cancel_worker)
        assert seal["pending_count"] == 0

    asyncio.run(exercise())


def test_finalization_waiter_cancellation_does_not_orphan_thread(
    tmp_path: Path,
) -> None:
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()

    def acquire(candidate: Any) -> Any:
        entered.set()
        assert release.wait(5)
        finished.set()
        return _context()

    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    d = DistanceShadowDispatcher(observer, fallback_provider=acquire, clock=lambda: NOW)

    async def exercise() -> None:
        cycle = d.enqueue(_plan(), NOW)
        d.production_completed(cycle, NOW)
        assert await asyncio.to_thread(entered.wait, 2)
        waiter = asyncio.create_task(d.finalize())
        await asyncio.sleep(0)
        waiter.cancel()
        await asyncio.sleep(0.02)
        assert not waiter.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert finished.is_set()
        assert (await d.finalize())["completed_count"] == 1

    asyncio.run(exercise())


def test_enqueue_failure_counts_only_qualified_and_leaves_no_partial_batch(
    tmp_path: Path,
) -> None:
    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    d = DistanceShadowDispatcher(observer, clock=lambda: NOW)
    qualified = _snapshot().decision
    unqualified = QtrSetupDecision(
        None, qualified.candidate, False, QtrNotificationReason.QUALITY_BELOW_THRESHOLD
    )

    async def exercise() -> None:
        plan = QtrSetupNotificationPlan(NOW, (qualified, unqualified, unqualified), {})
        with pytest.raises(ValueError):
            d.enqueue(plan, NOW.replace(tzinfo=None))
        await d.wait()
        counters = d.accounting.snapshot()
        assert counters["enqueue_dropped_count"] == 1
        assert counters["invalid_observations"] == 1
        assert counters["accepted_count"] == 0
        assert not d._cycles and d._queue.empty() and d._worker is None

    asyncio.run(exercise())


def test_mid_batch_preparation_failure_is_transactional(tmp_path: Path) -> None:
    calls = 0

    def clock() -> Any:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("second queued item preparation failed")
        return NOW

    d = DistanceShadowDispatcher(
        _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0)),
        clock=clock,
    )

    async def exercise() -> None:
        with pytest.raises(RuntimeError):
            d.enqueue(_plan(2), NOW)
        await d.wait()
        assert d._queue.empty() and not d._cycles and d._worker is None
        assert d.accounting.snapshot()["accepted_count"] == 0
        assert d.accounting.snapshot()["enqueue_dropped_count"] == 2

    asyncio.run(exercise())


def test_seal_does_not_hide_journal_accounting_mismatch(tmp_path: Path) -> None:
    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    d = DistanceShadowDispatcher(
        observer, fallback_provider=lambda c: _context(), clock=lambda: NOW
    )

    async def exercise() -> None:
        await _run(d)
        path = tmp_path / "evidence.jsonl"
        with path.open("ab") as stream:
            stream.write(path.read_bytes())  # unexpected duplicate evidence
        with pytest.raises(ValueError, match="Duplicate"):
            await d.finalize()

    asyncio.run(exercise())
    report = summarize_distance_shadow(
        tmp_path / "evidence.jsonl", invalid_observations=0
    )
    assert report["category"] == "EXPERIMENT_INVALID"
    assert not report["finalized"]


def test_cancellation_during_evidence_thread_cannot_appear_completed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    observe = observer.observe

    def delayed(*args: Any, **kwargs: Any) -> Any:
        result = observe(*args, **kwargs)
        entered.set()
        assert release.wait(5)
        finished.set()
        return result

    monkeypatch.setattr(observer, "observe", delayed)
    d = DistanceShadowDispatcher(
        observer, fallback_provider=lambda c: _context(), clock=lambda: NOW
    )

    async def exercise() -> None:
        cycle = d.enqueue(_plan(), NOW)
        d.production_completed(cycle, NOW)
        assert await asyncio.to_thread(entered.wait, 2)
        assert d._worker is not None
        worker = d._worker
        worker.cancel()
        finalization = asyncio.create_task(d.finalize())
        await asyncio.sleep(0.02)
        assert not worker.done() and not finalization.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await worker
        with pytest.raises(RuntimeError, match="mismatch"):
            await finalization
        assert finished.is_set()
        counters = d.accounting.snapshot()
        assert counters["completed_count"] == 0
        assert counters["cancelled_invalid_count"] == 1
        assert counters["invalid_observations"] == 1
        assert counters["pending_count"] == 0

    asyncio.run(exercise())
    assert not observer.accounting_path.with_suffix(".seal.json").exists()


def test_unqualified_only_plan_does_not_affect_admission_counters(
    tmp_path: Path,
) -> None:
    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    d = DistanceShadowDispatcher(observer, clock=lambda: NOW)
    unqualified = QtrSetupDecision(
        None,
        _snapshot().decision.candidate,
        False,
        QtrNotificationReason.QUALITY_BELOW_THRESHOLD,
    )

    async def exercise() -> None:
        assert (
            d.enqueue(
                QtrSetupNotificationPlan(NOW, (unqualified,) * 2, {}),
                NOW.replace(tzinfo=None),
            )
            is None
        )
        await d.wait()
        assert d.invalid_observations == 0
        assert d.accounting.snapshot()["enqueue_dropped_count"] == 0
        assert d.accounting.snapshot()["accepted_count"] == 0

    asyncio.run(exercise())
