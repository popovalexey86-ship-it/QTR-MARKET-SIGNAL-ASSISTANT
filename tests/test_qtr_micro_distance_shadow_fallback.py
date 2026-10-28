from __future__ import annotations

import asyncio
import json
import threading
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from test_qtr_micro import FakeClient, candidate, decision, position_from_plan
from test_qtr_micro_distance_shadow import (
    NOW,
    RULES,
    Clock,
    PriceReaderSpy,
    _context,
    _observe,
    _settings,
    _snapshot,
)

from market_signal_assistant.qtr_micro.distance_shadow import (
    DistanceShadowDispatcher,
    JsonlDistanceShadowEvidence,
    QtrMicroDistanceShadowObserver,
    TemporalEvidence,
    summarize_distance_shadow,
)
from market_signal_assistant.qtr_micro.distance_shadow_accounting import (
    read_shadow_accounting,
)
from market_signal_assistant.qtr_micro.distance_shadow_context import (
    ReadOnlyShadowContextProvider,
)
from market_signal_assistant.qtr_micro.journal import JsonlQtrMicroDecisionAudit
from market_signal_assistant.qtr_micro.models import MicroStage
from market_signal_assistant.qtr_micro.runtime import QtrMicroRuntime
from market_signal_assistant.qtr_micro.runtime_audit import JsonlQtrMicroRuntimeAudit
from market_signal_assistant.qtr_micro.state import JsonQtrMicroStateStore
from market_signal_assistant.qtr_setup_pilot.notifications import (
    QtrSetupNotificationPlan,
)


def _store(path: Path) -> JsonQtrMicroStateStore:
    store = JsonQtrMicroStateStore(path)
    store.save(_context().state)
    return store


def _provider(path: Path, **changes: Any) -> ReadOnlyShadowContextProvider:
    return ReadOnlyShadowContextProvider(
        **{
            "state_path": path,
            "equity_reader": lambda: 10_000.0,
            "rules_reader": lambda symbol: replace(RULES, symbol=symbol),
            "preflight_reader": lambda: _context().preflight,
            "clock": lambda: NOW,
            **changes,
        }
    )


async def _run(dispatcher: DistanceShadowDispatcher, *, publish: bool = False) -> None:
    snapshot = _snapshot(distance=0.20)
    cycle = dispatcher.enqueue(
        QtrSetupNotificationPlan(NOW, (snapshot.decision,), {}), NOW
    )
    assert cycle is not None
    if publish:
        dispatcher.capture_context(cycle, snapshot.decision.candidate, _context(), NOW)
    dispatcher.production_completed(cycle, NOW)
    await dispatcher.wait()


def _observer(path: Path, reader: PriceReaderSpy) -> QtrMicroDistanceShadowObserver:
    return QtrMicroDistanceShadowObserver(
        production_settings=_settings(),
        price_reader=reader,
        evidence=JsonlDistanceShadowEvidence(path),
        clock=lambda: NOW + timedelta(seconds=1),
    )


@pytest.mark.parametrize("cross_symbol", [False, True])
def test_real_production_episode_skip_has_fallback_and_identical_production_behavior(
    tmp_path: Path,
    cross_symbol: bool,
) -> None:
    async def exercise(enabled: bool, directory: Path) -> tuple[Any, ...]:
        directory.mkdir()
        store = _store(directory / "state.json")
        snapshot = _snapshot(distance=0.20)
        plan = decision(candidate(episode=snapshot.decision.candidate.episode_id)).plan
        assert plan is not None
        previous = position_from_plan(
            plan, stage=MicroStage.CLOSED, current_qty=0, journaled=True
        )
        if cross_symbol:
            previous = replace(previous, symbol="ETHUSDT")
        store.save(replace(_context().state, positions={previous.trade_id: previous}))
        client = FakeClient()
        quote = PriceReaderSpy(101.0)
        observer = _observer(directory / "evidence.jsonl", quote)
        dispatcher = DistanceShadowDispatcher(
            observer, fallback_provider=_provider(store.path), clock=lambda: NOW
        )
        runtime = QtrMicroRuntime(
            settings=_settings(),
            client=client,
            state_store=store,
            allowed_chat_ids=frozenset(),
            clock=lambda: NOW,
            decision_audit=JsonlQtrMicroDecisionAudit(directory / "decisions.jsonl"),
            runtime_audit=JsonlQtrMicroRuntimeAudit(directory / "runtime.jsonl"),
            distance_shadow_context_publisher=dispatcher.capture_context
            if enabled
            else None,
        )
        runtime._preflight_result = _context().preflight
        cycle = None
        if enabled:
            cycle = dispatcher.enqueue(
                QtrSetupNotificationPlan(NOW, (snapshot.decision,), {}), NOW
            )

        async def send(*args: object) -> None:
            raise AssertionError("Production skip must not send an entry.")

        await runtime.handle_candidates((snapshot.decision.candidate,), send, cycle)
        if enabled:
            dispatcher.production_completed(cycle, NOW)
            before = store.path.read_bytes()
            await dispatcher.wait()
            assert store.path.read_bytes() == before
            record = json.loads((directory / "evidence.jsonl").read_text())
            assert record["context_source"] == "SHADOW_READ_ONLY_FALLBACK"
            assert record["context_digest"] is not None
            assert record["fresh_price"] == 101.0
            assert all(b["initial"] is not None for b in record["branches"])
            assert all(
                b["initial_skip_reason"] == "duplicate_episode"
                for b in record["branches"]
            )
            assert quote.calls == ["BTCUSDT"]
            assert dispatcher.invalid_observations == 0
            assert (
                read_shadow_accounting(
                    observer.accounting_path,
                    run_id=observer.run_id,
                    require_sealed=False,
                )["completed_count"]
                == 1
            )
        return (
            client.orders,
            client.leverage_calls,
            client.cancelled_orders,
            client.instrument_rule_calls,
            store.path.read_bytes(),
        )

    off = asyncio.run(exercise(False, tmp_path / "off"))
    on = asyncio.run(exercise(True, tmp_path / "on"))
    assert on == off
    assert on[0] == []


def test_fallback_shares_one_isolated_context_and_quote(tmp_path: Path) -> None:
    store = _store(tmp_path / "state.json")
    before = store.path.read_bytes()
    context = _context()
    calls = []

    def acquire(item: Any) -> Any:
        calls.append(item)
        return context

    reader = PriceReaderSpy(101.0)
    observer = _observer(tmp_path / "evidence.jsonl", reader)
    dispatcher = DistanceShadowDispatcher(
        observer, fallback_provider=acquire, clock=Clock()
    )
    asyncio.run(_run(dispatcher))
    record = json.loads((tmp_path / "evidence.jsonl").read_text())
    context.state.positions.clear()
    assert len(calls) == 1
    assert reader.calls == ["BTCUSDT"]
    assert reader.order_calls == 0
    assert store.path.read_bytes() == before
    for key in ("candidate_id", "context_digest", "fresh_price", "quote_id"):
        assert {b[key] for b in record["branches"]} == {record[key]}
    assert [b["settings_override_fields"] for b in record["branches"]] == [
        [],
        ["max_entry_distance_atr"],
        ["max_entry_distance_atr"],
    ]
    assert record["context_acquired_at"] >= record["production_completed_at"]
    assert record["fresh_quote_at"] >= record["context_acquired_at"]
    asyncio.run(dispatcher.finalize())
    assert (
        summarize_distance_shadow(tmp_path / "evidence.jsonl")["invalid_observations"]
        == 0
    )


def test_provider_copies_factual_mutable_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path / "state.json")
    original = _context().state
    plan = decision(candidate()).plan
    assert plan is not None
    previous = position_from_plan(plan, stage=MicroStage.CLOSED)
    original.positions[previous.trade_id] = previous
    monkeypatch.setattr(
        "market_signal_assistant.qtr_micro.distance_shadow_context._state_from_json",
        lambda payload: original,
    )
    context = _provider(store.path)(_snapshot().decision.candidate)
    assert context.state == original
    assert context.state is not original
    original.positions.clear()
    assert len(context.state.positions) == 1
    store.save(replace(_context().state, blocked_reason="changed later"))
    assert context.state.blocked_reason is None


def test_published_context_never_uses_fallback(tmp_path: Path) -> None:
    def fail(item: Any) -> Any:
        raise AssertionError("Fallback called for a published context")

    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    dispatcher = DistanceShadowDispatcher(
        observer, fallback_provider=fail, clock=Clock()
    )
    asyncio.run(_run(dispatcher, publish=True))
    record = json.loads((tmp_path / "evidence.jsonl").read_text())
    assert record["context_source"] == "PUBLISHED_RUNTIME_CONTEXT"
    assert record["context_digest"] is not None
    assert dispatcher.accounting.snapshot()["fallback_failure_count"] == 0


@pytest.mark.parametrize("failure", ["corrupt", "missing", "unreadable"])
def test_invalid_state_never_repaired_and_failure_is_explicit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    state = tmp_path / "state.json"
    if failure != "missing":
        state.write_bytes(b"corrupt state")
    before = state.read_bytes() if state.exists() else None
    read_bytes = Path.read_bytes

    def guarded_read(path: Path) -> bytes:
        if path == state and failure == "unreadable":
            raise PermissionError("secret credential must not leak")
        return read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", guarded_read)

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("Production state repair/load/write/rename called")

    monkeypatch.setattr(JsonQtrMicroStateStore, "load", forbidden)
    monkeypatch.setattr(JsonQtrMicroStateStore, "save", forbidden)
    monkeypatch.setattr(JsonQtrMicroStateStore, "_backup_corrupt", forbidden)
    monkeypatch.setattr(Path, "rename", forbidden)
    monkeypatch.setattr(Path, "replace", forbidden)
    reader = PriceReaderSpy(101.0)
    observer = _observer(tmp_path / "evidence.jsonl", reader)
    dispatcher = DistanceShadowDispatcher(
        observer, fallback_provider=_provider(state), clock=Clock()
    )
    asyncio.run(_run(dispatcher))
    record = json.loads((tmp_path / "evidence.jsonl").read_text())
    assert record["fresh_price_error"] == "CONTEXT_FALLBACK_FAILED"
    assert record["context_failure_detail"].startswith("state_snapshot:")
    assert "secret" not in json.dumps(record)
    assert reader.calls == []
    assert (read_bytes(state) if state.exists() else None) == before
    assert not list(tmp_path.glob("*.corrupt"))
    assert dispatcher.accounting.snapshot()["fallback_failure_count"] == 1


def test_fallback_quote_failure_has_no_retry(tmp_path: Path) -> None:
    store = _store(tmp_path / "state.json")
    reader = PriceReaderSpy(RuntimeError("private secret"))
    observer = _observer(tmp_path / "evidence.jsonl", reader)
    dispatcher = DistanceShadowDispatcher(
        observer, fallback_provider=_provider(store.path), clock=Clock()
    )
    asyncio.run(_run(dispatcher))
    record = json.loads((tmp_path / "evidence.jsonl").read_text())
    assert reader.calls == ["BTCUSDT"]
    assert record["fresh_price_error"] == "RuntimeError"
    assert all(b["final_status"] == "EVIDENCE_MISSING" for b in record["branches"])


def test_symbol_scoped_episode_metrics(tmp_path: Path) -> None:
    path = tmp_path / "evidence.jsonl"
    first, _ = _observe(path, snapshot=_snapshot(distance=0.20), price=101.0)
    second = json.loads(json.dumps(first))
    second["symbol"] = "ETHUSDT"
    second["candidate_id"] = "other-symbol-candidate"
    for b in second["branches"]:
        b["candidate_id"] = second["candidate_id"]
    for alias, b in zip(
        ("control_025", "shadow_040", "shadow_060"), second["branches"], strict=True
    ):
        second[alias] = b
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(second) + "\n")
        stream.write(json.dumps(second) + "\n")
    summary = summarize_distance_shadow(path, invalid_observations=0)
    assert summary["unique_episodes"] == 2
    assert summary["valid_records"] == 2
    assert summary["duplicate_records"] == 1
    assert summary["thresholds"][0]["eligible_unique_episodes"] == 2


def test_accounting_is_durable_for_overflow_enqueue_and_worker_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    dispatcher = DistanceShadowDispatcher(observer, capacity=1, clock=Clock())

    async def exercise() -> None:
        plan = QtrSetupNotificationPlan(NOW, (_snapshot().decision,) * 2, {})
        assert dispatcher.enqueue(plan, NOW) is None
        await dispatcher.wait()
        checkpoint = read_shadow_accounting(
            observer.accounting_path, run_id=observer.run_id, require_sealed=False
        )
        assert checkpoint["queue_overflow_count"] == 1
        assert checkpoint["queue_dropped_count"] == 2
        assert checkpoint["invalid_observations"] == 2
        with pytest.raises(ValueError):
            dispatcher.enqueue(
                QtrSetupNotificationPlan(NOW, (_snapshot().decision,), {}),
                NOW.replace(tzinfo=None),
            )
        monkeypatch.setattr(
            observer,
            "observe_missing_context",
            lambda *a, **kw: (_ for _ in ()).throw(OSError("disk full")),
        )
        await _run(dispatcher)

    asyncio.run(exercise())
    checkpoint = read_shadow_accounting(
        observer.accounting_path, run_id=observer.run_id, require_sealed=False
    )
    assert checkpoint["worker_exception_count"] == 1
    assert checkpoint["journal_failure_count"] == 1
    assert checkpoint["enqueue_failure_count"] == 1
    assert checkpoint["invalid_observations"] == 4
    assert dispatcher.invalid_observations == 4


def test_accounting_failure_is_not_reported_as_authoritative_zero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    dispatcher = DistanceShadowDispatcher(
        observer, fallback_provider=lambda c: _context(), clock=Clock()
    )
    monkeypatch.setattr(
        dispatcher.accounting,
        "_append",
        lambda s: (_ for _ in ()).throw(OSError("disk full")),
    )
    asyncio.run(_run(dispatcher))
    assert not dispatcher.accounting.snapshot()["authoritative"]
    assert (
        summarize_distance_shadow(tmp_path / "evidence.jsonl")["invalid_observations"]
        is None
    )


def test_slow_fallback_runs_off_event_loop_and_after_completion(tmp_path: Path) -> None:
    entered = threading.Event()
    release = threading.Event()

    def slow(item: Any) -> Any:
        entered.set()
        assert release.wait(3)
        return _context()

    dispatcher = DistanceShadowDispatcher(
        _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0)),
        fallback_provider=slow,
        clock=Clock(),
    )

    async def exercise() -> None:
        cycle = dispatcher.enqueue(
            QtrSetupNotificationPlan(NOW, (_snapshot().decision,), {}), NOW
        )
        await asyncio.sleep(0.01)
        assert not entered.is_set()
        dispatcher.production_completed(cycle, NOW)
        assert await asyncio.to_thread(entered.wait, 2)
        # This coroutine can progress while fallback is blocked in its thread.
        assert not release.is_set()
        release.set()
        await dispatcher.wait()

    asyncio.run(exercise())


def test_legacy_evidence_and_unknown_accounting_fail_closed(tmp_path: Path) -> None:
    legacy = tmp_path / "qtr_micro_distance_shadow_v1.jsonl"
    legacy.write_bytes(b"old invalid sample\n")
    before = legacy.read_bytes()
    with pytest.raises(ValueError, match="immutable"):
        JsonlDistanceShadowEvidence(legacy)
    assert legacy.read_bytes() == before
    path = tmp_path / "evidence.jsonl"
    _observe(path)
    assert summarize_distance_shadow(path)["invalid_observations"] is None
    assert summarize_distance_shadow(path)["category"] == "EXPERIMENT_INVALID"


def test_context_source_temporal_rules_are_truthful() -> None:
    with pytest.raises(ValueError, match="after cycle completion"):
        TemporalEvidence(NOW, NOW, NOW, NOW, "cycle", NOW + timedelta(seconds=1))
    TemporalEvidence(
        NOW,
        NOW,
        NOW,
        NOW,
        "cycle",
        NOW + timedelta(seconds=1),
        "SHADOW_READ_ONLY_FALLBACK",
    )


@pytest.mark.parametrize("stage", ["equity", "instrument_rules", "preflight_snapshot"])
def test_dependency_failure_is_one_shot_and_has_safe_stage_detail(
    tmp_path: Path,
    stage: str,
) -> None:
    store = _store(tmp_path / "state.json")
    calls = []

    def fail(*args: object) -> Any:
        calls.append(stage)
        raise RuntimeError("api-secret must never be serialized")

    field = {
        "equity": "equity_reader",
        "instrument_rules": "rules_reader",
        "preflight_snapshot": "preflight_reader",
    }[stage]
    reader = PriceReaderSpy(101.0)
    observer = _observer(tmp_path / "evidence.jsonl", reader)
    dispatcher = DistanceShadowDispatcher(
        observer,
        fallback_provider=_provider(store.path, **{field: fail}),
        clock=Clock(),
    )
    asyncio.run(_run(dispatcher))
    record = json.loads((tmp_path / "evidence.jsonl").read_text())
    assert record["context_failure_detail"] == stage + ":RuntimeError"
    assert record["fresh_price_error"] == "CONTEXT_FALLBACK_FAILED"
    assert calls == [stage]
    assert reader.calls == []
    assert "api-secret" not in json.dumps(record)


def test_fallback_has_no_repair_execution_or_order_capabilities(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path / "state.json")
    before = store.path.read_bytes()

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("Forbidden production state operation")

    monkeypatch.setattr(JsonQtrMicroStateStore, "load", forbidden)
    monkeypatch.setattr(JsonQtrMicroStateStore, "save", forbidden)
    monkeypatch.setattr(JsonQtrMicroStateStore, "_backup_corrupt", forbidden)
    monkeypatch.setattr(Path, "rename", forbidden)
    monkeypatch.setattr(Path, "replace", forbidden)
    provider = _provider(store.path)
    assert set(provider.__slots__) == {
        "state_path",
        "equity_reader",
        "rules_reader",
        "preflight_reader",
        "clock",
    }
    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    dispatcher = DistanceShadowDispatcher(
        observer, fallback_provider=provider, clock=Clock()
    )
    asyncio.run(_run(dispatcher))
    assert store.path.read_bytes() == before
    assert dispatcher.invalid_observations == 0


def test_worker_exception_and_serialization_failure_have_distinct_accounting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    dispatcher = DistanceShadowDispatcher(observer, clock=Clock())

    def fail(*args: Any, **kwargs: Any) -> Any:
        raise ValueError("serialization")

    monkeypatch.setattr(observer._evidence, "append", fail)
    asyncio.run(_run(dispatcher))
    checkpoint = read_shadow_accounting(
        observer.accounting_path, run_id=observer.run_id, require_sealed=False
    )
    assert checkpoint["worker_exception_count"] == 1
    assert checkpoint["journal_failure_count"] == 1


def test_mixed_runs_and_temporal_tampering_invalidate_report(tmp_path: Path) -> None:
    path = tmp_path / "evidence.jsonl"
    first, _ = _observe(path)
    other = dict(first, experiment_run_id="other-run")
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(other) + "\n")
    summary = summarize_distance_shadow(path, invalid_observations=0)
    assert summary["category"] == "EXPERIMENT_INVALID"
    assert summary["mixed_experiment_runs"]
    path.write_text(json.dumps(dict(first, context_acquired_at="2026-01-01")) + "\n")
    assert (
        summarize_distance_shadow(path, invalid_observations=0)["invalid_records"] == 1
    )


def test_accounting_partial_tail_never_certifies_zero(tmp_path: Path) -> None:
    observer = _observer(tmp_path / "evidence.jsonl", PriceReaderSpy(101.0))
    dispatcher = DistanceShadowDispatcher(observer, clock=Clock())
    asyncio.run(_run(dispatcher))
    with observer.accounting_path.open("ab") as stream:
        stream.write(b'{"partial":')
    with pytest.raises(ValueError, match="Incomplete"):
        read_shadow_accounting(
            observer.accounting_path, run_id=observer.run_id, require_sealed=False
        )
