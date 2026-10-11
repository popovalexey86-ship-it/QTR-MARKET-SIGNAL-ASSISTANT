from __future__ import annotations

import asyncio
import itertools
import json
import subprocess
import sys
import types
from dataclasses import asdict, replace
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from test_qtr_micro import (
    NOW,
    RULES,
    FakeClient,
    candidate,
    decision,
    position_from_plan,
    preflight,
    settings,
    state,
)

from market_signal_assistant.qtr_micro.client import DemoApiError
from market_signal_assistant.qtr_micro.engine import QtrMicroEntryEngine
from market_signal_assistant.qtr_micro.execution import QtrMicroExecutionService
from market_signal_assistant.qtr_micro.journal import JsonlQtrMicroTradeJournal
from market_signal_assistant.qtr_micro.models import (
    DemoOrder,
    DemoPosition,
    ExecutionFill,
    MicroDirection,
    MicroExitReason,
    MicroStage,
)
from market_signal_assistant.qtr_micro.reconciliation import reconcile_demo_state
from market_signal_assistant.qtr_micro.runtime import QtrMicroRuntime
from market_signal_assistant.qtr_micro.state import JsonQtrMicroStateStore
from market_signal_assistant.setup_engine.models import SetupState, SetupType

BASE_SHA = "514b9607f689dbc3235c8adf1e7b49776ddda9a6"
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def baseline() -> dict[str, Any]:
    """Load the immutable pre-patch implementation, never a copied reimplementation."""
    modules = {}
    for name in ("client", "engine", "reconciliation", "execution", "runtime"):
        source = subprocess.check_output(
            [
                "git",
                "show",
                f"{BASE_SHA}:src/market_signal_assistant/qtr_micro/{name}.py",
            ],
            cwd=ROOT,
            text=True,
            encoding="utf-8",
        )
        module = types.ModuleType(f"_micro_p0_baseline_{name}")
        sys.modules[module.__name__] = module
        exec(compile(source, f"{BASE_SHA}/{name}.py", "exec"), module.__dict__)
        modules[name] = module
    modules["runtime"].QtrMicroEntryEngine = modules["engine"].QtrMicroEntryEngine
    modules["runtime"].QtrMicroExecutionService = modules[
        "execution"
    ].QtrMicroExecutionService
    return modules


def test_production_client_reconciliation_and_regressions_preserved() -> None:
    for path in (
        "src/market_signal_assistant/qtr_micro/client.py",
        "src/market_signal_assistant/qtr_micro/reconciliation.py",
    ):
        original = subprocess.check_output(
            ["git", "show", f"{BASE_SHA}:{path}"],
            cwd=ROOT,
            text=True,
            encoding="utf-8",
        )
        assert (ROOT / path).read_text(encoding="utf-8") == original
    test_path = "tests/test_qtr_micro.py"
    original = subprocess.check_output(
        ["git", "show", f"{BASE_SHA}:{test_path}"],
        cwd=ROOT,
        text=True,
        encoding="utf-8",
    )
    marker = "class SequenceDemoTransport:"
    assert (ROOT / test_path).read_text(encoding="utf-8").split(marker)[
        1
    ] == original.split(marker)[1]


@pytest.mark.parametrize("setup_state", list(SetupState))
@pytest.mark.parametrize("setup_type", list(SetupType))
def test_before_after_entry_decisions(
    baseline: dict[str, Any],
    setup_state: SetupState,
    setup_type: SetupType,
) -> None:
    item = candidate(
        result_changes={"setup_state": setup_state, "setup_type": setup_type}
    )
    engines = [
        baseline["engine"].QtrMicroEntryEngine(settings()),
        QtrMicroEntryEngine(settings()),
    ]
    args = dict(
        now=NOW, equity=10_000, rules=RULES, state=state(), preflight=preflight()
    )
    prepared = [engine.prepare_entry(item, **args) for engine in engines]
    # Complete plans include qty, risk, leverage, stop, TP1/TP2/runner prices/qty.
    assert asdict(prepared[0]) == asdict(prepared[1])
    valid_plan = decision().plan
    for price in (0, 99, 100.8, 101, 101.6, 105):
        results = [
            engine.revalidate_entry(item, valid_plan, current_price=price, **args)
            for engine in engines
        ]
        assert asdict(results[0]) == asdict(results[1])


@pytest.mark.parametrize(
    "stage",
    [MicroStage.OPEN, MicroStage.TP1_FILLED, MicroStage.TP2_FILLED, MicroStage.RUNNER],
)
@pytest.mark.parametrize("direction", list(MicroDirection))
def test_before_after_manage_boundaries(
    baseline: dict[str, Any],
    stage: MicroStage,
    direction: MicroDirection,
) -> None:
    config = settings(
        progress_check_minutes=15,
        normal_max_hold_minutes=45,
        runner_max_hold_minutes=90,
    )
    engines = [
        baseline["engine"].QtrMicroEntryEngine(config),
        QtrMicroEntryEngine(config),
    ]
    position = position_from_plan(decision().plan, stage=stage, direction=direction)
    sign = 1 if direction is MicroDirection.LONG else -1
    for seconds, r, flags in itertools.product(
        (0, 899, 900, 901, 1199, 1500, 2699, 2700, 2701, 5399, 5400, 5401),
        (-1, 0, 0.4999, 0.5, 1, 2, 3, 4),
        itertools.product((False, True), repeat=3),
    ):
        args = dict(
            current_price=position.average_fill + sign * r * position.initial_r,
            now=NOW + timedelta(seconds=seconds),
            setup_cancelled=flags[0],
            opposite_structure=flags[1],
            structure_degraded=flags[2],
        )
        before, after = [engine.manage(position, **args) for engine in engines]
        assert (before.action, before.close_qty, before.new_stop) == (
            after.action,
            after.close_qty,
            after.new_stop,
        )


class TracedClient(FakeClient):
    """Record every existing client API call, including reads and order arguments."""

    def __init__(self) -> None:
        super().__init__()
        self.trace: list[Any] = []

    def __getattribute__(self, name: str) -> Any:
        value = super().__getattribute__(name)
        if name.startswith("_") or not callable(value):
            return value

        def traced(*args: Any, **kwargs: Any) -> Any:
            self.trace.append((name, args, kwargs))
            return value(*args, **kwargs)

        return traced


@pytest.mark.parametrize(
    "ending", ["runner", "normal", "early", "stop", "human", "resize"]
)
def test_before_after_client_actions_and_lifecycle(
    tmp_path: Path,
    baseline: dict[str, Any],
    ending: str,
) -> None:
    traces, lifecycle = [], []
    for label, engine_type, service_type in (
        (
            "before",
            baseline["engine"].QtrMicroEntryEngine,
            baseline["execution"].QtrMicroExecutionService,
        ),
        ("after", QtrMicroEntryEngine, QtrMicroExecutionService),
    ):
        config = settings(
            progress_check_minutes=15,
            normal_max_hold_minutes=45,
            runner_max_hold_minutes=90,
        )
        engine = engine_type(config)
        plan = engine.prepare_entry(
            candidate(),
            now=NOW,
            equity=10_000,
            rules=RULES,
            state=state(),
            preflight=preflight(),
        ).plan
        store = JsonQtrMicroStateStore(tmp_path / label / "state.json")
        store.save(state())
        client = TracedClient()
        service = service_type(
            settings=config,
            client=client,
            state_store=store,
            engine=engine,
            journal=JsonlQtrMicroTradeJournal(tmp_path / label / "trades.jsonl"),
        )
        service.submit_entry(plan, NOW)
        entry_price = plan.entry_price + (2 if ending == "resize" else 0)
        client.fill = ExecutionFill("order-1", entry_price, plan.qty, 0.1, NOW)
        if ending == "resize":
            client.fills_by_order["order-2"] = client.fill
        opened = service.confirm_entry(plan.trade_id, NOW, rules=RULES)
        snapshots = []
        if ending == "runner":
            for price in (
                opened.tp1_price,
                opened.tp2_price,
                opened.runner_target_price,
            ):
                service.manage_position(plan.trade_id, current_price=price, now=NOW)
                pending = store.load(today=NOW.date(), trading_enabled=True).positions[
                    plan.trade_id
                ]
                client.fill = ExecutionFill(
                    pending.pending_exit_order_id,
                    price,
                    pending.pending_exit_qty,
                    0.2,
                    NOW,
                )
                service.manage_position(plan.trade_id, current_price=price, now=NOW)
                snapshots.append(
                    store.load(today=NOW.date(), trading_enabled=True).positions[
                        plan.trade_id
                    ]
                )
        elif ending == "human":
            pending = service.request_human_close(plan.trade_id, NOW)
            client.fill = ExecutionFill(
                pending.pending_exit_order_id,
                plan.entry_price - 1,
                pending.pending_exit_qty,
                0.2,
                NOW,
            )
            service.manage_position(
                plan.trade_id, current_price=plan.entry_price, now=NOW
            )
        elif ending == "stop":
            client.protective_stop_recovery_fill = ExecutionFill(
                "stop", plan.stop_price, opened.current_qty, 0.2, NOW
            )
            service.reconcile(NOW)
        else:
            age = 20 if ending == "early" else 45
            now = NOW + timedelta(minutes=age)
            service.manage_position(
                plan.trade_id,
                current_price=entry_price,
                now=now,
                structure_degraded=ending == "early",
            )
            pending = store.load(today=NOW.date(), trading_enabled=True).positions[
                plan.trade_id
            ]
            client.fill = ExecutionFill(
                pending.pending_exit_order_id,
                entry_price - 1,
                pending.pending_exit_qty,
                0.2,
                now,
            )
            service.manage_position(plan.trade_id, current_price=entry_price, now=now)
        snapshots.append(
            store.load(today=NOW.date(), trading_enabled=True).positions[plan.trade_id]
        )
        lifecycle.append(
            [
                (
                    p.stage,
                    p.current_qty,
                    p.current_stop,
                    p.realised_partial_pnl,
                    p.fees,
                    p.actual_risk_at_fill,
                    p.final_exit_reason,
                )
                for p in snapshots
            ]
        )
        traces.append(client.trace)
    assert traces[0] == traces[1]
    assert lifecycle[0] == lifecycle[1]


@pytest.mark.parametrize(
    ("minutes", "stage", "degraded", "detail", "reason"),
    [
        (
            20,
            MicroStage.OPEN,
            True,
            "EARLY_PROGRESS_STRUCTURE_DEGRADED",
            MicroExitReason.TIME_EXIT,
        ),
        (45, MicroStage.OPEN, True, "NORMAL_MAX_HOLD", MicroExitReason.TIME_EXIT),
        (
            90,
            MicroStage.RUNNER,
            True,
            "RUNNER_MAX_HOLD",
            MicroExitReason.RUNNER_TIME_EXIT,
        ),
    ],
)
def test_exit_decision_evidence_survives_restart(
    tmp_path: Path,
    minutes: int,
    stage: MicroStage,
    degraded: bool,
    detail: str,
    reason: MicroExitReason,
) -> None:
    plan = decision().plan
    config = settings(
        progress_check_minutes=15,
        normal_max_hold_minutes=45,
        runner_max_hold_minutes=90,
    )
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    position = position_from_plan(
        plan, stage=stage, actual_risk_at_fill=plan.risk_amount
    )
    store.save(state(positions={plan.trade_id: position}))
    client = FakeClient()
    journal = JsonlQtrMicroTradeJournal(tmp_path / "trades.jsonl")
    service = QtrMicroExecutionService(
        settings=config,
        client=client,
        state_store=store,
        engine=QtrMicroEntryEngine(config),
        journal=journal,
    )
    now = NOW + timedelta(minutes=minutes)
    service.manage_position(
        plan.trade_id,
        current_price=plan.entry_price,
        now=now,
        structure_degraded=degraded,
        setup_state_at_exit="CANCELLED",
        current_breakout_failure=True,
    )
    pending = store.load(today=NOW.date(), trading_enabled=True).positions[
        plan.trade_id
    ]
    assert pending.exit_detail == detail
    assert pending.pending_exit_reason is reason
    assert pending.position_age_seconds == minutes * 60
    client.fill = ExecutionFill(
        pending.pending_exit_order_id, plan.entry_price - 1, plan.qty, 0.2, now
    )
    # A new service instance confirms without overwriting the causal decision.
    restarted = QtrMicroExecutionService(
        settings=config,
        client=client,
        state_store=store,
        engine=QtrMicroEntryEngine(config),
        journal=journal,
    )
    restarted.manage_position(
        plan.trade_id, current_price=plan.entry_price, now=now + timedelta(seconds=5)
    )
    row = json.loads(journal.path.read_text(encoding="utf-8"))
    assert row["exit_detail"] == detail
    assert row["position_age_seconds"] == minutes * 60
    assert row["current_r_at_exit_decision"] == row["exit_current_r"] == 0
    assert row["structure_degraded"] == row["exit_structure_degraded"] is True
    assert row["setup_state_at_exit"] == row["exit_setup_state"] == "CANCELLED"
    assert row["current_breakout_failure"] is True
    assert row["setup_cancelled"] is False
    assert row["opposite_structure"] is False


@pytest.mark.parametrize("direction", list(MicroDirection))
@pytest.mark.parametrize("offset", [-0.00051, 0.00051])
@pytest.mark.parametrize(
    "path",
    ["confirm", "pending_recovery", "stop", "external", "closed_external", "durable"],
)
def test_full_lifecycle_excursion_and_canonical_r(
    tmp_path: Path,
    direction: MicroDirection,
    offset: float,
    path: str,
) -> None:
    plan = decision().plan
    entry, qty = 0.07334, 100_000
    fill_price = entry + offset
    position = position_from_plan(
        plan,
        average_fill=entry,
        direction=direction,
        initial_qty=qty,
        current_qty=qty,
        actual_risk_at_fill=293.30,
        risk_amount=999,
        max_favorable_price=entry,
        max_adverse_price=entry,
    )
    fill = ExecutionFill("exit", fill_price, qty, 0.2, NOW)
    client = FakeClient()
    if path in {"confirm", "pending_recovery"}:
        position = replace(
            position,
            stage=MicroStage.EXIT_ACKNOWLEDGED,
            pending_exit_order_id="exit",
            pending_exit_reason=MicroExitReason.TIME_EXIT,
            pending_exit_qty=qty,
        )
        client.fills_by_order["exit"] = fill
    elif path == "stop":
        client.protective_stop_recovery_fill = fill
    elif path in {"external", "closed_external"}:
        client.external_manual_recovery_fill = fill
        if path == "closed_external":
            position = replace(position, stage=MicroStage.CLOSED, current_qty=0)
    else:
        position = replace(
            position,
            stage=MicroStage.CLOSED,
            current_qty=0,
            runner_exit_price=fill_price,
            realised_partial_pnl=-117.59,
            fees=0.2,
            final_exit_reason=MicroExitReason.TIME_EXIT,
        )
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    store.save(state(positions={plan.trade_id: position}))
    journal = JsonlQtrMicroTradeJournal(tmp_path / "trades.jsonl")
    service = QtrMicroExecutionService(
        settings=settings(),
        client=client,
        state_store=store,
        engine=QtrMicroEntryEngine(settings()),
        journal=journal,
    )
    if path == "confirm":
        service.manage_position(plan.trade_id, current_price=entry, now=NOW)
    else:
        service.reconcile(NOW)
    service.reconcile(NOW + timedelta(seconds=1))
    rows = journal.path.read_text(encoding="utf-8").splitlines()
    assert len(rows) == 1
    row = json.loads(rows[0])
    sign = 1 if direction is MicroDirection.LONG else -1
    assert row["max_adverse_excursion"] == pytest.approx(min(0, sign * offset))
    assert row["max_favorable_excursion"] == pytest.approx(max(0, sign * offset))
    assert (
        row["realised_net_r_actual"]
        == row["net_r"]
        == pytest.approx(row["net_pnl"] / 293.30)
    )
    assert row["result_r"] == pytest.approx(row["net_pnl"] / 999)
    if path == "durable":
        assert row["realised_net_r_actual"] == pytest.approx(-0.4016024548)
    recovered = store.load(today=NOW.date(), trading_enabled=True).positions[
        plan.trade_id
    ]
    assert recovered.max_adverse_price == pytest.approx(
        min(entry, fill_price) if sign == 1 else max(entry, fill_price)
    )
    assert recovered.journaled
    assert row["exit_detail"] is None  # Historical detail is never guessed.


@pytest.mark.parametrize("actual_risk", [None, 0, -1, float("nan")])
def test_canonical_r_never_falls_back_to_planned_risk(
    tmp_path: Path, actual_risk: float | None
) -> None:
    plan = decision().plan
    position = position_from_plan(
        plan,
        stage=MicroStage.CLOSED,
        current_qty=0,
        runner_exit_price=plan.entry_price,
        realised_partial_pnl=-117.79,
        actual_risk_at_fill=actual_risk,
    )
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    store.save(state(positions={plan.trade_id: position}))
    journal = JsonlQtrMicroTradeJournal(tmp_path / "trades.jsonl")
    service = QtrMicroExecutionService(
        settings=settings(),
        client=FakeClient(),
        state_store=store,
        engine=QtrMicroEntryEngine(settings()),
        journal=journal,
    )
    service.reconcile(NOW)
    assert (
        json.loads(journal.path.read_text(encoding="utf-8"))["realised_net_r_actual"]
        is None
    )


def test_partial_exit_pending_prices_and_later_stop_context(tmp_path: Path) -> None:
    plan = decision().plan
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    client = FakeClient()
    journal = JsonlQtrMicroTradeJournal(tmp_path / "trades.jsonl")
    service = QtrMicroExecutionService(
        settings=settings(),
        client=client,
        state_store=store,
        engine=QtrMicroEntryEngine(settings()),
        journal=journal,
    )
    service.submit_entry(plan, NOW)
    client.fill = ExecutionFill("order-1", plan.entry_price, plan.qty, 0.1, NOW)
    opened = service.confirm_entry(plan.trade_id, NOW)
    assert opened.max_favorable_price == opened.max_adverse_price == plan.entry_price
    client.fill = None
    service.manage_position(plan.trade_id, current_price=opened.tp1_price, now=NOW)
    high = opened.tp1_price + 2
    service.manage_position(plan.trade_id, current_price=high, now=NOW)
    client.fill = ExecutionFill("order-2", high - 1, opened.tp1_qty, 0.2, NOW)
    service.manage_position(plan.trade_id, current_price=opened.tp1_price, now=NOW)
    partial = store.load(today=NOW.date(), trading_enabled=True).positions[
        plan.trade_id
    ]
    assert partial.stage is MicroStage.TP1_FILLED
    assert partial.max_favorable_price == high
    assert partial.max_adverse_price == plan.entry_price
    client.protective_stop_recovery_fill = ExecutionFill(
        "stop", plan.entry_price - 2, partial.current_qty, 0.2, NOW
    )
    service.reconcile(NOW)
    row = json.loads(journal.path.read_text(encoding="utf-8"))
    assert row["max_favorable_excursion"] == pytest.approx(high - plan.entry_price)
    assert row["max_adverse_excursion"] == -2
    assert row["exit_current_r"] is None  # No stale TP1 decision attached to stop.
    assert row["exit_trigger"] is None
    assert row["realised_net_r_actual"] == pytest.approx(
        row["net_pnl"] / opened.actual_risk_at_fill
    )


def test_runtime_causal_context_and_client_calls(
    tmp_path: Path, baseline: dict[str, Any]
) -> None:
    traces = []
    item = candidate(result_changes={"setup_type": SetupType.RETEST})
    for label, runtime_type in (
        ("before", baseline["runtime"].QtrMicroRuntime),
        ("after", QtrMicroRuntime),
    ):
        client = TracedClient()
        client.auto_fill = True
        store = JsonQtrMicroStateStore(tmp_path / label / "state.json")
        runtime = runtime_type(
            settings=settings(),
            client=client,
            state_store=store,
            allowed_chat_ids=frozenset(),
            clock=lambda: NOW,
        )

        async def discard(chat_id: int, text: str) -> None:
            del chat_id, text

        asyncio.run(runtime.initialize())
        asyncio.run(runtime.handle_candidates((item,), discard))
        traces.append(client.trace)
        position = next(
            iter(store.load(today=NOW.date(), trading_enabled=True).positions.values())
        )
        assert position.stage is MicroStage.OPEN
        if label == "after":
            assert position.setup_state_at_entry == item.result.setup_state.value
            assert position.classification_candidates == tuple(
                t.value for t in item.result.classification_candidates
            )
    assert traces[0] == traces[1]


@pytest.mark.parametrize("has_candidate", [False, True])
def test_runtime_exit_context_is_causal_or_unknown(
    tmp_path: Path,
    has_candidate: bool,
) -> None:
    plan = decision().plan
    position = position_from_plan(plan, opened_at=NOW - timedelta(minutes=45))
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    store.save(state(positions={plan.trade_id: position}))
    runtime = QtrMicroRuntime(
        settings=settings(),
        client=FakeClient(),
        state_store=store,
        allowed_chat_ids=frozenset(),
        clock=lambda: NOW,
    )
    item = candidate()

    async def discard(chat_id: int, text: str) -> None:
        del chat_id, text

    asyncio.run(
        runtime._manage_open(
            store.load(today=NOW.date(), trading_enabled=True),
            {position.symbol: item} if has_candidate else {},
            discard,
            NOW,
        )
    )
    pending = store.load(today=NOW.date(), trading_enabled=True).positions[
        plan.trade_id
    ]
    assert pending.exit_detail == "NORMAL_MAX_HOLD"
    if has_candidate:
        assert pending.setup_state_at_exit == item.result.setup_state.value
        assert pending.current_breakout_failure is False
        assert pending.structure_degraded is False
        assert pending.setup_cancelled is False
        assert pending.opposite_structure is False
    else:
        assert pending.setup_state_at_exit is None
        assert pending.current_breakout_failure is None
        assert pending.structure_degraded is None
        assert pending.setup_cancelled is None
        assert pending.opposite_structure is None


def test_legacy_state_does_not_invent_context(tmp_path: Path) -> None:
    plan = decision().plan
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    store.save(state(positions={plan.trade_id: position_from_plan(plan)}))
    payload = json.loads(store.path.read_text(encoding="utf-8"))
    new_fields = (
        "setup_state_at_entry",
        "classification_candidates",
        "exit_detail",
        "exit_trigger",
        "position_age_seconds",
        "current_r_at_exit_decision",
        "structure_degraded",
        "setup_cancelled",
        "opposite_structure",
        "current_breakout_failure",
        "setup_state_at_exit",
    )
    for field in new_fields:
        del payload["positions"][plan.trade_id][field]
    store.path.write_text(json.dumps(payload), encoding="utf-8")
    restored = store.load(today=NOW.date(), trading_enabled=True)
    assert restored.trading_enabled
    assert restored.blocked_reason is None
    for field in new_fields:
        assert getattr(restored.positions[plan.trade_id], field) is None


def test_confirmed_risk_resize_fill_is_observed(tmp_path: Path) -> None:
    plan = decision().plan
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    store.save(state())
    client = FakeClient()
    service = QtrMicroExecutionService(
        settings=settings(),
        client=client,
        state_store=store,
        engine=QtrMicroEntryEngine(settings()),
    )
    service.submit_entry(plan, NOW)
    entry_price = plan.entry_price + 2
    client.fill = ExecutionFill("order-1", entry_price, plan.qty, 0.1, NOW)
    client.fills_by_order["order-2"] = ExecutionFill(
        "order-2", entry_price - 1, plan.qty, 0.1, NOW
    )
    opened = service.confirm_entry(plan.trade_id, NOW, rules=RULES)
    assert opened.stage is MicroStage.OPEN
    assert opened.max_adverse_price == entry_price - 1
    assert opened.max_favorable_price == entry_price


@pytest.mark.parametrize(
    "scenario", ["owned", "foreign", "missing", "blocked", "recovered", "pending"]
)
def test_reconciliation_ownership_and_safety_identical(
    baseline: dict[str, Any],
    scenario: str,
) -> None:
    plan = decision().plan
    position = position_from_plan(plan)
    remote = (DemoPosition(plan.symbol, "Buy", plan.qty, plan.entry_price),)
    orders = ()
    records = {plan.trade_id: position}
    if scenario == "foreign":
        remote = (DemoPosition("ETHUSDT", "Sell", 1, 100),)
    elif scenario == "missing":
        remote = ()
    elif scenario == "blocked":
        records = {plan.trade_id: replace(position, stage=MicroStage.BLOCKED)}
    elif scenario == "recovered":
        records = {}
        orders = (
            DemoOrder(plan.symbol, "order", plan.trade_id, "Buy", plan.qty, "New"),
        )
    elif scenario == "pending":
        remote = ()
        orders = (
            DemoOrder(
                plan.symbol, "exit", plan.trade_id + "-TM", "Sell", plan.qty, "New"
            ),
        )
        records = {
            plan.trade_id: replace(
                position,
                stage=MicroStage.EXIT_ACKNOWLEDGED,
                pending_exit_order_id="exit",
                pending_exit_order_link_id=plan.trade_id + "-TM",
                pending_exit_reason=MicroExitReason.TIME_EXIT,
            )
        }
    initial = state(positions=records)
    args = dict(positions=remote, orders=orders, now=NOW)
    before = baseline["reconciliation"].reconcile_demo_state(initial, **args)
    after = reconcile_demo_state(initial, **args)
    assert asdict(before) == asdict(after)


@pytest.mark.parametrize(
    "path", ["pending", "stop", "external", "closed_external", "durable", "unknown"]
)
def test_before_after_recovery_state_and_io(
    tmp_path: Path,
    baseline: dict[str, Any],
    path: str,
) -> None:
    plan = decision().plan
    traces, states = [], []
    telemetry_fields = {
        "max_favorable_price",
        "max_adverse_price",
        "exit_detail",
        "exit_trigger",
        "position_age_seconds",
        "current_r_at_exit_decision",
        "structure_degraded",
        "setup_cancelled",
        "opposite_structure",
        "current_breakout_failure",
        "setup_state_at_exit",
    }
    for label, service_type, engine_type in (
        (
            "before",
            baseline["execution"].QtrMicroExecutionService,
            baseline["engine"].QtrMicroEntryEngine,
        ),
        ("after", QtrMicroExecutionService, QtrMicroEntryEngine),
    ):
        client = TracedClient()
        position = position_from_plan(plan, actual_risk_at_fill=plan.risk_amount)
        fill = ExecutionFill("exit", plan.entry_price - 1, plan.qty, 0.2, NOW)
        if path == "pending":
            position = replace(
                position,
                stage=MicroStage.EXIT_ACKNOWLEDGED,
                pending_exit_order_id="exit",
                pending_exit_reason=MicroExitReason.TIME_EXIT,
            )
            client.fills_by_order["exit"] = fill
        elif path == "stop":
            client.protective_stop_recovery_fill = fill
        elif path in {"external", "closed_external"}:
            client.external_manual_recovery_fill = fill
            if path == "closed_external":
                position = replace(position, stage=MicroStage.CLOSED, current_qty=0)
        elif path == "durable":
            position = replace(
                position,
                stage=MicroStage.CLOSED,
                current_qty=0,
                runner_exit_price=fill.average_price,
                realised_partial_pnl=-1,
                final_exit_reason=MicroExitReason.TIME_EXIT,
            )
        store = JsonQtrMicroStateStore(tmp_path / label / "state.json")
        store.save(state(positions={plan.trade_id: position}))
        service = service_type(
            settings=settings(),
            client=client,
            state_store=store,
            engine=engine_type(settings()),
            journal=JsonlQtrMicroTradeJournal(tmp_path / label / "trades.jsonl"),
        )
        first = service.reconcile(NOW)
        second = service.reconcile(NOW + timedelta(seconds=1))
        snapshots = [asdict(first), asdict(second)]
        for snapshot in snapshots:
            # Compare trading state including ownership, lifecycle, PnL, fees,
            # journaled marker and fail-closed blocks; omit observability only.
            for record in snapshot["positions"].values():
                for field in telemetry_fields:
                    record.pop(field)
        states.append(snapshots)
        traces.append(client.trace)
    assert states[0] == states[1]
    assert traces[0] == traces[1]


@pytest.mark.parametrize(
    ("method", "stage"),
    [
        ("position_mode", "position_mode"),
        ("instrument_rules", "instrument_rules"),
        ("current_market_price", "fresh_price"),
        ("create_market_order", "submit_entry"),
    ],
)
def test_production_failure_logging_and_calls_unchanged(
    tmp_path: Path,
    baseline: dict[str, Any],
    caplog: pytest.LogCaptureFixture,
    method: str,
    stage: str,
) -> None:
    traces, messages = [], []
    for label, runtime_type in (
        ("before", baseline["runtime"].QtrMicroRuntime),
        ("after", QtrMicroRuntime),
    ):
        client = TracedClient()
        runtime = runtime_type(
            settings=settings(),
            client=client,
            state_store=JsonQtrMicroStateStore(tmp_path / label / "state.json"),
            allowed_chat_ids=frozenset(),
            clock=lambda: NOW,
        )
        asyncio.run(runtime.initialize())

        def fail(*args: Any, **kwargs: Any) -> Any:
            raise DemoApiError("production retry boundary", ret_code=10002)

        setattr(client, method, fail)

        async def discard(chat_id: int, text: str) -> None:
            del chat_id, text

        caplog.clear()
        with caplog.at_level("WARNING"):
            asyncio.run(runtime.handle_candidates((candidate(),), discard))
        assert f"stage={stage}" in caplog.text
        assert "ret_code=10002" in caplog.text
        assert client.orders == []
        traces.append(client.trace)
        messages.append([record.getMessage() for record in caplog.records])
    assert traces[0] == traces[1]
    assert messages[0] == messages[1]


@pytest.mark.parametrize("codes", [(10002, None), (10002, 10002), (10006,)])
def test_production_position_mode_retry_differential(
    baseline: dict[str, Any],
    codes: tuple[int | None, ...],
) -> None:
    from market_signal_assistant.qtr_micro import client as current_client

    results, traces = [], []
    for module in (baseline["client"], current_client):

        class Transport:
            base_url = "https://api-demo.bybit.com"

            def __init__(self, error_type: Any) -> None:
                self.responses = iter(codes)
                self.calls: list[Any] = []
                self.error_type = error_type

            def request(
                self, method: str, path: str, params: Any, *, authenticated: bool
            ) -> Any:
                self.calls.append((method, path, dict(params), authenticated))
                code = next(self.responses)
                if code is not None:
                    raise self.error_type("retry boundary", ret_code=code)
                return {"result": {"list": [{"positionIdx": 0}]}}

        transport = Transport(module.DemoApiError)
        client = module.BybitDemoTradingClient(transport)
        try:
            result = client.position_mode("ETHUSDT")
        except module.DemoApiError as error:
            result = error.ret_code
        results.append(result)
        traces.append(transport.calls)
    assert results[0] == results[1]
    assert traces[0] == traces[1]
