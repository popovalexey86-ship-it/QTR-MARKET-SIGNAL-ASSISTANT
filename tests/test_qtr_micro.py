from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from market_signal_assistant.qtr_micro.cli import main as micro_cli_main
from market_signal_assistant.qtr_micro.client import (
    BybitDemoTradingClient,
    DemoApiError,
    DemoOrder,
    DemoPosition,
    ExecutionFill,
    InstrumentRules,
    InstrumentUniverseStatus,
    LeverageUpdateResult,
    OrderAcknowledgement,
    UrllibBybitDemoTransport,
)
from market_signal_assistant.qtr_micro.engine import (
    ManagementDecision,
    QtrMicroEntryEngine,
)
from market_signal_assistant.qtr_micro.execution import (
    QtrMicroExecutionService,
    record_trade_result,
)
from market_signal_assistant.qtr_micro.journal import (
    JsonlQtrMicroDecisionAudit,
    JsonlQtrMicroTradeJournal,
    TradeJournalEntry,
)
from market_signal_assistant.qtr_micro.models import (
    EntrySkipReason,
    MicroDirection,
    MicroExitReason,
    MicroPosition,
    MicroStage,
    PreflightResult,
)
from market_signal_assistant.qtr_micro.preflight import (
    QtrMicroPreflight,
    format_preflight,
    preflight_json,
)
from market_signal_assistant.qtr_micro.reconciliation import reconcile_demo_state
from market_signal_assistant.qtr_micro.runtime import (
    QtrMicroRuntime,
    QtrMicroRuntimeStatus,
)
from market_signal_assistant.qtr_micro.runtime_audit import (
    JsonlQtrMicroRuntimeAudit,
    QtrMicroRuntimeEvent,
)
from market_signal_assistant.qtr_micro.settings import (
    DEMO_BASE_URL,
    QtrMicroSettings,
)
from market_signal_assistant.qtr_micro.state import (
    JsonQtrMicroStateStore,
    empty_state,
)
from market_signal_assistant.qtr_setup_pilot.models import QtrSetupCandidate
from market_signal_assistant.setup_engine import (
    SetupAnalysisInput,
    SetupDirection,
    SetupState,
    SetupType,
    analyze_setup,
)
from market_signal_assistant.telegram.bot import execute_command
from market_signal_assistant.telegram.parsing import parse_command
from market_signal_assistant.telegram.qtr_micro import (
    format_micro_closed,
    format_micro_entry,
)

NOW = datetime(2026, 8, 10, 10, 0, tzinfo=UTC)
RULES = InstrumentRules("BTCUSDT", 0.001, 0.001, 1000, 5, 100)


def settings(**changes: Any) -> QtrMicroSettings:
    baseline = QtrMicroSettings(
        enabled=True,
        api_key="demo-key",
        api_secret="demo-secret",
    )
    return replace(baseline, **changes)


def source_input(**changes: Any) -> SetupAnalysisInput:
    baseline = SetupAnalysisInput(
        snapshot_ids=("scan-1",),
        source="early_discovery_v2",
        symbol="BTCUSDT",
        analyzed_at=NOW,
        direction=SetupDirection.UP,
        current_price=101.0,
        trigger_level=100.8,
        invalidation_level=99.0,
        price_change_24h_pct=2.0,
        distance_to_trigger_pct=0.2,
        distance_to_trigger_atr=0.2,
        breakout_age_bars=1,
        hold_candles=2,
        breakout_confirmed=True,
        correct_side_of_level=True,
        retest_detected=False,
        retest_held=False,
        volume_confirmation=True,
        volatility_confirmation=True,
        structure_confirmation=True,
        liquidity_ok=True,
        spread_pct=0.1,
        compression_detected=False,
        continuation_detected=False,
        reversal_detected=False,
        conflicting_confirmations=False,
        completed_candles=3,
        technical_data_complete=True,
    )
    return replace(baseline, **changes)


def candidate(
    *,
    episode: str = "episode-1",
    input_changes: dict[str, Any] | None = None,
    result_changes: dict[str, Any] | None = None,
    atr: float | None = 1.0,
) -> QtrSetupCandidate:
    source = source_input(**(input_changes or {}))
    result = analyze_setup(source)
    if result_changes:
        result = replace(result, **result_changes)
    return QtrSetupCandidate(episode, source, result, atr)


def state(**changes: Any) -> Any:
    baseline = empty_state(today=NOW.date(), trading_enabled=True, equity=10_000.0)
    return replace(baseline, **changes)


def preflight(ready: bool = True) -> PreflightResult:
    return PreflightResult(ready, None if ready else "blocked", equity=10_000)


async def _discard_message(chat_id: int, text: str) -> None:
    del chat_id, text


def decision(
    item: QtrSetupCandidate | None = None,
    *,
    config: QtrMicroSettings | None = None,
    current_state: Any | None = None,
    now: datetime = NOW,
    equity: float = 10_000,
    rules: InstrumentRules = RULES,
) -> Any:
    return QtrMicroEntryEngine(config or settings()).prepare_entry(
        item or candidate(),
        now=now,
        equity=equity,
        rules=rules,
        state=current_state or state(),
        preflight=preflight(),
    )


class ManagementExecutionSpy:
    def __init__(self, position: MicroPosition, engine: QtrMicroEntryEngine) -> None:
        self._position = position
        self._engine = engine
        self.flags: list[tuple[bool, bool, bool]] = []
        self.decisions: list[ManagementDecision] = []

    def manage_position(
        self,
        trade_id: str,
        *,
        current_price: float,
        now: datetime,
        setup_cancelled: bool = False,
        opposite_structure: bool = False,
        structure_degraded: bool = False,
    ) -> ManagementDecision:
        assert trade_id == self._position.trade_id
        self.flags.append(
            (setup_cancelled, opposite_structure, structure_degraded)
        )
        decision_result = self._engine.manage(
            self._position,
            current_price=current_price,
            now=now,
            setup_cancelled=setup_cancelled,
            opposite_structure=opposite_structure,
            structure_degraded=structure_degraded,
        )
        self.decisions.append(decision_result)
        return decision_result


@pytest.mark.parametrize(
    "url",
    ("https://api.bybit.com", "https://api-testnet.bybit.com"),
)
def test_non_demo_domains_are_rejected(url: str) -> None:
    with pytest.raises(ValueError, match="api-demo"):
        settings(base_url=url)


def test_demo_domain_is_accepted_and_secret_is_redacted() -> None:
    config = settings()
    assert config.base_url == DEMO_BASE_URL
    assert "demo-secret" not in repr(config)
    assert "demo-key" not in repr(config)


def test_micro_is_disabled_by_default_and_live_mode_is_impossible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("QTR_MICRO_ENABLED", raising=False)
    monkeypatch.delenv("BYBIT_DEMO_API_KEY", raising=False)
    monkeypatch.delenv("BYBIT_DEMO_API_SECRET", raising=False)
    assert QtrMicroSettings.from_environment().enabled is False
    with pytest.raises(ValueError, match="только значение demo"):
        settings(mode="live")


def test_micro_cli_help_is_lazy_and_opens_no_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_network(*args: object, **kwargs: object) -> None:
        raise AssertionError((args, kwargs))

    monkeypatch.setattr("urllib.request.urlopen", fail_network)
    with pytest.raises(SystemExit) as exit_info:
        micro_cli_main(("--help",))
    assert exit_info.value.code == 0


class FakeClient:
    base_url = DEMO_BASE_URL

    def __init__(self) -> None:
        self.fail = False
        self.auth_fail = False
        self.orders: list[dict[str, Any]] = []
        self.stop_calls = 0
        self.fill: ExecutionFill | None = None
        self.account = "UNIFIED"
        self.mode = "ONE_WAY"
        self.positions: tuple[DemoPosition, ...] = ()
        self.active_orders: tuple[DemoOrder, ...] = ()
        self.leverage_result = LeverageUpdateResult.CHANGED
        self.leverage_error: DemoApiError | None = None
        self.order_error: DemoApiError | None = None
        self.order_call_count = 0
        self.leverage_calls: list[tuple[str, int]] = []
        self.instrument_rule_calls: list[str] = []
        self.rules_by_symbol: dict[str, InstrumentRules] = {}
        self.market_prices: dict[str, float] = {}
        self.cancelled_orders: list[tuple[str, str]] = []
        self.auto_fill = False
        self.equity = 10_000.0
        self.stop_failures_remaining = 0
        self.fills_by_order: dict[str, ExecutionFill | None] = {}

    def connectivity(self) -> None:
        if self.fail:
            raise DemoApiError("offline")

    def wallet_equity(self) -> float:
        if self.auth_fail:
            raise DemoApiError("Demo authentication failed")
        return self.equity

    def account_type(self) -> str:
        return self.account

    def position_mode(self, symbol: str) -> str:
        del symbol
        return self.mode

    def list_positions(self) -> tuple[DemoPosition, ...]:
        return self.positions

    def list_active_orders(self) -> tuple[DemoOrder, ...]:
        return self.active_orders

    def instrument_rules(self, symbol: str) -> InstrumentRules:
        normalized = symbol.upper()
        self.instrument_rule_calls.append(normalized)
        return self.rules_by_symbol.get(normalized, replace(RULES, symbol=normalized))

    def current_market_price(self, symbol: str) -> float:
        return self.market_prices.get(symbol.upper(), 101.0)

    def set_leverage(self, symbol: str, leverage: int) -> LeverageUpdateResult:
        assert symbol
        assert 1 <= leverage <= 10
        self.leverage_calls.append((symbol, leverage))
        if self.leverage_error is not None:
            raise self.leverage_error
        return self.leverage_result

    def create_market_order(self, **kwargs: Any) -> OrderAcknowledgement:
        self.order_call_count += 1
        if self.order_error is not None:
            raise self.order_error
        self.orders.append(kwargs)
        return OrderAcknowledgement(
            f"order-{self.order_call_count}", str(kwargs["order_link_id"]), True
        )

    def execution_fill(self, order_id: str, symbol: str) -> ExecutionFill | None:
        if order_id in self.fills_by_order:
            return self.fills_by_order[order_id]
        if self.auto_fill and self.orders:
            return ExecutionFill(
                order_id,
                self.current_market_price(symbol),
                float(self.orders[-1]["qty"]),
                0.1,
                NOW,
            )
        return self.fill

    def cancel_order(self, symbol: str, order_id: str) -> None:
        self.cancelled_orders.append((symbol, order_id))

    def set_protective_stop(self, **kwargs: Any) -> None:
        del kwargs
        self.stop_calls += 1
        if self.stop_failures_remaining > 0:
            self.stop_failures_remaining -= 1
            raise DemoApiError("stop failed")
        if self.fail:
            raise DemoApiError("stop failed")


def test_missing_credentials_and_failed_preflight_are_blocked() -> None:
    missing = replace(settings(), api_key="", api_secret="")
    assert QtrMicroPreflight(missing, None).run().ready is False
    client = FakeClient()
    client.fail = True
    assert QtrMicroPreflight(settings(), client).run().ready is False
    assert client.orders == []


def test_successful_preflight_checks_demo_account() -> None:
    result = QtrMicroPreflight(settings(), FakeClient()).run()
    assert result.ready is True
    assert result.account_type == "UNIFIED"
    assert result.position_mode == "ONE_WAY"


@pytest.mark.parametrize("symbol", ("BTCUSDT", "ETHUSDT", "SOLUSDT"))
def test_preflight_is_dynamic_per_requested_symbol(symbol: str) -> None:
    client = FakeClient()

    result = QtrMicroPreflight(settings(), client).run(symbol)

    assert result.ready is True
    assert result.symbol == symbol
    assert client.instrument_rule_calls == [symbol]


def test_ready_preflight_has_detailed_safe_russian_output() -> None:
    result = QtrMicroPreflight(settings(), FakeClient()).run("BTCUSDT")
    text = format_preflight(result)
    assert "ПРЕДВАРИТЕЛЬНАЯ ПРОВЕРКА BYBIT DEMO" in text
    assert "✅ Demo host: api-demo.bybit.com" in text
    assert "✅ Соединение с Bybit Demo" in text
    assert "✅ Аутентификация" in text
    assert "✅ Тип аккаунта: UNIFIED" in text
    assert "✅ Режим позиции: односторонний" in text
    assert "✅ Инструмент BTCUSDT: доступен" in text
    assert "✅ Правила инструмента: загружены" in text
    assert "✅ Рабочее плечо: x5" in text
    assert "✅ Открытые QTR Micro позиции: 0" in text
    assert "✅ Активные QTR Micro ордера: 0" in text
    assert "✅ Reconciliation: OK" in text
    assert "Минимальный объём BTCUSDT" in text
    assert "Шаг количества" in text
    assert "Максимальное плечо инструмента: x100" in text
    assert "ГОТОВ К ДЕМО-ТОРГОВЛЕ" in text
    assert "demo-secret" not in text


def test_auth_failure_is_a_named_blocking_check() -> None:
    client = FakeClient()
    client.auth_fail = True
    result = QtrMicroPreflight(settings(), client).run()
    text = format_preflight(result)
    assert result.ready is False
    assert "❌ Аутентификация: Demo authentication failed" in text
    assert "🔴 QTR Micro Demo: ЗАБЛОКИРОВАН" in text
    assert client.orders == []


def test_preflight_treats_already_set_leverage_as_ready() -> None:
    client = FakeClient()
    client.leverage_result = LeverageUpdateResult.ALREADY_SET

    result = QtrMicroPreflight(settings(), client).run()
    text = format_preflight(result)

    assert result.ready is True
    assert "✅ Рабочее плечо: x5" in text
    assert "ℹ️ Плечо: x5 уже было установлено, изменение не требуется" in text
    assert "ГОТОВ К ДЕМО-ТОРГОВЛЕ" in text
    assert client.orders == []


def test_preflight_keeps_other_leverage_errors_blocking() -> None:
    client = FakeClient()
    client.leverage_error = DemoApiError(
        "Bybit Demo API отклонил запрос, код 110013.",
        ret_code=110013,
    )

    result = QtrMicroPreflight(settings(), client).run()

    assert result.ready is False
    assert result.blocking_reasons == ("Bybit Demo API отклонил запрос, код 110013.",)
    assert client.orders == []


def test_preflight_blocks_instrument_outside_qtr_universe() -> None:
    client = FakeClient()
    client.rules_by_symbol["JNJUSDT"] = replace(
        RULES,
        symbol="JNJUSDT",
        universe_status=InstrumentUniverseStatus.STOCK,
    )

    result = QtrMicroPreflight(settings(), client).run("JNJUSDT")

    assert result.ready is False
    assert result.reason == "Инструмент не входит в торговую вселенную QTR Micro."
    assert client.orders == []


def test_owned_and_foreign_objects_are_counted_without_claiming_manual(
    tmp_path: Path,
) -> None:
    plan = decision().plan
    assert plan is not None
    store = JsonQtrMicroStateStore(tmp_path / "qtr_micro_state.json")
    store.save(state(positions={plan.trade_id: position_from_plan(plan)}))
    client = FakeClient()
    client.positions = (
        DemoPosition("BTCUSDT", "Buy", plan.qty, plan.entry_price),
        DemoPosition("ETHUSDT", "Buy", 1, 10),
    )
    client.active_orders = (
        DemoOrder("BTCUSDT", "qtr", plan.order_link_id, "Buy", 1, "New"),
        DemoOrder("ETHUSDT", "manual", "MANUAL-1", "Buy", 1, "New"),
    )
    result = QtrMicroPreflight(
        settings(), client, state_store=store, clock=lambda: NOW
    ).run()
    assert result.ready is True
    assert result.qtr_positions == 1
    assert result.qtr_orders == 1
    assert result.foreign_positions == 1
    assert result.foreign_orders == 1
    text = format_preflight(result)
    assert "✅ Открытые QTR Micro позиции: 1" in text
    assert "✅ Активные QTR Micro ордера: 1" in text
    assert "⚠️ Foreign/manual позиции: 1" in text
    assert "управление запрещено" in text


def test_preflight_json_is_valid_and_contains_no_credentials(tmp_path: Path) -> None:
    result = QtrMicroPreflight(
        settings(),
        FakeClient(),
        state_store=JsonQtrMicroStateStore(tmp_path / "qtr_micro_state.json"),
        clock=lambda: NOW,
    ).run()
    raw = preflight_json(result)
    payload = json.loads(raw)
    assert payload["ready"] is True
    assert payload["host"] == "api-demo.bybit.com"
    assert payload["account_type"] == "UNIFIED"
    assert payload["instrument_rules"]["qty_step"] == 0.001
    assert payload["blocking_reasons"] == []
    assert "demo-key" not in raw
    assert "demo-secret" not in raw


def test_cli_preflight_ready_exit_code_and_json_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    ready = QtrMicroPreflight(settings(), FakeClient()).run()
    monkeypatch.setenv("QTR_MICRO_ENABLED", "true")
    monkeypatch.setenv("BYBIT_DEMO_API_KEY", "not-printed-key")
    monkeypatch.setenv("BYBIT_DEMO_API_SECRET", "not-printed-secret")
    monkeypatch.setattr(QtrMicroPreflight, "run", lambda self, symbol: ready)

    micro_cli_main(("preflight", "--symbol", "BTCUSDT", "--json"))

    raw = capsys.readouterr().out
    payload = json.loads(raw)
    assert payload["ready"] is True
    assert payload["symbol"] == "BTCUSDT"
    assert "not-printed-key" not in raw
    assert "not-printed-secret" not in raw


def test_cli_preflight_blocked_has_nonzero_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    blocked_client = FakeClient()
    blocked_client.auth_fail = True
    blocked = QtrMicroPreflight(settings(), blocked_client).run()
    monkeypatch.setenv("QTR_MICRO_ENABLED", "true")
    monkeypatch.setenv("BYBIT_DEMO_API_KEY", "demo-key")
    monkeypatch.setenv("BYBIT_DEMO_API_SECRET", "demo-secret")
    monkeypatch.setattr(QtrMicroPreflight, "run", lambda self, symbol: blocked)

    with pytest.raises(SystemExit) as exit_info:
        micro_cli_main(("preflight", "--symbol", "BTCUSDT"))

    assert exit_info.value.code == 2
    assert "ЗАБЛОКИРОВАН" in capsys.readouterr().out


def test_cli_wrong_host_is_displayed_as_blocked_without_network(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("QTR_MICRO_ENABLED", "true")
    monkeypatch.setenv("BYBIT_DEMO_BASE_URL", "https://api.bybit.com")
    monkeypatch.setenv("BYBIT_DEMO_API_KEY", "demo-key")
    monkeypatch.setenv("BYBIT_DEMO_API_SECRET", "demo-secret")

    with pytest.raises(SystemExit) as exit_info:
        micro_cli_main(("preflight", "--symbol", "BTCUSDT"))

    text = capsys.readouterr().out
    assert exit_info.value.code == 2
    assert "api.bybit.com" in text
    assert "ЗАБЛОКИРОВАН" in text
    assert "api-demo.bybit.com" in text


def test_preflight_never_calls_order_endpoint_or_changes_entry_decision() -> None:
    before = decision().plan
    client = FakeClient()

    result = QtrMicroPreflight(settings(), client).run()

    assert result.ready is True
    assert client.orders == []
    assert decision().plan == before


def test_runtime_entry_continues_when_leverage_is_already_set(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    client.leverage_result = LeverageUpdateResult.ALREADY_SET
    store = JsonQtrMicroStateStore(tmp_path / "qtr_micro_state.json")
    store.save(state())
    service = QtrMicroExecutionService(
        settings=settings(),
        client=client,
        state_store=store,
        engine=QtrMicroEntryEngine(settings()),
    )
    plan = decision().plan
    assert plan is not None

    submitted = service.submit_entry(plan, NOW)

    assert submitted.stage is MicroStage.ENTRY_ACKNOWLEDGED
    assert len(client.orders) == 1
    assert client.order_call_count == 1
    assert client.orders[0]["order_link_id"] == plan.order_link_id


def test_ready_retest_eth_runs_prepare_submit_ack_fill_and_protection(
    tmp_path: Path,
) -> None:
    item = candidate(
        episode="eth-retest",
        input_changes={"symbol": "ETHUSDT"},
        result_changes={"setup_type": SetupType.RETEST},
    )
    plan = decision(item, rules=replace(RULES, symbol="ETHUSDT")).plan
    assert plan is not None
    client = FakeClient()
    state_path = tmp_path / "qtr_micro_state.json"
    audit_path = tmp_path / "qtr_micro_runtime_audit.jsonl"
    store = JsonQtrMicroStateStore(state_path)
    store.save(state())
    service = QtrMicroExecutionService(
        settings=settings(),
        client=client,
        state_store=store,
        engine=QtrMicroEntryEngine(settings()),
        runtime_audit=JsonlQtrMicroRuntimeAudit(audit_path),
    )

    acknowledged = service.submit_entry(plan, NOW)

    assert acknowledged.stage is MicroStage.ENTRY_ACKNOWLEDGED
    assert client.leverage_calls == [("ETHUSDT", plan.leverage)]
    assert len(client.orders) == 1
    assert client.order_call_count == 1
    assert client.orders[0]["symbol"] == "ETHUSDT"
    persisted = store.load(today=NOW.date(), trading_enabled=True)
    assert persisted.positions[plan.trade_id].entry_order_id == "order-1"
    assert persisted.positions[plan.trade_id].order_submitted_at == NOW
    assert service.confirm_entry(plan.trade_id, NOW) is None
    client.fill = ExecutionFill("order-1", plan.entry_price, plan.qty, 0.1, NOW)

    protected = service.confirm_entry(plan.trade_id, NOW)

    assert protected is not None
    assert protected.stage is MicroStage.OPEN
    assert protected.average_fill == plan.entry_price
    assert protected.filled_qty == plan.qty
    assert client.stop_calls == 1
    assert not state_path.with_suffix(".json.tmp").exists()
    rows = [
        json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines()
    ]
    assert [row["event"] for row in rows] == [
        "PREPARED",
        "LEVERAGE_READY",
        "ENTRY_SUBMIT_ATTEMPT",
        "ENTRY_ACK",
        "ENTRY_CONFIRMATION_WAIT",
        "ENTRY_FILLED",
        "ACTUAL_RISK_CHECK",
        "PROTECTION_ATTEMPT",
        "ENTRY_PROTECTED",
    ]
    assert all(row["symbol"] == "ETHUSDT" for row in rows)
    assert "demo-secret" not in audit_path.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    ("failure_point", "expected_event", "order_calls"),
    (
        ("leverage", QtrMicroRuntimeEvent.ENTRY_ABORTED, 0),
        ("order", QtrMicroRuntimeEvent.ENTRY_REJECTED, 1),
    ),
)
def test_failure_after_prepared_records_exact_abort_boundary(
    tmp_path: Path,
    failure_point: str,
    expected_event: QtrMicroRuntimeEvent,
    order_calls: int,
) -> None:
    plan = decision(
        candidate(input_changes={"symbol": "ETHUSDT"}),
        rules=replace(RULES, symbol="ETHUSDT"),
    ).plan
    assert plan is not None
    client = FakeClient()
    client.auto_fill = True
    error = DemoApiError("Bybit Demo API отклонил запрос, код 110013.")
    if failure_point == "leverage":
        client.leverage_error = error
    else:
        client.order_error = error
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    store.save(state())
    audit_path = tmp_path / "runtime.jsonl"
    service = QtrMicroExecutionService(
        settings=settings(),
        client=client,
        state_store=store,
        engine=QtrMicroEntryEngine(settings()),
        runtime_audit=JsonlQtrMicroRuntimeAudit(audit_path),
    )

    with pytest.raises(DemoApiError):
        service.submit_entry(plan, NOW)

    persisted = store.load(today=NOW.date(), trading_enabled=True)
    assert persisted.positions[plan.trade_id].stage is MicroStage.PREPARED
    assert client.order_call_count == order_calls
    rows = [
        json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines()
    ]
    events = [row["event"] for row in rows]
    assert events[0] == "PREPARED"
    assert events[-1] == expected_event.value
    assert bool("ENTRY_SUBMIT_ATTEMPT" in events) is (failure_point == "order")
    assert rows[-1]["reason"] == ("Bybit Demo API отклонил запрос, код 110013.")


def test_reconciliation_audits_prepared_without_remote_order_as_aborted(
    tmp_path: Path,
) -> None:
    plan = decision().plan
    assert plan is not None
    prepared = replace(
        position_from_plan(plan),
        stage=MicroStage.PREPARED,
        entry_order_id=None,
        average_fill=None,
        filled_qty=0.0,
        current_qty=0.0,
        order_submitted_at=None,
    )
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    store.save(state(positions={plan.trade_id: prepared}))
    audit_path = tmp_path / "runtime.jsonl"
    service = QtrMicroExecutionService(
        settings=settings(),
        client=FakeClient(),
        state_store=store,
        engine=QtrMicroEntryEngine(settings()),
        runtime_audit=JsonlQtrMicroRuntimeAudit(audit_path),
    )

    reconciled = service.reconcile(NOW)

    assert reconciled.positions[plan.trade_id].stage is MicroStage.CLOSED
    row = json.loads(audit_path.read_text(encoding="utf-8"))
    assert row["event"] == "ENTRY_ABORTED"
    assert "Reconciliation" in row["reason"]


@pytest.mark.parametrize("symbol", ("BTCUSDT", "ETHUSDT", "SOLUSDT"))
def test_runtime_uses_setup_symbol_end_to_end_without_btc_substitution(
    tmp_path: Path,
    symbol: str,
) -> None:
    client = FakeClient()
    client.auto_fill = True
    client.rules_by_symbol[symbol] = replace(RULES, symbol=symbol)
    store = JsonQtrMicroStateStore(tmp_path / f"{symbol}-state.json")
    runtime = QtrMicroRuntime(
        settings=settings(),
        client=client,
        state_store=store,
        allowed_chat_ids=frozenset(),
        clock=lambda: NOW,
        decision_audit=JsonlQtrMicroDecisionAudit(
            tmp_path / f"{symbol}-decisions.jsonl"
        ),
        runtime_audit=JsonlQtrMicroRuntimeAudit(tmp_path / f"{symbol}-runtime.jsonl"),
    )
    item = candidate(
        episode=f"episode-{symbol}",
        input_changes={"symbol": symbol},
    )

    async def exercise() -> None:
        assert (await runtime.initialize()).ready
        await runtime.handle_candidates((item,), _discard_message)

    asyncio.run(exercise())

    assert client.instrument_rule_calls == [symbol]
    assert len(client.orders) == 1
    assert client.orders[0]["symbol"] == symbol
    assert "BTCUSDT" not in str(client.orders[0]) or symbol == "BTCUSDT"


@pytest.mark.parametrize(
    ("symbol", "universe_status"),
    (
        ("JNJUSDT", InstrumentUniverseStatus.STOCK),
        ("UNITREEUSDT", InstrumentUniverseStatus.PRELAUNCH),
    ),
)
def test_runtime_skips_stock_and_prelaunch_without_order(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    symbol: str,
    universe_status: InstrumentUniverseStatus,
) -> None:
    client = FakeClient()
    client.rules_by_symbol[symbol] = replace(
        RULES,
        symbol=symbol,
        universe_status=universe_status,
    )
    runtime = QtrMicroRuntime(
        settings=settings(),
        client=client,
        state_store=JsonQtrMicroStateStore(tmp_path / f"{symbol}-state.json"),
        allowed_chat_ids=frozenset(),
        clock=lambda: NOW,
        decision_audit=JsonlQtrMicroDecisionAudit(
            tmp_path / f"{symbol}-decisions.jsonl"
        ),
        runtime_audit=JsonlQtrMicroRuntimeAudit(tmp_path / f"{symbol}-runtime.jsonl"),
    )
    item = candidate(input_changes={"symbol": symbol})

    async def exercise() -> None:
        assert (await runtime.initialize()).ready
        await runtime.handle_candidates((item,), _discard_message)

    with caplog.at_level(logging.INFO):
        asyncio.run(exercise())

    assert client.orders == []
    assert "Инструмент не входит в торговую вселенную QTR Micro" in caplog.text
    assert universe_status.value in caplog.text
    audit_row = json.loads(
        (tmp_path / f"{symbol}-decisions.jsonl").read_text(encoding="utf-8")
    )
    assert audit_row["symbol"] == symbol
    assert audit_row["skip_reason"] == "unsupported_instrument"
    assert audit_row["instrument_status"] == universe_status.value
    assert audit_row["skip_detail"] == (
        "Инструмент не входит в торговую вселенную QTR Micro."
    )


def test_ready_trade_eligible_creates_market_candidate_with_base_risk() -> None:
    plan = decision().plan
    assert plan is not None
    assert plan.direction is MicroDirection.LONG
    assert plan.risk_pct == 0.5
    assert plan.risk_amount == 50
    assert plan.leverage == 5
    assert plan.order_link_id.startswith("QTRM-")


def test_operational_leverage_never_drops_below_five_or_above_ten() -> None:
    low = decision(config=settings(base_leverage=1)).plan
    assert low is not None and low.leverage == 5
    high = decision(config=settings(base_leverage=10)).plan
    assert high is not None and high.leverage == 10


@pytest.mark.parametrize(
    "setup_state",
    (
        SetupState.FORMING,
        SetupState.CONFIRMING,
        SetupState.LATE,
        SetupState.CANCELLED,
    ),
)
def test_non_ready_states_do_not_trade(setup_state: SetupState) -> None:
    result = decision(candidate(result_changes={"setup_state": setup_state}))
    assert result.skip_reason is EntrySkipReason.INVALID_STATE


@pytest.mark.parametrize(
    "setup_type",
    (
        SetupType.FALSE_BREAKOUT,
        SetupType.REVERSAL,
        SetupType.IMPULSE,
        SetupType.COMPRESSION,
        SetupType.NO_TRADE,
    ),
)
def test_invalid_setup_types_do_not_trade(setup_type: SetupType) -> None:
    result = decision(candidate(result_changes={"setup_type": setup_type}))
    assert result.skip_reason is EntrySkipReason.INVALID_TYPE


def test_neutral_stale_and_chased_signals_are_skipped() -> None:
    neutral = candidate(result_changes={"direction": SetupDirection.NEUTRAL})
    assert decision(neutral).skip_reason is EntrySkipReason.INVALID_DIRECTION
    assert (
        decision(now=NOW + timedelta(seconds=61)).skip_reason is EntrySkipReason.STALE
    )
    chased = candidate(result_changes={"distance_to_trigger_atr": 0.251})
    assert decision(chased).skip_reason is EntrySkipReason.TOO_FAR


def test_structural_stop_and_atr_are_required() -> None:
    missing_stop = candidate(result_changes={"invalidation_level": None})
    assert decision(missing_stop).skip_reason is EntrySkipReason.STRUCTURAL_STOP_MISSING
    assert decision(candidate(atr=None)).skip_reason is EntrySkipReason.ATR_MISSING


def test_qty_is_risk_divided_by_stop_and_rounded_down() -> None:
    plan = decision().plan
    assert plan is not None
    expected = plan.risk_amount / abs(plan.entry_price - plan.stop_price)
    assert plan.qty <= expected
    assert round(plan.qty / RULES.qty_step) == plan.qty / RULES.qty_step


def test_alt_uses_own_qty_step_instead_of_btc_rules() -> None:
    alt = candidate(
        episode="alt-episode",
        input_changes={"symbol": "ALTUSDT"},
    )
    btc_plan = decision().plan
    alt_plan = decision(
        alt,
        rules=InstrumentRules("ALTUSDT", 1.0, 1.0, 1000.0, 5.0, 20),
    ).plan

    assert btc_plan is not None and alt_plan is not None
    assert btc_plan.qty != int(btc_plan.qty)
    assert alt_plan.qty == int(alt_plan.qty)
    assert alt_plan.symbol == "ALTUSDT"


def test_instrument_specific_min_notional_max_qty_and_leverage_are_enforced() -> None:
    alt = candidate(input_changes={"symbol": "ALTUSDT"})
    base = InstrumentRules("ALTUSDT", 1.0, 1.0, 1000.0, 5.0, 20)

    assert (
        decision(alt, rules=replace(base, min_notional_value=100_000)).skip_reason
        is EntrySkipReason.QUANTITY_LIMITS
    )
    assert (
        decision(alt, rules=replace(base, max_market_order_qty=1)).skip_reason
        is EntrySkipReason.QUANTITY_LIMITS
    )
    assert (
        decision(alt, rules=replace(base, max_leverage=4)).skip_reason
        is EntrySkipReason.LEVERAGE_LIMIT
    )


@pytest.mark.parametrize(
    "universe_status",
    (
        InstrumentUniverseStatus.STOCK,
        InstrumentUniverseStatus.PRELAUNCH,
        InstrumentUniverseStatus.NON_USDT,
        InstrumentUniverseStatus.UNSUPPORTED_CONTRACT,
        InstrumentUniverseStatus.UNSUPPORTED_STATUS,
    ),
)
def test_ready_setup_outside_qtr_universe_is_skipped_with_auditable_reason(
    universe_status: InstrumentUniverseStatus,
) -> None:
    item = candidate(input_changes={"symbol": "BLOCKEDUSDT"})
    result = decision(
        item,
        rules=replace(
            RULES,
            symbol="BLOCKEDUSDT",
            universe_status=universe_status,
        ),
    )

    assert result.skip_reason is EntrySkipReason.UNSUPPORTED_INSTRUMENT
    assert result.instrument_status is universe_status
    assert result.skip_detail == (
        "Инструмент не входит в торговую вселенную QTR Micro."
    )


def test_order_link_id_is_symbol_episode_specific_without_btc_dependency() -> None:
    eth = decision(
        candidate(episode="eth-episode", input_changes={"symbol": "ETHUSDT"}),
        rules=replace(RULES, symbol="ETHUSDT"),
    ).plan
    sol = decision(
        candidate(episode="sol-episode", input_changes={"symbol": "SOLUSDT"}),
        rules=replace(RULES, symbol="SOLUSDT"),
    ).plan

    assert eth is not None and sol is not None
    assert eth.order_link_id.startswith("QTRM-")
    assert sol.order_link_id.startswith("QTRM-")
    assert eth.order_link_id != sol.order_link_id
    assert "BTC" not in eth.order_link_id
    assert "BTC" not in sol.order_link_id


def test_min_notional_and_leverage_limits_skip() -> None:
    strict_rules = replace(RULES, min_notional_value=100_000)
    assert decision(rules=strict_rules).skip_reason is EntrySkipReason.QUANTITY_LIMITS
    narrow_stop = candidate(result_changes={"invalidation_level": 100.99}, atr=0.01)
    low_equity = decision(
        narrow_stop,
        config=settings(
            max_estimated_fees_r_pct=100,
            max_notional_equity_pct=1000,
        ),
        equity=100,
        rules=replace(RULES, min_notional_value=1, max_leverage=5),
    )
    assert low_equity.skip_reason is EntrySkipReason.LEVERAGE_LIMIT


def test_partial_quantities_are_40_30_and_safe_rounding() -> None:
    plan = decision().plan
    assert plan is not None
    assert plan.tp1_qty <= plan.qty * 0.40
    assert plan.tp2_qty <= plan.qty * 0.30
    assert plan.tp1_qty + plan.tp2_qty + plan.runner_qty <= plan.qty
    assert plan.runner_qty > 0


def test_position_and_loss_safety_gates() -> None:
    plan = decision().plan
    assert plan is not None
    existing = position_from_plan(plan)
    duplicate_state = state(positions={plan.trade_id: existing})
    assert (
        decision(current_state=duplicate_state).skip_reason
        is EntrySkipReason.DUPLICATE_EPISODE
    )
    other = replace(existing, trade_id="QTRM-other", setup_episode_id="other")
    assert (
        decision(current_state=state(positions={other.trade_id: other})).skip_reason
        is EntrySkipReason.SYMBOL_ALREADY_OPEN
    )
    two = {
        "one": replace(other, trade_id="one", symbol="ETHUSDT"),
        "two": replace(other, trade_id="two", symbol="SOLUSDT"),
    }
    assert (
        decision(current_state=state(positions=two)).skip_reason
        is EntrySkipReason.MAX_POSITIONS
    )


def test_kill_switch_three_loss_pause_and_daily_limit() -> None:
    assert (
        decision(config=settings(kill_switch=True)).skip_reason
        is EntrySkipReason.KILL_SWITCH
    )
    current = state()
    for _ in range(3):
        current = record_trade_result(current, pnl=-10, now=NOW, settings=settings())
    assert current.loss_pause_until == NOW + timedelta(minutes=30)
    assert decision(current_state=current).skip_reason is EntrySkipReason.LOSS_PAUSE
    daily = state(realised_daily_pnl=-200)
    assert decision(current_state=daily).skip_reason is EntrySkipReason.DAILY_LOSS_LIMIT
    assert decision(
        current_state=daily,
        now=NOW + timedelta(days=1),
        item=candidate(result_changes={"analyzed_at": NOW + timedelta(days=1)}),
    ).accepted


def position_from_plan(plan: Any, **changes: Any) -> MicroPosition:
    baseline = MicroPosition(
        trade_id=plan.trade_id,
        setup_episode_id=plan.setup_episode_id,
        symbol=plan.symbol,
        direction=plan.direction,
        setup_type=plan.setup_type,
        setup_confidence=plan.setup_confidence,
        entry_order_link_id=plan.order_link_id,
        entry_order_id="order-1",
        average_fill=plan.entry_price,
        filled_qty=plan.qty,
        initial_qty=plan.qty,
        current_qty=plan.qty,
        leverage=plan.leverage,
        risk_pct=plan.risk_pct,
        risk_amount=plan.risk_amount,
        structural_stop=plan.stop_price,
        current_stop=plan.stop_price,
        initial_r=plan.initial_r,
        tp1_price=plan.tp1_price,
        tp1_qty=plan.tp1_qty,
        tp2_price=plan.tp2_price,
        tp2_qty=plan.tp2_qty,
        runner_target_price=plan.runner_target_price,
        runner_qty=plan.runner_qty,
        realised_partial_pnl=0,
        fees=0,
        opened_at=NOW,
        last_updated=NOW,
        stage=MicroStage.OPEN,
        signal_at=NOW,
        signal_price=plan.signal_price,
    )
    return replace(baseline, **changes)


def runtime_management_decision(
    tmp_path: Path,
    *,
    item: QtrSetupCandidate,
    position: MicroPosition,
    now: datetime,
) -> tuple[ManagementDecision, tuple[bool, bool, bool]]:
    store = JsonQtrMicroStateStore(tmp_path / "management-state.json")
    store.save(state(positions={position.trade_id: position}))
    runtime = QtrMicroRuntime(
        settings=settings(),
        client=FakeClient(),
        state_store=store,
        allowed_chat_ids=frozenset(),
        clock=lambda: now,
    )
    spy = ManagementExecutionSpy(position, QtrMicroEntryEngine(settings()))
    runtime._execution = spy  # type: ignore[assignment]  # noqa: SLF001
    current_state = store.load(today=now.date(), trading_enabled=True)

    asyncio.run(
        runtime._manage_open(  # noqa: SLF001
            current_state,
            {position.symbol: item},
            _discard_message,
            now,
        )
    )

    assert len(spy.decisions) == 1
    assert len(spy.flags) == 1
    return spy.decisions[0], spy.flags[0]


def test_management_tp_breakeven_and_time_structure_exits() -> None:
    engine = QtrMicroEntryEngine(settings())
    plan = decision().plan
    assert plan is not None
    position = position_from_plan(plan)
    tp1 = engine.manage(position, current_price=plan.tp1_price, now=NOW)
    assert tp1.action is MicroExitReason.TP1
    assert tp1.close_qty == plan.tp1_qty
    assert tp1.new_stop == plan.entry_price
    time_exit = engine.manage(
        position,
        current_price=plan.entry_price,
        now=NOW + timedelta(minutes=45),
    )
    assert time_exit.action is MicroExitReason.TIME_EXIT
    runner = replace(position, stage=MicroStage.RUNNER, current_qty=plan.runner_qty)
    runner_exit = engine.manage(
        runner,
        current_price=plan.entry_price,
        now=NOW + timedelta(minutes=90),
    )
    assert runner_exit.action is MicroExitReason.RUNNER_TIME_EXIT
    structure = engine.manage(
        position, current_price=plan.entry_price, now=NOW, setup_cancelled=True
    )
    assert structure.action is MicroExitReason.STRUCTURE_EXIT


def test_create_order_ack_is_not_fill_and_execution_confirms_stop(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    store = JsonQtrMicroStateStore(tmp_path / "qtr_micro_state.json")
    store.save(state())
    engine = QtrMicroEntryEngine(settings())
    service = QtrMicroExecutionService(
        settings=settings(), client=client, state_store=store, engine=engine
    )
    plan = decision().plan
    assert plan is not None
    acknowledged = service.submit_entry(plan, NOW)
    assert acknowledged.stage is MicroStage.ENTRY_ACKNOWLEDGED
    assert acknowledged.average_fill is None
    assert service.confirm_entry(plan.trade_id, NOW) is None
    client.fill = ExecutionFill("order-1", 101.1, plan.qty, 0.2, NOW)
    opened = service.confirm_entry(plan.trade_id, NOW)
    assert opened is not None
    assert opened.stage is MicroStage.OPEN
    assert client.stop_calls == 1


def test_stop_failure_emergency_closes_reduce_only_and_blocks(tmp_path: Path) -> None:
    client = FakeClient()
    store = JsonQtrMicroStateStore(tmp_path / "qtr_micro_state.json")
    store.save(state())
    service = QtrMicroExecutionService(
        settings=settings(),
        client=client,
        state_store=store,
        engine=QtrMicroEntryEngine(settings()),
    )
    plan = decision().plan
    assert plan is not None
    service.submit_entry(plan, NOW)
    client.fill = ExecutionFill("order-1", 101, plan.qty, 0, NOW)
    client.fail = True
    blocked = service.confirm_entry(plan.trade_id, NOW)
    assert blocked is not None and blocked.stage is MicroStage.BLOCKED
    assert client.orders[-1]["reduce_only"] is True
    persisted = store.load(today=NOW.date(), trading_enabled=True)
    assert persisted.trading_enabled is False


def test_partial_exit_ack_waits_for_execution_before_qty_and_breakeven(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    store = JsonQtrMicroStateStore(tmp_path / "qtr_micro_state.json")
    store.save(state())
    service = QtrMicroExecutionService(
        settings=settings(),
        client=client,
        state_store=store,
        engine=QtrMicroEntryEngine(settings()),
    )
    plan = decision().plan
    assert plan is not None
    service.submit_entry(plan, NOW)
    client.fill = ExecutionFill("order-1", plan.entry_price, plan.qty, 0, NOW)
    opened = service.confirm_entry(plan.trade_id, NOW)
    assert opened is not None
    first = service.manage_position(
        plan.trade_id, current_price=plan.tp1_price, now=NOW
    )
    assert first.action is None
    pending = store.load(today=NOW.date(), trading_enabled=True).positions[
        plan.trade_id
    ]
    assert pending.stage is MicroStage.EXIT_ACKNOWLEDGED
    assert pending.current_qty == plan.qty
    client.fill = ExecutionFill("order-1", plan.tp1_price, plan.tp1_qty, 0.1, NOW)
    confirmed = service.manage_position(
        plan.trade_id, current_price=plan.tp1_price, now=NOW
    )
    assert confirmed.action is MicroExitReason.TP1
    updated = store.load(today=NOW.date(), trading_enabled=True).positions[
        plan.trade_id
    ]
    assert updated.stage is MicroStage.TP1_FILLED
    assert updated.current_qty == pytest.approx(plan.qty - plan.tp1_qty)
    assert updated.current_stop == plan.entry_price


def test_confirmed_full_exit_writes_one_lifecycle_journal_row(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    store = JsonQtrMicroStateStore(tmp_path / "qtr_micro_state.json")
    journal_path = tmp_path / "qtr_micro_trades.jsonl"
    store.save(state())
    service = QtrMicroExecutionService(
        settings=settings(),
        client=client,
        state_store=store,
        engine=QtrMicroEntryEngine(settings()),
        journal=JsonlQtrMicroTradeJournal(journal_path),
    )
    plan = decision().plan
    assert plan is not None
    service.submit_entry(plan, NOW)
    client.fill = ExecutionFill("order-1", plan.entry_price, plan.qty, 0.1, NOW)
    service.confirm_entry(plan.trade_id, NOW)
    pending = service.manage_position(
        plan.trade_id,
        current_price=plan.entry_price,
        now=NOW + timedelta(minutes=1),
        setup_cancelled=True,
    )
    assert pending.action is None
    client.fill = ExecutionFill(
        "order-1",
        plan.entry_price + 0.5,
        plan.qty,
        0.2,
        NOW + timedelta(minutes=1),
    )
    closed = service.manage_position(
        plan.trade_id,
        current_price=plan.entry_price + 0.5,
        now=NOW + timedelta(minutes=1),
        setup_cancelled=True,
    )
    assert closed.action is MicroExitReason.STRUCTURE_EXIT
    rows = journal_path.read_text(encoding="utf-8").splitlines()
    assert len(rows) == 1
    payload = json.loads(rows[0])
    assert payload["exit_reason"] == "STRUCTURE_EXIT"
    assert payload["realised_net_pnl"] > 0


def test_state_atomic_corruption_recovery_and_no_secrets(tmp_path: Path) -> None:
    path = tmp_path / "qtr_micro_state.json"
    store = JsonQtrMicroStateStore(path)
    plan = decision().plan
    assert plan is not None
    store.save(state(positions={plan.trade_id: position_from_plan(plan)}))
    raw = path.read_text(encoding="utf-8")
    assert "demo-secret" not in raw
    assert json.loads(raw)["schema_version"] == 2
    assert not path.with_suffix(".json.tmp").exists()
    path.write_text("{broken", encoding="utf-8")
    recovered = store.load(today=NOW.date(), trading_enabled=True)
    assert recovered.trading_enabled is False
    assert path.with_suffix(".json.corrupt").exists()


def test_repair_v2_doge_narrow_stop_is_capped_or_rejected_for_fees() -> None:
    item = candidate(
        input_changes={
            "symbol": "DOGEUSDT",
            "current_price": 0.1000,
            "trigger_level": 0.0999,
            "invalidation_level": 0.0998,
            "distance_to_trigger_atr": 0.1,
        },
        result_changes={
            "symbol": "DOGEUSDT",
            "current_price": 0.1000,
            "trigger_level": 0.0999,
            "invalidation_level": 0.0998,
            "distance_to_trigger_atr": 0.1,
        },
        atr=0.001,
    )
    result = decision(
        item,
        rules=InstrumentRules("DOGEUSDT", 1, 1, 10_000_000, 5, 100),
    )
    assert result.skip_reason is EntrySkipReason.FEES_TOO_HIGH or (
        result.plan is not None
        and result.plan.notional <= settings().max_notional_usdt
        and result.plan.notional <= 10_000
    )


def test_repair_v2_cys_price_deviation_revalidation_blocks_submit() -> None:
    item = candidate(
        input_changes={
            "symbol": "CYSUSDT",
            "current_price": 1.0483,
            "trigger_level": 1.0480,
            "invalidation_level": 1.0300,
        },
        result_changes={
            "symbol": "CYSUSDT",
            "current_price": 1.0483,
            "trigger_level": 1.0480,
            "invalidation_level": 1.0300,
        },
        atr=0.01,
    )
    rules = InstrumentRules("CYSUSDT", 0.1, 0.1, 1_000_000, 5, 100)
    engine = QtrMicroEntryEngine(settings())
    initial = engine.prepare_entry(
        item,
        now=NOW,
        equity=10_000,
        rules=rules,
        state=state(),
        preflight=preflight(),
    )
    assert initial.plan is not None
    fresh = engine.revalidate_entry(
        item,
        initial.plan,
        current_price=1.1021,
        now=NOW,
        equity=10_000,
        rules=rules,
        state=state(),
        preflight=preflight(),
    )
    assert fresh.plan is None
    assert fresh.skip_reason is EntrySkipReason.TOO_FAR


def test_repair_v2_btc_excessive_fee_share_is_rejected() -> None:
    item = candidate(
        input_changes={
            "symbol": "BTCUSDT",
            "current_price": 60_000.0,
            "trigger_level": 59_999.9,
            "invalidation_level": 59_999.0,
            "distance_to_trigger_atr": 0.1,
        },
        result_changes={
            "symbol": "BTCUSDT",
            "current_price": 60_000.0,
            "trigger_level": 59_999.9,
            "invalidation_level": 59_999.0,
            "distance_to_trigger_atr": 0.1,
        },
        atr=1.0,
    )
    result = decision(
        item,
        rules=InstrumentRules("BTCUSDT", 0.001, 0.001, 100, 5, 100),
    )
    assert result.skip_reason is EntrySkipReason.FEES_TOO_HIGH


@pytest.mark.parametrize(
    ("symbol", "price", "trigger", "invalidation", "atr", "step"),
    (
        ("XRPUSDT", 0.50, 0.499, 0.48, 0.01, 1.0),
        ("ZECUSDT", 40.0, 39.9, 38.5, 1.0, 0.01),
    ),
)
def test_repair_v2_normal_profitable_paths_remain_entry_eligible(
    symbol: str,
    price: float,
    trigger: float,
    invalidation: float,
    atr: float,
    step: float,
) -> None:
    item = candidate(
        input_changes={
            "symbol": symbol,
            "current_price": price,
            "trigger_level": trigger,
            "invalidation_level": invalidation,
            "distance_to_trigger_atr": abs(price - trigger) / atr,
        },
        result_changes={
            "symbol": symbol,
            "current_price": price,
            "trigger_level": trigger,
            "invalidation_level": invalidation,
            "distance_to_trigger_atr": abs(price - trigger) / atr,
        },
        atr=atr,
    )
    result = decision(
        item,
        rules=InstrumentRules(symbol, step, step, 1_000_000, 5, 100),
    )
    assert result.plan is not None
    assert result.plan.notional <= 10_000
    assert result.plan.estimated_fees_r_pct <= 20


def test_repair_v2_fast_fill_partial_fill_and_immediate_stop(tmp_path: Path) -> None:
    plan = decision().plan
    assert plan is not None
    client = FakeClient()
    client.fill = ExecutionFill("order-1", plan.entry_price, plan.qty / 2, 0.1, NOW)
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    store.save(state())
    service = QtrMicroExecutionService(
        settings=settings(),
        client=client,
        state_store=store,
        engine=QtrMicroEntryEngine(settings()),
        sleeper=lambda _: None,
    )
    opened = service.submit_and_confirm_entry(plan, NOW, RULES)
    assert opened.stage is MicroStage.OPEN
    assert opened.filled_qty == plan.qty / 2
    assert client.cancelled_orders == [(plan.symbol, "order-1")]
    assert client.stop_calls == 1


def test_repair_v2_fill_slippage_resizes_actual_risk_before_stop(
    tmp_path: Path,
) -> None:
    plan = decision().plan
    assert plan is not None
    client = FakeClient()
    client.fill = ExecutionFill("order-1", 105.0, plan.qty, 0.2, NOW)
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    store.save(state())
    service = QtrMicroExecutionService(
        settings=settings(),
        client=client,
        state_store=store,
        engine=QtrMicroEntryEngine(settings()),
        sleeper=lambda _: None,
    )
    opened = service.submit_and_confirm_entry(plan, NOW, RULES)
    assert opened.stage is MicroStage.OPEN
    assert opened.current_qty < plan.qty
    assert opened.actual_risk_at_fill is not None
    assert opened.actual_risk_at_fill <= plan.risk_amount * 1.1
    assert client.orders[-1]["reduce_only"] is True
    assert client.stop_calls == 1


def test_repair_v2_confirmation_timeout_cancels_without_open(tmp_path: Path) -> None:
    plan = decision().plan
    assert plan is not None
    client = FakeClient()
    ticks = iter((0.0, 21.0))
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    store.save(state())
    service = QtrMicroExecutionService(
        settings=settings(),
        client=client,
        state_store=store,
        engine=QtrMicroEntryEngine(settings()),
        sleeper=lambda _: None,
        monotonic=lambda: next(ticks),
    )
    result = service.submit_and_confirm_entry(plan, NOW, RULES)
    assert result.stage is MicroStage.CLOSED
    assert client.cancelled_orders == [(plan.symbol, "order-1")]


@pytest.mark.parametrize(
    ("ret_code", "reason_fragment"),
    ((110007, "баланса"), (110090, "лимит инструмента")),
)
def test_repair_v2_typed_order_rejections_keep_ret_code(
    tmp_path: Path, ret_code: int, reason_fragment: str
) -> None:
    plan = decision().plan
    assert plan is not None
    client = FakeClient()
    client.order_error = DemoApiError("rejected", ret_code=ret_code)
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    store.save(state())
    audit_path = tmp_path / "audit.jsonl"
    service = QtrMicroExecutionService(
        settings=settings(),
        client=client,
        state_store=store,
        engine=QtrMicroEntryEngine(settings()),
        runtime_audit=JsonlQtrMicroRuntimeAudit(audit_path),
    )
    with pytest.raises(DemoApiError):
        service.submit_entry(plan, NOW)
    row = json.loads(audit_path.read_text(encoding="utf-8").splitlines()[-1])
    assert row["ret_code"] == ret_code
    assert reason_fragment in row["reason"]


def test_repair_v2_journal_append_is_idempotent_after_restart(tmp_path: Path) -> None:
    path = tmp_path / "trades.jsonl"
    journal = JsonlQtrMicroTradeJournal(path)
    entry = TradeJournalEntry(
        trade_id="QTRM-restart",
        setup_episode="episode",
        symbol="XRPUSDT",
        direction=MicroDirection.LONG,
        setup_type="РЕТЕСТ",
        setup_confidence=80,
        entry_signal_timestamp=NOW,
        order_submit_timestamp=NOW,
        fill_timestamp=NOW,
        signal_price=1,
        average_fill=1,
        slippage=0,
        initial_stop=0.99,
        risk_pct=0.5,
        risk_usdt=50,
        leverage=5,
        qty=100,
        tp1_fill=None,
        tp2_fill=None,
        runner_exit=1.01,
        structure_time_exits=(),
        fees=0.1,
        funding=None,
        realised_gross_pnl=1,
        realised_net_pnl=0.9,
        result_r=0.018,
        max_favorable_excursion=0.01,
        max_adverse_excursion=0,
        hold_duration_seconds=60,
        exit_reason="TIME_EXIT",
        outcome="winning",
    )
    assert journal.append_once(entry) is True
    restarted = JsonlQtrMicroTradeJournal(path)
    assert restarted.append_once(entry) is False
    assert len(path.read_text(encoding="utf-8").splitlines()) == 1


def test_reconciliation_recovers_owned_and_ignores_foreign() -> None:
    remote = DemoPosition("BTCUSDT", "Buy", 1, 100)
    orders = (
        DemoOrder("BTCUSDT", "owned", "QTRM-owned", "Buy", 1, "Filled"),
        DemoOrder("ETHUSDT", "foreign", "MANUAL-1", "Buy", 1, "Filled"),
    )
    reconciled = reconcile_demo_state(
        state(), positions=(remote,), orders=orders, now=NOW
    )
    assert "QTRM-owned" in reconciled.positions
    assert all(item.symbol != "ETHUSDT" for item in reconciled.positions.values())
    assert reconciled.trading_enabled is False


def test_reconciliation_preserves_multiple_dynamic_symbols() -> None:
    btc_plan = decision().plan
    eth_plan = decision(
        candidate(episode="eth", input_changes={"symbol": "ETHUSDT"}),
        rules=replace(RULES, symbol="ETHUSDT"),
    ).plan
    assert btc_plan is not None and eth_plan is not None
    current = state(
        positions={
            btc_plan.trade_id: position_from_plan(btc_plan),
            eth_plan.trade_id: position_from_plan(eth_plan),
        }
    )

    reconciled = reconcile_demo_state(
        current,
        positions=(
            DemoPosition("BTCUSDT", "Buy", btc_plan.qty, btc_plan.entry_price),
            DemoPosition("ETHUSDT", "Buy", eth_plan.qty, eth_plan.entry_price),
        ),
        orders=(),
        now=NOW,
    )

    assert {item.symbol for item in reconciled.positions.values()} == {
        "BTCUSDT",
        "ETHUSDT",
    }
    assert reconciled.trading_enabled is True


def test_journal_complete_and_telegram_always_says_demo(tmp_path: Path) -> None:
    entry = TradeJournalEntry(
        trade_id="QTRM-1",
        setup_episode="episode",
        symbol="BTCUSDT",
        direction=MicroDirection.LONG,
        setup_type="РЕТЕСТ",
        setup_confidence=90,
        entry_signal_timestamp=NOW,
        order_submit_timestamp=NOW,
        fill_timestamp=NOW,
        signal_price=100,
        average_fill=100.1,
        slippage=0.1,
        initial_stop=99,
        risk_pct=0.5,
        risk_usdt=50,
        leverage=5,
        qty=1,
        tp1_fill=101,
        tp2_fill=102,
        runner_exit=103,
        structure_time_exits=(),
        fees=0.2,
        funding=None,
        realised_gross_pnl=3,
        realised_net_pnl=2.8,
        result_r=2.8,
        max_favorable_excursion=3,
        max_adverse_excursion=-0.2,
        hold_duration_seconds=1200,
        exit_reason="RUNNER_TARGET",
        outcome="winning",
    )
    path = tmp_path / "qtr_micro_trades.jsonl"
    JsonlQtrMicroTradeJournal(path).append(entry)
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["result_r"] == 2.8
    plan = decision().plan
    assert plan is not None
    text = format_micro_entry(plan)
    assert "BYBIT DEMO" in text
    assert "ЛОНГ" in text
    closed = format_micro_closed(
        position_from_plan(plan),
        reason="TIME_EXIT",
        pnl=1,
        result_r=0.2,
        hold_minutes=45,
    )
    assert closed.endswith("DEMO")


def test_secret_is_not_logged(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        result = QtrMicroPreflight(
            replace(settings(), api_key="", api_secret="super-secret"), None
        ).run()
    assert result.ready is False
    assert "super-secret" not in caplog.text


class FakeTransport:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    def request(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        del args, kwargs
        return {"retCode": 0, "result": {}}


class InstrumentInfoTransport:
    base_url = DEMO_BASE_URL

    def __init__(self, symbol: str, **changes: object) -> None:
        self.row: dict[str, object] = {
            "symbol": symbol,
            "quoteCoin": "USDT",
            "settleCoin": "USDT",
            "contractType": "LinearPerpetual",
            "symbolType": "",
            "status": "Trading",
            "isPreListing": False,
            "lotSizeFilter": {
                "qtyStep": "0.01",
                "minOrderQty": "0.01",
                "maxMktOrderQty": "500",
                "minNotionalValue": "5",
            },
            "leverageFilter": {"maxLeverage": "25"},
        }
        self.row.update(changes)

    def request(self, *args: object, **kwargs: object) -> dict[str, object]:
        del args, kwargs
        return {"retCode": 0, "result": {"list": [self.row]}}


class RetCodeTransport:
    base_url = DEMO_BASE_URL

    def __init__(self, ret_code: int) -> None:
        self.ret_code = ret_code
        self.paths: list[str] = []

    def request(
        self,
        method: str,
        path: str,
        params: object,
        *,
        authenticated: bool,
    ) -> dict[str, Any]:
        del method, params, authenticated
        self.paths.append(path)
        raise DemoApiError(
            f"Bybit Demo API отклонил запрос, код {self.ret_code}.",
            ret_code=self.ret_code,
        )


class FakeHttpResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def __enter__(self) -> FakeHttpResponse:
        return self

    def __exit__(self, *args: object) -> None:
        del args

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")


def test_set_leverage_success_distinguishes_changed_and_already_set() -> None:
    changed = BybitDemoTradingClient(FakeTransport(DEMO_BASE_URL))
    already = BybitDemoTradingClient(RetCodeTransport(110043))

    assert changed.set_leverage("BTCUSDT", 5) is LeverageUpdateResult.CHANGED
    assert already.set_leverage("BTCUSDT", 5) is LeverageUpdateResult.ALREADY_SET


def test_real_transport_ret_code_110043_reaches_only_set_leverage_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_urlopen(*args: object, **kwargs: object) -> FakeHttpResponse:
        del args, kwargs
        return FakeHttpResponse(
            {"retCode": 110043, "retMsg": "Set leverage has not been modified."}
        )

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    transport = UrllibBybitDemoTransport("demo-key", "demo-secret")
    client = BybitDemoTradingClient(transport)

    assert client.set_leverage("BTCUSDT", 5) is LeverageUpdateResult.ALREADY_SET
    with pytest.raises(DemoApiError) as error_info:
        client.connectivity()
    assert error_info.value.ret_code == 110043


def test_set_leverage_does_not_suppress_other_ret_codes() -> None:
    client = BybitDemoTradingClient(RetCodeTransport(110013))

    with pytest.raises(DemoApiError) as error_info:
        client.set_leverage("BTCUSDT", 5)

    assert error_info.value.ret_code == 110013


def test_ret_code_110043_is_not_ignored_outside_set_leverage() -> None:
    transport = RetCodeTransport(110043)
    client = BybitDemoTradingClient(transport)

    with pytest.raises(DemoApiError) as error_info:
        client.connectivity()

    assert error_info.value.ret_code == 110043
    assert transport.paths == ["/v5/market/time"]


def test_client_has_second_hard_domain_guard() -> None:
    BybitDemoTradingClient(FakeTransport(DEMO_BASE_URL))
    with pytest.raises(DemoApiError, match="TRADE BLOCKED"):
        BybitDemoTradingClient(FakeTransport("https://api.bybit.com"))


@pytest.mark.parametrize(
    ("symbol", "changes", "expected"),
    (
        (
            "JNJUSDT",
            {"symbolType": "stock"},
            InstrumentUniverseStatus.STOCK,
        ),
        (
            "UNITREEUSDT",
            {"status": "PreLaunch", "isPreListing": True},
            InstrumentUniverseStatus.PRELAUNCH,
        ),
        (
            "BTCUSD",
            {"quoteCoin": "USD", "settleCoin": "BTC"},
            InstrumentUniverseStatus.NON_USDT,
        ),
        (
            "BTCUSDT",
            {"contractType": "InversePerpetual"},
            InstrumentUniverseStatus.UNSUPPORTED_CONTRACT,
        ),
        (
            "PAUSEDUSDT",
            {"status": "Settled"},
            InstrumentUniverseStatus.UNSUPPORTED_STATUS,
        ),
        ("ETHUSDT", {}, InstrumentUniverseStatus.ELIGIBLE),
    ),
)
def test_instrument_metadata_defines_qtr_micro_universe(
    symbol: str,
    changes: dict[str, object],
    expected: InstrumentUniverseStatus,
) -> None:
    client = BybitDemoTradingClient(InstrumentInfoTransport(symbol, **changes))

    rules = client.instrument_rules(symbol)

    assert rules.symbol == symbol
    assert rules.universe_status is expected


def test_instrument_info_cannot_substitute_btc_rules_for_another_symbol() -> None:
    client = BybitDemoTradingClient(InstrumentInfoTransport("BTCUSDT"))

    with pytest.raises(DemoApiError, match="запрошенному symbol"):
        client.instrument_rules("ALTUSDT")


def test_status_reports_demo_safety_without_secrets() -> None:
    class Screening:
        def screen(self, request: object) -> object:
            raise AssertionError(request)

    status = QtrMicroRuntimeStatus(True, True, None, 1, -12.5, True)
    execution = execute_command(
        "/status",
        chat_id=1,
        allowed_chat_ids=frozenset((1,)),
        service=Screening(),  # type: ignore[arg-type]
        qtr_micro_status=status,
    )
    text = execution.messages[0]
    assert "QTR Micro Demo: включён" in text
    assert "Demo API: готов" in text
    assert "Открытые Micro позиции: 1" in text
    assert "Дневной Demo PnL: -12.50 USDT" in text
    assert "demo-secret" not in text


@pytest.mark.parametrize("command", ("/buy", "/sell", "/open", "/close"))
def test_remote_trading_commands_do_not_exist(command: str) -> None:
    with pytest.raises(ValueError, match="не поддерживается"):
        parse_command(command)


@pytest.mark.parametrize(
    "setup_type", (SetupType.RETEST, SetupType.BREAKOUT, SetupType.CONTINUATION)
)
def test_completion_valid_ready_types_run_full_fake_lifecycle(
    tmp_path: Path, setup_type: SetupType
) -> None:
    item = candidate(
        episode=f"completion-{setup_type.value}",
        result_changes={"setup_type": setup_type},
    )
    plan = decision(item).plan
    assert plan is not None and plan.setup_type is setup_type
    client = FakeClient()
    client.auto_fill = True
    store = JsonQtrMicroStateStore(tmp_path / f"{setup_type.value}.json")
    store.save(state())
    audit_path = tmp_path / f"{setup_type.value}.jsonl"
    service = QtrMicroExecutionService(
        settings=settings(), client=client, state_store=store,
        engine=QtrMicroEntryEngine(settings()),
        runtime_audit=JsonlQtrMicroRuntimeAudit(audit_path), sleeper=lambda _: None,
    )
    opened = service.submit_and_confirm_entry(plan, NOW, RULES)
    assert opened.stage is MicroStage.OPEN
    assert client.order_call_count == 1
    assert client.stop_calls == 1
    events = {
        json.loads(line)["event"]
        for line in audit_path.read_text(encoding="utf-8").splitlines()
    }
    assert {"PREPARED", "LEVERAGE_READY", "ENTRY_SUBMIT_ATTEMPT", "ENTRY_ACK",
            "ENTRY_CONFIRMATION_POLL", "ENTRY_FILLED", "ACTUAL_RISK_CHECK",
            "PROTECTION_ATTEMPT", "ENTRY_PROTECTED"} <= events


def test_completion_explicit_incomplete_and_blocked_state_fail_closed() -> None:
    incomplete = candidate(result_changes={
        "trade_eligible": True, "setup_state": SetupState.READY_TO_CONSIDER,
        "data_quality": "INCOMPLETE", "missing_data": ("volume",),
    })
    assert decision(incomplete).skip_reason is EntrySkipReason.INVALID_STATE
    blocked = state(trading_enabled=False, blocked_reason="reconciliation required")
    assert decision(current_state=blocked).skip_reason is EntrySkipReason.STATE_BLOCKED


def test_completion_partial_fill_recalculates_40_30_30_from_filled_qty(
    tmp_path: Path,
) -> None:
    plan = decision().plan
    assert plan is not None
    client = FakeClient()
    filled_qty = plan.qty / 2
    client.fill = ExecutionFill("order-1", plan.entry_price, filled_qty, 0.1, NOW)
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    store.save(state())
    service = QtrMicroExecutionService(
        settings=settings(), client=client, state_store=store,
        engine=QtrMicroEntryEngine(settings()),
    )
    opened = service.submit_and_confirm_entry(plan, NOW, RULES)
    assert opened.stage is MicroStage.OPEN
    assert opened.initial_qty == filled_qty
    assert opened.tp1_qty <= filled_qty * 0.40
    assert opened.tp2_qty <= filled_qty * 0.30
    assert opened.tp1_qty + opened.tp2_qty + opened.runner_qty <= filled_qty


def test_completion_protection_retry_succeeds_without_duplicate_entry(
    tmp_path: Path,
) -> None:
    plan = decision().plan
    assert plan is not None
    client = FakeClient()
    client.auto_fill = True
    client.stop_failures_remaining = 2
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    store.save(state())
    service = QtrMicroExecutionService(
        settings=settings(), client=client, state_store=store,
        engine=QtrMicroEntryEngine(settings()), sleeper=lambda _: None,
    )
    opened = service.submit_and_confirm_entry(plan, NOW, RULES)
    assert opened.stage is MicroStage.OPEN
    assert client.stop_calls == 3
    assert client.order_call_count == 1


def test_completion_tp2_and_runner_are_reduce_only(tmp_path: Path) -> None:
    plan = decision().plan
    assert plan is not None
    client = FakeClient()
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    tp1_done = position_from_plan(
        plan, stage=MicroStage.TP1_FILLED,
        current_qty=plan.qty - plan.tp1_qty, tp1_fill_price=plan.tp1_price,
    )
    store.save(state(positions={plan.trade_id: tp1_done}))
    service = QtrMicroExecutionService(
        settings=settings(), client=client, state_store=store,
        engine=QtrMicroEntryEngine(settings()),
    )
    service.manage_position(plan.trade_id, current_price=plan.tp2_price, now=NOW)
    assert client.orders[-1]["qty"] == plan.tp2_qty
    assert client.orders[-1]["reduce_only"] is True
    client.fills_by_order["order-1"] = ExecutionFill(
        "order-1", plan.tp2_price, plan.tp2_qty, 0, NOW
    )
    service.manage_position(plan.trade_id, current_price=plan.tp2_price, now=NOW)
    after_tp2 = store.load(today=NOW.date(), trading_enabled=True).positions[
        plan.trade_id
    ]
    assert after_tp2.stage is MicroStage.TP2_FILLED
    service.manage_position(
        plan.trade_id, current_price=plan.runner_target_price, now=NOW
    )
    assert client.orders[-1]["qty"] == pytest.approx(after_tp2.current_qty)
    assert client.orders[-1]["reduce_only"] is True


def test_completion_tp1_breakeven_failure_blocks_and_emergency_closes(
    tmp_path: Path,
) -> None:
    plan = decision().plan
    assert plan is not None
    client = FakeClient()
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    store.save(state(positions={plan.trade_id: position_from_plan(plan)}))
    service = QtrMicroExecutionService(
        settings=settings(), client=client, state_store=store,
        engine=QtrMicroEntryEngine(settings()),
    )
    service.manage_position(plan.trade_id, current_price=plan.tp1_price, now=NOW)
    client.fills_by_order["order-1"] = ExecutionFill(
        "order-1", plan.tp1_price, plan.tp1_qty, 0, NOW
    )
    client.stop_failures_remaining = 3
    service.manage_position(plan.trade_id, current_price=plan.tp1_price, now=NOW)
    persisted = store.load(today=NOW.date(), trading_enabled=True)
    assert persisted.trading_enabled is False
    assert persisted.positions[plan.trade_id].stage is MicroStage.BLOCKED
    assert client.orders[-1]["reduce_only"] is True


def test_completion_reconciliation_never_invents_exit_fill_or_journal(
    tmp_path: Path,
) -> None:
    plan = decision().plan
    assert plan is not None
    client = FakeClient()
    journal_path = tmp_path / "trades.jsonl"
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    pending = position_from_plan(
        plan, stage=MicroStage.EXIT_ACKNOWLEDGED, current_qty=plan.qty,
        pending_exit_order_id="exit-1",
        pending_exit_order_link_id=f"{plan.trade_id}-SX",
        pending_exit_reason=MicroExitReason.STRUCTURE_EXIT,
        pending_exit_qty=plan.qty,
    )
    store.save(state(positions={plan.trade_id: pending}))
    service = QtrMicroExecutionService(
        settings=settings(), client=client, state_store=store,
        engine=QtrMicroEntryEngine(settings()),
        journal=JsonlQtrMicroTradeJournal(journal_path),
    )
    reconciled = service.reconcile(NOW)
    assert reconciled.trading_enabled is False
    assert reconciled.blocked_reason is not None
    assert not journal_path.exists()


def test_completion_reconciliation_after_exit_writes_exactly_once(
    tmp_path: Path,
) -> None:
    plan = decision().plan
    assert plan is not None
    client = FakeClient()
    client.fills_by_order["exit-1"] = ExecutionFill(
        "exit-1", plan.entry_price + 1, plan.qty, 0.2, NOW
    )
    journal_path = tmp_path / "trades.jsonl"
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    pending = position_from_plan(
        plan, stage=MicroStage.EXIT_ACKNOWLEDGED, current_qty=plan.qty,
        pending_exit_order_id="exit-1",
        pending_exit_order_link_id=f"{plan.trade_id}-SX",
        pending_exit_reason=MicroExitReason.STRUCTURE_EXIT,
        pending_exit_qty=plan.qty,
    )
    store.save(state(positions={plan.trade_id: pending}))
    service = QtrMicroExecutionService(
        settings=settings(), client=client, state_store=store,
        engine=QtrMicroEntryEngine(settings()),
        journal=JsonlQtrMicroTradeJournal(journal_path),
    )
    service.reconcile(NOW)
    service.reconcile(NOW + timedelta(seconds=1))
    assert len(journal_path.read_text(encoding="utf-8").splitlines()) == 1


def test_completion_runtime_manages_without_current_setup_and_kill_switch(
    tmp_path: Path,
) -> None:
    plan = decision().plan
    assert plan is not None
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    store.save(state(positions={plan.trade_id: position_from_plan(
        plan, opened_at=NOW - timedelta(minutes=45)
    )}))
    client = FakeClient()
    client.positions = (DemoPosition(plan.symbol, "Buy", plan.qty, plan.entry_price),)
    client.market_prices[plan.symbol] = plan.entry_price
    runtime = QtrMicroRuntime(
        settings=settings(kill_switch=True), client=client, state_store=store,
        allowed_chat_ids=frozenset(), clock=lambda: NOW,
    )
    runtime._preflight_result = preflight()  # noqa: SLF001
    asyncio.run(runtime.handle_candidates((), _discard_message))
    assert client.order_call_count == 1
    assert client.orders[0]["reduce_only"] is True


def test_completion_runtime_refreshes_equity_before_fresh_price_sizing(
    tmp_path: Path,
) -> None:
    item = candidate(episode="fresh-equity")
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    store.save(state())
    client = FakeClient()
    client.equity = 20_000
    client.auto_fill = True
    runtime = QtrMicroRuntime(
        settings=settings(), client=client, state_store=store,
        allowed_chat_ids=frozenset(), clock=lambda: NOW,
    )
    runtime._preflight_result = preflight()  # noqa: SLF001
    asyncio.run(runtime.handle_candidates((item,), _discard_message))
    positions = store.load(today=NOW.date(), trading_enabled=True).positions.values()
    opened = next(iter(positions))
    assert opened.risk_amount == pytest.approx(100.0)


def test_completion_runtime_utc_day_reset_uses_fresh_equity(
    tmp_path: Path,
) -> None:
    old_day = NOW - timedelta(days=1)
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    store.save(state(
        trading_day=old_day.date(), day_start_equity=10_000,
        realised_daily_pnl=-200, consecutive_losses=3,
        loss_pause_until=NOW + timedelta(minutes=20),
    ))
    client = FakeClient()
    client.equity = 9_800
    runtime = QtrMicroRuntime(
        settings=settings(kill_switch=True), client=client, state_store=store,
        allowed_chat_ids=frozenset(), clock=lambda: NOW,
    )
    runtime._preflight_result = preflight()  # noqa: SLF001
    asyncio.run(runtime.handle_candidates((), _discard_message))
    reset = store.load(today=NOW.date(), trading_enabled=True)
    assert reset.trading_day == NOW.date()
    assert reset.day_start_equity == 9_800
    assert reset.realised_daily_pnl == 0
    assert reset.consecutive_losses == 0
    assert reset.loss_pause_until is None


def test_completion_actual_risk_resize_failure_emergency_closes(
    tmp_path: Path,
) -> None:
    plan = decision().plan
    assert plan is not None
    client = FakeClient()
    client.fills_by_order["order-1"] = ExecutionFill(
        "order-1", 105.0, plan.qty, 0.2, NOW
    )
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    store.save(state())
    service = QtrMicroExecutionService(
        settings=settings(), client=client, state_store=store,
        engine=QtrMicroEntryEngine(settings()), sleeper=lambda _: None,
    )
    blocked = service.submit_and_confirm_entry(plan, NOW, RULES)
    assert blocked.stage is MicroStage.BLOCKED
    assert client.orders[-1]["reduce_only"] is True
    assert store.load(today=NOW.date(), trading_enabled=True).trading_enabled is False


def test_completion_runner_fallback_is_three_r() -> None:
    plan = decision().plan
    assert plan is not None
    assert plan.runner_target_price == pytest.approx(
        plan.entry_price + 3 * plan.initial_r
    )


def test_completion_fifteen_minute_exit_requires_both_conditions() -> None:
    plan = decision().plan
    assert plan is not None
    position = position_from_plan(plan)
    engine = QtrMicroEntryEngine(settings())
    at_fifteen = NOW + timedelta(minutes=15)
    assert engine.manage(
        position, current_price=plan.entry_price, now=at_fifteen,
        structure_degraded=True,
    ).action is MicroExitReason.TIME_EXIT
    assert engine.manage(
        position, current_price=plan.entry_price, now=at_fifteen,
        structure_degraded=False,
    ).action is None
    assert engine.manage(
        position,
        current_price=plan.entry_price + 0.5 * plan.initial_r,
        now=at_fifteen,
        structure_degraded=True,
    ).action is None


def test_open_current_breakout_failure_before_progress_check_does_not_exit(
    tmp_path: Path,
) -> None:
    plan = decision().plan
    assert plan is not None
    position = position_from_plan(plan)
    degraded = candidate(
        result_changes={
            "setup_state": SetupState.CANCELLED,
            "current_breakout_failure": True,
            "current_price": plan.entry_price,
        }
    )

    result, flags = runtime_management_decision(
        tmp_path,
        item=degraded,
        position=position,
        now=NOW + timedelta(minutes=10),
    )

    assert flags == (False, False, True)
    assert result.action is None


def test_open_current_breakout_failure_uses_progress_time_exit(
    tmp_path: Path,
) -> None:
    plan = decision().plan
    assert plan is not None
    position = position_from_plan(plan)
    degraded = candidate(
        result_changes={
            "setup_state": SetupState.CANCELLED,
            "current_breakout_failure": True,
            "current_price": plan.entry_price,
        }
    )

    result, flags = runtime_management_decision(
        tmp_path,
        item=degraded,
        position=position,
        now=NOW + timedelta(minutes=15),
    )

    assert flags == (False, False, True)
    assert result.action is MicroExitReason.TIME_EXIT


def test_open_confirmed_opposite_structure_still_exits_immediately(
    tmp_path: Path,
) -> None:
    plan = decision().plan
    assert plan is not None
    position = position_from_plan(plan)
    opposite = candidate(
        result_changes={
            "direction": SetupDirection.DOWN,
            "structure_confirmation": True,
            "current_breakout_failure": False,
            "current_price": plan.entry_price,
        }
    )

    result, flags = runtime_management_decision(
        tmp_path,
        item=opposite,
        position=position,
        now=NOW + timedelta(minutes=1),
    )

    assert flags == (False, True, False)
    assert result.action is MicroExitReason.STRUCTURE_EXIT


@pytest.mark.parametrize(
    ("stage", "price_attribute", "expected"),
    (
        (MicroStage.OPEN, "tp1_price", MicroExitReason.TP1),
        (MicroStage.TP1_FILLED, "tp2_price", MicroExitReason.TP2),
    ),
)
def test_current_breakout_failure_does_not_mask_reached_targets(
    tmp_path: Path,
    stage: MicroStage,
    price_attribute: str,
    expected: MicroExitReason,
) -> None:
    plan = decision().plan
    assert plan is not None
    position = position_from_plan(plan, stage=stage)
    target_price = float(getattr(plan, price_attribute))
    degraded = candidate(
        result_changes={
            "setup_state": SetupState.CANCELLED,
            "current_breakout_failure": True,
            "current_price": target_price,
        }
    )

    result, flags = runtime_management_decision(
        tmp_path,
        item=degraded,
        position=position,
        now=NOW + timedelta(minutes=10),
    )

    assert flags == (False, False, True)
    assert result.action is expected


def test_independent_hard_setup_cancellation_still_exits(
    tmp_path: Path,
) -> None:
    plan = decision().plan
    assert plan is not None
    position = position_from_plan(plan)
    cancelled = candidate(
        result_changes={
            "setup_state": SetupState.CANCELLED,
            "current_breakout_failure": False,
            "current_price": plan.entry_price,
        }
    )

    result, flags = runtime_management_decision(
        tmp_path,
        item=cancelled,
        position=position,
        now=NOW + timedelta(minutes=1),
    )

    assert flags == (True, False, False)
    assert result.action is MicroExitReason.STRUCTURE_EXIT


def test_entry_still_rejects_current_breakout_failure() -> None:
    failed = candidate(
        result_changes={
            "setup_state": SetupState.READY_TO_CONSIDER,
            "trade_eligible": True,
            "current_breakout_failure": True,
        }
    )

    assert decision(failed).skip_reason is EntrySkipReason.CURRENT_FAILURE


def test_completion_opposite_structure_exits_without_reverse() -> None:
    plan = decision().plan
    assert plan is not None
    result = QtrMicroEntryEngine(settings()).manage(
        position_from_plan(plan), current_price=plan.entry_price,
        now=NOW, opposite_structure=True,
    )
    assert result.action is MicroExitReason.STRUCTURE_EXIT
    assert result.close_qty == plan.qty


def test_completion_restart_after_ack_and_after_fill_before_protection(
    tmp_path: Path,
) -> None:
    plan = decision().plan
    assert plan is not None
    store = JsonQtrMicroStateStore(tmp_path / "state.json")
    acknowledged = position_from_plan(
        plan, stage=MicroStage.ENTRY_ACKNOWLEDGED,
        average_fill=None, filled_qty=0.0, opened_at=None,
    )
    store.save(state(positions={plan.trade_id: acknowledged}))
    client = FakeClient()
    client.active_orders = (
        DemoOrder(plan.symbol, "order-1", plan.order_link_id, "Buy", plan.qty, "New"),
    )
    service = QtrMicroExecutionService(
        settings=settings(), client=client, state_store=store,
        engine=QtrMicroEntryEngine(settings()),
    )
    after_ack_restart = service.reconcile(NOW)
    assert (
        after_ack_restart.positions[plan.trade_id].stage
        is MicroStage.ENTRY_ACKNOWLEDGED
    )
    client.positions = (
        DemoPosition(plan.symbol, "Buy", plan.qty, plan.entry_price),
    )
    client.fills_by_order["order-1"] = ExecutionFill(
        "order-1", plan.entry_price, plan.qty, 0.1, NOW
    )
    restarted_service = QtrMicroExecutionService(
        settings=settings(), client=client, state_store=store,
        engine=QtrMicroEntryEngine(settings()),
    )
    restarted_service.reconcile(NOW)
    protected = restarted_service.confirm_entry(plan.trade_id, NOW, rules=RULES)
    assert protected is not None and protected.stage is MicroStage.OPEN
    assert client.stop_calls == 1
