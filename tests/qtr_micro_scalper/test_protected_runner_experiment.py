from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
from test_micro_profit_experiment import (
    NOW,
    baseline,
    evidence,
    opened,
    runtime,
    trade,
)

from market_signal_assistant.qtr_micro_scalper.data.market_state import MarketBias
from market_signal_assistant.qtr_micro_scalper.data.models import PublicTradeEvent
from market_signal_assistant.qtr_micro_scalper.micro_profit_experiment import (
    CostAccrual,
    MicroCostModelConfig,
    MicroExperimentRecordType,
    MicroProfitExperimentConfig,
    MicroProfitExperimentRuntime,
    MicroProfitRecord,
    MicroTarget,
    calculate_cost_breakdown,
)
from market_signal_assistant.qtr_micro_scalper.protected_runner_experiment import (
    ProtectedRunnerConfig,
    ProtectedRunnerExitReason,
    ProtectedRunnerJournal,
    ProtectedRunnerRecord,
    ProtectedRunnerRecordType,
    ProtectedRunnerRuntime,
    deserialize_protected_runner_record,
    iter_protected_runner_records,
    serialize_protected_runner_record,
)


def target_hit(
    tmp_path: Path,
    *,
    target: MicroTarget = MicroTarget.M05,
    stop: float = 90.0,
    trade_id: str = "baseline-1",
    cost: MicroCostModelConfig | None = None,
) -> tuple[
    MicroProfitExperimentRuntime,
    MicroProfitRecord,
    PublicTradeEvent,
]:
    experiment, _, _ = runtime(
        tmp_path,
        stop=stop,
        cost=cost,
    )
    source = baseline(stop=stop, trade_id=trade_id)
    if trade_id != "baseline-1":
        activation = experiment.activate(source, evidence(), score=82)
        assert activation.accepted
        experiment.sync_baseline(opened(source))
    target_price = source.entry_price + source.risk_per_unit * (
        target.target_r + 1e-9
    )
    event = trade(2, target_price)
    records = experiment.process_event(event)
    record = next(
        item
        for item in records
        if item.target is target
        and item.baseline_trade_id == trade_id
        and item.record_type is MicroExperimentRecordType.TARGET_REACHED
    )
    return experiment, record, event


def protected_runtime(
    tmp_path: Path,
    *,
    cost: MicroCostModelConfig | None = None,
    maximum_active: int = 1_000,
    maximum_bars: int = 300,
) -> tuple[ProtectedRunnerRuntime, ProtectedRunnerJournal]:
    journal = ProtectedRunnerJournal(tmp_path / "protected.jsonl")
    runner = ProtectedRunnerRuntime(
        journal,
        ProtectedRunnerConfig(
            enabled=True,
            maximum_active_branches=maximum_active,
            maximum_safety_bars=maximum_bars,
            cost_model=cost or MicroCostModelConfig(),
        ),
    )
    return runner, journal


def activate_protected(
    runner: ProtectedRunnerRuntime,
    source: MicroProfitRecord,
    event: PublicTradeEvent,
) -> tuple[ProtectedRunnerRecord, ...]:
    return runner.observe_micro_records(
        (source,),
        evidence=evidence(at=event.exchange_at, price=event.price),
        event=event,
    )


def test_positive_net_at_m05_arms_floor(tmp_path: Path) -> None:
    _, source, event = target_hit(tmp_path)
    runner, _ = protected_runtime(tmp_path)

    records = activate_protected(runner, source, event)

    assert [item.record_type for item in records] == [
        ProtectedRunnerRecordType.PROTECTED_RUNNER_CREATED,
        ProtectedRunnerRecordType.NET_FLOOR_ARMED,
    ]
    armed = records[-1]
    assert armed.estimated_net_r_before_exit > 0
    assert armed.net_r_at_floor_arm == pytest.approx(
        armed.estimated_net_r_before_exit
    )


def test_high_cost_m05_does_not_falsely_arm(tmp_path: Path) -> None:
    cost = MicroCostModelConfig(slippage_bps=2.0)
    _, source, event = target_hit(tmp_path, stop=99.0, cost=cost)
    runner, _ = protected_runtime(tmp_path, cost=cost)

    records = activate_protected(runner, source, event)

    assert len(records) == 1
    assert records[0].estimated_net_r_before_exit < 0
    assert runner.metrics().branches_armed == 0


def test_high_cost_branch_arms_only_after_later_positive_net(
    tmp_path: Path,
) -> None:
    cost = MicroCostModelConfig(slippage_bps=2.0)
    _, source, event = target_hit(tmp_path, stop=99.0, cost=cost)
    runner, _ = protected_runtime(tmp_path, cost=cost)
    activate_protected(runner, source, event)

    records = runner.process_event(trade(3, 100.25))

    assert len(records) == 1
    assert records[0].record_type is ProtectedRunnerRecordType.NET_FLOOR_ARMED
    assert records[0].estimated_net_r_before_exit > 0


def test_armed_branch_exits_at_observed_price_when_net_floor_is_breached(
    tmp_path: Path,
) -> None:
    _, source, event = target_hit(tmp_path)
    runner, _ = protected_runtime(tmp_path)
    activate_protected(runner, source, event)
    observed = trade(3, 100.0)

    records = runner.process_event(observed)

    assert len(records) == 1
    exited = records[0]
    assert exited.record_type is ProtectedRunnerRecordType.PROTECTED_RUNNER_EXITED
    assert exited.exit_reason == ProtectedRunnerExitReason.NET_PROFIT_FLOOR.value
    assert exited.actual_exit_price == observed.price
    assert exited.actual_net_r is not None and exited.actual_net_r < 0
    assert exited.floor_breach_amount_r is not None
    assert exited.floor_breach_amount_r == pytest.approx(-exited.actual_net_r)


def test_existing_continuation_condition_closes_before_floor(tmp_path: Path) -> None:
    cost = MicroCostModelConfig(slippage_bps=2.0)
    _, source, event = target_hit(tmp_path, stop=99.0, cost=cost)
    runner, _ = protected_runtime(tmp_path, cost=cost)
    activate_protected(runner, source, event)

    records = runner.update_evidence(
        evidence(
            at=event.exchange_at + timedelta(milliseconds=1),
            price=event.price,
            bias=MarketBias.BEARISH,
        )
    )

    assert len(records) == 1
    assert records[0].exit_reason == (
        ProtectedRunnerExitReason.OPPOSITE_MARKET_STATE.value
    )


def test_existing_trailing_condition_has_priority_over_floor(tmp_path: Path) -> None:
    _, source, event = target_hit(tmp_path)
    runner, _ = protected_runtime(tmp_path)
    activate_protected(runner, source, event)
    runner.process_event(trade(3, 102.0))

    records = runner.process_event(trade(4, 100.9))

    assert len(records) == 1
    assert records[0].exit_reason == (
        ProtectedRunnerExitReason.TRAILING_EXCURSION.value
    )
    assert records[0].actual_net_r is not None
    assert records[0].actual_net_r > 0


def test_existing_maximum_safety_horizon_is_preserved(tmp_path: Path) -> None:
    _, source, event = target_hit(tmp_path)
    runner, _ = protected_runtime(tmp_path, maximum_bars=1)
    activate_protected(runner, source, event)

    records = runner.process_event(trade(20, 100.6))

    assert len(records) == 1
    assert records[0].exit_reason == (
        ProtectedRunnerExitReason.MAXIMUM_SAFETY_HORIZON.value
    )


def test_stop_uses_real_observed_event_price(tmp_path: Path) -> None:
    cost = MicroCostModelConfig(slippage_bps=2.0)
    _, source, event = target_hit(tmp_path, cost=cost)
    runner, _ = protected_runtime(tmp_path, cost=cost)
    activate_protected(runner, source, event)
    stopped = trade(3, 89.5)

    records = runner.process_event(stopped)

    assert records[0].exit_reason == ProtectedRunnerExitReason.STOP.value
    assert records[0].actual_exit_price == 89.5
    assert records[0].actual_gross_r == pytest.approx(-1.05)


def test_entry_exit_costs_match_existing_cost_model(tmp_path: Path) -> None:
    cost = MicroCostModelConfig(slippage_bps=1.0)
    _, source, event = target_hit(tmp_path, cost=cost)
    runner, _ = protected_runtime(tmp_path, cost=cost)
    activate_protected(runner, source, event)
    exited_at = trade(3, 100.0)

    exited = runner.process_event(exited_at)[0]
    assert source.entry_at is not None
    expected = calculate_cost_breakdown(
        cost,
        entry_price=source.entry_price,
        exit_price=exited_at.price,
        risk_per_unit=source.risk_per_unit,
        gross_r=0.0,
        duration_seconds=(exited_at.exchange_at-source.entry_at).total_seconds(),
        entry_spread_bps=2.0,
        exit_spread_bps=2.0,
        accrual=CostAccrual.ROUND_TRIP,
    )

    assert exited.actual_total_cost_r == pytest.approx(expected.total_cost_r)
    assert exited.actual_net_r == pytest.approx(expected.net_r)


def test_control_runner_result_is_unchanged_by_protected_branch(
    tmp_path: Path,
) -> None:
    control, source, target_event = target_hit(tmp_path / "control")
    protected_control, protected_source, protected_event = target_hit(
        tmp_path / "protected"
    )
    runner, _ = protected_runtime(tmp_path / "protected")
    activate_protected(runner, protected_source, protected_event)
    reversal = trade(3, 99.4)

    expected = control.process_event(reversal)
    actual = protected_control.process_event(reversal)
    runner.process_event(reversal)
    expected_exit = next(
        item
        for item in expected
        if item.target is MicroTarget.M05
        and item.record_type is MicroExperimentRecordType.RUNNER_EXITED
    )
    actual_exit = next(
        item
        for item in actual
        if item.target is MicroTarget.M05
        and item.record_type is MicroExperimentRecordType.RUNNER_EXITED
    )

    assert target_event.price == protected_event.price
    assert actual_exit.costs == expected_exit.costs
    assert actual_exit.runner_exit_reason == expected_exit.runner_exit_reason


def test_all_existing_micro_target_thresholds_are_unchanged() -> None:
    assert [(item.value, item.target_r) for item in MicroTarget] == [
        ("M05", 0.05),
        ("M10", 0.10),
        ("M15", 0.15),
        ("M20", 0.20),
        ("M25", 0.25),
    ]


def test_duplicate_target_does_not_create_duplicate_branch(tmp_path: Path) -> None:
    _, source, event = target_hit(tmp_path)
    runner, journal = protected_runtime(tmp_path)
    first = activate_protected(runner, source, event)
    second = activate_protected(runner, source, event)

    assert first
    assert second == ()
    assert runner.metrics().active_branches == 1
    assert runner.metrics().duplicate_targets == 1
    assert sum(
        item.record_type is ProtectedRunnerRecordType.PROTECTED_RUNNER_CREATED
        for item in iter_protected_runner_records(journal.path)
    ) == 1


def test_capacity_rejection_is_fail_safe(tmp_path: Path) -> None:
    first_micro, first, first_event = target_hit(tmp_path / "first")
    del first_micro
    _, second, second_event = target_hit(
        tmp_path / "second",
        trade_id="baseline-2",
    )
    runner, _ = protected_runtime(tmp_path, maximum_active=1)
    activate_protected(runner, first, first_event)

    rejected = activate_protected(runner, second, second_event)

    assert rejected == ()
    assert runner.metrics().active_branches == 1
    assert runner.metrics().capacity_rejections == 1


def test_restart_marks_active_branch_interrupted_without_fabricated_exit(
    tmp_path: Path,
) -> None:
    _, source, event = target_hit(tmp_path)
    runner, journal = protected_runtime(tmp_path)
    activate_protected(runner, source, event)
    journal.flush()
    recovered = ProtectedRunnerJournal(journal.path)
    restarted = ProtectedRunnerRuntime(
        recovered,
        ProtectedRunnerConfig(enabled=True),
    )

    records = restarted.start(at=NOW + timedelta(minutes=1))

    assert len(records) == 1
    interrupted = records[0]
    assert interrupted.record_type is ProtectedRunnerRecordType.INTERRUPTED
    assert interrupted.outcome.value == "INCOMPLETE"
    assert interrupted.actual_exit_price is None
    assert interrupted.actual_net_r is None


def test_journal_is_transition_only_deterministic_and_streaming(
    tmp_path: Path,
) -> None:
    _, source, event = target_hit(tmp_path)
    runner, journal = protected_runtime(tmp_path)
    activate_protected(runner, source, event)
    before = journal.metrics.records_processed

    for index in range(3, 30):
        runner.process_event(trade(index, 100.6))

    records = tuple(iter_protected_runner_records(journal.path))
    assert journal.metrics.records_processed == before
    assert len(records) == 2
    encoded = serialize_protected_runner_record(records[0])
    assert deserialize_protected_runner_record(encoded) == records[0]
    assert serialize_protected_runner_record(records[0]) == encoded


def test_journal_recovery_state_is_bounded_and_never_reads_whole_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, source, event = target_hit(tmp_path)
    runner, journal = protected_runtime(tmp_path)
    created = activate_protected(runner, source, event)[0]
    bounded_path = tmp_path / "bounded-recovery.jsonl"
    bounded = ProtectedRunnerJournal(
        bounded_path,
        maximum_recovered_active=1,
    )
    assert bounded.append(created)
    assert bounded.append(
        replace(
            created,
            branch_id=f"{created.branch_id}-second",
            source_variant_id=f"{created.source_variant_id}-second",
        )
    )
    assert len(bounded.recovery.active_records) == 1

    def forbidden_read_bytes(self: Path) -> bytes:
        del self
        raise AssertionError("whole-file byte loading is forbidden")

    def forbidden_read_text(self: Path, *args: object, **kwargs: object) -> str:
        del self, args, kwargs
        raise AssertionError("whole-file text loading is forbidden")

    monkeypatch.setattr(Path, "read_bytes", forbidden_read_bytes)
    monkeypatch.setattr(Path, "read_text", forbidden_read_text)
    recovered = ProtectedRunnerJournal(
        journal.path,
        maximum_recovered_active=1,
    )
    assert recovered.metrics.bootstrap_scans == 1
    assert recovered.metrics.active_recovered_branches == 1


def test_disabled_by_default_and_contains_no_order_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("QTR_SCALPER_V2_PROTECTED_RUNNER_ENABLED", raising=False)
    config = ProtectedRunnerConfig.from_environment(
        MicroProfitExperimentConfig()
    )

    assert not config.enabled
    source = Path(
        "src/market_signal_assistant/qtr_micro_scalper/"
        "protected_runner_experiment.py"
    ).read_text(encoding="utf-8")
    assert "create_order" not in source
    assert "Telegram" not in source
