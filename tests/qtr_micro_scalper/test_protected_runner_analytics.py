from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest
from test_micro_profit_experiment import NOW, trade
from test_protected_runner_experiment import (
    activate_protected,
    protected_runtime,
    target_hit,
)

from market_signal_assistant.qtr_micro_scalper.micro_profit_experiment import (
    MicroExperimentRecordType,
    MicroTarget,
    iter_micro_profit_records,
    serialize_micro_profit_record,
)
from market_signal_assistant.qtr_micro_scalper.protected_runner_analytics import (
    ProtectedRunnerAnalyticsEngine,
    format_protected_runner_report,
)
from market_signal_assistant.qtr_micro_scalper.protected_runner_experiment import (
    iter_protected_runner_records,
    serialize_protected_runner_record,
)


def completed_pair(tmp_path: Path) -> tuple[Path, Path]:
    control, source, target_event = target_hit(tmp_path)
    protected, protected_journal = protected_runtime(tmp_path)
    activate_protected(protected, source, target_event)
    floor_event = trade(3, 100.0)
    control.process_event(floor_event)
    protected.process_event(floor_event)
    control.process_event(trade(4, 99.4))
    return control.journal.path, protected_journal.path


def test_paired_analytics_joins_same_baseline_and_target(tmp_path: Path) -> None:
    control_path, protected_path = completed_pair(tmp_path)

    result = ProtectedRunnerAnalyticsEngine().analyze(
        control_path,
        protected_path,
    )

    assert result.overall.paired_n == 1
    pair = result.pairs[0]
    assert pair.baseline_trade_id == "baseline-1"
    assert pair.target is MicroTarget.M05
    assert pair.protected_final_net_r > pair.control_final_net_r
    assert pair.delta_net_r == pytest.approx(
        pair.protected_final_net_r - pair.control_final_net_r
    )
    assert pair.profit_saved_by_protection_r == pytest.approx(pair.delta_net_r)
    assert result.overall.protected_beats_control == 1


def test_analytics_reports_negative_rate_floor_and_profit_giveback(
    tmp_path: Path,
) -> None:
    control_path, protected_path = completed_pair(tmp_path)

    item = ProtectedRunnerAnalyticsEngine().analyze(
        control_path,
        protected_path,
    ).overall

    assert item.control_final_net_negative_rate == 100
    assert item.protected_final_net_negative_rate == 100
    assert item.net_floor_armed_pct == 100
    assert item.net_floor_exit_pct == 100
    assert item.average_floor_breach_r > 0
    assert item.average_control_profit_giveback_r_estimated > 0
    assert item.average_protected_profit_giveback_r > 0


def test_breakdowns_cover_target_direction_symbol_and_score(tmp_path: Path) -> None:
    control_path, protected_path = completed_pair(tmp_path)

    result = ProtectedRunnerAnalyticsEngine().analyze(
        control_path,
        protected_path,
    )
    keys = {(row.scope, row.key) for row in result.rows}

    assert ("TARGET", "M05") in keys
    assert ("DIRECTION", "LONG") in keys
    assert ("SYMBOL", "BTCUSDT") in keys
    assert ("SCORE_BAND", "80-100") in keys


def test_interrupted_branch_is_excluded_from_strategy_pnl(tmp_path: Path) -> None:
    control, source, target_event = target_hit(tmp_path)
    protected, journal = protected_runtime(tmp_path)
    activate_protected(protected, source, target_event)
    protected.stop(at=NOW)

    result = ProtectedRunnerAnalyticsEngine().analyze(
        control.journal.path,
        journal.path,
    )

    assert result.interrupted_excluded == 1
    assert result.overall.paired_n == 0
    assert result.overall.control_net_total_r == 0
    assert result.overall.protected_net_total_r == 0


def test_report_preregisters_checkpoints_without_selecting_winner(
    tmp_path: Path,
) -> None:
    control_path, protected_path = completed_pair(tmp_path)

    report = format_protected_runner_report(
        ProtectedRunnerAnalyticsEngine().analyze(control_path, protected_path)
    )

    assert "Engineering: first 3 Protected exits" in report
    assert "no winner is selected automatically" in report


def test_analytics_pair_state_and_samples_are_bounded(tmp_path: Path) -> None:
    control_path, protected_path = completed_pair(tmp_path)

    result = ProtectedRunnerAnalyticsEngine(
        maximum_pair_states=1,
        maximum_pairs=1,
    ).analyze(control_path, protected_path)

    assert result.retained_pair_states <= 1
    assert len(result.pairs) <= 1


def test_bounded_join_keeps_latest_protected_pairs(tmp_path: Path) -> None:
    control_source, protected_source = completed_pair(tmp_path / "source")
    control_record = next(
        item
        for item in iter_micro_profit_records(control_source)
        if item.record_type is MicroExperimentRecordType.RUNNER_EXITED
        and item.target is MicroTarget.M05
    )
    protected_records = tuple(iter_protected_runner_records(protected_source))
    control_path = tmp_path / "many-control.jsonl"
    protected_path = tmp_path / "many-protected.jsonl"
    for index in range(10):
        baseline_id = f"baseline-{index}"
        with control_path.open("a", encoding="utf-8") as stream:
            stream.write(
                serialize_micro_profit_record(
                    replace(
                        control_record,
                        baseline_trade_id=baseline_id,
                        variant_id=f"variant-{index}",
                    )
                )
                + "\n"
            )
        with protected_path.open("a", encoding="utf-8") as stream:
            for record in protected_records:
                stream.write(
                    serialize_protected_runner_record(
                        replace(
                            record,
                            branch_id=f"branch-{index}",
                            baseline_trade_id=baseline_id,
                            source_variant_id=f"variant-{index}",
                        )
                    )
                    + "\n"
                )

    result = ProtectedRunnerAnalyticsEngine(
        maximum_pair_states=3,
        maximum_pairs=3,
    ).analyze(control_path, protected_path)

    assert result.overall.paired_n == 3
    assert {pair.baseline_trade_id for pair in result.pairs} == {
        "baseline-7",
        "baseline-8",
        "baseline-9",
    }
