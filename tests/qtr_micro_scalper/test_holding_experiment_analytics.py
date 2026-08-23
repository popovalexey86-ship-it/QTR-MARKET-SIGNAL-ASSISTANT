from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
from test_holding_experiment import NOW, activate, baseline_trade, runtime

from market_signal_assistant.qtr_micro_scalper.holding_experiment import (
    HoldingExperimentJournal,
    HoldingExperimentOutcome,
    HoldingExperimentRecord,
    HoldingExperimentRecordType,
    HoldingExperimentStage,
    HoldingVariant,
)
from market_signal_assistant.qtr_micro_scalper.holding_experiment_analytics import (
    HoldingExperimentAnalyticsEngine,
    format_holding_experiment_report,
)
from market_signal_assistant.qtr_micro_scalper.setup_context import ShadowDirection


def terminal_records(
    created: tuple[HoldingExperimentRecord, ...],
    results: dict[HoldingVariant, float],
) -> tuple[HoldingExperimentRecord, ...]:
    records: list[HoldingExperimentRecord] = []
    for item in created:
        result = results[item.variant]
        outcome = (
            HoldingExperimentOutcome.WIN
            if result > 0
            else (
                HoldingExperimentOutcome.LOSS
                if result < 0
                else HoldingExperimentOutcome.BREAKEVEN
            )
        )
        records.append(
            replace(
                item,
                recorded_at=NOW + timedelta(seconds=item.maximum_holding_bars),
                record_type=HoldingExperimentRecordType.TERMINAL,
                stage=HoldingExperimentStage.CLOSED,
                entry_time=NOW + timedelta(seconds=1),
                exit_reason="TIME_EXIT",
                exit_time=NOW + timedelta(seconds=item.maximum_holding_bars),
                holding_completed_bars=item.maximum_holding_bars,
                holding_wall_clock_seconds=float(item.maximum_holding_bars - 1),
                result_r=result,
                mfe=max(result, 0.0) + 0.10,
                mae=max(-result, 0.0) + 0.05,
                outcome=outcome,
            )
        )
    return tuple(records)


def append_terminals(
    journal: HoldingExperimentJournal,
    records: tuple[HoldingExperimentRecord, ...],
) -> None:
    for record in records:
        assert journal.append(record)


def test_analytics_builds_variant_direction_and_score_band_rows(
    tmp_path: Path,
) -> None:
    experiment, journal = runtime(tmp_path)
    long_created = activate(experiment, baseline_trade(trade_id="long"))
    short_created = activate(
        experiment,
        baseline_trade(
            trade_id="short",
            direction=ShadowDirection.SHORT,
            stop=110.0,
            tp1=90.0,
            tp2=80.0,
        ),
    )
    append_terminals(
        journal,
        terminal_records(
            long_created,
            {
                HoldingVariant.A30: -0.10,
                HoldingVariant.B60: 0.20,
                HoldingVariant.C120: 0.10,
                HoldingVariant.D300: 0.00,
            },
        ),
    )
    append_terminals(
        journal,
        terminal_records(
            short_created,
            {
                HoldingVariant.A30: 0.10,
                HoldingVariant.B60: 0.05,
                HoldingVariant.C120: 0.30,
                HoldingVariant.D300: -0.10,
            },
        ),
    )

    result = HoldingExperimentAnalyticsEngine().analyze(journal.path)

    assert result.terminal_variants == 8
    assert {row.scope for row in result.rows} == {
        "ALL",
        "DIRECTION",
        "SCORE_BAND",
    }
    all_a30 = next(
        row
        for row in result.rows
        if row.scope == "ALL" and row.variant is HoldingVariant.A30
    )
    assert all_a30.performance.total == 2
    assert all_a30.performance.wins == 1
    assert all_a30.performance.losses == 1
    assert all_a30.performance.total_r == pytest.approx(0.0)
    assert all_a30.performance.median_mfe is not None
    assert all_a30.performance.p90_mae is not None
    assert dict(all_a30.performance.exit_counts) == {"TIME_EXIT": 2}
    assert dict(all_a30.performance.mfe_threshold_counts)[0.05] == 2


def test_paired_comparison_uses_same_group_entries(tmp_path: Path) -> None:
    experiment, journal = runtime(tmp_path)
    created = activate(experiment, baseline_trade(trade_id="paired"))
    append_terminals(
        journal,
        terminal_records(
            created,
            {
                HoldingVariant.A30: 0.00,
                HoldingVariant.B60: 0.20,
                HoldingVariant.C120: -0.10,
                HoldingVariant.D300: 0.00,
            },
        ),
    )

    result = HoldingExperimentAnalyticsEngine().analyze(journal.path)
    all_pairs = {
        item.challenger: item
        for item in result.paired_comparisons
        if item.direction == "ALL"
    }

    assert all_pairs[HoldingVariant.B60].improved == 1
    assert all_pairs[HoldingVariant.B60].mean_delta_r == pytest.approx(0.20)
    assert all_pairs[HoldingVariant.C120].worsened == 1
    assert all_pairs[HoldingVariant.D300].unchanged == 1


def test_incomplete_variants_are_excluded_and_report_is_human_readable(
    tmp_path: Path,
) -> None:
    experiment, journal = runtime(tmp_path)
    activate(experiment)
    experiment.stop(at=NOW + timedelta(minutes=5))

    result = HoldingExperimentAnalyticsEngine().analyze(journal.path)
    report = format_holding_experiment_report(result)

    assert result.terminal_variants == 0
    assert result.incomplete_variants == 4
    assert "Variant | N | WIN | LOSS | WR" in report
    assert "PAIRED A30 COMPARISON" in report
    assert "INCOMPLETE excluded: 4" in report
