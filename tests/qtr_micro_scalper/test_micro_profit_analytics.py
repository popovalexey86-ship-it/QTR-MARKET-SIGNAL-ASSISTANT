from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
from test_micro_profit_experiment import NOW, baseline, evidence, opened, runtime, trade

from market_signal_assistant.qtr_micro_scalper.micro_profit_analytics import (
    MicroProfitAnalyticsEngine,
    format_micro_profit_report,
)
from market_signal_assistant.qtr_micro_scalper.micro_profit_experiment import (
    CostScenario,
    MicroCostModelConfig,
    MicroExperimentRecordType,
    MicroProfitExperimentConfig,
    MicroProfitExperimentRuntime,
    MicroProfitJournal,
    MicroTarget,
    calculate_cost_breakdown,
    iter_micro_profit_records,
    serialize_micro_profit_record,
)
from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
    ShadowTrade,
    ShadowTradeEvent,
    ShadowTradeEventType,
    ShadowTradeStage,
)


def close_baseline(
    source: ShadowTrade,
    *,
    realized_r: float,
    price: float,
) -> ShadowTrade:
    opened_source = opened(source)
    at = NOW + timedelta(seconds=30)
    event = ShadowTradeEvent(
        event_type=ShadowTradeEventType.TIME_EXIT,
        occurred_at=at,
        price=price,
        quantity_fraction=1.0,
        realized_r=realized_r,
        reason="Baseline time exit.",
    )
    return replace(
        opened_source,
        stage=ShadowTradeStage.CLOSED,
        closed_at=at,
        last_processed_at=at,
        realized_r=realized_r,
        remaining_fraction=0.0,
        events=(*opened_source.events, event),
    )


def test_analytics_reports_gross_net_costs_and_viability(tmp_path: Path) -> None:
    cost = MicroCostModelConfig(
        scenario=CostScenario.TAKER_TAKER,
        slippage_bps=1.0,
    )
    experiment, journal, source = runtime(tmp_path, stop=99.0, cost=cost)
    experiment.process_event(trade(2, 100.25))
    experiment.sync_baseline(
        close_baseline(source, realized_r=0.02, price=100.02)
    )
    experiment.process_event(trade(3, 100.10))

    result = MicroProfitAnalyticsEngine().analyze(journal.path)
    m05 = next(
        row
        for row in result.rows
        if row.scope == "ALL" and row.target is MicroTarget.M05
    )

    assert m05.performance.total == 1
    assert m05.performance.hits == 1
    assert m05.performance.gross_total_r == pytest.approx(0.05)
    assert m05.performance.net_total_r < 0
    assert m05.performance.fees > 0
    assert m05.performance.spread_cost > 0
    assert m05.performance.slippage_cost > 0
    assert m05.performance.economically_viable == 0
    assert result.cost_floor.median is not None
    assert result.cost_floor.median > 0.05


def test_five_not_triggered_variants_are_plans_not_monetary_losses(
    tmp_path: Path,
) -> None:
    journal = MicroProfitJournal(tmp_path / "not-triggered.jsonl")
    experiment = MicroProfitExperimentRuntime(
        journal,
        MicroProfitExperimentConfig(enabled=True),
    )
    source = baseline()
    assert experiment.activate(source, evidence(), score=82).accepted
    experiment.sync_baseline(
        replace(
            source,
            stage=ShadowTradeStage.EXPIRED,
            closed_at=source.entry_expires_at,
            last_processed_at=source.entry_expires_at,
        )
    )

    result = MicroProfitAnalyticsEngine().analyze(journal.path)
    all_rows = tuple(row for row in result.rows if row.scope == "ALL")

    assert len(all_rows) == 5
    assert all(row.performance.plans == 1 for row in all_rows)
    assert all(row.performance.triggered == 0 for row in all_rows)
    assert all(row.performance.trigger_rate == 0 for row in all_rows)
    assert all(row.performance.total == 0 for row in all_rows)
    assert sum(row.performance.net_total_r for row in all_rows) == 0
    assert sum(row.performance.total_cost_r for row in all_rows) == 0
    assert sum(row.performance.fees for row in all_rows) == 0
    assert sum(row.performance.spread_cost for row in all_rows) == 0
    assert sum(row.performance.slippage_cost for row in all_rows) == 0
    assert sum(row.performance.funding_cost for row in all_rows) == 0
    assert all(
        row.performance.signal_net_expectancy_r == 0 for row in all_rows
    )
    assert result.cost_floor.count == 1
    assert result.cost_floor.median is not None
    assert result.cost_floor.median > 0


def test_signal_expectancy_includes_not_triggered_plan_as_zero_r(
    tmp_path: Path,
) -> None:
    journal = MicroProfitJournal(tmp_path / "signal-expectancy.jsonl")
    experiment = MicroProfitExperimentRuntime(
        journal,
        MicroProfitExperimentConfig(enabled=True),
    )
    expired_source = baseline(trade_id="expired")
    assert experiment.activate(expired_source, evidence(), score=82).accepted
    experiment.sync_baseline(
        replace(
            expired_source,
            stage=ShadowTradeStage.EXPIRED,
            closed_at=expired_source.entry_expires_at,
        )
    )
    opened_source = baseline(trade_id="opened")
    assert experiment.activate(opened_source, evidence(), score=82).accepted
    experiment.sync_baseline(opened(opened_source))
    experiment.process_event(trade(2, 100.5))

    result = MicroProfitAnalyticsEngine().analyze(journal.path)
    m05 = next(
        row
        for row in result.rows
        if row.scope == "ALL" and row.target is MicroTarget.M05
    )

    assert m05.performance.plans == 2
    assert m05.performance.triggered == 1
    assert m05.performance.total == 1
    assert m05.performance.net_expectancy_r == pytest.approx(
        m05.performance.net_total_r
    )
    assert m05.performance.signal_net_expectancy_r == pytest.approx(
        m05.performance.net_total_r / 2
    )


def test_legacy_not_triggered_costs_do_not_enter_opened_trade_analytics(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source.jsonl"
    journal = MicroProfitJournal(source_path)
    experiment = MicroProfitExperimentRuntime(
        journal,
        MicroProfitExperimentConfig(enabled=True),
    )
    source = baseline()
    assert experiment.activate(source, evidence(), score=82).accepted
    experiment.sync_baseline(
        replace(
            source,
            stage=ShadowTradeStage.EXPIRED,
            closed_at=source.entry_expires_at,
        )
    )
    expired = next(
        item
        for item in iter_micro_profit_records(source_path)
        if item.target is MicroTarget.M05
        and item.record_type is MicroExperimentRecordType.EXPIRED
    )
    legacy = replace(
        expired,
        costs=calculate_cost_breakdown(
            MicroCostModelConfig(),
            entry_price=expired.entry_price,
            exit_price=expired.current_price,
            risk_per_unit=expired.risk_per_unit,
            gross_r=0,
            duration_seconds=0,
            entry_spread_bps=2,
            exit_spread_bps=2,
        ),
    )
    legacy_path = tmp_path / "legacy.jsonl"
    legacy_path.write_text(
        serialize_micro_profit_record(legacy) + "\n",
        encoding="utf-8",
    )

    result = MicroProfitAnalyticsEngine().analyze(legacy_path)
    row = next(item for item in result.rows if item.scope == "ALL")

    assert result.records_processed == 1
    assert row.performance.total == 0
    assert row.performance.net_total_r == 0
    assert row.performance.total_cost_r == 0


def test_analytics_has_direction_score_and_symbol_breakdowns(tmp_path: Path) -> None:
    experiment, journal, _ = runtime(tmp_path)
    experiment.process_event(trade(2, 102.5))

    result = MicroProfitAnalyticsEngine().analyze(journal.path)

    scopes = {row.scope for row in result.rows}
    assert {"ALL", "DIRECTION", "SCORE_BAND", "SYMBOL"} <= scopes
    assert any(row.key == "LONG" for row in result.rows)
    assert any(row.key == "80-100" for row in result.rows)
    assert any(row.key == "BTCUSDT" for row in result.rows)


def test_btc_like_persistent_episode_aggregates_baseline_and_runner(
    tmp_path: Path,
) -> None:
    experiment, journal, source = runtime(tmp_path, trailing_r=0.10)
    experiment.process_event(trade(2, 102.5))
    experiment.sync_baseline(
        close_baseline(source, realized_r=0.02, price=100.2)
    )
    experiment.process_event(trade(3, 104.0))
    experiment.process_event(trade(4, 102.9))

    second = baseline(trade_id="baseline-2")
    activation = experiment.activate(
        second,
        evidence(at=NOW + timedelta(seconds=5)),
        score=87,
    )
    assert activation.accepted
    second_open = replace(
        opened(second, seconds=6),
        planned_at=NOW + timedelta(seconds=5),
        entry_expires_at=NOW + timedelta(seconds=65),
    )
    experiment.sync_baseline(second_open)
    experiment.process_event(trade(7, 102.5))
    experiment.sync_baseline(
        replace(
            second_open,
            stage=ShadowTradeStage.CLOSED,
            closed_at=NOW + timedelta(seconds=35),
            last_processed_at=NOW + timedelta(seconds=35),
            realized_r=0.01,
            remaining_fraction=0.0,
            events=(
                *second_open.events,
                ShadowTradeEvent(
                    event_type=ShadowTradeEventType.TIME_EXIT,
                    occurred_at=NOW + timedelta(seconds=35),
                    price=100.1,
                    quantity_fraction=1.0,
                    realized_r=0.01,
                    reason="Baseline time exit.",
                ),
            ),
        )
    )
    experiment.process_event(trade(8, 104.0))
    experiment.process_event(trade(9, 102.9))

    result = MicroProfitAnalyticsEngine().analyze(journal.path)
    episode = next(item for item in result.episodes if item.symbol == "BTCUSDT")

    assert episode.direction.value == "LONG"
    assert episode.baseline_entries == 2
    assert episode.baseline_gross_r == pytest.approx(0.03)
    assert episode.runner_net_r > episode.baseline_net_r
    assert episode.profit_left_uncaptured_r > 0
    assert episode.maximum_favorable_price_move_pct > 0
    assert episode.setup_confidence_min >= 80
    assert episode.setup_confidence_max >= episode.setup_confidence_min


def test_episode_gap_creates_independent_episode(tmp_path: Path) -> None:
    experiment, journal, _ = runtime(tmp_path)
    late = baseline(trade_id="late")
    late = replace(
        late,
        planned_at=NOW + timedelta(hours=1),
        entry_expires_at=NOW + timedelta(hours=1, seconds=60),
    )
    activation = experiment.activate(
        late,
        evidence(at=NOW + timedelta(hours=1)),
        score=82,
    )
    assert activation.accepted

    result = MicroProfitAnalyticsEngine().analyze(journal.path)

    assert len(result.episodes) == 2


def test_report_never_calls_gross_only_result_profitable(tmp_path: Path) -> None:
    experiment, journal, _ = runtime(
        tmp_path,
        stop=99.0,
        cost=MicroCostModelConfig(slippage_bps=1),
    )
    experiment.process_event(trade(2, 100.05))

    report = format_micro_profit_report(
        MicroProfitAnalyticsEngine().analyze(journal.path)
    )

    assert "Gross ΣR" in report
    assert "Net ΣR" in report
    assert "Shadow-only" in report


def test_streaming_analytics_is_memory_bounded(tmp_path: Path) -> None:
    experiment, journal, _ = runtime(tmp_path)
    experiment.process_event(trade(2, 102.5))

    result = MicroProfitAnalyticsEngine(
        maximum_episodes=1,
        maximum_group_tracking=2,
    ).analyze(journal.path)

    assert result.retained_samples <= 4_096
    assert result.retained_episodes <= 1
    assert result.retained_group_states <= 2
