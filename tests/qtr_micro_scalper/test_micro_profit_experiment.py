from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_signal_assistant.qtr_micro_scalper.data.liquidity import (
    PressureDirection,
)
from market_signal_assistant.qtr_micro_scalper.data.market_state import MarketBias
from market_signal_assistant.qtr_micro_scalper.data.models import (
    PublicTradeEvent,
    TradeSide,
)
from market_signal_assistant.qtr_micro_scalper.micro_profit_experiment import (
    ContinuationEvidence,
    ContinuationExitReason,
    CostAccrual,
    CostScenario,
    MicroCostModelConfig,
    MicroExperimentRecordType,
    MicroProfitExperimentConfig,
    MicroProfitExperimentRuntime,
    MicroProfitJournal,
    MicroTarget,
    calculate_cost_breakdown,
    deserialize_micro_profit_record,
    iter_micro_profit_records,
    serialize_micro_profit_record,
)
from market_signal_assistant.qtr_micro_scalper.setup_context import ShadowDirection
from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
    ShadowTrade,
    ShadowTradeEvent,
    ShadowTradeEventType,
    ShadowTradeStage,
)

NOW = datetime(2026, 8, 24, 12, tzinfo=UTC)


def baseline(
    *,
    direction: ShadowDirection = ShadowDirection.LONG,
    entry: float = 100.0,
    stop: float = 90.0,
    trade_id: str = "baseline-1",
) -> ShadowTrade:
    risk = abs(entry - stop)
    sign = 1.0 if direction is ShadowDirection.LONG else -1.0
    return ShadowTrade(
        trade_id=trade_id,
        symbol="BTCUSDT",
        direction=direction,
        planned_at=NOW,
        entry_expires_at=NOW + timedelta(seconds=60),
        opportunity_score=82.0,
        confidence=88.0,
        stage=ShadowTradeStage.WAITING_ENTRY,
        entry_price=entry,
        initial_stop=stop,
        current_stop=stop,
        tp1_price=entry + sign * risk,
        tp2_price=entry + sign * risk * 2,
        risk_per_unit=risk,
    )


def opened(trade: ShadowTrade, *, seconds: float = 1.0) -> ShadowTrade:
    at = NOW + timedelta(seconds=seconds)
    event = ShadowTradeEvent(
        event_type=ShadowTradeEventType.ENTRY,
        occurred_at=at,
        price=trade.entry_price,
        quantity_fraction=1.0,
        realized_r=0.0,
        reason="Observed baseline entry.",
    )
    return replace(
        trade,
        stage=ShadowTradeStage.OPEN,
        entry_at=at,
        last_processed_at=at,
        events=(event,),
    )


def evidence(
    *,
    direction: ShadowDirection = ShadowDirection.LONG,
    at: datetime = NOW,
    price: float = 100.0,
    bias: MarketBias = MarketBias.BULLISH,
    setup_state: str = "SHADOW_CANDIDATE",
    delta: float = 10_000.0,
    imbalance: float = 0.25,
    pressure: PressureDirection = PressureDirection.BUY,
    invalidation: float = 90.0,
    spread_bps: float = 2.0,
) -> ContinuationEvidence:
    return ContinuationEvidence(
        symbol="BTCUSDT",
        observed_at=at,
        direction=direction,
        market_price=price,
        invalidation_price=invalidation,
        setup_state=setup_state,
        setup_confidence=85.0,
        market_state="BUY_PRESSURE",
        market_bias=bias,
        delta_5s=delta,
        orderbook_imbalance=imbalance,
        liquidity_pressure=pressure,
        spread_bps=spread_bps,
    )


def trade(index: int, price: float) -> PublicTradeEvent:
    at = NOW + timedelta(seconds=index, milliseconds=100)
    return PublicTradeEvent(
        symbol="BTCUSDT",
        trade_id=f"trade-{index}-{price}",
        exchange_at=at,
        received_at=at,
        side=TradeSide.BUY,
        price=price,
        quantity=1.0,
        quote_notional=price,
    )


def runtime(
    tmp_path: Path,
    *,
    stop: float = 90.0,
    direction: ShadowDirection = ShadowDirection.LONG,
    trailing_r: float = 0.10,
    maximum_bars: int = 300,
    cost: MicroCostModelConfig | None = None,
) -> tuple[MicroProfitExperimentRuntime, MicroProfitJournal, ShadowTrade]:
    journal = MicroProfitJournal(tmp_path / "micro.jsonl")
    experiment = MicroProfitExperimentRuntime(
        journal,
        MicroProfitExperimentConfig(
            enabled=True,
            runner_trailing_r=trailing_r,
            maximum_safety_bars=maximum_bars,
            cost_model=cost or MicroCostModelConfig(),
        ),
    )
    source = baseline(direction=direction, stop=stop)
    activation = experiment.activate(source, evidence(direction=direction), score=82)
    assert activation.accepted
    experiment.sync_baseline(opened(source))
    return experiment, journal, source


def test_targets_are_exact_parallel_r_ladder_and_immutable(tmp_path: Path) -> None:
    experiment, journal, _ = runtime(tmp_path)

    created = tuple(
        item
        for item in iter_micro_profit_records(journal.path)
        if item.record_type is MicroExperimentRecordType.CREATED
    )

    assert [item.target for item in created] == list(MicroTarget)
    assert [item.target_price for item in created] == [
        100.5,
        101.0,
        101.5,
        102.0,
        102.5,
    ]
    assert experiment.metrics().active_variants == 5
    with pytest.raises(FrozenInstanceError):
        created[0].score = 1.0  # type: ignore[misc]


def test_m05_reached_then_reversal_closes_runner(tmp_path: Path) -> None:
    experiment, journal, _ = runtime(tmp_path)

    experiment.process_event(trade(2, 100.5))
    experiment.process_event(trade(3, 99.4))

    persisted = tuple(iter_micro_profit_records(journal.path))
    reached = next(
        item
        for item in persisted
        if item.target is MicroTarget.M05
        and item.record_type is MicroExperimentRecordType.TARGET_REACHED
    )
    exited = next(
        item
        for item in persisted
        if item.target is MicroTarget.M05
        and item.record_type is MicroExperimentRecordType.RUNNER_EXITED
    )
    assert reached.costs.gross_r == pytest.approx(0.05)
    assert exited.runner_exit_reason == ContinuationExitReason.TRAILING_EXCURSION
    assert exited.costs.gross_r == pytest.approx(-0.06)
    assert exited.costs.entry_fee > 0
    assert exited.costs.exit_fee > 0
    assert exited.costs.net_r < exited.costs.gross_r


def test_m10_reached_then_reversal(tmp_path: Path) -> None:
    experiment, journal, _ = runtime(tmp_path)

    experiment.process_event(trade(2, 101.0))
    experiment.process_event(trade(3, 99.9))

    m10 = [
        item
        for item in iter_micro_profit_records(journal.path)
        if item.target is MicroTarget.M10
    ]
    assert any(
        item.record_type is MicroExperimentRecordType.TARGET_REACHED for item in m10
    )
    assert any(
        item.record_type is MicroExperimentRecordType.RUNNER_EXITED for item in m10
    )


def test_strong_continuation_reaches_all_targets_and_safety_exit(
    tmp_path: Path,
) -> None:
    experiment, journal, _ = runtime(
        tmp_path,
        trailing_r=0.5,
        maximum_bars=2,
    )

    experiment.process_event(trade(2, 102.5))
    experiment.process_event(trade(5, 104.0))

    persisted = tuple(iter_micro_profit_records(journal.path))
    assert sum(
        item.record_type is MicroExperimentRecordType.TARGET_REACHED
        for item in persisted
    ) == 5
    exits = [
        item
        for item in persisted
        if item.record_type is MicroExperimentRecordType.RUNNER_EXITED
    ]
    assert len(exits) == 5
    assert {item.runner_exit_reason for item in exits} == {
        ContinuationExitReason.MAXIMUM_SAFETY_HORIZON.value
    }
    assert all(item.costs.gross_r == pytest.approx(0.4) for item in exits)


def test_opposite_market_state_exits_runner(tmp_path: Path) -> None:
    experiment, journal, _ = runtime(tmp_path)
    experiment.process_event(trade(2, 100.5))

    experiment.update_evidence(
        evidence(
            at=NOW + timedelta(seconds=3),
            price=100.4,
            bias=MarketBias.BEARISH,
        )
    )

    exit_record = next(
        item
        for item in iter_micro_profit_records(journal.path)
        if item.target is MicroTarget.M05
        and item.record_type is MicroExperimentRecordType.RUNNER_EXITED
    )
    assert exit_record.runner_exit_reason == "OPPOSITE_MARKET_STATE"


def test_conflicted_evidence_exits_runner(tmp_path: Path) -> None:
    experiment, journal, _ = runtime(tmp_path)
    experiment.process_event(trade(2, 100.5))

    experiment.update_evidence(
        evidence(
            at=NOW + timedelta(seconds=3),
            price=100.4,
            setup_state="CONFLICTED",
        )
    )

    exit_record = next(
        item
        for item in iter_micro_profit_records(journal.path)
        if item.target is MicroTarget.M05
        and item.record_type is MicroExperimentRecordType.RUNNER_EXITED
    )
    assert exit_record.runner_exit_reason == "DIRECTIONAL_EVIDENCE_LOST"


def test_structural_invalidation_exits_runner(tmp_path: Path) -> None:
    experiment, journal, _ = runtime(tmp_path)
    experiment.process_event(trade(2, 100.5))

    experiment.update_evidence(
        evidence(
            at=NOW + timedelta(seconds=3),
            price=89.9,
            invalidation=90.0,
        )
    )

    exit_record = next(
        item
        for item in iter_micro_profit_records(journal.path)
        if item.target is MicroTarget.M05
        and item.record_type is MicroExperimentRecordType.RUNNER_EXITED
    )
    assert exit_record.runner_exit_reason == "STRUCTURAL_INVALIDATION"


@pytest.mark.parametrize(
    ("direction", "target_price", "supporting_evidence"),
    [
        (
            ShadowDirection.LONG,
            100.5,
            evidence(at=NOW + timedelta(seconds=3), price=100.6),
        ),
        (
            ShadowDirection.SHORT,
            99.5,
            evidence(
                direction=ShadowDirection.SHORT,
                at=NOW + timedelta(seconds=3),
                price=99.4,
                bias=MarketBias.BEARISH,
                delta=-10_000,
                imbalance=-0.25,
                pressure=PressureDirection.SELL,
                invalidation=110.0,
            ),
        ),
    ],
)
def test_supporting_bullish_or_bearish_evidence_keeps_runner_active(
    tmp_path: Path,
    direction: ShadowDirection,
    target_price: float,
    supporting_evidence: ContinuationEvidence,
) -> None:
    experiment, journal, _ = runtime(
        tmp_path,
        direction=direction,
        stop=90.0 if direction is ShadowDirection.LONG else 110.0,
    )
    experiment.process_event(trade(2, target_price))

    persisted = experiment.update_evidence(supporting_evidence)

    assert persisted == ()
    assert experiment.metrics().active_variants == 5
    assert not any(
        item.record_type is MicroExperimentRecordType.RUNNER_EXITED
        for item in iter_micro_profit_records(journal.path)
    )


def test_gross_m05_can_be_net_loss_after_round_trip_costs() -> None:
    costs = calculate_cost_breakdown(
        MicroCostModelConfig(
            scenario=CostScenario.TAKER_TAKER,
            slippage_bps=1.0,
        ),
        entry_price=100.0,
        exit_price=100.05,
        risk_per_unit=1.0,
        gross_r=0.05,
        duration_seconds=10,
        entry_spread_bps=2.0,
        exit_spread_bps=2.0,
    )

    assert costs.gross_r == pytest.approx(0.05)
    assert costs.cost_floor_r > 0.05
    assert costs.net_r < 0


@pytest.mark.parametrize("target", list(MicroTarget))
def test_not_triggered_variant_has_projected_floor_but_no_actual_costs(
    tmp_path: Path,
    target: MicroTarget,
) -> None:
    journal = MicroProfitJournal(tmp_path / "micro.jsonl")
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

    expired = next(
        item
        for item in iter_micro_profit_records(journal.path)
        if item.target is target
        and item.record_type is MicroExperimentRecordType.EXPIRED
    )
    assert expired.outcome.value == "NOT_TRIGGERED"
    assert expired.entry_at is None
    assert expired.costs.projected_cost_floor_r > 0
    assert expired.costs.entry_fee == 0
    assert expired.costs.exit_fee == 0
    assert expired.costs.spread_cost == 0
    assert expired.costs.slippage_cost == 0
    assert expired.costs.funding_cost == 0
    assert expired.costs.total_cost == 0
    assert expired.costs.actual_total_cost_r == 0
    assert expired.costs.actual_net_r == 0


def test_entry_opened_accrues_entry_side_only(tmp_path: Path) -> None:
    _, journal, _ = runtime(
        tmp_path,
        cost=MicroCostModelConfig(slippage_bps=1.0),
    )

    entry = next(
        item
        for item in iter_micro_profit_records(journal.path)
        if item.target is MicroTarget.M05
        and item.record_type is MicroExperimentRecordType.ENTRY_OPENED
    )
    assert entry.costs.entry_fee > 0
    assert entry.costs.spread_cost > 0
    assert entry.costs.slippage_cost > 0
    assert entry.costs.exit_fee == 0
    assert entry.costs.funding_cost == 0
    assert entry.costs.total_cost_r > 0
    assert entry.costs.net_r < 0


def test_terminal_virtual_exit_accrues_entry_and_exit_costs(tmp_path: Path) -> None:
    experiment, journal, _ = runtime(
        tmp_path,
        cost=MicroCostModelConfig(slippage_bps=1.0),
    )

    experiment.process_event(trade(2, 100.5))

    reached = next(
        item
        for item in iter_micro_profit_records(journal.path)
        if item.target is MicroTarget.M05
        and item.record_type is MicroExperimentRecordType.TARGET_REACHED
    )
    assert reached.costs.entry_fee > 0
    assert reached.costs.exit_fee > 0
    assert reached.costs.spread_cost > 0
    assert reached.costs.slippage_cost > 0


def test_time_exit_of_open_variant_accrues_actual_round_trip_costs(
    tmp_path: Path,
) -> None:
    experiment, journal, _ = runtime(
        tmp_path,
        maximum_bars=1,
        cost=MicroCostModelConfig(slippage_bps=1.0),
    )

    experiment.process_event(trade(2, 100.1))

    closed = next(
        item
        for item in iter_micro_profit_records(journal.path)
        if item.target is MicroTarget.M05
        and item.record_type is MicroExperimentRecordType.TARGET_CLOSED
    )
    assert closed.outcome.value == "TARGET_MISSED"
    assert closed.costs.entry_fee > 0
    assert closed.costs.exit_fee > 0
    assert closed.costs.total_cost_r > 0
    assert closed.costs.net_r < closed.costs.gross_r


def test_projected_cost_calculation_does_not_accrue_execution_costs() -> None:
    costs = calculate_cost_breakdown(
        MicroCostModelConfig(slippage_bps=1.0),
        entry_price=100,
        exit_price=100,
        risk_per_unit=1,
        gross_r=0,
        duration_seconds=60,
        entry_spread_bps=2,
        exit_spread_bps=2,
        accrual=CostAccrual.PROJECTED_ONLY,
    )

    assert costs.projected_cost_floor_r > 0
    assert costs.actual_total_cost_r == 0
    assert costs.actual_net_r == 0


@pytest.mark.parametrize(
    ("scenario", "entry_rate", "exit_rate"),
    [
        (CostScenario.TAKER_TAKER, 0.00055, 0.00055),
        (CostScenario.MAKER_TAKER, 0.00020, 0.00055),
        (CostScenario.MAKER_MAKER, 0.00020, 0.00020),
    ],
)
def test_fee_scenarios(
    scenario: CostScenario,
    entry_rate: float,
    exit_rate: float,
) -> None:
    costs = calculate_cost_breakdown(
        MicroCostModelConfig(scenario=scenario),
        entry_price=100,
        exit_price=101,
        risk_per_unit=10,
        gross_r=0.1,
        duration_seconds=0,
        entry_spread_bps=0,
        exit_spread_bps=0,
    )

    assert costs.entry_fee == pytest.approx(100 * entry_rate)
    assert costs.exit_fee == pytest.approx(101 * exit_rate)


def test_custom_fee_and_funding_are_configurable() -> None:
    costs = calculate_cost_breakdown(
        MicroCostModelConfig(
            scenario=CostScenario.CUSTOM,
            custom_entry_fee_rate=0.0001,
            custom_exit_fee_rate=0.0003,
            funding_rate_8h=0.0001,
        ),
        entry_price=100,
        exit_price=101,
        risk_per_unit=10,
        gross_r=0.1,
        duration_seconds=8 * 60 * 60,
        entry_spread_bps=0,
        exit_spread_bps=0,
    )

    assert costs.entry_fee == pytest.approx(0.01)
    assert costs.exit_fee == pytest.approx(0.0303)
    assert costs.funding_cost == pytest.approx(0.01)


def test_short_targets_use_same_r_geometry(tmp_path: Path) -> None:
    experiment, journal, _ = runtime(
        tmp_path,
        direction=ShadowDirection.SHORT,
        stop=110.0,
    )

    experiment.process_event(trade(2, 99.5))

    reached = next(
        item
        for item in iter_micro_profit_records(journal.path)
        if item.target is MicroTarget.M05
        and item.record_type is MicroExperimentRecordType.TARGET_REACHED
    )
    assert reached.target_price == pytest.approx(99.5)


def test_baseline_object_and_authoritative_levels_are_not_mutated(
    tmp_path: Path,
) -> None:
    experiment, _, source = runtime(tmp_path)
    original = source

    experiment.process_event(trade(2, 102.5))

    assert source == original
    assert source.tp1_price == 110.0
    assert source.tp2_price == 120.0
    assert source.initial_stop == 90.0


def test_restart_marks_unfinished_variants_incomplete(tmp_path: Path) -> None:
    experiment, journal, _ = runtime(tmp_path)
    experiment.process_event(trade(2, 100.5))
    journal.flush()

    recovered = MicroProfitJournal(journal.path)
    restarted = MicroProfitExperimentRuntime(
        recovered,
        MicroProfitExperimentConfig(enabled=True),
    )
    interrupted = restarted.start(at=NOW + timedelta(minutes=1))

    assert interrupted
    assert all(
        item.record_type is MicroExperimentRecordType.INTERRUPTED
        for item in interrupted
    )
    assert all(item.outcome.value == "INCOMPLETE" for item in interrupted)


def test_separate_journal_streaming_recovery_and_corruption(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    experiment, journal, _ = runtime(tmp_path)
    experiment.stop(at=NOW + timedelta(minutes=1))
    with journal.path.open("a", encoding="utf-8") as stream:
        stream.write("{broken}\n")

    def forbidden_read_text(self: Path, *args: object, **kwargs: object) -> str:
        del self, args, kwargs
        raise AssertionError("whole-file text loading is forbidden")

    def forbidden_read_bytes(self: Path) -> bytes:
        del self
        raise AssertionError("whole-file byte loading is forbidden")

    monkeypatch.setattr(Path, "read_text", forbidden_read_text)
    monkeypatch.setattr(Path, "read_bytes", forbidden_read_bytes)
    recovered = MicroProfitJournal(journal.path, maximum_recent_record_ids=8)

    assert recovered.metrics.bootstrap_scans == 1
    assert recovered.metrics.malformed_lines == 1
    assert recovered.metrics.recent_record_ids == 8


def test_serialization_is_deterministic(tmp_path: Path) -> None:
    _, journal, _ = runtime(tmp_path)
    item = next(iter_micro_profit_records(journal.path))

    encoded = serialize_micro_profit_record(item)

    assert deserialize_micro_profit_record(encoded) == item
    assert serialize_micro_profit_record(item) == encoded


def test_existing_schema_with_legacy_phantom_costs_is_readable(
    tmp_path: Path,
) -> None:
    journal = MicroProfitJournal(tmp_path / "source.jsonl")
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
        for item in iter_micro_profit_records(journal.path)
        if item.record_type is MicroExperimentRecordType.EXPIRED
    )
    legacy_costs = calculate_cost_breakdown(
        MicroCostModelConfig(),
        entry_price=expired.entry_price,
        exit_price=expired.current_price,
        risk_per_unit=expired.risk_per_unit,
        gross_r=0,
        duration_seconds=0,
        entry_spread_bps=2,
        exit_spread_bps=2,
    )
    legacy = replace(expired, costs=legacy_costs)

    decoded = deserialize_micro_profit_record(
        serialize_micro_profit_record(legacy)
    )

    assert decoded.schema_version == 1
    assert decoded.outcome.value == "NOT_TRIGGERED"
    assert decoded.costs.total_cost_r > 0


def test_experiment_is_disabled_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(
        "QTR_SCALPER_V2_MICRO_PROFIT_EXPERIMENT_ENABLED",
        raising=False,
    )

    config = MicroProfitExperimentConfig.from_environment()

    assert not config.enabled
    assert config.maximum_active_groups == 1_000


def test_ticks_without_lifecycle_transition_are_not_journaled(tmp_path: Path) -> None:
    experiment, journal, _ = runtime(tmp_path)
    before = journal.metrics.records_processed

    for index in range(2, 50):
        experiment.process_event(trade(index, 100.01))

    assert journal.metrics.records_processed == before


def test_runtime_capacity_bounds_groups_and_variants(tmp_path: Path) -> None:
    journal = MicroProfitJournal(tmp_path / "bounded.jsonl")
    experiment = MicroProfitExperimentRuntime(
        journal,
        MicroProfitExperimentConfig(enabled=True, maximum_active_groups=1),
    )
    first = experiment.activate(baseline(), evidence(), score=82)
    second = experiment.activate(
        baseline(trade_id="second"),
        evidence(),
        score=82,
    )

    assert first.accepted
    assert not second.accepted
    assert experiment.metrics().active_groups == 1
    assert experiment.metrics().active_variants == 5
    assert experiment.metrics().capacity_rejections == 1
