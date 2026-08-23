from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_signal_assistant.qtr_micro_scalper.data.models import (
    OrderBookEvent,
    OrderBookEventType,
    PublicTradeEvent,
    TradeSide,
)
from market_signal_assistant.qtr_micro_scalper.holding_experiment import (
    HoldingExperimentConfig,
    HoldingExperimentJournal,
    HoldingExperimentOutcome,
    HoldingExperimentRecord,
    HoldingExperimentRecordType,
    HoldingExperimentRuntime,
    HoldingExperimentStage,
    HoldingVariant,
    deserialize_holding_experiment_record,
    iter_holding_experiment_records,
    serialize_holding_experiment_record,
)
from market_signal_assistant.qtr_micro_scalper.setup_context import ShadowDirection
from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
    ShadowTrade,
    ShadowTradeEventType,
    ShadowTradeStage,
)

NOW = datetime(2026, 8, 23, 12, tzinfo=UTC)


def baseline_trade(
    *,
    trade_id: str = "shadow-baseline",
    direction: ShadowDirection = ShadowDirection.LONG,
    entry: float = 100.0,
    stop: float = 90.0,
    tp1: float = 110.0,
    tp2: float = 120.0,
) -> ShadowTrade:
    return ShadowTrade(
        trade_id=trade_id,
        symbol="BTCUSDT",
        direction=direction,
        planned_at=NOW,
        entry_expires_at=NOW + timedelta(seconds=60),
        opportunity_score=78.0,
        confidence=82.0,
        stage=ShadowTradeStage.WAITING_ENTRY,
        entry_price=entry,
        initial_stop=stop,
        current_stop=stop,
        tp1_price=tp1,
        tp2_price=tp2,
        risk_per_unit=abs(entry - stop),
    )


def public_trade(index: int, price: float = 100.0) -> PublicTradeEvent:
    observed_at = NOW + timedelta(seconds=index, milliseconds=100)
    return PublicTradeEvent(
        symbol="BTCUSDT",
        trade_id=f"trade-{index}",
        exchange_at=observed_at,
        received_at=observed_at,
        side=TradeSide.BUY,
        price=price,
        quantity=1.0,
        quote_notional=price,
    )


def market_clock(seconds: float, update_id: int) -> OrderBookEvent:
    observed_at = NOW + timedelta(seconds=seconds)
    return OrderBookEvent(
        symbol="BTCUSDT",
        event_type=OrderBookEventType.DELTA,
        exchange_at=observed_at,
        received_at=observed_at,
        update_id=update_id,
    )


def runtime(
    tmp_path: Path,
    *,
    maximum_active_groups: int = 1_000,
) -> tuple[HoldingExperimentRuntime, HoldingExperimentJournal]:
    journal = HoldingExperimentJournal(tmp_path / "holding.jsonl")
    experiment = HoldingExperimentRuntime(
        journal,
        HoldingExperimentConfig(
            enabled=True,
            maximum_active_groups=maximum_active_groups,
        ),
    )
    return experiment, journal


def activate(
    experiment: HoldingExperimentRuntime,
    trade: ShadowTrade | None = None,
) -> tuple[HoldingExperimentRecord, ...]:
    result = experiment.activate(
        trade or baseline_trade(),
        score=78.0,
        market_state="BUY_PRESSURE",
        setup_context="SHADOW_CANDIDATE",
    )
    assert result.accepted
    return result.records


def feed_bars(
    experiment: HoldingExperimentRuntime,
    *,
    start: int,
    stop: int,
    price: float = 100.0,
) -> None:
    for index in range(start, stop + 1):
        experiment.process_event(public_trade(index, price))


def test_one_baseline_entry_creates_four_controlled_variants(
    tmp_path: Path,
) -> None:
    experiment, _ = runtime(tmp_path)

    records = activate(experiment)

    assert [record.variant for record in records] == list(HoldingVariant)
    assert {record.experiment_group_id for record in records} == {
        records[0].experiment_group_id
    }
    assert [record.maximum_holding_bars for record in records] == [30, 60, 120, 300]
    controlled = {
        (
            item.symbol,
            item.direction,
            item.planned_at,
            item.entry,
            item.stop,
            item.tp1,
            item.tp2,
            item.score,
            item.market_state,
            item.setup_context,
        )
        for item in records
    }
    assert len(controlled) == 1
    with pytest.raises(FrozenInstanceError):
        records[0].score = 1.0  # type: ignore[misc]


def test_horizons_time_exit_independently_on_same_observed_stream(
    tmp_path: Path,
) -> None:
    experiment, _ = runtime(tmp_path)
    activate(experiment)

    feed_bars(experiment, start=0, stop=30)
    records = tuple(iter_holding_experiment_records(tmp_path / "holding.jsonl"))
    a30 = [record for record in records if record.variant is HoldingVariant.A30]
    assert a30[-1].exit_reason == ShadowTradeEventType.TIME_EXIT.value
    assert a30[-1].holding_completed_bars == 30
    assert experiment.metrics().active_variants == 3
    assert experiment.protected_symbols() == ("BTCUSDT",)

    feed_bars(experiment, start=31, stop=60)
    assert experiment.metrics().active_variants == 2
    feed_bars(experiment, start=61, stop=120)
    assert experiment.metrics().active_variants == 1
    feed_bars(experiment, start=121, stop=300)

    terminal = {
        record.variant: record
        for record in iter_holding_experiment_records(tmp_path / "holding.jsonl")
        if record.record_type is HoldingExperimentRecordType.TERMINAL
    }
    assert {
        variant: record.holding_completed_bars
        for variant, record in terminal.items()
    } == {
        HoldingVariant.A30: 30,
        HoldingVariant.B60: 60,
        HoldingVariant.C120: 120,
        HoldingVariant.D300: 300,
    }
    assert all(record.exit_reason == "TIME_EXIT" for record in terminal.values())
    assert experiment.metrics().active_groups == 0
    assert experiment.protected_symbols() == ()


def test_all_active_variants_receive_identical_completed_bars(
    tmp_path: Path,
) -> None:
    experiment, _ = runtime(tmp_path)
    activate(experiment)

    experiment.process_event(public_trade(0, 100.0))
    feed_bars(experiment, start=1, stop=10, price=100.5)

    states = experiment.active_variant_states()
    assert len(states) == 4
    assert {state.trade.bars_held for state in states} == {10}
    assert {state.trade.entry_at for state in states} == {
        NOW + timedelta(seconds=1)
    }
    assert {state.trade.max_favorable_excursion_r for state in states} == {0.05}
    assert {state.trade.max_adverse_excursion_r for state in states} == {0.0}


def test_market_ticks_without_lifecycle_change_do_not_amplify_journal_writes(
    tmp_path: Path,
) -> None:
    experiment, journal = runtime(tmp_path)
    activate(experiment)

    experiment.process_event(public_trade(0, 100.0))
    feed_bars(experiment, start=1, stop=20, price=100.1)

    records = tuple(iter_holding_experiment_records(journal.path))
    assert len(records) == 8
    assert [record.record_type for record in records].count(
        HoldingExperimentRecordType.CREATED
    ) == 4
    assert [record.record_type for record in records].count(
        HoldingExperimentRecordType.ENTRY_OPENED
    ) == 4


def test_tp_stop_and_breakeven_reuse_existing_lifecycle_semantics(
    tmp_path: Path,
) -> None:
    experiment, _ = runtime(tmp_path)
    activate(
        experiment,
        baseline_trade(stop=99.0, tp1=101.0, tp2=102.0),
    )

    experiment.process_event(public_trade(0, 100.0))
    experiment.process_event(public_trade(1, 101.1))
    experiment.process_event(public_trade(2, 99.9))
    experiment.process_event(market_clock(3.2, 1))

    terminal = [
        record
        for record in iter_holding_experiment_records(tmp_path / "holding.jsonl")
        if record.record_type is HoldingExperimentRecordType.TERMINAL
    ]
    assert len(terminal) == 4
    assert all(record.exit_reason == "STOP" for record in terminal)
    assert all(record.result_r == pytest.approx(0.5) for record in terminal)
    assert all(record.tp1_hit for record in terminal)
    assert all(not record.tp2_hit for record in terminal)


def test_experiment_never_writes_authoritative_baseline_journal(
    tmp_path: Path,
) -> None:
    baseline_path = tmp_path / "baseline.jsonl"
    baseline_path.write_text("authoritative\n", encoding="utf-8")
    before = baseline_path.read_bytes()
    experiment, journal = runtime(tmp_path)

    activate(experiment)
    feed_bars(experiment, start=0, stop=30)

    assert baseline_path.read_bytes() == before
    assert journal.path.name == "holding.jsonl"


def test_restart_marks_unfinished_variants_incomplete(tmp_path: Path) -> None:
    experiment, _ = runtime(tmp_path)
    activate(experiment)
    experiment.process_event(public_trade(0))
    experiment.process_event(market_clock(1.2, 1))

    recovered = HoldingExperimentJournal(tmp_path / "holding.jsonl")
    assert len(recovered.recovery.active_records) == 4
    restarted = HoldingExperimentRuntime(
        recovered,
        HoldingExperimentConfig(enabled=True),
    )
    interrupted = restarted.start(at=NOW + timedelta(minutes=10))

    assert len(interrupted) == 4
    assert all(
        record.stage is HoldingExperimentStage.INTERRUPTED
        for record in interrupted
    )
    assert all(
        record.outcome is HoldingExperimentOutcome.INCOMPLETE
        for record in interrupted
    )
    assert not recovered.recovery.active_records


def test_streaming_recovery_is_bounded_and_skips_corruption(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_runtime, _ = runtime(tmp_path)
    created = activate(source_runtime)[0]
    path = tmp_path / "large.jsonl"
    lines: list[str] = []
    for index in range(500):
        active = replace(
            created,
            recorded_at=NOW + timedelta(seconds=index * 2),
            experiment_group_id=f"group-{index}",
            baseline_trade_id=f"baseline-{index}",
            variant_trade_id=f"variant-{index}",
        )
        interrupted = replace(
            active,
            recorded_at=NOW + timedelta(seconds=index * 2 + 1),
            record_type=HoldingExperimentRecordType.INTERRUPTED,
            stage=HoldingExperimentStage.INTERRUPTED,
            exit_reason="INTERRUPTED",
            exit_time=NOW + timedelta(seconds=index * 2 + 1),
            outcome=HoldingExperimentOutcome.INCOMPLETE,
        )
        lines.extend(
            (
                serialize_holding_experiment_record(active),
                serialize_holding_experiment_record(interrupted),
            )
        )
    path.write_text("\n".join((*lines, "{broken")) + "\n", encoding="utf-8")

    def forbidden_read_bytes(self: Path) -> bytes:
        del self
        raise AssertionError("full-file byte materialization is forbidden")

    def forbidden_read_text(self: Path, *args: object, **kwargs: object) -> str:
        del self, args, kwargs
        raise AssertionError("full-file text materialization is forbidden")

    monkeypatch.setattr(Path, "read_bytes", forbidden_read_bytes)
    monkeypatch.setattr(Path, "read_text", forbidden_read_text)
    journal = HoldingExperimentJournal(path, maximum_recent_record_ids=32)

    assert journal.metrics.bootstrap_scans == 1
    assert journal.metrics.records_processed == 1_000
    assert journal.metrics.recent_record_ids == 32
    assert journal.metrics.active_recovered_variants == 0
    assert journal.metrics.malformed_lines == 1


def test_capacity_warning_does_not_change_baseline_trade(tmp_path: Path) -> None:
    experiment, _ = runtime(tmp_path, maximum_active_groups=1)
    first = baseline_trade(trade_id="baseline-1")
    second = baseline_trade(trade_id="baseline-2")
    assert activate(experiment, first)

    rejected = experiment.activate(
        second,
        score=78.0,
        market_state="BUY_PRESSURE",
        setup_context="SHADOW_CANDIDATE",
    )

    assert not rejected.accepted
    assert "baseline shadow trade remains unaffected" in rejected.reason
    assert second.stage is ShadowTradeStage.WAITING_ENTRY
    assert experiment.metrics().capacity_rejections == 1


def test_late_event_after_terminal_is_harmless(tmp_path: Path) -> None:
    experiment, _ = runtime(tmp_path)
    activate(experiment, baseline_trade(stop=99.0, tp1=101.0, tp2=102.0))
    experiment.process_event(public_trade(0, 100.0))
    experiment.process_event(public_trade(1, 102.1))
    experiment.process_event(market_clock(2.2, 1))
    assert experiment.metrics().active_groups == 0

    assert experiment.process_event(public_trade(3, 100.0)) == ()


def test_json_round_trip_is_deterministic(tmp_path: Path) -> None:
    experiment, _ = runtime(tmp_path)
    record = activate(experiment)[0]

    encoded = serialize_holding_experiment_record(record)

    assert deserialize_holding_experiment_record(encoded) == record
    assert serialize_holding_experiment_record(record) == encoded


def test_experiment_is_disabled_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("QTR_SCALPER_V2_HOLDING_EXPERIMENT_ENABLED", raising=False)

    settings = HoldingExperimentConfig.from_environment()

    assert not settings.enabled
    assert settings.maximum_active_groups == 1_000
