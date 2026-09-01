from __future__ import annotations

import asyncio
import json
from dataclasses import FrozenInstanceError, fields, replace
from datetime import timedelta
from pathlib import Path

import pytest
from test_orchestrator import activate, analysis_input
from test_pipeline import price_context, ready_result
from test_pipeline import target as pipeline_target
from test_pipeline import trade as market_trade
from test_scoring import liquidity as directional_liquidity
from test_scoring import market_state as directional_market_state
from test_scoring import opportunity as directional_opportunity
from test_scoring import orderbook as directional_orderbook
from test_scoring import trade_flow as directional_trade_flow
from test_shadow_runtime import bar
from test_snapshot import NOW, components

from market_signal_assistant.qtr_micro_scalper import service as service_module
from market_signal_assistant.qtr_micro_scalper.entry_telemetry import (
    ENTRY_FEATURE_SCHEMA_VERSION,
    EntryFeatureJournal,
    EntryFeatureSnapshot,
    EntryFeatureTelemetry,
    EntryFeatureTelemetrySettings,
    build_entry_feature_snapshot,
)
from market_signal_assistant.qtr_micro_scalper.micro_profit_experiment import (
    MicroProfitExperimentConfig,
    MicroProfitExperimentRuntime,
    MicroProfitJournal,
    iter_micro_profit_records,
)
from market_signal_assistant.qtr_micro_scalper.orchestrator import (
    ShadowAnalysisInput,
    ShadowOrchestrator,
)
from market_signal_assistant.qtr_micro_scalper.pipeline import LiveShadowPipeline
from market_signal_assistant.qtr_micro_scalper.protected_runner_experiment import (
    ProtectedRunnerConfig,
    ProtectedRunnerJournal,
    ProtectedRunnerRuntime,
    iter_protected_runner_records,
)
from market_signal_assistant.qtr_micro_scalper.setup_context import ShadowDirection
from market_signal_assistant.qtr_micro_scalper.shadow_journal import (
    ShadowTradeJournal,
)


def _telemetry_orchestrator(
    tmp_path: Path,
    *,
    telemetry: EntryFeatureTelemetry | None,
    suffix: str,
) -> ShadowOrchestrator:
    return ShadowOrchestrator(
        journal=ShadowTradeJournal(tmp_path / f"shadow-{suffix}.jsonl"),
        entry_telemetry=telemetry,
    )


def _long_analysis_with_verified_source() -> ShadowAnalysisInput:
    values = components()
    source_time = NOW - timedelta(seconds=30)
    price = replace(
        values.setup_context.price_context,
        local_range_low=98.0,
        local_range_high=102.0,
        verified_setup_state="READY_TO_CONSIDER",
        verified_setup_confidence=94.0,
        volume_confirmation=True,
        volatility_confirmation=True,
        liquidity_confirmation=True,
        source_observed_at=source_time,
    )
    setup = replace(values.setup_context, price_context=price)
    liquidity = replace(
        values.liquidity,
        sweep=replace(values.liquidity.sweep, delta_acceleration=0.75),
    )
    return analysis_input(
        replace(values, setup_context=setup, liquidity=liquidity)
    )


def _short_analysis() -> ShadowAnalysisInput:
    return ShadowAnalysisInput(
        symbol="BTCUSDT",
        generated_at=NOW,
        trade_flow=directional_trade_flow(-1.0),
        orderbook=directional_orderbook(-1.0),
        liquidity=directional_liquidity(-1.0),
        market_state=directional_market_state(-1.0),
        setup_context=directional_opportunity(-1.0),
    )


def _create_snapshot(tmp_path: Path) -> EntryFeatureSnapshot:
    coordinator = _telemetry_orchestrator(
        tmp_path,
        telemetry=None,
        suffix="snapshot",
    )
    activate(coordinator)
    analysis = _long_analysis_with_verified_source()
    result = coordinator.analyze(analysis)
    assert result.score is not None
    assert result.trade is not None
    return build_entry_feature_snapshot(analysis, result.score, result.trade)


def _payloads(path: Path) -> list[dict[str, object]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_snapshot_schema_is_immutable_and_contains_causal_features(
    tmp_path: Path,
) -> None:
    snapshot = _create_snapshot(tmp_path)

    assert snapshot.schema_version == ENTRY_FEATURE_SCHEMA_VERSION
    assert snapshot.direction is ShadowDirection.LONG
    assert snapshot.decision_timestamp == NOW
    assert snapshot.total_score > 0.0
    assert snapshot.delta_5s == 200.0
    assert snapshot.bid_depth == 20_000.0
    assert snapshot.ask_depth == 10_000.0
    assert snapshot.sweep_delta_acceleration == 0.75
    assert snapshot.verified_setup_state == "READY_TO_CONSIDER"
    assert snapshot.verified_setup_confidence == 94.0
    assert snapshot.source_age_seconds == 30.0
    with pytest.raises(FrozenInstanceError):
        snapshot.total_score = 0.0  # type: ignore[misc]


def test_snapshot_has_no_future_or_outcome_fields(tmp_path: Path) -> None:
    snapshot = _create_snapshot(tmp_path)
    names = {field.name.lower() for field in fields(snapshot)}
    forbidden = {
        "mfe",
        "mae",
        "outcome",
        "result_r",
        "exit_time",
        "terminal_stage",
        "future_bars",
    }

    assert names.isdisjoint(forbidden)


def test_one_snapshot_is_written_per_trade_id(tmp_path: Path) -> None:
    path = tmp_path / "entry-features.jsonl"
    telemetry = EntryFeatureTelemetry(EntryFeatureJournal(path))
    coordinator = _telemetry_orchestrator(
        tmp_path,
        telemetry=telemetry,
        suffix="once",
    )
    activate(coordinator)

    first = coordinator.analyze(_long_analysis_with_verified_source())
    duplicate = coordinator.analyze(_long_analysis_with_verified_source())

    assert first.trade is not None
    assert duplicate.trade is None
    assert len(_payloads(path)) == 1
    assert telemetry.journal.metrics.records_written == 1


def test_journal_append_and_restart_recovery_suppress_duplicate(
    tmp_path: Path,
) -> None:
    path = tmp_path / "entry-features.jsonl"
    snapshot = _create_snapshot(tmp_path)
    first = EntryFeatureJournal(path)
    assert first.append(snapshot)

    recovered = EntryFeatureJournal(path)

    assert not recovered.append(snapshot)
    assert recovered.metrics.bootstrap_scans == 1
    assert recovered.metrics.records_seen == 1
    assert recovered.metrics.duplicates_suppressed == 1
    assert len(_payloads(path)) == 1


def test_malformed_and_incomplete_trailing_lines_are_fail_safe(
    tmp_path: Path,
) -> None:
    path = tmp_path / "entry-features.jsonl"
    snapshot = _create_snapshot(tmp_path)
    path.write_text("not-json\n{\"trade_id\":", encoding="utf-8")

    journal = EntryFeatureJournal(path)
    appended = journal.append(snapshot)

    assert appended
    assert journal.metrics.malformed_lines == 1
    assert journal.metrics.incomplete_trailing_lines == 1
    last_line = path.read_text(encoding="utf-8").splitlines()[-1]
    assert json.loads(last_line)["trade_id"] == snapshot.trade_id


def test_dedup_state_is_bounded(tmp_path: Path) -> None:
    path = tmp_path / "entry-features.jsonl"
    snapshot = _create_snapshot(tmp_path)
    journal = EntryFeatureJournal(path, dedup_capacity=2)

    assert journal.append(replace(snapshot, trade_id="trade-a"))
    assert journal.append(replace(snapshot, trade_id="trade-b"))
    assert journal.append(replace(snapshot, trade_id="trade-c"))
    assert journal.metrics.retained_trade_ids == 2
    assert journal.append(replace(snapshot, trade_id="trade-a"))
    assert journal.metrics.retained_trade_ids == 2


def test_telemetry_is_disabled_by_default_and_opens_no_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "disabled.jsonl"
    monkeypatch.delenv("QTR_SCALPER_V2_ENTRY_TELEMETRY_ENABLED", raising=False)
    monkeypatch.setenv(
        "QTR_SCALPER_V2_ENTRY_TELEMETRY_JOURNAL_PATH",
        str(path),
    )

    settings = EntryFeatureTelemetrySettings.from_environment()

    assert not settings.enabled
    assert settings.journal_path == path
    assert not path.exists()


def test_disabled_service_composition_does_not_build_telemetry_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_journal(*args: object, **kwargs: object) -> EntryFeatureJournal:
        del args, kwargs
        raise AssertionError("disabled telemetry journal was constructed")

    monkeypatch.setenv("QTR_SCALPER_V2_ENABLED", "false")
    monkeypatch.setenv("QTR_SCALPER_V2_SHADOW_MODE", "true")
    monkeypatch.setenv("QTR_SCALPER_V2_ENTRY_TELEMETRY_ENABLED", "false")
    monkeypatch.setenv(
        "QTR_SCALPER_V2_JOURNAL_PATH",
        str(tmp_path / "shadow.jsonl"),
    )
    monkeypatch.setenv(
        "QTR_SCALPER_V2_DECISION_JOURNAL_PATH",
        str(tmp_path / "decisions.jsonl"),
    )
    monkeypatch.setattr(service_module, "EntryFeatureJournal", unexpected_journal)

    runtime = service_module.build_shadow_service_from_environment()

    assert runtime.health().status.value == "STOPPED"


class _FailingJournal(EntryFeatureJournal):
    def __init__(self) -> None:
        pass

    def append(self, snapshot: EntryFeatureSnapshot) -> bool:
        del snapshot
        raise OSError("simulated telemetry disk failure")


def test_write_failure_warns_but_shadow_trade_continues(tmp_path: Path) -> None:
    telemetry = EntryFeatureTelemetry(_FailingJournal())
    coordinator = _telemetry_orchestrator(
        tmp_path,
        telemetry=telemetry,
        suffix="failure",
    )
    activate(coordinator)

    with pytest.warns(RuntimeWarning, match="shadow trade continues"):
        result = coordinator.analyze(_long_analysis_with_verified_source())

    assert result.successful
    assert result.trade is not None
    assert coordinator.entry_telemetry_warning is not None


def test_long_and_short_are_persisted_without_direction_guessing(
    tmp_path: Path,
) -> None:
    for suffix, analysis, expected in (
        ("long", _long_analysis_with_verified_source(), "LONG"),
        ("short", _short_analysis(), "SHORT"),
    ):
        path = tmp_path / f"{suffix}.jsonl"
        coordinator = _telemetry_orchestrator(
            tmp_path,
            telemetry=EntryFeatureTelemetry(EntryFeatureJournal(path)),
            suffix=suffix,
        )
        activate(coordinator)

        result = coordinator.analyze(analysis)

        assert result.trade is not None
        assert _payloads(path)[0]["direction"] == expected


def test_genuinely_unavailable_features_are_serialized_as_null(
    tmp_path: Path,
) -> None:
    path = tmp_path / "none.jsonl"
    coordinator = _telemetry_orchestrator(
        tmp_path,
        telemetry=EntryFeatureTelemetry(EntryFeatureJournal(path)),
        suffix="none",
    )
    activate(coordinator)

    result = coordinator.analyze(analysis_input())

    assert result.trade is not None
    payload = _payloads(path)[0]
    assert payload["verified_setup_state"] is None
    assert payload["source_observed_at"] is None
    assert payload["source_age_seconds"] is None
    assert payload["sweep_delta_acceleration"] is None


def test_enabled_and_disabled_telemetry_have_identical_behavior(
    tmp_path: Path,
) -> None:
    baseline = _telemetry_orchestrator(
        tmp_path,
        telemetry=None,
        suffix="baseline",
    )
    observed = _telemetry_orchestrator(
        tmp_path,
        telemetry=EntryFeatureTelemetry(
            EntryFeatureJournal(tmp_path / "observed-features.jsonl")
        ),
        suffix="observed",
    )
    for coordinator in (baseline, observed):
        activate(coordinator)

    baseline_entry = baseline.analyze(_long_analysis_with_verified_source())
    observed_entry = observed.analyze(_long_analysis_with_verified_source())

    assert baseline_entry.score == observed_entry.score
    assert baseline_entry.trade == observed_entry.trade
    bars = (
        bar(1),
        bar(3, high=102.3, low=100.0, close=102.0),
        bar(5, open_price=102.0, high=104.5, low=101.0, close=104.0),
    )
    baseline_lifecycle = baseline.process_bars(bars)
    observed_lifecycle = observed.process_bars(bars)

    assert [result.trade for result in baseline_lifecycle] == [
        result.trade for result in observed_lifecycle
    ]


def test_control_and_protected_shadow_behavior_is_identical(
    tmp_path: Path,
) -> None:
    async def branch(
        suffix: str,
        telemetry: EntryFeatureTelemetry | None,
    ) -> tuple[object, ...]:
        shadow_journal = ShadowTradeJournal(tmp_path / f"shadow-{suffix}.jsonl")
        micro_journal = MicroProfitJournal(tmp_path / f"micro-{suffix}.jsonl")
        protected_journal = ProtectedRunnerJournal(
            tmp_path / f"protected-{suffix}.jsonl"
        )
        micro = MicroProfitExperimentRuntime(
            micro_journal,
            MicroProfitExperimentConfig(enabled=True),
        )
        protected = ProtectedRunnerRuntime(
            protected_journal,
            ProtectedRunnerConfig(enabled=True),
        )
        pipeline = LiveShadowPipeline(
            symbols=("BTCUSDT",),
            price_context_provider=price_context,
            orchestrator=ShadowOrchestrator(
                journal=shadow_journal,
                entry_telemetry=telemetry,
            ),
            micro_profit_experiment=micro,
            protected_runner=protected,
            clock=lambda: NOW + timedelta(seconds=5),
        )
        assert pipeline.register_target(pipeline_target(), observed_at=NOW)
        created = await ready_result(pipeline)
        assert created.trade is not None
        source = created.trade
        for trade_id, offset, price in (
            ("parity-entry", 0.3, source.entry_price),
            ("parity-entry-close", 1.3, source.entry_price),
            (
                "parity-target",
                2.3,
                source.entry_price + source.risk_per_unit * 0.050001,
            ),
        ):
            await pipeline.process_event(
                market_trade(
                    trade_id=trade_id,
                    at=NOW + timedelta(seconds=offset),
                    price=price,
                )
            )
        return (
            created.score,
            created.trade,
            shadow_journal.records(),
            tuple(iter_micro_profit_records(micro_journal.path)),
            tuple(iter_protected_runner_records(protected_journal.path)),
        )

    baseline = asyncio.run(branch("baseline", None))
    observed = asyncio.run(
        branch(
            "observed",
            EntryFeatureTelemetry(
                EntryFeatureJournal(tmp_path / "entry-features.jsonl")
            ),
        )
    )

    assert baseline == observed
