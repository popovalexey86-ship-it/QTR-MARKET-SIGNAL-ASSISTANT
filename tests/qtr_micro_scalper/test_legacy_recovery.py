from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import asdict
from pathlib import Path

import pytest
from test_metrics import NOW, record, sample_records
from test_service import FakePipeline
from test_snapshot import build_complete

from market_signal_assistant.qtr_micro_scalper.analytics import (
    IncrementalDecisionAnalytics,
)
from market_signal_assistant.qtr_micro_scalper.legacy_recovery import (
    classify_legacy_shadow_recovery,
)
from market_signal_assistant.qtr_micro_scalper.metrics import ShadowMetricsAggregator
from market_signal_assistant.qtr_micro_scalper.orchestrator import ShadowOrchestrator
from market_signal_assistant.qtr_micro_scalper.service import (
    LiveShadowPipeline,
    build_shadow_service_from_environment,
)
from market_signal_assistant.qtr_micro_scalper.shadow_decision import ShadowTradeStage
from market_signal_assistant.qtr_micro_scalper.shadow_journal import (
    ShadowTradeJournal,
    build_shadow_trade_record,
    serialize_shadow_trade_record,
)
from market_signal_assistant.qtr_micro_scalper.shadow_runtime import ShadowRuntime


@pytest.mark.parametrize(
    "stage", [ShadowTradeStage.WAITING_ENTRY, ShadowTradeStage.OPEN]
)
def test_legacy_startup_preserves_history_and_never_hydrates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    stage: ShadowTradeStage,
) -> None:
    journal = ShadowTradeJournal(tmp_path / "trades.jsonl")
    for row in (
        *sample_records(),
        record("legacy", stage=stage, entered=stage is ShadowTradeStage.OPEN),
    ):
        journal.append(row)
    before = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    metrics = ShadowMetricsAggregator(journal).snapshot(generated_at=NOW)
    analytics = IncrementalDecisionAnalytics().snapshot(
        journal.snapshot(), generated_at=NOW
    )
    monkeypatch.setenv("QTR_SCALPER_V2_ENABLED", "true")
    monkeypatch.setenv("QTR_SCALPER_V2_LIVE_ENABLED", "false")
    monkeypatch.setenv("QTR_SCALPER_V2_SHADOW_MODE", "true")
    monkeypatch.setenv(
        "QTR_SCALPER_V2_DECISION_JOURNAL_PATH", str(tmp_path / "decisions.jsonl")
    )
    orchestrators: list[ShadowOrchestrator] = []

    def factory(**kwargs: object) -> FakePipeline:
        orchestrator = kwargs["orchestrator"]
        assert isinstance(orchestrator, ShadowOrchestrator)
        assert orchestrator.metrics().active_shadow_trades == 0
        orchestrators.append(orchestrator)
        return FakePipeline()

    monkeypatch.setattr(LiveShadowPipeline, "with_live_collectors", factory)

    # Production factory, two process-like rebuilds, repeated starts/health reads.
    async def scenario() -> None:
        for _ in range(2):
            service = build_shadow_service_from_environment(journal=journal)
            await service.start()
            await service.start()
            for _ in range(3):
                health = service.health()
                operational = service.metrics_snapshot()
                assert health.ready and health.last_error is None
                assert "historical journal" in (health.legacy_quarantine_warning or "")
                assert operational.quarantined_legacy_trades == 1
                assert operational.quarantined_trade_ids_and_stages == (
                    ("legacy", stage.value),
                )
                assert operational.resumable_active_trades == 0
                assert operational.runtime_restored_legacy_trades == 0
            await service.stop()
            await service.start()
            await service.stop()

    asyncio.run(scenario())
    assert (
        len([r for r in caplog.records if "quarantined from runtime" in r.message]) == 2
    )
    assert all(o.metrics().active_shadow_trades == 0 for o in orchestrators)
    assert hashlib.sha256(journal.path.read_bytes()).hexdigest() == before
    assert journal.latest("legacy") is not None
    assert len(journal.historical_unfinished_records) == 1
    assert ShadowMetricsAggregator(journal).snapshot(generated_at=NOW) == metrics
    assert (
        IncrementalDecisionAnalytics().snapshot(journal.snapshot(), generated_at=NOW)
        == analytics
    )


@pytest.mark.parametrize("terminal", [False, True])
def test_empty_and_all_terminal_startup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    terminal: bool,
) -> None:
    journal = ShadowTradeJournal(tmp_path / "trades.jsonl")
    if terminal:
        for row in sample_records():
            journal.append(row)
    monkeypatch.setenv("QTR_SCALPER_V2_ENABLED", "true")
    monkeypatch.setenv("QTR_SCALPER_V2_SHADOW_MODE", "true")
    monkeypatch.setattr(
        LiveShadowPipeline, "with_live_collectors", lambda **kwargs: FakePipeline()
    )

    async def scenario() -> None:
        service = build_shadow_service_from_environment(journal=journal)
        await service.start()
        assert service.health().ready
        assert service.metrics_snapshot().quarantined_legacy_trades == 0
        await service.stop()

    asyncio.run(scenario())


@pytest.mark.parametrize("extension", ["checkpoint", "schema", "malformed", "partial"])
def test_unknown_future_checkpoint_or_damaged_history_fails_closed_without_quarantine(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    extension: str,
) -> None:
    row = json.loads(
        serialize_shadow_trade_record(record("future", stage=ShadowTradeStage.OPEN))
    )
    if extension == "checkpoint":
        # Even complete runtime/score/context data must go to a dedicated versioned
        # validator/hydrator, never disappear into schema-1 historical quarantine.
        bundle = build_complete()
        result = ShadowRuntime().process_snapshot(bundle)
        assert result.trade is not None and result.score is not None
        persisted = build_shadow_trade_record(
            result.trade, result.score, recorded_at=result.trade.planned_at
        )
        row = json.loads(serialize_shadow_trade_record(persisted))
        row["runtime_checkpoint"] = {
            "trade": asdict(result.trade),
            "score": asdict(result.score),
            "context": asdict(bundle),
        }
    elif extension == "schema":
        row["schema_version"] = 2
    raw = json.dumps(row, default=str).encode() + b"\n"
    if extension == "malformed":
        raw = b"not json\n"
    elif extension == "partial":
        raw = raw[:-3]
    path = tmp_path / "trades.jsonl"
    path.write_bytes(raw)
    journal = ShadowTradeJournal(path)
    monkeypatch.setenv("QTR_SCALPER_V2_ENABLED", "true")
    with pytest.raises(RuntimeError, match="requires review"):
        classify_legacy_shadow_recovery(journal)
    with pytest.raises(RuntimeError, match="requires review"):
        build_shadow_service_from_environment(journal=journal)
    assert path.read_bytes() == raw


def test_quarantine_is_deterministic_and_does_not_call_full_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal = ShadowTradeJournal(tmp_path / "trades.jsonl")
    for i in range(5):
        journal.append(
            record(f"waiting-{i}", stage=ShadowTradeStage.WAITING_ENTRY, entered=False)
        )
    journal.append(record("open", stage=ShadowTradeStage.OPEN))

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("full recovery called")

    monkeypatch.setattr(ShadowTradeJournal, "records", forbidden)
    monkeypatch.setattr(
        "market_signal_assistant.qtr_micro_scalper.shadow_journal.recover_shadow_journal",
        forbidden,
    )
    initial = journal.index_metrics
    first = classify_legacy_shadow_recovery(journal)
    for _ in range(30):
        assert classify_legacy_shadow_recovery(journal) == first
    assert journal.index_metrics == initial
    assert len(first.legacy_unresumable_records) == 6
    assert "'OPEN': 1" in (first.warning or "")
    assert "'WAITING_ENTRY': 5" in (first.warning or "")
