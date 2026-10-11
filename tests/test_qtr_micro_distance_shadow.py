from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from market_signal_assistant.qtr_micro.distance_shadow import (
    CONTROL_THRESHOLD,
    DISTANCE_THRESHOLDS,
    DistanceShadowDispatcher,
    ExperimentCategory,
    FrozenRuntimeContext,
    JsonlDistanceShadowEvidence,
    QtrMicroDistanceShadowObserver,
    QualifiedScannerSnapshot,
    ShadowStatus,
    _meets_review_threshold,
    summarize_distance_shadow,
)
from market_signal_assistant.qtr_micro.engine import QtrMicroEntryEngine
from market_signal_assistant.qtr_micro.models import InstrumentRules, PreflightResult
from market_signal_assistant.qtr_micro.settings import QtrMicroSettings
from market_signal_assistant.qtr_micro.state import empty_state
from market_signal_assistant.qtr_setup_pilot.models import QtrSetupCandidate
from market_signal_assistant.qtr_setup_pilot.notifications import (
    QtrNotificationReason,
    QtrSetupDecision,
    QtrSetupEvent,
    QtrSetupNotificationPlan,
)
from market_signal_assistant.setup_engine import (
    SetupAnalysisInput,
    SetupDirection,
    analyze_setup,
)

NOW = datetime(2026, 9, 26, 10, 0, tzinfo=UTC)
RULES = InstrumentRules("BTCUSDT", 0.001, 0.001, 1000, 5, 100)


def _settings(**changes: Any) -> QtrMicroSettings:
    baseline = QtrMicroSettings(
        enabled=True,
        api_key="must-not-appear",
        api_secret="must-not-appear-either",
    )
    return replace(baseline, **changes)


def _candidate(
    *, episode: str = "episode-1", distance: float = 0.35
) -> QtrSetupCandidate:
    source = SetupAnalysisInput(
        snapshot_ids=(f"scan-{episode}",),
        source="early_discovery_v2",
        symbol="BTCUSDT",
        analyzed_at=NOW,
        direction=SetupDirection.UP,
        current_price=100.8 + distance,
        trigger_level=100.8,
        invalidation_level=99.0,
        price_change_24h_pct=2.0,
        distance_to_trigger_pct=distance,
        distance_to_trigger_atr=distance,
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
    result = analyze_setup(source)
    return QtrSetupCandidate(episode, source, result, atr_value=1.0)


def _snapshot(
    *,
    episode: str = "episode-1",
    distance: float = 0.35,
    reason: QtrNotificationReason = QtrNotificationReason.DUPLICATE,
    should_notify: bool = False,
) -> QualifiedScannerSnapshot:
    candidate = _candidate(episode=episode, distance=distance)
    result = candidate.result
    event = QtrSetupEvent(
        candidate=candidate,
        direction=result.direction,
        setup_type=result.setup_type,
        state=result.setup_state,
        trade_eligibility=result.trade_eligibility,
        current_failure=result.current_breakout_failure,
        retest_held=result.retest_held,
        visible_confirmations=result.reasons,
        warnings=result.warnings,
        quality_components={},
        quality_score=95.0,
        semantic_fingerprint="fingerprint",
    )
    return QualifiedScannerSnapshot(
        QtrSetupDecision(event, candidate, should_notify, reason), NOW
    )


def _context() -> FrozenRuntimeContext:
    return FrozenRuntimeContext.capture(
        now=NOW,
        equity=10_000.0,
        rules=RULES,
        state=empty_state(today=NOW.date(), trading_enabled=True, equity=10_000),
        preflight=PreflightResult(True, None, equity=10_000),
    )


class PriceReaderSpy:
    def __init__(self, price: float | Exception) -> None:
        self.price = price
        self.calls: list[str] = []
        self.order_calls = 0

    def current_market_price(self, symbol: str) -> float:
        self.calls.append(symbol)
        if isinstance(self.price, Exception):
            raise self.price
        return self.price

    def submit_order(self) -> None:
        self.order_calls += 1
        raise AssertionError("Order boundary must never be called.")


class Clock:
    def __init__(self) -> None:
        self.value = NOW

    def __call__(self) -> datetime:
        result = self.value
        self.value += timedelta(milliseconds=1)
        return result


def _observe(
    path: Path,
    *,
    snapshot: QualifiedScannerSnapshot | None = None,
    price: float | Exception = 101.15,
) -> tuple[dict[str, Any], PriceReaderSpy]:
    reader = PriceReaderSpy(price)
    observer = QtrMicroDistanceShadowObserver(
        production_settings=_settings(),
        price_reader=reader,
        evidence=JsonlDistanceShadowEvidence(path),
        clock=Clock(),
    )
    return observer.observe(snapshot or _snapshot(), _context()), reader


def _branch(record: dict[str, Any], threshold: float) -> dict[str, Any]:
    return next(
        branch
        for branch in record["branches"]
        if branch["threshold_atr"] == threshold
    )


def test_same_candidate_context_quote_and_only_threshold_changes(
    tmp_path: Path,
) -> None:
    record, reader = _observe(tmp_path / "evidence.jsonl")

    assert reader.calls == ["BTCUSDT"]
    assert reader.order_calls == 0
    assert record["schema_version"] == 2
    assert record["direction"] == "UP"
    assert record["setup_type"]
    assert record["control_025"] == record["branches"][0]
    assert record["shadow_040"] == record["branches"][1]
    assert record["shadow_060"] == record["branches"][2]
    assert [branch["threshold_atr"] for branch in record["branches"]] == list(
        DISTANCE_THRESHOLDS
    )
    assert {branch["candidate_id"] for branch in record["branches"]} == {
        record["candidate_id"]
    }
    assert {branch["context_digest"] for branch in record["branches"]} == {
        record["context_digest"]
    }
    assert {branch["fresh_price"] for branch in record["branches"]} == {101.15}
    assert {branch["quote_id"] for branch in record["branches"]} == {
        record["quote_id"]
    }
    assert _branch(record, 0.25)["settings_override_fields"] == []
    assert _branch(record, 0.40)["settings_override_fields"] == [
        "max_entry_distance_atr"
    ]
    assert _branch(record, 0.60)["settings_override_fields"] == [
        "max_entry_distance_atr"
    ]
    assert all(branch["correct_side_of_level"] for branch in record["branches"])
    freshness_snapshots = {
        json.dumps(branch["freshness"], sort_keys=True)
        for branch in record["branches"]
    }
    assert len(freshness_snapshots) == 1


def test_control_matches_production_engine_and_shadows_only_expand_distance(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot(distance=0.35)
    context = _context()
    record, _ = _observe(tmp_path / "evidence.jsonl", snapshot=snapshot)
    direct = QtrMicroEntryEngine(_settings()).prepare_entry(
        snapshot.decision.candidate,
        now=context.now,
        equity=context.equity,
        rules=context.rules,
        state=context.state,
        preflight=context.preflight,
    )

    control = _branch(record, CONTROL_THRESHOLD)
    assert control["initial"]["accepted"] == direct.accepted is False
    assert direct.skip_reason is not None
    assert control["initial"]["reason"] == direct.skip_reason.value == "too_far"
    assert control["final_status"] == ShadowStatus.REJECT
    accepted = [_branch(record, threshold) for threshold in (0.40, 0.60)]
    assert all(branch["initial"]["accepted"] for branch in accepted)
    assert all(branch["revalidation"]["accepted"] for branch in accepted)
    assert all(
        branch["final_status"] == ShadowStatus.PRE_SUBMIT_ELIGIBLE
        for branch in accepted
    )
    assert accepted[0]["plan_metadata"] == accepted[1]["plan_metadata"]


def test_risk_sizing_leverage_and_safety_are_identical_when_all_accept(
    tmp_path: Path,
) -> None:
    record, _ = _observe(
        tmp_path / "evidence.jsonl",
        snapshot=_snapshot(distance=0.20),
        price=101.0,
    )
    metadata = [branch["plan_metadata"] for branch in record["branches"]]
    assert metadata[0] == metadata[1] == metadata[2]
    assert all(branch["initial"]["accepted"] for branch in record["branches"])
    assert all(branch["revalidation"]["accepted"] for branch in record["branches"])


def test_missing_fresh_price_is_evidence_missing_without_retry(tmp_path: Path) -> None:
    record, reader = _observe(
        tmp_path / "evidence.jsonl", price=TimeoutError("secret must not leak")
    )

    assert reader.calls == ["BTCUSDT"]
    assert record["fresh_price"] is None
    assert record["fresh_price_error"] == "TimeoutError"
    assert all(
        branch["final_status"] == ShadowStatus.EVIDENCE_MISSING
        for branch in record["branches"]
    )
    assert all(not branch["revalidation_executed"] for branch in record["branches"])
    assert "secret must not leak" not in (tmp_path / "evidence.jsonl").read_text()


def test_append_only_all_observations_and_no_execution_identity(tmp_path: Path) -> None:
    path = tmp_path / "evidence.jsonl"
    first, _ = _observe(path, snapshot=_snapshot(episode="same"))
    first_bytes = path.read_bytes()
    second, _ = _observe(path, snapshot=_snapshot(episode="same", distance=0.20))
    lines = path.read_text(encoding="utf-8").splitlines()

    assert len(lines) == 2
    assert path.read_bytes().startswith(first_bytes)
    assert json.loads(lines[0]) == first
    assert json.loads(lines[1]) == second
    assert "trade_id" not in path.read_text(encoding="utf-8")
    assert "order_link_id" not in path.read_text(encoding="utf-8")
    assert "must-not-appear" not in path.read_text(encoding="utf-8")


def test_observer_never_writes_factual_micro_state(tmp_path: Path) -> None:
    factual_state = tmp_path / "qtr_micro_state.json"
    factual_state.write_text('{"sentinel":"unchanged"}', encoding="utf-8")
    before = factual_state.read_bytes()

    _observe(tmp_path / "shadow.jsonl")

    assert factual_state.read_bytes() == before


def test_episode_aggregation_caps_success_but_keeps_snapshot_statistics(
    tmp_path: Path,
) -> None:
    path = tmp_path / "evidence.jsonl"
    _observe(path, snapshot=_snapshot(episode="episode-a", distance=0.20), price=101.0)
    _observe(path, snapshot=_snapshot(episode="episode-a", distance=0.20), price=101.0)
    _observe(path, snapshot=_snapshot(episode="episode-b", distance=0.35))

    report = summarize_distance_shadow(
        path,
        minimum_conclusive_snapshots=3,
        minimum_conclusive_episodes=2,
    )
    control = report["thresholds"][0]
    shadow = report["thresholds"][1]
    assert control["eligible_unique_episodes"] == 1
    assert control["total_unique_episodes"] == 2
    assert control["eligible_snapshots"] == 2
    assert control["total_snapshots"] == 3
    assert shadow["eligible_unique_episodes"] == 2
    assert shadow["delta_eligible_episodes_vs_control"] == 1
    assert shadow["delta_episode_rate_pp_vs_control"] == 50.0
    assert report["category"] == ExperimentCategory.NO_MATERIAL_FUNNEL_CHANGE
    assert report["sample_gate"] == {
        "minimum_valid_snapshots": 3,
        "minimum_unique_episodes": 2,
        "valid_snapshots_met": True,
        "unique_episodes_met": True,
        "invalid_observations_must_equal": 0,
        "invalid_observations_requirement_met": True,
    }


def test_verdict_threshold_requires_both_approved_deltas() -> None:
    assert _meets_review_threshold(9.99, 5) is False
    assert _meets_review_threshold(10.0, 4) is False
    assert _meets_review_threshold(10.0, 5) is True
    assert _meets_review_threshold(9.999999999999998, 5) is True


def test_sample_gate_requires_snapshots_episodes_and_zero_invalid_observations(
    tmp_path: Path,
) -> None:
    path = tmp_path / "evidence.jsonl"
    _observe(path, snapshot=_snapshot(episode="episode-a", distance=0.20))
    _observe(path, snapshot=_snapshot(episode="episode-a", distance=0.20))
    _observe(path, snapshot=_snapshot(episode="episode-b", distance=0.35))

    snapshots_short = summarize_distance_shadow(
        path,
        minimum_conclusive_snapshots=4,
        minimum_conclusive_episodes=2,
    )
    assert snapshots_short["category"] == ExperimentCategory.INCONCLUSIVE_SAMPLE
    assert snapshots_short["sample_gate"]["valid_snapshots_met"] is False
    assert snapshots_short["sample_gate"]["unique_episodes_met"] is True

    episodes_short = summarize_distance_shadow(
        path,
        minimum_conclusive_snapshots=3,
        minimum_conclusive_episodes=3,
    )
    assert episodes_short["category"] == ExperimentCategory.INCONCLUSIVE_SAMPLE
    assert episodes_short["sample_gate"]["valid_snapshots_met"] is True
    assert episodes_short["sample_gate"]["unique_episodes_met"] is False

    invalid = summarize_distance_shadow(
        path,
        minimum_conclusive_snapshots=3,
        minimum_conclusive_episodes=2,
        invalid_observations=1,
    )
    assert invalid["category"] == ExperimentCategory.EXPERIMENT_INVALID
    assert invalid["invalid_observations"] == 1
    assert (
        invalid["sample_gate"]["invalid_observations_requirement_met"] is False
    )


def test_inconclusive_and_invalid_report_categories(tmp_path: Path) -> None:
    path = tmp_path / "evidence.jsonl"
    _observe(path)
    report = summarize_distance_shadow(path)
    assert report["category"] == ExperimentCategory.INCONCLUSIVE_SAMPLE
    assert report["sample_gate"]["minimum_valid_snapshots"] == 100
    assert report["sample_gate"]["minimum_unique_episodes"] == 30
    with path.open("a", encoding="utf-8") as stream:
        stream.write("not-json\n")
    assert summarize_distance_shadow(path)["category"] == (
        ExperimentCategory.EXPERIMENT_INVALID
    )


def test_unqualified_snapshot_and_non_control_base_are_rejected(tmp_path: Path) -> None:
    candidate = _candidate()
    decision = QtrSetupDecision(
        event=None,
        candidate=candidate,
        should_notify=False,
        reason=QtrNotificationReason.QUALITY_BELOW_THRESHOLD,
    )
    with pytest.raises(ValueError, match="not a qualified"):
        QualifiedScannerSnapshot(decision, NOW)
    with pytest.raises(ValueError, match="exactly 0.25"):
        QtrMicroDistanceShadowObserver(
            production_settings=_settings(max_entry_distance_atr=0.40),
            price_reader=PriceReaderSpy(101.0),
            evidence=JsonlDistanceShadowEvidence(tmp_path / "evidence.jsonl"),
        )


def test_dispatcher_joins_exact_cycle_context_after_production(
    tmp_path: Path,
) -> None:
    path = tmp_path / "evidence.jsonl"
    reader = PriceReaderSpy(101.15)
    clock = Clock()
    clock.value = NOW + timedelta(seconds=2)
    observer = QtrMicroDistanceShadowObserver(
        production_settings=_settings(),
        price_reader=reader,
        evidence=JsonlDistanceShadowEvidence(path),
        clock=clock,
    )
    dispatcher = DistanceShadowDispatcher(observer, clock=clock)
    snapshot = _snapshot(should_notify=False)
    plan = QtrSetupNotificationPlan(
        NOW, (snapshot.decision,), {}
    )

    async def exercise() -> str:
        cycle_id = dispatcher.enqueue(plan, NOW + timedelta(seconds=1))
        assert cycle_id is not None
        dispatcher.capture_context(
            cycle_id,
            snapshot.decision.candidate,
            _context(),
            NOW + timedelta(seconds=2),
        )
        assert not path.exists()
        assert reader.calls == []
        clock.value = NOW + timedelta(seconds=5)
        dispatcher.production_completed(
            cycle_id, NOW + timedelta(seconds=3)
        )
        await dispatcher.wait()
        return cycle_id

    cycle_id = asyncio.run(exercise())
    record = json.loads(path.read_text(encoding="utf-8"))
    assert record["production_cycle_id"] == cycle_id
    assert record["context_captured_at"] == (
        NOW + timedelta(seconds=2)
    ).isoformat()
    assert record["qualified_at"] == (NOW + timedelta(seconds=1)).isoformat()
    assert record["production_handler_completed_at"] == (
        NOW + timedelta(seconds=3)
    ).isoformat()
    assert record["quote_requested_at"] is not None
    assert record["quote_received_at"] is not None
    assert record["queue_delay_ms"] >= 0
    assert record["post_production_delay_ms"] >= 0
    assert record["qualification_to_quote_ms"] >= 0
    assert record["quote_latency_ms"] >= 0
    assert reader.calls == ["BTCUSDT"]


def test_dispatcher_missing_matching_context_never_uses_quote(
    tmp_path: Path,
) -> None:
    path = tmp_path / "evidence.jsonl"
    reader = PriceReaderSpy(101.15)
    observer = QtrMicroDistanceShadowObserver(
        production_settings=_settings(),
        price_reader=reader,
        evidence=JsonlDistanceShadowEvidence(path),
        clock=Clock(),
    )
    dispatcher = DistanceShadowDispatcher(observer, clock=Clock())
    snapshot = _snapshot()
    plan = QtrSetupNotificationPlan(NOW, (snapshot.decision,), {})

    async def exercise() -> None:
        cycle_id = dispatcher.enqueue(plan, NOW)
        assert cycle_id is not None
        dispatcher.production_completed(cycle_id, NOW)
        await dispatcher.wait()

    asyncio.run(exercise())
    record = json.loads(path.read_text(encoding="utf-8"))
    assert record["fresh_price_error"] == "MATCHING_CONTEXT_MISSING"
    assert record["context_captured_at"] is None
    assert record["quote_requested_at"] is None
    assert all(
        branch["final_status"] == ShadowStatus.EVIDENCE_MISSING
        for branch in record["branches"]
    )
    assert reader.calls == []


def test_dispatcher_journal_failure_and_queue_overflow_do_not_propagate(
    tmp_path: Path,
) -> None:
    class FailedEvidence(JsonlDistanceShadowEvidence):
        def append(self, record: Any) -> None:
            del record
            raise OSError("disk full")

    observer = QtrMicroDistanceShadowObserver(
        production_settings=_settings(),
        price_reader=PriceReaderSpy(101.15),
        evidence=FailedEvidence(tmp_path / "failed.jsonl"),
        clock=Clock(),
    )
    dispatcher = DistanceShadowDispatcher(observer, capacity=1, clock=Clock())
    first = _snapshot(episode="first")
    plan = QtrSetupNotificationPlan(NOW, (first.decision,), {})

    async def failed_write() -> None:
        cycle_id = dispatcher.enqueue(plan, NOW)
        assert cycle_id is not None
        dispatcher.capture_context(
            cycle_id, first.decision.candidate, _context(), NOW
        )
        dispatcher.production_completed(cycle_id, NOW)
        await dispatcher.wait()

    asyncio.run(failed_write())
    assert dispatcher.invalid_observations == 1

    overflow = DistanceShadowDispatcher(observer, capacity=1, clock=Clock())
    second = _snapshot(episode="second")
    two = QtrSetupNotificationPlan(
        NOW, (first.decision, second.decision), {}
    )
    assert overflow.enqueue(two, NOW) is None
    assert overflow.invalid_observations == 2


def test_journal_restart_boundary_and_temporal_report(tmp_path: Path) -> None:
    path = tmp_path / "evidence.jsonl"
    path.write_text('{"partial":true}', encoding="utf-8")
    _observe(path)
    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines[0] == '{"partial":true}'
    assert len(lines) == 2

    report = summarize_distance_shadow(path)
    assert report["invalid_records"] == 1
    request = report["temporal_evidence"][
        "qualification_to_quote_request_ms"
    ]
    received = report["temporal_evidence"][
        "qualification_to_quote_received_ms"
    ]
    assert request["count"] == 1
    assert received["count"] == 1
    assert request["p50"] == request["p95"] == request["max"]
    assert received["p50"] == received["p95"] == received["max"]
