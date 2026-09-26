from __future__ import annotations

import json
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
from watchdog_evidence_helpers import NOW, evidence

from market_signal_assistant.watchdog.events.journal import WatchdogEventJournal
from market_signal_assistant.watchdog.evidence_baseline import (
    DatasetProvenance,
    analyze_dataset,
    render_report,
    render_rows_jsonl,
)
from market_signal_assistant.watchdog.models import AnomalyType
from market_signal_assistant.watchdog.outcomes.journal import WatchdogOutcomeJournal
from market_signal_assistant.watchdog.outcomes.models import (
    ForwardOutcome,
    OutcomeDataQuality,
    PriceObservation,
    outcome_id,
)
from market_signal_assistant.watchdog.outcomes.price_journal import (
    WatchdogPriceJournal,
)


def _manifest(root: Path, *, dataset_id: str = "fixture") -> DatasetProvenance:
    manifest = root.parent / f"{dataset_id}.json"
    manifest.write_text(
        json.dumps(
            {
                "dataset_id": dataset_id,
                "path": str(root),
                "known_commit_sha": "a" * 40,
                "duration_seconds": 3600,
                "compatibility": "PARTIALLY_COMPATIBLE",
                "limitations": ["synthetic test fixture"],
                "analysis_as_of": (NOW + timedelta(minutes=35)).isoformat(),
            }
        ),
        encoding="utf-8",
    )
    return DatasetProvenance.from_manifest(manifest)


def _outcome(
    event_id: str,
    horizon: int,
    minute: int,
    price: float,
    quality: OutcomeDataQuality,
) -> ForwardOutcome:
    missing = quality is OutcomeDataQuality.MISSING
    target = NOW + timedelta(minutes=horizon)
    observed = NOW + timedelta(minutes=minute)
    return ForwardOutcome(
        outcome_id=outcome_id(event_id, horizon),
        event_id=event_id,
        symbol="ABCUSDT",
        horizon_minutes=horizon,
        target_time=target,
        observed_at=observed,
        available_at=observed,
        reference_price=100.0,
        horizon_price=None if missing else price,
        signed_return=None if missing else price / 100 - 1,
        abs_return=None if missing else abs(price / 100 - 1),
        mfe_up=None if missing else max(0.0, price / 100 - 1),
        mfe_down=None if missing else max(0.0, 1 - price / 100),
        max_abs_excursion=None if missing else abs(price / 100 - 1),
        realized_range_after_event=None if missing else abs(price / 100 - 1),
        time_to_mfe_up_seconds=None if missing else 0.0,
        time_to_mfe_down_seconds=None if missing else 0.0,
        lateness_seconds=(observed - target).total_seconds(),
        data_quality=quality,
    )


def _dataset(root: Path) -> tuple[DatasetProvenance, str]:
    event = evidence(anomaly_types=(AnomalyType.COMPRESSION, AnomalyType.RANGE_BUILDUP))
    WatchdogEventJournal(root / "events" / "events.jsonl").append(event)
    journal = WatchdogOutcomeJournal(root / "outcomes" / "outcomes.jsonl")
    journal.append(_outcome(event.event_id, 1, 5, 101, OutcomeDataQuality.ON_TIME))
    journal.append(_outcome(event.event_id, 5, 7, 99, OutcomeDataQuality.LATE))
    journal.append(_outcome(event.event_id, 15, 15, 0, OutcomeDataQuality.MISSING))
    prices = WatchdogPriceJournal(root / "outcomes" / "price_observations.jsonl")
    prices.append(
        PriceObservation(
            "ABCUSDT",
            NOW + timedelta(minutes=5),
            NOW + timedelta(minutes=5),
            101,
            source="bybit-public-5m",
        )
    )
    prices.append(
        PriceObservation(
            "ABCUSDT",
            NOW + timedelta(minutes=7),
            NOW + timedelta(minutes=7),
            99,
            source="bybit-public-1m",
        )
    )
    return _manifest(root), event.event_id


def test_read_only_deterministic_coverage_and_multilabel(tmp_path: Path) -> None:
    root = tmp_path / "dataset"
    provenance, event_id = _dataset(root)
    source_paths = sorted(root.rglob("*"))
    before = {
        path: (path.stat().st_size, path.stat().st_mtime_ns, path.read_bytes())
        for path in source_paths
        if path.is_file()
    }
    first = analyze_dataset(provenance, verify_sqlite=True)
    second = analyze_dataset(provenance, verify_sqlite=True)
    assert first.valid, first.issues
    assert render_report(first) == render_report(second)
    assert len(first.events) == 1
    assert {row.status for row in first.rows} == {
        "ON_TIME",
        "LATE",
        "MISSING",
        "PENDING_DUE_UNRESOLVED",
        "PENDING_NOT_DUE",
    }
    assert [row.observation.source for row in first.rows if row.observation] == [
        "bybit-public-5m",
        "bybit-public-1m",
    ]
    assert first.rows[0].actual_lag_seconds == 240
    report = render_report(first)
    assert "COMPRESSION" in report and "RANGE_BUILDUP" in report
    assert "COMPRESSION+RANGE_BUILDUP" in report
    assert report.count(f"{event_id}") == 0  # aggregate rows, no raw event listing
    assert "PENDING_DUE_UNRESOLVED" not in report  # human-readable column name
    assert "pending due unresolved" in report
    rows = [json.loads(line) for line in render_rows_jsonl(first).splitlines()]
    assert len(rows) == 5
    assert rows[0]["target_time"] == (NOW + timedelta(minutes=1)).isoformat()
    assert rows[0]["observed_at"] == (NOW + timedelta(minutes=5)).isoformat()
    assert rows[0]["raw_source"] == "bybit-public-5m"
    assert rows[0]["source_interval_seconds"] == 300
    assert rows[2]["status"] == "MISSING"
    assert rows[2]["observed_at"] is None
    assert rows[2]["missing_recorded_at"] is not None
    assert rows[3]["status"] == "PENDING_DUE_UNRESOLVED"
    assert rows[4]["status"] == "PENDING_NOT_DUE"
    after = {
        path: (path.stat().st_size, path.stat().st_mtime_ns, path.read_bytes())
        for path in source_paths
        if path.is_file()
    }
    assert before == after


def test_duplicate_conflict_and_corruption_are_not_repaired(tmp_path: Path) -> None:
    root = tmp_path / "dataset"
    provenance, _ = _dataset(root)
    path = root / "events" / "events.jsonl"
    original = path.read_bytes().splitlines()[0]
    with path.open("ab") as stream:
        stream.write(original + b"\n")
        changed = json.loads(original)
        changed["anomaly_score"] = 20
        stream.write(json.dumps(changed).encode() + b"\n")
        stream.write(b"{broken\n")
    result = analyze_dataset(provenance)
    assert not result.valid
    assert any("duplicate ID" in issue for issue in result.issues)
    assert any("conflicting payload" in issue for issue in result.issues)
    assert any("corrupt record" in issue for issue in result.issues)
    assert "No descriptive statistics" in render_report(result)


def test_orphan_target_pit_and_raw_join_fail_closed(tmp_path: Path) -> None:
    root = tmp_path / "dataset"
    provenance, event_id = _dataset(root)
    outcomes = root / "outcomes" / "outcomes.jsonl"
    records = [json.loads(line) for line in outcomes.read_text().splitlines()]
    records[0]["target_time"] = (NOW + timedelta(minutes=2)).isoformat()
    records[1]["event_id"] = "unknown"
    outcomes.write_text("".join(json.dumps(item) + "\n" for item in records))
    prices = root / "outcomes" / "price_observations.jsonl"
    extra = PriceObservation(
        "ABCUSDT",
        NOW + timedelta(minutes=5),
        NOW + timedelta(minutes=5),
        101,
        high=102,
        source="another-5m",
    )
    # Same outcome join key but distinct immutable observation ID.
    with prices.open("a", encoding="utf-8") as stream:
        stream.write(
            json.dumps(
                {
                    "observation_id": extra.observation_id,
                    "symbol": extra.symbol,
                    "observed_at": extra.observed_at.isoformat(),
                    "available_at": extra.available_at.isoformat(),
                    "price": extra.price,
                    "high": extra.high,
                    "low": extra.low,
                    "source": extra.source,
                }
            )
            + "\n"
        )
    result = analyze_dataset(provenance)
    assert not result.valid
    assert any("orphan outcome" in item for item in result.issues)
    assert any("outcome target mismatch" in item for item in result.issues)
    assert any("ambiguous raw observation join" in item for item in result.issues)
    assert event_id in result.events


def test_event_pit_and_observation_reuse_are_detected(tmp_path: Path) -> None:
    root = tmp_path / "dataset"
    provenance, _ = _dataset(root)
    events = root / "events" / "events.jsonl"
    payload = json.loads(events.read_text().splitlines()[0])
    payload["features"][0]["available_at"] = (NOW + timedelta(minutes=1)).isoformat()
    events.write_text(json.dumps(payload) + "\n")
    pit_result = analyze_dataset(provenance)
    assert not pit_result.valid
    assert any("future features" in item for item in pit_result.issues)
    payload["features"][0]["available_at"] = NOW.isoformat()
    events.write_text(json.dumps(payload) + "\n")
    outcomes = root / "outcomes" / "outcomes.jsonl"
    records = [json.loads(line) for line in outcomes.read_text().splitlines()]
    records[1]["observed_at"] = records[0]["observed_at"]
    outcomes.write_text("".join(json.dumps(item) + "\n" for item in records))
    result = analyze_dataset(provenance)
    assert not result.valid
    assert any("observation reused" in item for item in result.issues)


def test_timing_class_and_cutoff_are_validated(tmp_path: Path) -> None:
    root = tmp_path / "dataset"
    provenance, _ = _dataset(root)
    outcomes = root / "outcomes" / "outcomes.jsonl"
    records = [json.loads(line) for line in outcomes.read_text().splitlines()]
    records[0]["data_quality"] = "LATE"
    outcomes.write_text("".join(json.dumps(item) + "\n" for item in records))
    result = analyze_dataset(provenance)
    assert any("timing class mismatch" in item for item in result.issues)
    too_early = replace(provenance, analysis_as_of=NOW)
    result = analyze_dataset(too_early)
    assert any("after frozen analysis cutoff" in item for item in result.issues)


def test_duplicate_outcome_and_outcome_chronology_are_detected(
    tmp_path: Path,
) -> None:
    root = tmp_path / "dataset"
    provenance, _ = _dataset(root)
    outcomes = root / "outcomes" / "outcomes.jsonl"
    records = [json.loads(line) for line in outcomes.read_text().splitlines()]
    records[1]["observed_at"] = (NOW + timedelta(minutes=4)).isoformat()
    outcomes.write_text(
        "".join(json.dumps(item) + "\n" for item in records)
        + json.dumps(records[0])
        + "\n"
    )
    result = analyze_dataset(provenance)
    assert not result.valid
    assert any("duplicate ID" in item for item in result.issues)
    assert any("Outcome chronology is invalid" in item for item in result.issues)


def test_cross_dataset_identity_and_incompatible_rejection(tmp_path: Path) -> None:
    first, event_id = _dataset(tmp_path / "a")
    second, _ = _dataset(tmp_path / "b")
    assert first.dataset_id == second.dataset_id == "fixture"
    distinct = replace(second, dataset_id="other")
    a = analyze_dataset(first)
    b = analyze_dataset(distinct)
    assert a.valid and b.valid
    assert (a.rows[0].dataset_id, event_id) != (b.rows[0].dataset_id, event_id)
    with pytest.raises(ValueError, match="INCOMPATIBLE"):
        analyze_dataset(replace(first, compatibility="INCOMPATIBLE"))


def test_manifest_rejects_incompatible_and_requires_frozen_cutoff(
    tmp_path: Path,
) -> None:
    root = tmp_path / "dataset"
    _dataset(root)
    manifest = tmp_path / "fixture.json"
    payload = json.loads(manifest.read_text())
    payload["compatibility"] = "INCOMPATIBLE"
    manifest.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="INCOMPATIBLE"):
        DatasetProvenance.from_manifest(manifest)
    payload["compatibility"] = "PARTIALLY_COMPATIBLE"
    payload.pop("analysis_as_of")
    manifest.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="analysis_as_of"):
        DatasetProvenance.from_manifest(manifest)
