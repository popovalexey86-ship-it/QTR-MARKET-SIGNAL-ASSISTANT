from __future__ import annotations

import csv
import json
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest

from market_signal_assistant.setup_engine.analyzer import analyze_setup
from market_signal_assistant.setup_engine.models import (
    SetupDirection,
    SetupState,
    SetupType,
)
from market_signal_assistant.setup_engine.offline_analyzer import (
    AnalyzerConfig,
    Episode,
    ReplaySnapshot,
    _calculate_metrics,
    _reason_categories,
    _safety_violations,
    analyze_audit,
    build_episodes,
    outcome_for_snapshot,
    read_v2_audit,
    write_outputs,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_AUDIT = (
    ROOT / "tests" / "fixtures" / "early_discovery_v2_recovered_retest.jsonl"
)


@pytest.fixture(scope="module")
def replayed() -> tuple[ReplaySnapshot, ...]:
    return read_v2_audit(FIXTURE_AUDIT).snapshots


def _snapshot(
    base: ReplaySnapshot,
    *,
    minutes: int,
    direction: SetupDirection = SetupDirection.UP,
    setup_type: SetupType = SetupType.BREAKOUT,
    state: SetupState = SetupState.WATCHING,
    price: float = 100.0,
    line: int | None = None,
    retest_detected: bool = False,
    retest_held: bool = False,
    failed: bool = False,
    spread: float | None = 0.05,
    distance_atr: float | None = 0.5,
    change_24h: float | None = 1.0,
    missing: tuple[str, ...] = (),
) -> ReplaySnapshot:
    at = base.at + timedelta(minutes=minutes)
    source = replace(
        base.source,
        scan_id=f"test-{minutes}-{line or minutes}",
        scanned_at=at,
        symbol="TESTUSDT",
        current_price=price,
        spread_pct=spread,
        absolute_distance_atr=distance_atr,
        price_change_24h_pct=change_24h,
        breakout_failure=failed,
        technical_error=("test error" if "technical" in missing else None),
    )
    setup_input = replace(
        base.setup_input,
        snapshot_ids=(source.scan_id,),
        symbol="TESTUSDT",
        analyzed_at=at,
        direction=direction,
        current_price=price,
        distance_to_trigger_atr=distance_atr,
        spread_pct=spread,
    )
    result = replace(
        base.result,
        symbol="TESTUSDT",
        analyzed_at=at,
        direction=direction,
        setup_type=setup_type,
        setup_state=state,
        current_price=price,
        distance_to_trigger_atr=distance_atr,
        retest_detected=retest_detected,
        retest_held=retest_held,
        breakout_failed=failed,
        spread_ok=spread is not None and spread <= 0.2,
        missing_data=missing,
    )
    return ReplaySnapshot(line or minutes + 1, source, setup_input, result)


def test_reads_fixture_jsonl_and_replays_production_engine(
    replayed: tuple[ReplaySnapshot, ...],
) -> None:
    assert replayed
    assert all(analyze_setup(item.setup_input) == item.result for item in replayed)


def test_corrupted_row_is_rejected_without_losing_valid_row(tmp_path: Path) -> None:
    first = FIXTURE_AUDIT.read_text(encoding="utf-8").splitlines()[0]
    audit = tmp_path / "audit.jsonl"
    audit.write_text(first + "\n{bad json\n", encoding="utf-8")
    result = read_v2_audit(audit)
    assert len(result.snapshots) == 1
    assert result.rejected[0].line_number == 2


def test_reader_is_offline_and_does_not_import_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import socket

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("network access is forbidden")

    monkeypatch.setattr(socket, "create_connection", forbidden)
    assert read_v2_audit(FIXTURE_AUDIT).snapshots


@pytest.mark.parametrize(
    ("second_changes", "expected"),
    [
        ({"minutes": 30}, 2),
        ({"minutes": 1, "direction": SetupDirection.DOWN}, 2),
        ({"minutes": 1, "setup_type": SetupType.RETEST}, 2),
    ],
)
def test_episode_split_on_gap_direction_or_type(
    replayed: tuple[ReplaySnapshot, ...],
    second_changes: dict[str, object],
    expected: int,
) -> None:
    first = _snapshot(replayed[0], minutes=0)
    second = _snapshot(replayed[0], **second_changes)  # type: ignore[arg-type]
    assert len(build_episodes((first, second), AnalyzerConfig())) == expected


def test_episode_split_after_two_no_trade_scans(
    replayed: tuple[ReplaySnapshot, ...],
) -> None:
    snapshots = (
        _snapshot(replayed[0], minutes=0, setup_type=SetupType.NO_TRADE),
        _snapshot(replayed[0], minutes=1, setup_type=SetupType.NO_TRADE),
        _snapshot(replayed[0], minutes=2, setup_type=SetupType.NO_TRADE),
    )
    assert [
        len(ep.snapshots) for ep in build_episodes(snapshots, AnalyzerConfig())
    ] == [
        2,
        1,
    ]


def test_episode_split_after_cancelled_structure(
    replayed: tuple[ReplaySnapshot, ...],
) -> None:
    cancelled = _snapshot(replayed[0], minutes=0, state=SetupState.CANCELLED)
    restarted = _snapshot(replayed[0], minutes=1, state=SetupState.FORMING)
    assert len(build_episodes((cancelled, restarted), AnalyzerConfig())) == 2


def test_technical_error_does_not_create_episode_or_reset_short_gap(
    replayed: tuple[ReplaySnapshot, ...],
) -> None:
    first = _snapshot(replayed[0], minutes=0, state=SetupState.FORMING)
    error = _snapshot(
        replayed[0],
        minutes=5,
        direction=SetupDirection.NEUTRAL,
        setup_type=SetupType.FALSE_BREAKOUT,
        state=SetupState.CANCELLED,
        missing=("technical",),
    )
    recovered = _snapshot(replayed[0], minutes=10, state=SetupState.CONFIRMING)
    episodes = build_episodes((first, error, recovered), AnalyzerConfig())
    assert len(episodes) == 1
    assert [item.line_number for item in episodes[0].snapshots] == [
        first.line_number,
        recovered.line_number,
    ]


@pytest.mark.parametrize(
    ("direction", "future_price", "expected"),
    [
        (SetupDirection.UP, 101.0, 1.0),
        (SetupDirection.DOWN, 99.0, 1.0),
    ],
)
def test_directional_return_up_and_down(
    replayed: tuple[ReplaySnapshot, ...],
    direction: SetupDirection,
    future_price: float,
    expected: float,
) -> None:
    anchor = _snapshot(replayed[0], minutes=0, direction=direction, price=100)
    future = _snapshot(replayed[0], minutes=15, direction=direction, price=future_price)
    outcome = outcome_for_snapshot(anchor, (anchor, future), 15, 0)
    assert outcome is not None
    assert outcome.directed_return_pct == pytest.approx(expected)


def test_horizon_requires_future_and_never_uses_past(
    replayed: tuple[ReplaySnapshot, ...],
) -> None:
    past = _snapshot(replayed[0], minutes=0, price=90)
    anchor = _snapshot(replayed[0], minutes=10, price=100)
    too_early = _snapshot(replayed[0], minutes=20, price=110)
    assert outcome_for_snapshot(anchor, (past, anchor, too_early), 15, 0) is None


def test_no_trade_reason_classification(
    replayed: tuple[ReplaySnapshot, ...],
) -> None:
    snapshot = _snapshot(
        replayed[0],
        minutes=0,
        direction=SetupDirection.NEUTRAL,
        setup_type=SetupType.NO_TRADE,
        spread=0.3,
        missing=("missing candle",),
    )
    assert set(_reason_categories(snapshot)) >= {
        "нейтральное направление",
        "неполные данные",
        "широкий спред",
    }


def test_safety_violation_detector_covers_ready_guards(
    replayed: tuple[ReplaySnapshot, ...],
) -> None:
    snapshot = _snapshot(
        replayed[0],
        minutes=0,
        direction=SetupDirection.NEUTRAL,
        state=SetupState.READY_TO_CONSIDER,
        spread=0.3,
        distance_atr=2.5,
        change_24h=15,
        failed=True,
        missing=("technical",),
    )
    assert len(_safety_violations(snapshot)) == 7


def test_false_breakout_retest_and_confidence_metrics_are_emitted(
    replayed: tuple[ReplaySnapshot, ...],
) -> None:
    ready = _snapshot(
        replayed[0],
        minutes=0,
        setup_type=SetupType.FALSE_BREAKOUT,
        state=SetupState.READY_TO_CONSIDER,
        retest_detected=True,
        retest_held=True,
    )
    future = _snapshot(replayed[0], minutes=60, price=101)
    audit = replace(
        read_v2_audit(FIXTURE_AUDIT),
        snapshots=(ready, future),
        total_lines=2,
        rejected=(),
    )
    episode = Episode(1, [ready])
    observed = outcome_for_snapshot(ready, audit.snapshots, 60, 0)
    assert observed is not None
    metrics = _calculate_metrics(
        audit,
        (episode,),
        AnalyzerConfig(outcome_tolerance_minutes=0),
    )
    assert metrics["false_breakout"]
    assert metrics["retest"]
    assert metrics["confidence_bands"]


def test_outputs_have_russian_headers_bom_and_leave_source_unchanged(
    tmp_path: Path,
) -> None:
    before = FIXTURE_AUDIT.read_bytes()
    analysis = analyze_audit(FIXTURE_AUDIT)
    files = write_outputs(analysis, tmp_path)
    assert files.episodes.read_bytes().startswith(b"\xef\xbb\xbf")
    with files.episodes.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, delimiter=";")
        assert next(reader)[:3] == ["Номер эпизода", "Символ", "Направление"]
    assert json.loads(files.metrics.read_text(encoding="utf-8"))["source"]["unchanged"]
    assert FIXTURE_AUDIT.read_bytes() == before
