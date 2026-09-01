from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest

from market_signal_assistant.setup_engine.analyzer import analyze_setup
from market_signal_assistant.setup_engine.diagnostics import (
    DiagnosticConfig,
    ScanDiagnostic,
    adapter_mapping,
    analyze_diagnostics,
    build_scan_diagnostics,
    candidate_setup_types,
    diagnostic_no_trade_reasons,
    false_breakout_groups,
    parse_runtime_log,
    sources_unchanged,
    symbol_missing_between,
    transition_matrix,
    write_outputs,
)
from market_signal_assistant.setup_engine.models import (
    SetupDirection,
    SetupState,
    SetupType,
)
from market_signal_assistant.setup_engine.offline_analyzer import (
    AnalyzerConfig,
    ReplaySnapshot,
    build_episodes,
    read_v2_audit,
)

ROOT = Path(__file__).resolve().parents[1]
REAL_AUDIT = ROOT / "data" / "inplay_early_discovery_v2_audit.jsonl"


@pytest.fixture(scope="module")
def base() -> ReplaySnapshot:
    return read_v2_audit(REAL_AUDIT).snapshots[0]


def _snapshot(
    base: ReplaySnapshot,
    *,
    minutes: int = 0,
    symbol: str = "TESTUSDT",
    scan_id: str | None = None,
    direction: SetupDirection = SetupDirection.UP,
    setup_type: SetupType = SetupType.BREAKOUT,
    state: SetupState = SetupState.WATCHING,
    price: float = 100.0,
    technical_error: str | None = None,
) -> ReplaySnapshot:
    at = base.at + timedelta(minutes=minutes)
    source = replace(
        base.source,
        scan_id=scan_id or f"scan-{minutes}",
        scanned_at=at,
        symbol=symbol,
        current_price=None if technical_error else price,
        technical_error=technical_error,
    )
    setup_input = replace(
        base.setup_input,
        snapshot_ids=(source.scan_id,),
        symbol=symbol,
        analyzed_at=at,
        direction=direction,
        current_price=None if technical_error else price,
        technical_data_complete=technical_error is None,
        extra_missing_data=("technical",) if technical_error else (),
    )
    result = replace(
        base.result,
        symbol=symbol,
        analyzed_at=at,
        direction=direction,
        setup_type=setup_type,
        setup_state=state,
        current_price=None if technical_error else price,
        missing_data=("technical",) if technical_error else (),
    )
    return ReplaySnapshot(minutes + 1, source, setup_input, result)


def test_false_breakout_and_retest_are_simultaneous_candidates(
    base: ReplaySnapshot,
) -> None:
    data = replace(
        base.setup_input,
        breakout_failed=True,
        current_breakout_failure=True,
        structure_recovered=False,
        correct_side_of_level=False,
        retest_detected=True,
        technical_data_complete=True,
        extra_missing_data=(),
    )
    candidates = candidate_setup_types(data)
    assert candidates[:2] == (SetupType.FALSE_BREAKOUT, SetupType.RETEST)


def test_historical_false_breakout_does_not_override_recovery(
    base: ReplaySnapshot,
) -> None:
    data = replace(
        base.setup_input,
        breakout_failed=False,
        returned_inside_range=True,
        historical_breakout_failure=True,
        current_breakout_failure=False,
        structure_recovered=True,
        correct_side_of_level=True,
        breakout_confirmed=True,
        retest_detected=True,
        retest_held=True,
        technical_data_complete=True,
        extra_missing_data=(),
    )
    assert candidate_setup_types(data)[:2] == (
        SetupType.RETEST,
        SetupType.BREAKOUT,
    )
    assert analyze_setup(data).setup_type is SetupType.RETEST


def test_no_trade_is_reachable(base: ReplaySnapshot) -> None:
    data = replace(
        base.setup_input,
        direction=SetupDirection.NEUTRAL,
        technical_data_complete=True,
        extra_missing_data=(),
    )
    result = analyze_setup(data)
    assert candidate_setup_types(data)
    assert result.trade_eligible is False
    assert "направление не подтверждено" in result.no_trade_reasons


def test_diagnostic_no_trade_can_be_intercepted(base: ReplaySnapshot) -> None:
    snapshot = _snapshot(base)
    data = replace(
        snapshot.setup_input,
        breakout_failed=True,
        current_breakout_failure=True,
        structure_recovered=False,
        correct_side_of_level=False,
    )
    result = analyze_setup(data)
    snapshot = replace(snapshot, setup_input=data, result=result)
    assert "текущий пробой провален" in diagnostic_no_trade_reasons(snapshot)
    assert result.setup_type is SetupType.FALSE_BREAKOUT


def test_forming_to_confirming_transition_and_short_gap_continuity(
    base: ReplaySnapshot,
) -> None:
    forming = _snapshot(base, state=SetupState.FORMING)
    confirming = _snapshot(base, minutes=5, state=SetupState.CONFIRMING)
    counts = transition_matrix((forming, confirming))
    assert counts[(SetupState.FORMING, SetupState.CONFIRMING)] == 1


def test_transition_does_not_cross_episode_gap(base: ReplaySnapshot) -> None:
    forming = _snapshot(base, state=SetupState.FORMING)
    confirming = _snapshot(base, minutes=31, state=SetupState.CONFIRMING)
    assert not transition_matrix((forming, confirming))


def test_episode_continues_across_short_gap(base: ReplaySnapshot) -> None:
    first = _snapshot(base, state=SetupState.CONFIRMING)
    second = _snapshot(base, minutes=5, state=SetupState.CONFIRMING)
    episodes = build_episodes((first, second), AnalyzerConfig(gap_minutes=30))
    assert len(episodes) == 1
    assert len(episodes[0].snapshots) == 2


def test_market_data_error_runtime_log_parsing(tmp_path: Path) -> None:
    path = tmp_path / "runtime.log"
    path.write_text(
        "Сканирование завершилось ошибкой (MarketDataError).\n"
        "Модуль раннего обнаружения V2: сканирование завершено.\n"
        "Инструментов: 5.\n"
        "Подтверждённых наблюдений: 1.\n"
        "Формирующихся ситуаций: 2.\n"
        "Ошибок отдельных инструментов: 3.\n",
        encoding="utf-8",
    )
    stats = parse_runtime_log(path)
    assert stats.market_data_error_messages == 1
    assert stats.completed_scans[0].errors == 3


def test_partial_scan_and_sharp_drop_detection(base: ReplaySnapshot) -> None:
    first = tuple(
        _snapshot(base, symbol=f"S{index}", scan_id="one") for index in range(10)
    )
    second = tuple(
        _snapshot(base, minutes=5, symbol=f"S{index}", scan_id="two")
        for index in range(8)
    )
    scans = build_scan_diagnostics(first + second, DiagnosticConfig())
    assert scans[1].sharp_drop
    assert scans[1].incomplete


def test_missing_symbol_between_scans(base: ReplaySnapshot) -> None:
    previous = ScanDiagnostic(
        1,
        "one",
        base.at,
        (_snapshot(base, symbol="AAA"),),
        1,
        0,
        None,
        None,
        None,
        (),
        (),
        False,
    )
    assert symbol_missing_between(previous, "BBB")
    assert not symbol_missing_between(previous, "AAA")


def test_adapter_mapping_includes_derived_and_fallback_fields() -> None:
    rows = {row["Поле Setup Engine"]: row for row in adapter_mapping()}
    assert "breakout_confirmed" in rows
    assert "structure_confirmation" in rows
    assert rows["conflicting_confirmations"]["Fallback / null"] == "всегда False"
    assert "SetupAnalysisResult.missing_data" in rows


def test_multiple_candidate_setup_types(base: ReplaySnapshot) -> None:
    data = replace(
        base.setup_input,
        breakout_failed=True,
        retest_detected=True,
        reversal_detected=True,
        volume_confirmation=True,
        volatility_confirmation=True,
        technical_data_complete=True,
        extra_missing_data=(),
    )
    assert len(candidate_setup_types(data)) >= 3


def test_false_group_marks_previous_error(base: ReplaySnapshot) -> None:
    current = _snapshot(base)
    current = replace(
        current,
        result=replace(current.result, setup_type=SetupType.FALSE_BREAKOUT),
        source=replace(
            current.source,
            breakout_failure=True,
            is_correct_side_of_level=False,
            signed_distance_atr=-0.2,
        ),
    )
    error = _snapshot(base, technical_error="MarketDataError")
    previous_scan = replace(
        build_scan_diagnostics((error,))[0],
        error_count=1,
    )
    assert "E" in false_breakout_groups(current, previous_scan, error)


def test_outputs_use_bom_and_sources_remain_unchanged(tmp_path: Path) -> None:
    source = tmp_path / "audit.jsonl"
    source.write_text(
        REAL_AUDIT.read_text(encoding="utf-8-sig").splitlines()[0] + "\n",
        encoding="utf-8",
    )
    runtime = tmp_path / "runtime.log"
    runtime.write_text("", encoding="utf-8")
    state = tmp_path / "state.json"
    state.write_text('{"version": 1, "records": {}}', encoding="utf-8")
    before = {path: path.read_bytes() for path in (source, runtime, state)}
    analysis = analyze_diagnostics(
        source,
        runtime,
        state,
        tmp_path / "missing-qtr.jsonl",
    )
    files = write_outputs(analysis, tmp_path / "output")
    assert files.false_breakouts.read_bytes().startswith(b"\xef\xbb\xbf")
    assert files.adapter_map.read_bytes().startswith(b"\xef\xbb\xbf")
    assert sources_unchanged(analysis)
    assert all(path.read_bytes() == payload for path, payload in before.items())


def test_diagnostics_do_not_use_network(
    base: ReplaySnapshot,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import socket

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("network access is forbidden")

    monkeypatch.setattr(socket, "create_connection", forbidden)
    assert candidate_setup_types(base.setup_input)


def test_diagnostics_module_has_no_telegram_dependency() -> None:
    source = (
        ROOT / "src" / "market_signal_assistant" / "setup_engine" / "diagnostics.py"
    ).read_text(encoding="utf-8")
    assert "market_signal_assistant.telegram" not in source
