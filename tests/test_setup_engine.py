import json
import socket
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from market_signal_assistant.inplay.early_discovery import (
    DiscoveryStage,
    MarketDirection,
)
from market_signal_assistant.inplay.early_discovery_v2 import (
    EarlyDiscoveryV2Result,
    RetestState,
    ScoreComponent,
)
from market_signal_assistant.setup_engine import (
    SETUP_CLASSIFICATION_PRIORITY,
    JsonlSetupAuditStore,
    SetupAnalysisInput,
    SetupDirection,
    SetupEngine,
    SetupEngineSettings,
    SetupState,
    SetupType,
    TradeEligibility,
    analyze_setup,
    input_from_early_discovery_v2,
)

NOW = datetime(2026, 8, 9, 12, 0, tzinfo=UTC)
ROOT = Path(__file__).resolve().parents[1]


def setup_input(**changes: Any) -> SetupAnalysisInput:
    baseline = SetupAnalysisInput(
        snapshot_ids=("snapshot-1",),
        source="test_snapshot",
        symbol="BTCUSDT",
        analyzed_at=NOW,
        direction=SetupDirection.UP,
        current_price=101.0,
        trigger_level=100.0,
        invalidation_level=99.0,
        price_change_24h_pct=2.0,
        distance_to_trigger_pct=1.0,
        distance_to_trigger_atr=0.5,
        breakout_age_bars=1,
        hold_candles=2,
        breakout_confirmed=True,
        correct_side_of_level=True,
        returned_inside_range=False,
        retest_detected=False,
        retest_held=False,
        breakout_failed=False,
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
    return replace(baseline, **changes)


def test_confirmed_up_breakout_is_ready_to_consider() -> None:
    result = analyze_setup(setup_input())
    assert result.direction is SetupDirection.UP
    assert result.setup_type is SetupType.BREAKOUT
    assert result.setup_state is SetupState.READY_TO_CONSIDER
    assert result.confidence == 95.0


def test_confirmed_down_breakout_is_ready_to_consider() -> None:
    result = analyze_setup(
        setup_input(
            direction=SetupDirection.DOWN,
            current_price=99.0,
            invalidation_level=101.0,
        )
    )
    assert result.direction is SetupDirection.DOWN
    assert result.setup_type is SetupType.BREAKOUT
    assert result.setup_state is SetupState.READY_TO_CONSIDER


def test_forming_retest_is_not_a_trade_readiness_claim() -> None:
    result = analyze_setup(
        setup_input(retest_detected=True, retest_held=False, hold_candles=1)
    )
    assert result.setup_type is SetupType.RETEST
    assert result.setup_state is SetupState.CONFIRMING
    assert result.trade_eligibility is TradeEligibility.CONFIRMING


def test_confirmed_retest_is_ready_to_consider() -> None:
    result = analyze_setup(setup_input(retest_detected=True, retest_held=True))
    assert result.setup_type is SetupType.RETEST
    assert result.setup_state is SetupState.READY_TO_CONSIDER


def test_failed_breakout_is_cancelled() -> None:
    result = analyze_setup(
        setup_input(
            breakout_failed=True,
            returned_inside_range=True,
            correct_side_of_level=False,
            structure_confirmation=False,
        )
    )
    assert result.setup_type is SetupType.FALSE_BREAKOUT
    assert result.setup_state is SetupState.CANCELLED
    assert result.breakout_failed is True
    assert result.current_breakout_failure is True
    assert result.historical_breakout_failure is True
    assert result.structure_recovered is False


def test_historical_breakout_failure_is_removed_after_held_retest() -> None:
    result = analyze_setup(
        setup_input(
            returned_inside_range=True,
            retest_detected=True,
            retest_held=True,
            current_breakout_failure=False,
            historical_breakout_failure=True,
            structure_recovered=True,
        )
    )
    assert result.current_breakout_failure is False
    assert result.historical_breakout_failure is True
    assert result.structure_recovered is True
    assert result.setup_type is SetupType.RETEST


def test_state_ladder_is_reachable_without_relaxing_ready_gates() -> None:
    forming = analyze_setup(
        setup_input(
            hold_candles=0,
            structure_confirmation=False,
            volume_confirmation=False,
            volatility_confirmation=False,
        )
    )
    confirming = analyze_setup(setup_input(hold_candles=1))
    ready = analyze_setup(setup_input())
    assert [forming.setup_state, confirming.setup_state, ready.setup_state] == [
        SetupState.FORMING,
        SetupState.CONFIRMING,
        SetupState.READY_TO_CONSIDER,
    ]
    assert forming.trade_eligible is False
    assert confirming.trade_eligible is False
    assert ready.trade_eligible is True


def test_retest_type_can_exist_while_trade_is_ineligible() -> None:
    result = analyze_setup(
        setup_input(retest_detected=True, retest_held=True, spread_pct=0.3)
    )
    assert result.setup_type is SetupType.RETEST
    assert result.trade_eligibility is TradeEligibility.NO_TRADE
    assert "широкий спред" in result.no_trade_reasons


def test_impulse_requires_volume_and_volatility() -> None:
    result = analyze_setup(setup_input(breakout_confirmed=False))
    assert result.setup_type is SetupType.IMPULSE
    assert result.setup_state is SetupState.READY_TO_CONSIDER


def test_compression_remains_forming() -> None:
    result = analyze_setup(
        setup_input(
            breakout_confirmed=False,
            volume_confirmation=False,
            volatility_confirmation=False,
            structure_confirmation=False,
            correct_side_of_level=False,
            hold_candles=0,
            compression_detected=True,
        )
    )
    assert result.setup_type is SetupType.COMPRESSION
    assert result.setup_state is SetupState.FORMING


def test_continuation_precedes_impulse() -> None:
    result = analyze_setup(
        setup_input(breakout_confirmed=False, continuation_detected=True)
    )
    assert result.setup_type is SetupType.CONTINUATION
    assert result.setup_state is SetupState.READY_TO_CONSIDER


def test_reversal_precedes_continuation() -> None:
    result = analyze_setup(
        setup_input(
            breakout_confirmed=False,
            continuation_detected=True,
            reversal_detected=True,
        )
    )
    assert result.setup_type is SetupType.REVERSAL
    assert result.setup_state is SetupState.READY_TO_CONSIDER


def test_no_trade_is_a_first_class_result() -> None:
    result = analyze_setup(
        setup_input(
            breakout_confirmed=False,
            volume_confirmation=False,
            volatility_confirmation=False,
            structure_confirmation=False,
            correct_side_of_level=False,
            hold_candles=0,
        )
    )
    assert result.setup_type is SetupType.NO_TRADE
    assert result.setup_state is SetupState.WATCHING
    assert "Достаточная торговая конструкция не подтверждена." in result.reasons


def test_neutral_direction_is_never_ready() -> None:
    result = analyze_setup(setup_input(direction=SetupDirection.NEUTRAL))
    assert result.setup_type is SetupType.BREAKOUT
    assert result.setup_state is not SetupState.READY_TO_CONSIDER
    assert result.trade_eligibility is TradeEligibility.NO_TRADE
    assert "Направление не подтверждено." in result.reasons


@pytest.mark.parametrize("change", [15.0, -15.0, 30.0, -30.0])
def test_extended_24h_move_is_late(change: float) -> None:
    result = analyze_setup(setup_input(price_change_24h_pct=change))
    assert result.setup_type is SetupType.BREAKOUT
    assert result.setup_state is SetupState.LATE
    assert result.is_late is True


def test_distance_over_two_atr_is_not_ready() -> None:
    result = analyze_setup(setup_input(distance_to_trigger_atr=2.01))
    assert result.setup_state is SetupState.LATE
    assert result.is_late is True
    assert "Цена находится дальше 2 ATR от уровня." in result.warnings


def test_stale_breakout_is_not_ready() -> None:
    result = analyze_setup(setup_input(breakout_age_bars=7))
    assert result.setup_type is SetupType.BREAKOUT
    assert result.setup_state is not SetupState.READY_TO_CONSIDER
    assert result.freshness_confirmation is False
    assert "Движение старше 6 завершённых баров." in result.warnings


def test_spread_over_point_two_percent_is_not_ready() -> None:
    result = analyze_setup(setup_input(spread_pct=0.201))
    assert result.setup_state is not SetupState.READY_TO_CONSIDER
    assert result.spread_ok is False
    assert "Спред выше 0,2% или не определён." in result.warnings


def test_incomplete_technical_data_is_no_trade() -> None:
    result = analyze_setup(
        setup_input(
            technical_data_complete=False,
            extra_missing_data=("completed_5m_candles",),
        )
    )
    assert result.setup_type is SetupType.BREAKOUT
    assert result.trade_eligibility is TradeEligibility.NO_TRADE
    assert "technical_data" in result.missing_data
    assert "completed_5m_candles" in result.missing_data


def test_conflicting_confirmations_are_no_trade() -> None:
    result = analyze_setup(setup_input(conflicting_confirmations=True))
    assert result.setup_type is SetupType.BREAKOUT
    assert result.trade_eligibility is TradeEligibility.NO_TRADE
    assert "Подтверждения противоречат друг другу." in result.reasons


def test_priority_is_explicit_and_deterministic() -> None:
    assert SETUP_CLASSIFICATION_PRIORITY == (
        SetupType.FALSE_BREAKOUT,
        SetupType.REVERSAL,
        SetupType.RETEST,
        SetupType.BREAKOUT,
        SetupType.CONTINUATION,
        SetupType.IMPULSE,
        SetupType.COMPRESSION,
        SetupType.NO_TRADE,
    )
    result = analyze_setup(
        setup_input(
            breakout_failed=True,
            returned_inside_range=True,
            correct_side_of_level=False,
            reversal_detected=True,
            retest_detected=True,
            continuation_detected=True,
            compression_detected=True,
        )
    )
    assert result.setup_type is SetupType.FALSE_BREAKOUT


def test_same_input_produces_same_immutable_output() -> None:
    data = setup_input(retest_detected=True, retest_held=True)
    first = analyze_setup(data)
    second = analyze_setup(data)
    assert first == second
    with pytest.raises(FrozenInstanceError):
        first.confidence = 0.0  # type: ignore[misc]


def test_all_user_facing_names_are_russian() -> None:
    assert [item.name_ru for item in SetupType] == [
        "ПРОБОЙ",
        "РЕТЕСТ",
        "ИМПУЛЬС",
        "СЖАТИЕ",
        "ПРОДОЛЖЕНИЕ",
        "ЛОЖНЫЙ ПРОБОЙ",
        "РАЗВОРОТ",
        "НЕТ СДЕЛКИ",
    ]
    assert [item.name_ru for item in SetupDirection] == [
        "ВВЕРХ",
        "ВНИЗ",
        "НЕЙТРАЛЬНО",
    ]
    assert [item.name_ru for item in SetupState] == [
        "НАБЛЮДАЕМ",
        "ФОРМИРУЕТСЯ",
        "ПОДТВЕРЖДАЕТСЯ",
        "ГОТОВО К РАССМОТРЕНИЮ",
        "ПОЗДНО",
        "ОТМЕНЕНО",
    ]


def test_audit_is_disabled_by_default(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("QTR_SETUP_ENGINE_ENABLED", raising=False)
    path = tmp_path / "qtr_setup_engine_audit.jsonl"
    settings = SetupEngineSettings.from_environment()
    assert settings.enabled is False
    SetupEngine(replace(settings, audit_path=path)).analyze(setup_input())
    assert not path.exists()


def test_enabled_explicit_analysis_writes_separate_audit(tmp_path: Path) -> None:
    path = tmp_path / "qtr_setup_engine_audit.jsonl"
    engine = SetupEngine(
        SetupEngineSettings(enabled=True, audit_path=path),
        JsonlSetupAuditStore(path),
    )
    result = engine.analyze(setup_input())
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 2
    assert payload["input"]["snapshot_ids"] == ["snapshot-1"]
    assert payload["input"]["source"] == "test_snapshot"
    assert payload["result"]["setup_type_ru"] == "ПРОБОЙ"
    assert payload["result"]["setup_state_ru"] == "ГОТОВО К РАССМОТРЕНИЮ"
    assert payload["reasons"] == list(result.reasons)
    assert payload["confirmations"]["structure"] is True
    assert payload["confirmations"]["freshness"] is True
    assert payload["result"]["trade_eligible"] is True
    assert payload["result"]["classification_candidates"]


def test_audit_schema_v2_appends_without_rewriting_legacy_rows(tmp_path: Path) -> None:
    path = tmp_path / "qtr_setup_engine_audit.jsonl"
    legacy = '{"schema_version":1,"legacy":true}\n'
    path.write_text(legacy, encoding="utf-8")
    JsonlSetupAuditStore(path).append(setup_input(), analyze_setup(setup_input()))
    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines[0] == legacy.strip()
    assert json.loads(lines[1])["schema_version"] == 2


def test_v2_adapter_uses_existing_result_without_mutating_v2() -> None:
    v2_path = (
        ROOT / "src" / "market_signal_assistant" / "inplay" / "early_discovery_v2.py"
    )
    before = v2_path.read_bytes()
    adapted = input_from_early_discovery_v2(v2_result())
    result = analyze_setup(adapted)
    assert adapted.snapshot_ids == ("scan-1",)
    assert adapted.source == "early_discovery_v2"
    assert result.setup_type is SetupType.BREAKOUT
    assert result.setup_state is SetupState.READY_TO_CONSIDER
    assert v2_path.read_bytes() == before


def test_v2_adapter_preserves_unknown_component_and_hold() -> None:
    source = replace(
        v2_result(),
        breakout_hold_candles=None,
        component_scores=tuple(
            item
            for item in v2_result().component_scores
            if item.component_id != "liquidity"
        ),
    )
    adapted = input_from_early_discovery_v2(source)
    assert adapted.hold_candles is None
    assert adapted.liquidity_ok is None
    assert "hold_candles" in adapted.extra_missing_data
    assert "liquidity_ok" in adapted.extra_missing_data


def test_analysis_opens_no_network_and_touches_no_notification_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def fail_network(*args: object, **kwargs: object) -> None:
        raise AssertionError("network must not be used")

    monkeypatch.setattr(socket, "create_connection", fail_network)
    state = tmp_path / "inplay_notifications.json"
    state.write_text('{"sentinel": true}', encoding="utf-8")
    before = state.read_bytes()
    analyze_setup(setup_input())
    analyze_setup(input_from_early_discovery_v2(v2_result()))
    assert state.read_bytes() == before

    sources = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (ROOT / "src" / "market_signal_assistant" / "setup_engine").glob(
            "*.py"
        )
    ).lower()
    assert "telegram" not in sources
    assert "requests" not in sources
    assert "pybit" not in sources


def v2_result() -> EarlyDiscoveryV2Result:
    components = (
        component("volume_acceleration", 10.0, 20.0),
        component("breakout_volume", 10.0, 20.0),
        component("atr_expansion", 7.5, 15.0),
        component("liquidity", 7.0, 10.0),
        component("compression", 0.0, 10.0),
    )
    return EarlyDiscoveryV2Result(
        schema_version=1,
        scan_id="scan-1",
        scanned_at=NOW,
        symbol="BTCUSDT",
        market_direction=MarketDirection.UP,
        direction_v1=MarketDirection.UP,
        direction_v2=MarketDirection.UP,
        stage_v1=DiscoveryStage.SETUP_FORMING,
        stage_v2=DiscoveryStage.READY_CANDIDATE,
        display_stage_v2_ru="ПОДТВЕРЖДЁННОЕ НАБЛЮДЕНИЕ",
        discovery_score_v1=60.0,
        discovery_score_v2=65.0,
        readiness_score_v1=60.0,
        readiness_score_v2=70.0,
        consecutive_active_scans=3,
        consecutive_ready_scans=3,
        first_detected_at=NOW,
        first_ready_at=NOW,
        second_confirmation_at=NOW,
        third_confirmation_at=NOW,
        reset_reason=None,
        breakout_level=100.0,
        current_price=100.5,
        absolute_distance=0.5,
        signed_distance_atr=0.5,
        absolute_distance_atr=0.5,
        distance_sign=1,
        is_correct_side_of_level=True,
        breakout_hold_candles=2,
        returned_to_level=False,
        retest_state=RetestState.HOLDING,
        breakout_failure=False,
        returned_inside_range=False,
        breakout_age_bars=1,
        spread_pct=0.1,
        price_change_24h_pct=3.0,
        production_rank=1,
        is_in_production_top20=True,
        component_scores=components,
        confirmations=(),
        warnings=(),
        technical_error=None,
        reason_v2_ru="Тестовый снимок.",
    )


def component(component_id: str, points: float, maximum: float) -> ScoreComponent:
    return ScoreComponent(
        score_kind="test",
        score_name_ru="Тест",
        component_id=component_id,
        raw_value=None,
        points=points,
        maximum_points=maximum,
        reason="test",
        explanation_ru="Тест.",
    )
