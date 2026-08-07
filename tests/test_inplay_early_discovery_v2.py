from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_signal_assistant.inplay.early_discovery import (
    DiscoveryStage,
    JsonlEarlyDiscoveryAuditStore,
    MarketDirection,
)
from market_signal_assistant.inplay.early_discovery_v2 import (
    DISPLAY_STAGE_RU,
    READINESS_WEIGHTS,
    BreakoutAssessment,
    EarlyDiscoveryV2Config,
    EarlyDiscoveryV2FixedSchedule,
    EarlyDiscoveryV2SequenceTracker,
    EarlyDiscoveryV2Service,
    JsonEarlyDiscoveryV2StateStore,
    JsonlEarlyDiscoveryV2AuditStore,
    RetestState,
    SequenceSnapshot,
    _breakout_assessment,
    _completed_series,
    _v2_stage,
    build_parser,
)
from market_signal_assistant.inplay.models import CatalogInstrument
from market_signal_assistant.models import AssetClass, Candle, Instrument, MarketSeries
from market_signal_assistant.providers import MarketDataError
from market_signal_assistant.settings import EarlyDiscoveryV2Settings

NOW = datetime(2026, 8, 5, 12, tzinfo=UTC)


def catalog(symbol: str = "BTCUSDT", *, spread_pct: float = 0.1) -> CatalogInstrument:
    midpoint = 100.0
    half = midpoint * spread_pct / 200.0
    return CatalogInstrument(
        symbol=symbol,
        quote_coin="USDT",
        status="Trading",
        turnover_24h=50_000_000.0,
        bid=midpoint - half,
        ask=midpoint + half,
        base_coin=symbol.removesuffix("USDT"),
        settle_coin="USDT",
        contract_type="LinearPerpetual",
        symbol_type="",
        is_pre_listing=False,
    )


def candles(
    interval: str,
    *,
    direction: MarketDirection | None = None,
    failed: bool = False,
    retest: bool = False,
    unfinished_spike: bool = False,
) -> MarketSeries:
    minutes = {"5m": 5, "15m": 15, "1h": 60}[interval]
    count = 50 if interval != "1h" else 30
    values: list[Candle] = []
    for index in range(count):
        timestamp = NOW - timedelta(minutes=minutes * (count - index))
        close = 100.4 if interval == "15m" else 100.0
        high = 100.5
        low = 99.5
        volume = 100.0
        if interval == "5m" and direction is not None and index >= count - 3:
            if direction is MarketDirection.UP:
                close = 102.0
                high = 102.5
                low = 101.5
            else:
                close = 98.0
                high = 98.5
                low = 97.5
            volume = 300.0 if index == count - 3 else 250.0
        if interval == "5m" and retest and index == count - 2:
            if direction is MarketDirection.UP:
                close, high, low = 101.0, 101.5, 100.4
            else:
                close, high, low = 99.0, 100.6, 98.5
        if interval == "5m" and failed and index == count - 1:
            if direction is MarketDirection.UP:
                close, high, low = 100.0, 101.0, 99.5
            else:
                close, high, low = 101.0, 101.5, 99.5
        values.append(Candle(timestamp, close, high, low, close, volume))
    if unfinished_spike:
        values.append(Candle(NOW, 130.0, 131.0, 129.0, 130.0, 1000.0))
    return MarketSeries(
        Instrument("BTCUSDT", AssetClass.CRYPTO), interval, tuple(values)
    )


class Catalog:
    def __init__(self, items: tuple[CatalogInstrument, ...]) -> None:
        self.items = items

    def list_instruments(self) -> tuple[CatalogInstrument, ...]:
        return self.items


class Provider:
    def __init__(self, *, direction: MarketDirection = MarketDirection.UP) -> None:
        self.direction = direction
        self.calls: list[tuple[str, str]] = []

    def load(self, instrument: Instrument, interval: str, limit: int) -> MarketSeries:
        del limit
        self.calls.append((instrument.symbol, interval))
        return candles(
            interval,
            direction=self.direction if interval == "5m" else None,
            retest=interval == "5m",
        )


class FailingProvider(Provider):
    def load(self, instrument: Instrument, interval: str, limit: int) -> MarketSeries:
        del instrument, interval, limit
        raise MarketDataError("офлайн-ошибка")


def assessment(
    *,
    direction: MarketDirection = MarketDirection.UP,
    distance: float = 0.5,
    failure: bool = False,
    correct_side: bool = True,
    retest_state: RetestState = RetestState.HOLDING,
) -> BreakoutAssessment:
    return BreakoutAssessment(
        direction,
        "5m",
        100.0,
        101.0 if direction is MarketDirection.UP else 99.0,
        1.0,
        distance if direction is MarketDirection.UP else -distance,
        abs(distance),
        1 if direction is MarketDirection.UP else -1,
        correct_side,
        2,
        retest_state in {RetestState.RETEST_IN_PROGRESS, RetestState.RETEST_HELD},
        retest_state,
        failure,
        failure,
        1,
        2.0,
    )


def sequence(active: int, ready: int) -> SequenceSnapshot:
    return SequenceSnapshot(
        active,
        ready,
        NOW,
        NOW,
        NOW if ready >= 2 else None,
        NOW if ready >= 3 else None,
        DiscoveryStage.SETUP_FORMING,
        MarketDirection.UP,
        NOW,
        None,
    )


def stage(
    active: int,
    ready: int,
    *,
    value: BreakoutAssessment | None = None,
    spread: float = 0.1,
    change_24h: float = 0.0,
) -> DiscoveryStage:
    return _v2_stage(
        direction=MarketDirection.UP,
        discovery_score=80.0,
        readiness_score=80.0,
        sequence=sequence(active, ready),
        assessment=value or assessment(),
        spread_pct=spread,
        price_change_24h=change_24h,
        config=EarlyDiscoveryV2Config(),
    )


def test_v2_is_disabled_by_default() -> None:
    assert EarlyDiscoveryV2Settings().enabled is False


def test_cli_help_is_russian_and_does_not_build_providers(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as error:
        build_parser().parse_args(["--help"])

    assert error.value.code == 0
    output = capsys.readouterr().out
    assert "использование:" in output
    assert "параметры:" in output
    assert "показать справку и выйти" in output


def test_v2_settings_are_loaded_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("INPLAY_EARLY_DISCOVERY_V2_ENABLED", "true")
    monkeypatch.setenv("INPLAY_EARLY_DISCOVERY_V2_INTERVAL_MINUTES", "10")
    monkeypatch.setenv("INPLAY_EARLY_DISCOVERY_V2_REQUIRED_READY_SCANS", "4")
    monkeypatch.setenv("INPLAY_EARLY_DISCOVERY_V2_FORMING_SCANS", "2")
    monkeypatch.setenv("INPLAY_EARLY_DISCOVERY_V2_EPISODE_GAP_MINUTES", "45")

    settings = EarlyDiscoveryV2Settings.from_environment()

    assert settings.enabled is True
    assert settings.interval_minutes == 10
    assert settings.required_ready_scans == 4
    assert settings.forming_scans == 2
    assert settings.episode_gap_minutes == 45


def test_one_ready_scan_is_not_confirmed() -> None:
    assert stage(1, 1) is DiscoveryStage.EARLY_ATTENTION


def test_two_ready_scans_are_forming() -> None:
    assert stage(2, 2) is DiscoveryStage.SETUP_FORMING


def test_three_ready_scans_are_confirmed() -> None:
    assert stage(3, 3) is DiscoveryStage.READY_CANDIDATE


def test_direction_change_resets_sequence(tmp_path: Path) -> None:
    tracker = EarlyDiscoveryV2SequenceTracker(
        JsonEarlyDiscoveryV2StateStore(tmp_path / "state.json"),
        EarlyDiscoveryV2Config(),
    )
    tracker.update(
        symbol="BTCUSDT",
        direction=MarketDirection.UP,
        active=True,
        ready=True,
        observed_at=NOW,
    )
    changed = tracker.update(
        symbol="BTCUSDT",
        direction=MarketDirection.DOWN,
        active=True,
        ready=True,
        observed_at=NOW + timedelta(minutes=5),
    )

    assert changed.consecutive_ready_scans == 1
    assert changed.reset_reason == "направление изменилось"


def test_two_quiet_scans_end_episode(tmp_path: Path) -> None:
    tracker = EarlyDiscoveryV2SequenceTracker(
        JsonEarlyDiscoveryV2StateStore(tmp_path / "state.json"),
        EarlyDiscoveryV2Config(),
    )
    tracker.update(
        symbol="BTCUSDT",
        direction=MarketDirection.UP,
        active=True,
        ready=True,
        observed_at=NOW,
    )
    tracker.update(
        symbol="BTCUSDT",
        direction=MarketDirection.NEUTRAL,
        active=False,
        ready=False,
        observed_at=NOW + timedelta(minutes=5),
    )
    quiet = tracker.update(
        symbol="BTCUSDT",
        direction=MarketDirection.NEUTRAL,
        active=False,
        ready=False,
        observed_at=NOW + timedelta(minutes=10),
    )

    assert quiet.consecutive_active_scans == 0
    assert quiet.reset_reason == "два последовательных состояния без сигнала"


def test_brief_technical_error_preserves_sequence(tmp_path: Path) -> None:
    tracker = EarlyDiscoveryV2SequenceTracker(
        JsonEarlyDiscoveryV2StateStore(tmp_path / "state.json"),
        EarlyDiscoveryV2Config(),
    )
    tracker.update(
        symbol="BTCUSDT",
        direction=MarketDirection.UP,
        active=True,
        ready=True,
        observed_at=NOW,
    )

    error_state = tracker.technical_error("BTCUSDT")
    resumed = tracker.update(
        symbol="BTCUSDT",
        direction=MarketDirection.UP,
        active=True,
        ready=True,
        observed_at=NOW + timedelta(minutes=10),
    )

    assert error_state.consecutive_ready_scans == 1
    assert resumed.consecutive_ready_scans == 2


def test_thirty_minute_gap_starts_new_episode(tmp_path: Path) -> None:
    tracker = EarlyDiscoveryV2SequenceTracker(
        JsonEarlyDiscoveryV2StateStore(tmp_path / "state.json"),
        EarlyDiscoveryV2Config(),
    )
    tracker.update(
        symbol="BTCUSDT",
        direction=MarketDirection.UP,
        active=True,
        ready=True,
        observed_at=NOW,
    )
    resumed = tracker.update(
        symbol="BTCUSDT",
        direction=MarketDirection.UP,
        active=True,
        ready=True,
        observed_at=NOW + timedelta(minutes=30),
    )

    assert resumed.consecutive_ready_scans == 1
    assert "отсутствовал" in (resumed.reset_reason or "")


def test_state_survives_restart_and_is_atomic(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    store = JsonEarlyDiscoveryV2StateStore(path)
    tracker = EarlyDiscoveryV2SequenceTracker(store, EarlyDiscoveryV2Config())
    tracker.update(
        symbol="BTCUSDT",
        direction=MarketDirection.UP,
        active=True,
        ready=True,
        observed_at=NOW,
    )
    tracker.save()

    restored = EarlyDiscoveryV2SequenceTracker(store, EarlyDiscoveryV2Config())

    assert restored.records["BTCUSDT"].consecutive_ready_scans == 1
    assert json.loads(path.read_text(encoding="utf-8"))["version"] == 1
    assert not path.with_suffix(".json.tmp").exists()


def test_corrupt_state_is_backed_up_and_recovers(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    path.write_text("{broken", encoding="utf-8")

    records = JsonEarlyDiscoveryV2StateStore(path).load()

    assert records == {}
    assert tuple(tmp_path.glob("state.json.corrupt-*"))


def test_legacy_state_without_current_version_is_loaded(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    path.write_text(
        json.dumps(
            {
                "records": {
                    "BTCUSDT": {
                        "consecutive_active_scans": 2,
                        "consecutive_ready_scans": 1,
                        "last_direction": "UP",
                        "last_seen_at": NOW.isoformat(),
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    restored = JsonEarlyDiscoveryV2StateStore(path).load()["BTCUSDT"]

    assert restored.consecutive_active_scans == 2
    assert restored.consecutive_ready_scans == 1
    assert restored.last_direction is MarketDirection.UP


def test_up_and_down_use_symmetric_correct_side() -> None:
    up = _breakout_assessment(
        candles("5m", direction=MarketDirection.UP), candles("15m")
    )
    down = _breakout_assessment(
        candles("5m", direction=MarketDirection.DOWN), candles("15m")
    )

    assert up is not None and up.is_correct_side_of_level
    assert up.signed_distance_atr is not None and up.signed_distance_atr > 0
    assert down is not None and down.is_correct_side_of_level
    assert down.signed_distance_atr is not None and down.signed_distance_atr < 0


def test_failed_breakout_blocks_confirmation() -> None:
    failed = _breakout_assessment(
        candles("5m", direction=MarketDirection.UP, failed=True),
        candles("15m"),
    )

    assert failed is not None
    assert failed.retest_state is RetestState.FAILED
    assert stage(3, 3, value=failed) is not DiscoveryStage.READY_CANDIDATE


def test_confirmed_retest_is_recorded() -> None:
    result = _breakout_assessment(
        candles("5m", direction=MarketDirection.UP, retest=True),
        candles("15m"),
    )

    assert result is not None
    assert result.returned_to_level is True
    assert result.retest_state is RetestState.RETEST_HELD


def test_unfinished_candle_is_not_used() -> None:
    raw = candles("5m", direction=MarketDirection.UP, unfinished_spike=True)

    completed = _completed_series(raw, NOW, 5)

    assert completed is not None
    assert len(completed.candles) == len(raw.candles) - 1
    assert completed.candles[-1].close != 130.0


def test_absolute_distance_spread_and_24h_safety_gates() -> None:
    assert (
        stage(3, 3, value=assessment(distance=-2.1))
        is not DiscoveryStage.READY_CANDIDATE
    )
    assert stage(3, 3, spread=0.21) is not DiscoveryStage.READY_CANDIDATE
    assert stage(3, 3, change_24h=15.0) is DiscoveryStage.LATE


def test_all_readiness_components_are_declared() -> None:
    assert set(READINESS_WEIGHTS) == {
        "breakout_freshness",
        "distance",
        "correct_side",
        "hold",
        "retest",
        "direction_stability",
        "ready_stability",
        "spread_quality",
        "liquidity",
        "breakout_volume",
    }


def test_russian_display_names_are_used() -> None:
    assert (
        DISPLAY_STAGE_RU[DiscoveryStage.READY_CANDIDATE] == "ПОДТВЕРЖДЁННОЕ НАБЛЮДЕНИЕ"
    )
    assert DISPLAY_STAGE_RU[DiscoveryStage.QUIET] == "БЕЗ СИГНАЛА"


def test_service_uses_one_shared_snapshot_for_v1_and_v2(tmp_path: Path) -> None:
    provider = Provider()
    now = [NOW]
    service = EarlyDiscoveryV2Service(
        catalog_provider=Catalog((catalog(),)),
        market_provider=provider,
        audit_store=JsonlEarlyDiscoveryV2AuditStore(tmp_path / "audit.jsonl"),
        state_store=JsonEarlyDiscoveryV2StateStore(tmp_path / "state.json"),
        config=EarlyDiscoveryV2Config(),
        clock=lambda: now[0],
        maximum_workers=1,
    )

    first = service.scan().results[0]
    now[0] += timedelta(minutes=5)
    second = service.scan().results[0]
    now[0] += timedelta(minutes=5)
    third = service.scan().results[0]

    assert len(provider.calls) == 9
    assert first.stage_v2 is not DiscoveryStage.READY_CANDIDATE
    assert second.stage_v2 is DiscoveryStage.SETUP_FORMING
    assert third.stage_v2 is DiscoveryStage.READY_CANDIDATE
    assert first.stage_v1 is not None
    assert len(first.component_scores) == 17
    for component in first.component_scores:
        assert component.score_kind in {"discovery", "readiness"}
        assert component.score_name_ru
        assert component.maximum_points > 0
        assert 0 <= component.points <= component.maximum_points
        assert component.reason
        assert component.explanation_ru


def test_v1_and_v2_audits_share_the_same_loaded_snapshot(tmp_path: Path) -> None:
    provider = Provider()
    v1_audit = tmp_path / "v1.jsonl"
    v2_audit = tmp_path / "v2.jsonl"
    service = EarlyDiscoveryV2Service(
        catalog_provider=Catalog((catalog(),)),
        market_provider=provider,
        audit_store=JsonlEarlyDiscoveryV2AuditStore(v2_audit),
        state_store=JsonEarlyDiscoveryV2StateStore(tmp_path / "state.json"),
        config=EarlyDiscoveryV2Config(),
        v1_audit_store=JsonlEarlyDiscoveryAuditStore(v1_audit),
        clock=lambda: NOW,
        maximum_workers=1,
    )

    service.scan()

    assert provider.calls == [("BTCUSDT", "5m"), ("BTCUSDT", "15m"), ("BTCUSDT", "1h")]
    assert json.loads(v1_audit.read_text(encoding="utf-8"))["symbol"] == "BTCUSDT"
    assert json.loads(v2_audit.read_text(encoding="utf-8"))["symbol"] == "BTCUSDT"


def test_technical_error_is_audited_without_resetting_state(tmp_path: Path) -> None:
    audit = tmp_path / "audit.jsonl"
    service = EarlyDiscoveryV2Service(
        catalog_provider=Catalog((catalog(),)),
        market_provider=FailingProvider(),
        audit_store=JsonlEarlyDiscoveryV2AuditStore(audit),
        state_store=JsonEarlyDiscoveryV2StateStore(tmp_path / "state.json"),
        config=EarlyDiscoveryV2Config(),
        clock=lambda: NOW,
        maximum_workers=1,
    )

    report = service.scan()
    payload = json.loads(audit.read_text(encoding="utf-8"))

    assert report.errors == 1
    assert report.results[0].technical_error == "EarlyDiscoveryDataError"
    assert payload["technical_error"] == "EarlyDiscoveryDataError"
    assert payload["discovery_score_v2"] is None


def test_audit_contains_all_required_comparison_fields(tmp_path: Path) -> None:
    audit = tmp_path / "audit.jsonl"
    service = EarlyDiscoveryV2Service(
        catalog_provider=Catalog((catalog(),)),
        market_provider=Provider(),
        audit_store=JsonlEarlyDiscoveryV2AuditStore(audit),
        state_store=JsonEarlyDiscoveryV2StateStore(tmp_path / "state.json"),
        config=EarlyDiscoveryV2Config(),
        clock=lambda: NOW,
        maximum_workers=1,
    )

    service.scan()
    payload = json.loads(audit.read_text(encoding="utf-8"))

    assert {
        "schema_version",
        "scan_id",
        "scanned_at",
        "symbol",
        "market_direction",
        "direction_v1",
        "direction_v2",
        "stage_v1",
        "stage_v2",
        "display_stage_v2_ru",
        "discovery_score_v1",
        "discovery_score_v2",
        "readiness_score_v1",
        "readiness_score_v2",
        "consecutive_active_scans",
        "consecutive_ready_scans",
        "breakout_level",
        "current_price",
        "signed_distance_atr",
        "absolute_distance_atr",
        "is_correct_side_of_level",
        "breakout_hold_candles",
        "retest_state",
        "breakout_failure",
        "spread_pct",
        "price_change_24h_pct",
        "production_rank",
        "is_in_production_top20",
        "component_scores",
        "confirmations",
        "warnings",
        "technical_error",
        "reason_v2_ru",
    } <= payload.keys()


def test_service_does_not_touch_notification_state(tmp_path: Path) -> None:
    notification = tmp_path / "inplay_notifications.json"
    notification.write_text('{"version":2,"records":{}}', encoding="utf-8")
    service = EarlyDiscoveryV2Service(
        catalog_provider=Catalog((catalog(),)),
        market_provider=Provider(),
        audit_store=JsonlEarlyDiscoveryV2AuditStore(tmp_path / "audit.jsonl"),
        state_store=JsonEarlyDiscoveryV2StateStore(tmp_path / "v2_state.json"),
        config=EarlyDiscoveryV2Config(),
        clock=lambda: NOW,
        maximum_workers=1,
    )

    service.scan()

    assert notification.read_text(encoding="utf-8") == '{"version":2,"records":{}}'


def test_audit_has_schema_components_and_survives_corrupt_tail(tmp_path: Path) -> None:
    audit = tmp_path / "audit.jsonl"
    audit.write_text("{broken", encoding="utf-8")
    service = EarlyDiscoveryV2Service(
        catalog_provider=Catalog((catalog(),)),
        market_provider=Provider(),
        audit_store=JsonlEarlyDiscoveryV2AuditStore(audit),
        state_store=JsonEarlyDiscoveryV2StateStore(tmp_path / "state.json"),
        config=EarlyDiscoveryV2Config(),
        clock=lambda: NOW,
        maximum_workers=1,
    )

    service.scan()
    lines = audit.read_text(encoding="utf-8").splitlines()
    payload = json.loads(lines[-1])

    assert lines[0] == "{broken"
    assert payload["schema_version"] == 1
    assert len(payload["component_scores"]) == 17
    assert payload["display_stage_v2_ru"] in DISPLAY_STAGE_RU.values()


def test_audit_prunes_rows_older_than_seven_days(tmp_path: Path) -> None:
    audit = tmp_path / "audit.jsonl"
    audit.write_text(
        json.dumps({"scanned_at": (NOW - timedelta(days=8)).isoformat()}) + "\n",
        encoding="utf-8",
    )
    service = EarlyDiscoveryV2Service(
        catalog_provider=Catalog((catalog(),)),
        market_provider=Provider(),
        audit_store=JsonlEarlyDiscoveryV2AuditStore(audit),
        state_store=JsonEarlyDiscoveryV2StateStore(tmp_path / "state.json"),
        config=EarlyDiscoveryV2Config(),
        clock=lambda: NOW,
        maximum_workers=1,
    )

    service.scan()

    assert (NOW - timedelta(days=8)).isoformat() not in audit.read_text(
        encoding="utf-8"
    )


class ScheduledService:
    def __init__(self, clock: list[float], durations: tuple[float, ...]) -> None:
        self.clock = clock
        self.durations = durations
        self.calls = 0

    def scan(self) -> object:
        self.clock[0] += self.durations[self.calls]
        self.calls += 1
        return object()


def test_fixed_schedule_and_no_catch_up_burst() -> None:
    clock = [0.0]
    sleeps: list[float] = []
    service = ScheduledService(clock, (700.0, 10.0))

    def sleeper(delay: float) -> None:
        sleeps.append(delay)
        clock[0] += delay

    schedule = EarlyDiscoveryV2FixedSchedule(
        service,  # type: ignore[arg-type]
        interval_seconds=300.0,
        monotonic=lambda: clock[0],
        sleeper=sleeper,
        reporter=lambda report: None,
    )
    schedule.run(maximum_scans=2)

    assert service.calls == 2
    assert sleeps == [200.0]


def test_fixed_schedule_does_not_add_scan_duration() -> None:
    clock = [0.0]
    sleeps: list[float] = []
    service = ScheduledService(clock, (120.0, 120.0, 120.0))

    def sleeper(delay: float) -> None:
        sleeps.append(delay)
        clock[0] += delay

    schedule = EarlyDiscoveryV2FixedSchedule(
        service,  # type: ignore[arg-type]
        interval_seconds=300.0,
        monotonic=lambda: clock[0],
        sleeper=sleeper,
        reporter=lambda report: None,
    )
    schedule.run(maximum_scans=3)

    assert sleeps == [180.0, 180.0]
