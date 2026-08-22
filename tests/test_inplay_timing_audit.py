from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_signal_assistant.inplay.audit import (
    DEFAULT_INPLAY_DETECTION_STATE_PATH,
    DEFAULT_INPLAY_TIMING_AUDIT_PATH,
    InPlayAuditCandidate,
    InPlayTimingAuditor,
    JsonInPlayDetectionStore,
    JsonlInPlayTimingAuditStore,
)
from market_signal_assistant.inplay.early_discovery import (
    JsonlEarlyDiscoveryAuditStore,
)
from market_signal_assistant.inplay.early_discovery_v2 import (
    JsonlEarlyDiscoveryV2AuditStore,
)
from market_signal_assistant.inplay.models import (
    CatalogInstrument,
    InPlayDirection,
    InPlayReport,
    InPlayResult,
    ListingStatus,
)
from market_signal_assistant.models import (
    AssetClass,
    Candle,
    Instrument,
    MarketSeries,
    MarketSignal,
    SignalDirection,
    SignalEvidence,
)
from market_signal_assistant.settings import InPlayTimingAuditSettings
from market_signal_assistant.telegram.formatting import format_inplay_report

NOW = datetime(2026, 8, 3, 12, tzinfo=UTC)


def series(
    symbol: str = "BTCUSDT",
    *,
    closes: tuple[float, ...] | None = None,
    interval: str = "1h",
) -> MarketSeries:
    values = closes or tuple(100.0 for _ in range(24)) + (105.0,)
    step = timedelta(minutes=5) if interval == "5m" else timedelta(hours=1)
    candles = tuple(
        Candle(
            NOW - step * (len(values) - index),
            close,
            close + 1,
            close - 1,
            close,
            100.0 if index < len(values) - 1 else 250.0,
        )
        for index, close in enumerate(values)
    )
    return MarketSeries(Instrument(symbol, AssetClass.CRYPTO), interval, candles)


def candidate(
    *,
    price: float = 105.0,
    score: float = 75.0,
    direction: InPlayDirection = InPlayDirection.LONG,
    one_hour_series: MarketSeries | None = None,
    intraday_series: MarketSeries | None = None,
    technical: MarketSignal | None = None,
) -> InPlayAuditCandidate:
    symbol = "BTCUSDT"
    market = one_hour_series or series(
        symbol,
        closes=tuple(100.0 for _ in range(24)) + (price,),
    )
    result = InPlayResult(
        symbol=symbol,
        direction=direction,
        inplay_score=score,
        directional_score=(
            None if direction is InPlayDirection.WATCH else 80.0
        ),
        reasons=("Цена вышла из локального диапазона",),
        warnings=("Тестовое предупреждение",),
        first_seen=NOW - timedelta(days=30),
    )
    catalog = CatalogInstrument(
        symbol=symbol,
        quote_coin="USDT",
        status="Trading",
        turnover_24h=250_000_000.0,
        bid=price - 0.05,
        ask=price + 0.05,
        base_coin="BTC",
        settle_coin="USDT",
        contract_type="LinearPerpetual",
        symbol_type="",
        is_pre_listing=False,
    )
    return InPlayAuditCandidate(
        catalog=catalog,
        listing=ListingStatus(symbol, result.first_seen, False, 0.0),
        series=market,
        intraday_series=intraday_series,
        technical=technical,
        result=result,
    )


def auditor(
    tmp_path: Path,
    *,
    reset_minutes: int = 60,
) -> InPlayTimingAuditor:
    return InPlayTimingAuditor(
        JsonlInPlayTimingAuditStore(tmp_path / "inplay_timing_audit.jsonl"),
        JsonInPlayDetectionStore(tmp_path / "inplay_detection_state.json"),
        episode_reset=timedelta(minutes=reset_minutes),
    )


def test_timing_audit_is_disabled_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("INPLAY_TIMING_AUDIT_ENABLED", raising=False)
    monkeypatch.delenv("INPLAY_TIMING_AUDIT_AUTO_ENABLED", raising=False)
    monkeypatch.delenv("INPLAY_TIMING_AUDIT_INTERVAL_MINUTES", raising=False)
    monkeypatch.delenv("INPLAY_AUDIT_EPISODE_SCORE", raising=False)
    monkeypatch.delenv("INPLAY_AUDIT_EPISODE_RESET_MINUTES", raising=False)

    settings = InPlayTimingAuditSettings.from_environment()

    assert settings.enabled is False
    assert settings.auto_enabled is False
    assert settings.interval_minutes == 5
    assert settings.episode_score == 40
    assert settings.episode_reset_minutes == 60


def test_timing_audit_can_be_enabled_explicitly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("INPLAY_TIMING_AUDIT_ENABLED", "true")
    monkeypatch.setenv("INPLAY_TIMING_AUDIT_AUTO_ENABLED", "true")
    monkeypatch.setenv("INPLAY_TIMING_AUDIT_INTERVAL_MINUTES", "10")
    monkeypatch.setenv("INPLAY_AUDIT_EPISODE_SCORE", "42.5")
    monkeypatch.setenv("INPLAY_AUDIT_EPISODE_RESET_MINUTES", "90")

    settings = InPlayTimingAuditSettings.from_environment()

    assert settings.enabled is True
    assert settings.auto_enabled is True
    assert settings.interval_minutes == 10
    assert settings.episode_score == 42.5
    assert settings.episode_reset_minutes == 90


@pytest.mark.parametrize(
    ("interval", "score", "reset", "message"),
    (
        (4, 40.0, 60, "interval"),
        (61, 40.0, 60, "interval"),
        (5, -1.0, 60, "score"),
        (5, 101.0, 60, "score"),
        (5, 40.0, 14, "reset"),
        (5, 40.0, 1441, "reset"),
    ),
)
def test_timing_audit_settings_validate_ranges(
    interval: int,
    score: float,
    reset: int,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        InPlayTimingAuditSettings(
            interval_minutes=interval,
            episode_score=score,
            episode_reset_minutes=reset,
        )


def test_first_observation_below_episode_threshold_does_not_start_episode(
    tmp_path: Path,
) -> None:
    snapshot = auditor(tmp_path).record((candidate(score=30),), NOW)[0]

    assert snapshot.first_observed_at == NOW
    assert snapshot.episode_id is None
    assert snapshot.episode_started_at is None
    assert snapshot.first_qualified_at is None


def test_episode_starts_when_score_crosses_diagnostic_threshold(
    tmp_path: Path,
) -> None:
    timing = auditor(tmp_path)
    timing.record((candidate(score=35, price=100),), NOW)

    snapshot = timing.record(
        (candidate(score=40, price=104),),
        NOW + timedelta(minutes=5),
    )[0]

    assert snapshot.first_observed_at == NOW
    assert snapshot.episode_id is not None
    assert snapshot.episode_started_at == NOW + timedelta(minutes=5)
    assert snapshot.episode_started_price == 104
    assert snapshot.first_qualified_at is None


def test_first_qualification_and_move_are_measured_from_episode_start(
    tmp_path: Path,
) -> None:
    timing = auditor(tmp_path, reset_minutes=120)
    started = timing.record((candidate(score=40, price=100),), NOW)[0]

    qualified = timing.record(
        (candidate(score=50, price=110),),
        NOW + timedelta(minutes=65),
    )[0]

    assert qualified.episode_id == started.episode_id
    assert qualified.first_qualified_at == NOW + timedelta(minutes=65)
    assert qualified.first_qualified_price == 110
    assert qualified.move_from_episode_start_to_qualification_pct == pytest.approx(
        10.0
    )
    assert qualified.move_from_episode_start_to_current_pct == pytest.approx(10.0)
    assert qualified.bars_from_episode_start_to_qualification == 1


def test_repeated_scan_does_not_reset_episode(tmp_path: Path) -> None:
    timing = auditor(tmp_path)
    first = timing.record((candidate(score=45),), NOW)[0]
    repeated = timing.record(
        (candidate(score=46),),
        NOW + timedelta(minutes=5),
        scan_source="timing_audit_auto",
    )[0]

    assert repeated.episode_id == first.episode_id
    assert repeated.episode_started_at == first.episode_started_at
    assert repeated.scan_source == "timing_audit_auto"


def test_reappearance_after_reset_window_creates_new_episode(tmp_path: Path) -> None:
    timing = auditor(tmp_path)
    first = timing.record((candidate(score=45),), NOW)[0]

    reappeared = timing.record(
        (candidate(score=45),),
        NOW + timedelta(minutes=60),
    )[0]

    assert reappeared.episode_id != first.episode_id
    assert reappeared.episode_started_at == NOW + timedelta(minutes=60)


def test_new_direction_after_watch_fade_starts_independent_episode(
    tmp_path: Path,
) -> None:
    timing = auditor(tmp_path)
    original = timing.record(
        (candidate(score=45, direction=InPlayDirection.LONG),), NOW
    )[0]
    timing.record(
        (candidate(score=45, direction=InPlayDirection.WATCH),),
        NOW + timedelta(minutes=5),
    )

    reversed_episode = timing.record(
        (candidate(score=45, direction=InPlayDirection.SHORT),),
        NOW + timedelta(minutes=10),
    )[0]

    assert reversed_episode.episode_id != original.episode_id
    assert reversed_episode.episode_started_at == NOW + timedelta(minutes=10)


def test_near_simultaneous_sources_do_not_create_duplicate_episode(
    tmp_path: Path,
) -> None:
    timing = auditor(tmp_path)
    manual = timing.record(
        (candidate(score=45),), NOW, scan_source="manual"
    )[0]
    shadow = timing.record(
        (candidate(score=45),),
        NOW + timedelta(seconds=1),
        scan_source="timing_audit_auto",
    )[0]

    assert shadow.episode_id == manual.episode_id
    lines = (tmp_path / "inplay_timing_audit.jsonl").read_text(
        encoding="utf-8"
    ).splitlines()
    assert [json.loads(line)["scan_source"] for line in lines] == [
        "manual",
        "timing_audit_auto",
    ]


def test_version_one_detection_state_is_loaded_backward_compatibly(
    tmp_path: Path,
) -> None:
    path = tmp_path / "inplay_detection_state.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "records": {
                    "BTCUSDT": {
                        "first_detected_at": NOW.isoformat(),
                        "first_detected_price": 100.0,
                        "last_seen_at": (NOW + timedelta(minutes=5)).isoformat(),
                        "peak_price_since_detection": 110.0,
                        "trough_price_since_detection": 95.0,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    record = JsonInPlayDetectionStore(path).load().records["BTCUSDT"]

    assert record.first_observed_at == NOW
    assert record.first_observed_price == 100.0
    assert record.episode_id is None
    assert record.episode_started_at is None


def test_enabled_audit_writes_one_complete_utf8_jsonl_snapshot(
    tmp_path: Path,
) -> None:
    technical = MarketSignal(
        instrument=Instrument("BTCUSDT", AssetClass.CRYPTO),
        interval="1h",
        timestamp=NOW,
        direction=SignalDirection.BULLISH,
        score=80,
        confidence=75,
        confirmations=4,
        conflicts=0,
        price=105,
        evidence=(SignalEvidence("breakout", SignalDirection.BULLISH, 1, "ok"),),
    )
    five_minute = series(
        interval="5m",
        closes=(100.0, 101.0, 102.0, 103.0, 104.0),
    )
    snapshots = auditor(tmp_path).record(
        (candidate(intraday_series=five_minute, technical=technical),),
        NOW,
        scan_source="manual",
    )

    assert len(snapshots) == 1
    payload = json.loads(
        (tmp_path / "inplay_timing_audit.jsonl").read_text(encoding="utf-8")
    )
    assert payload["scanned_at"] == NOW.isoformat()
    assert payload["symbol"] == "BTCUSDT"
    assert payload["internal_direction"] == "LONG"
    assert payload["display_status"] == "ЛОНГ"
    assert payload["price_change_5m_pct"] == pytest.approx(104 / 103 * 100 - 100)
    assert payload["price_change_15m_pct"] == pytest.approx(104 / 101 * 100 - 100)
    assert payload["price_change_1h_pct"] == pytest.approx(5.0)
    assert payload["price_change_24h_pct"] == pytest.approx(5.0)
    assert payload["confirmations"] == 4
    assert payload["first_detected_at"] == NOW.isoformat()
    assert payload["first_observed_at"] == NOW.isoformat()
    assert payload["episode_id"] is not None
    assert payload["scan_source"] == "manual"
    assert payload["move_before_first_detection_pct"] == pytest.approx(5.0)
    assert set(payload) == {
        "scanned_at",
        "symbol",
        "internal_direction",
        "display_status",
        "inplay_score",
        "last_price",
        "spread_pct",
        "price_change_5m_pct",
        "price_change_15m_pct",
        "price_change_1h_pct",
        "price_change_24h_pct",
        "relative_volume",
        "atr_pct",
        "breakout_direction",
        "breakout_age_bars",
        "breakout_level",
        "distance_from_breakout_pct",
        "distance_from_breakout_atr",
        "confirmations",
        "warnings",
        "is_new_listing",
        "instrument_status",
        "symbol_type",
        "turnover_24h",
        "first_detected_at",
        "first_detected_price",
        "move_before_first_detection_pct",
        "first_observed_at",
        "episode_id",
        "episode_started_at",
        "episode_started_price",
        "first_qualified_at",
        "first_qualified_price",
        "price_change_24h_at_episode_start_pct",
        "move_from_episode_start_to_qualification_pct",
        "move_from_episode_start_to_current_pct",
        "bars_from_episode_start_to_qualification",
        "scan_source",
    }


def test_missing_metrics_are_serialized_as_json_null(tmp_path: Path) -> None:
    short = series(closes=(100.0,))

    auditor(tmp_path).record((candidate(one_hour_series=short),), NOW)

    payload = json.loads(
        (tmp_path / "inplay_timing_audit.jsonl").read_text(encoding="utf-8")
    )
    assert payload["price_change_5m_pct"] is None
    assert payload["price_change_15m_pct"] is None
    assert payload["price_change_1h_pct"] is None
    assert payload["price_change_24h_pct"] is None
    assert payload["breakout_direction"] is None
    assert payload["confirmations"] is None


def test_detection_time_and_price_do_not_reset_and_peak_trough_update(
    tmp_path: Path,
) -> None:
    timing = auditor(tmp_path)
    timing.record((candidate(price=105),), NOW)
    timing.record((candidate(price=112),), NOW + timedelta(minutes=15))
    timing.record((candidate(price=98),), NOW + timedelta(minutes=30))

    state = JsonInPlayDetectionStore(
        tmp_path / "inplay_detection_state.json"
    ).load()
    record = state.records["BTCUSDT"]
    assert record.first_observed_at == NOW
    assert record.first_observed_price == 105
    assert record.last_seen_at == NOW + timedelta(minutes=30)
    assert record.peak_price_since_episode_start == 112
    assert record.trough_price_since_episode_start == 98
    assert not (tmp_path / "inplay_detection_state.json.tmp").exists()


@pytest.mark.parametrize(
    ("closes", "direction", "level", "distance"),
    (
        (tuple(100.0 for _ in range(21)) + (105.0, 104.0), "UP", 101.0, 3.0),
        (tuple(100.0 for _ in range(21)) + (95.0, 96.0), "DOWN", 99.0, -3.0),
    ),
)
def test_breakout_freshness_uses_last_confirmed_one_hour_breakout(
    tmp_path: Path,
    closes: tuple[float, ...],
    direction: str,
    level: float,
    distance: float,
) -> None:
    snapshot = auditor(tmp_path).record(
        (candidate(one_hour_series=series(closes=closes)),),
        NOW,
    )[0]

    assert snapshot.breakout_direction == direction
    assert snapshot.breakout_age_bars == 1
    assert snapshot.breakout_level == level
    assert snapshot.distance_from_breakout_pct == pytest.approx(
        distance / level * 100
    )
    assert snapshot.distance_from_breakout_atr is not None


def test_corrupted_detection_state_recovers_safely(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    state_path = tmp_path / "inplay_detection_state.json"
    state_path.write_text("{broken", encoding="utf-8")

    auditor(tmp_path).record((candidate(),), NOW)

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["records"]["BTCUSDT"]["first_observed_at"] == NOW.isoformat()
    assert "повреждён" in caplog.text


def test_audit_is_separate_from_notification_and_news_state(tmp_path: Path) -> None:
    notifications = tmp_path / "inplay_notifications.json"
    news = tmp_path / "news_notifications.json"
    notifications.write_text('{"sentinel": 1}', encoding="utf-8")
    news.write_text('{"sentinel": 2}', encoding="utf-8")

    auditor(tmp_path).record((candidate(),), NOW)

    assert notifications.read_text(encoding="utf-8") == '{"sentinel": 1}'
    assert news.read_text(encoding="utf-8") == '{"sentinel": 2}'
    assert (
        Path("data/inplay_timing_audit.jsonl")
        == DEFAULT_INPLAY_TIMING_AUDIT_PATH
    )
    assert Path(
        "data/inplay_detection_state.json"
    ) == DEFAULT_INPLAY_DETECTION_STATE_PATH


def test_audit_write_failure_is_best_effort_and_format_is_unchanged(
    tmp_path: Path,
) -> None:
    blocked = tmp_path / "blocked"
    blocked.write_text("not a directory", encoding="utf-8")
    timing = InPlayTimingAuditor(
        JsonlInPlayTimingAuditStore(blocked / "audit.jsonl"),
        JsonInPlayDetectionStore(tmp_path / "detection.json"),
    )
    item = candidate().result
    before = format_inplay_report(InPlayReport(NOW, (item,)))

    timing.record((candidate(),), NOW)

    after = format_inplay_report(InPlayReport(NOW, (item,)))
    assert after == before


def test_retention_removes_only_rows_older_than_seven_days(tmp_path: Path) -> None:
    path = tmp_path / "inplay_timing_audit.jsonl"
    old = {"scanned_at": (NOW - timedelta(days=8)).isoformat(), "symbol": "OLD"}
    recent = {
        "scanned_at": (NOW - timedelta(days=6)).isoformat(),
        "symbol": "RECENT",
    }
    path.write_text(
        "\n".join((json.dumps(old), json.dumps(recent))) + "\n",
        encoding="utf-8",
    )

    auditor(tmp_path).record((candidate(),), NOW)

    rows = tuple(
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
    )
    assert tuple(row["symbol"] for row in rows) == ("RECENT", "BTCUSDT")


def test_large_telegram_audit_retention_streams_without_full_text_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recent = json.dumps(
        {"scanned_at": (NOW - timedelta(days=1)).isoformat(), "symbol": "RECENT"}
    )
    old = json.dumps(
        {"scanned_at": (NOW - timedelta(days=8)).isoformat(), "symbol": "OLD"}
    )
    stores = (
        JsonlEarlyDiscoveryAuditStore(tmp_path / "v1.jsonl"),
        JsonlEarlyDiscoveryV2AuditStore(tmp_path / "v2.jsonl"),
        JsonlInPlayTimingAuditStore(tmp_path / "timing.jsonl"),
    )
    for store in stores:
        store.path.write_text(
            "".join(f"{old if value == 0 else recent}\n" for value in range(20_000)),
            encoding="utf-8",
        )

    def forbidden_read_text(self: Path, *args: object, **kwargs: object) -> str:
        del self, args, kwargs
        raise AssertionError("full-file read_text retention is forbidden")

    monkeypatch.setattr(Path, "read_text", forbidden_read_text)
    for store in stores:
        store._prune_if_due(NOW)
        store._prune_if_due(NOW)
        with store.path.open("r", encoding="utf-8") as stream:
            assert sum(1 for _ in stream) == 19_999


def test_restart_after_partial_jsonl_line_starts_a_new_record(tmp_path: Path) -> None:
    path = tmp_path / "inplay_timing_audit.jsonl"
    path.write_text('{"scanned_at":', encoding="utf-8")

    auditor(tmp_path).record((candidate(),), NOW)

    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines[0] == '{"scanned_at":'
    assert json.loads(lines[1])["symbol"] == "BTCUSDT"
