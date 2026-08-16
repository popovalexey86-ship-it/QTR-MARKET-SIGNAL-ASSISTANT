from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import cast

from market_signal_assistant.inplay.early_discovery import MarketDirection
from market_signal_assistant.inplay.early_discovery_v2 import EarlyDiscoveryV2Result
from market_signal_assistant.qtr_setup_pilot.audit import (
    JsonlQtrSetupTelegramAuditStore,
    QtrSetupAuditOutcome,
)
from market_signal_assistant.qtr_setup_pilot.models import QtrSetupCandidate
from market_signal_assistant.qtr_setup_pilot.notifications import (
    JsonQtrSetupNotificationStore,
    QtrSetupNotificationService,
)
from market_signal_assistant.qtr_setup_pilot.service import _structural_invalidation
from market_signal_assistant.setup_engine.analyzer import analyze_setup
from market_signal_assistant.setup_engine.models import (
    SetupAnalysisInput,
    SetupDirection,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def _source(*, invalidation: float | None = 98.0) -> SetupAnalysisInput:
    return SetupAnalysisInput(
        snapshot_ids=("scan-1",),
        source="early_discovery_v2",
        symbol="BTCUSDT",
        analyzed_at=NOW,
        direction=SetupDirection.UP,
        current_price=101.0,
        trigger_level=100.0,
        invalidation_level=invalidation,
        distance_to_trigger_pct=1.0,
        distance_to_trigger_atr=0.5,
        breakout_age_bars=1,
        hold_candles=2,
        breakout_confirmed=True,
        correct_side_of_level=True,
        volume_confirmation=True,
        volatility_confirmation=True,
        structure_confirmation=True,
        liquidity_ok=True,
        spread_pct=0.1,
        compression_detected=False,
        conflicting_confirmations=False,
        completed_candles=3,
    )


def _write_record(path: Path, *, complete: bool) -> dict[str, object]:
    source = _source(invalidation=98.0 if complete else None)
    candidate = QtrSetupCandidate(
        episode_id="episode-1",
        source_input=source,
        result=analyze_setup(source),
        atr_value=2.0 if complete else None,
        local_range_low=98.0 if complete else None,
        local_range_high=100.0 if complete else None,
    )
    notifications = QtrSetupNotificationService(
        JsonQtrSetupNotificationStore(path.with_suffix(".state.json"))
    )
    decision = notifications.prepare((candidate,), NOW).decisions[0]
    JsonlQtrSetupTelegramAuditStore(path).append(
        (QtrSetupAuditOutcome(decision, False, False),),
        NOW,
    )
    return cast(dict[str, object], json.loads(path.read_text(encoding="utf-8")))


def _scalper_price_context_fixture(
    record: dict[str, object],
) -> dict[str, object] | None:
    projection = record.get("price_context")
    if not isinstance(projection, dict):
        return None
    required = (
        "observed_at",
        "source_direction",
        "setup_direction",
        "market_price",
        "atr",
        "trigger_price",
        "invalidation_price",
        "local_range_low",
        "local_range_high",
        "setup_state",
    )
    if any(projection.get(name) is None for name in required):
        return None
    if projection["source_direction"] != projection["setup_direction"]:
        return None
    return projection


def test_producer_writes_complete_verified_projection(tmp_path: Path) -> None:
    record = _write_record(tmp_path / "audit.jsonl", complete=True)

    projection = _scalper_price_context_fixture(record)

    assert projection is not None
    assert record["symbol"] == "BTCUSDT"
    assert projection["setup_direction"] == "UP"
    assert projection["market_price"] == 101.0
    assert projection["trigger_price"] == 100.0
    assert projection["invalidation_price"] == 98.0
    assert projection["atr"] == 2.0
    assert projection["local_range_low"] == 98.0
    assert projection["local_range_high"] == 100.0
    assert projection["observed_at"] == NOW.isoformat()


def test_missing_verified_values_remain_null_without_fallback(tmp_path: Path) -> None:
    record = _write_record(tmp_path / "audit.jsonl", complete=False)

    assert _scalper_price_context_fixture(record) is None
    projection = cast(dict[str, object], record["price_context"])
    assert projection["atr"] is None
    assert projection["invalidation_price"] is None
    assert projection["local_range_low"] is None
    assert projection["local_range_high"] is None


def test_structural_invalidation_uses_observed_range_only() -> None:
    long_source = cast(
        EarlyDiscoveryV2Result,
        SimpleNamespace(
            local_range_low=98.0,
            local_range_high=100.0,
            market_direction=MarketDirection.UP,
        ),
    )
    short_source = cast(
        EarlyDiscoveryV2Result,
        SimpleNamespace(
            local_range_low=98.0,
            local_range_high=100.0,
            market_direction=MarketDirection.DOWN,
        ),
    )
    missing = cast(
        EarlyDiscoveryV2Result,
        SimpleNamespace(
            local_range_low=None,
            local_range_high=None,
            market_direction=MarketDirection.UP,
        ),
    )

    assert _structural_invalidation(long_source) == 98.0
    assert _structural_invalidation(short_source) == 100.0
    assert _structural_invalidation(missing) is None


def test_legacy_audit_record_remains_readable_and_has_no_projection(
    tmp_path: Path,
) -> None:
    path = tmp_path / "audit.jsonl"
    legacy = {"timestamp": NOW.isoformat(), "symbol": "BTCUSDT", "decision": "send"}
    path.write_text(json.dumps(legacy) + "\n", encoding="utf-8")

    loaded = cast(dict[str, object], json.loads(path.read_text(encoding="utf-8")))

    assert loaded == legacy
    assert _scalper_price_context_fixture(loaded) is None


def test_production_scanner_and_telegram_wiring_import_cleanly() -> None:
    import market_signal_assistant.composition as composition
    import market_signal_assistant.qtr_setup_pilot as qtr_setup_pilot
    import market_signal_assistant.telegram.bot as telegram_bot

    assert callable(composition.build_early_discovery_v2_service)
    assert qtr_setup_pilot.QtrSetupCandidate is QtrSetupCandidate
    assert callable(telegram_bot._run_sdk_bot_handlers)
