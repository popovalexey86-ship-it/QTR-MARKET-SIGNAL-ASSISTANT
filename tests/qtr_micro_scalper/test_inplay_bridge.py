from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta, timezone
from threading import Thread

import pytest

from market_signal_assistant.qtr_micro_scalper.inplay_bridge import (
    InPlayTargetBridge,
    InPlayTargetBridgeConfig,
    ScalperTarget,
    ScalperTargetLifecycle,
)

NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def target(
    symbol: str = "BTCUSDT",
    *,
    discovered_at: datetime = NOW,
    priority: float = 80.0,
    volatility: float = 70.0,
    volume: float = 75.0,
    liquidity: float = 85.0,
    reason: str = "🔥 Эта монета сейчас в игре.",
) -> ScalperTarget:
    return ScalperTarget(
        symbol=symbol,
        discovered_at=discovered_at,
        source="QTR_SCANNER",
        reason=reason,
        priority=priority,
        volatility_score=volatility,
        volume_score=volume,
        liquidity_score=liquidity,
    )


def test_target_is_immutable_normalized_and_utc_aware() -> None:
    berlin = timezone(timedelta(hours=2))
    item = target(" btcusdt ", discovered_at=NOW.astimezone(berlin))
    assert item.symbol == "BTCUSDT"
    assert item.discovered_at == NOW
    assert item.discovered_at.tzinfo is UTC
    with pytest.raises(FrozenInstanceError):
        item.priority = 1.0  # type: ignore[misc]


def test_discovery_starts_deep_analysis_lifecycle() -> None:
    bridge = InPlayTargetBridge()
    decision = bridge.discover(target(), observed_at=NOW)
    assert decision.accepted is True
    assert decision.target is not None
    assert decision.target.lifecycle is ScalperTargetLifecycle.DISCOVERED
    assert decision.reason.startswith("🔥")
    assert bridge.watched_targets() == (decision.target,)


def test_target_lifecycle_progresses_to_active_and_removed() -> None:
    bridge = InPlayTargetBridge()
    bridge.discover(target(), observed_at=NOW)
    watching = bridge.begin_watching(
        "BTCUSDT",
        changed_at=NOW + timedelta(seconds=1),
    )
    active = bridge.activate(
        "BTCUSDT",
        changed_at=NOW + timedelta(seconds=2),
    )
    removed = bridge.remove(
        "BTCUSDT",
        changed_at=NOW + timedelta(seconds=3),
        reason="Scanner activity faded.",
    )
    assert watching.lifecycle is ScalperTargetLifecycle.WATCHING
    assert active.lifecycle is ScalperTargetLifecycle.ACTIVE
    assert removed.lifecycle is ScalperTargetLifecycle.REMOVED
    assert removed.reason == "Scanner activity faded."
    assert bridge.watched_targets() == ()


def test_invalid_lifecycle_transition_is_rejected() -> None:
    bridge = InPlayTargetBridge()
    bridge.discover(target(), observed_at=NOW)
    with pytest.raises(ValueError, match="requires WATCHING"):
        bridge.activate("BTCUSDT", changed_at=NOW + timedelta(seconds=1))


def test_duplicate_refresh_does_not_create_second_target() -> None:
    bridge = InPlayTargetBridge()
    first = target(priority=70.0, reason="Initial activity.")
    stronger = target(priority=90.0, reason="Breakout activity.")
    bridge.discover(first, observed_at=NOW)
    refreshed = bridge.discover(
        stronger,
        observed_at=NOW + timedelta(seconds=30),
    )
    assert refreshed.accepted is True
    assert refreshed.target is not None
    assert refreshed.target.priority == 90.0
    assert refreshed.target.reason == "Breakout activity."
    assert refreshed.target.discovered_at == NOW
    assert len(bridge.watched_targets()) == 1


def test_sync_deduplicates_symbol_and_keeps_stronger_candidate() -> None:
    bridge = InPlayTargetBridge()
    result = bridge.sync(
        (
            target(priority=60.0, reason="Weak observation."),
            target(priority=95.0, reason="Strong observation."),
        ),
        observed_at=NOW,
    )
    assert len(result.discovered) == 1
    assert result.watched[0].priority == 95.0
    assert result.watched[0].reason == "Strong observation."


def test_max_watched_limit_selects_highest_priority_deterministically() -> None:
    candidates = (
        target("CCCUSDT", priority=60.0),
        target("AAAUSDT", priority=90.0),
        target("BBBUSDT", priority=80.0),
    )
    bridge = InPlayTargetBridge(InPlayTargetBridgeConfig(max_watched_symbols=2))
    result = bridge.sync(candidates, observed_at=NOW)
    assert [item.symbol for item in result.watched] == ["AAAUSDT", "BBBUSDT"]
    assert result.suppressed_symbols == ("CCCUSDT",)


def test_ordering_uses_priority_liquidity_volume_volatility_and_symbol() -> None:
    candidates = (
        target("DDDUSDT", priority=80.0, liquidity=80.0, volume=90.0),
        target("CCCUSDT", priority=90.0, liquidity=70.0),
        target("BBBUSDT", priority=80.0, liquidity=90.0),
        target("AAAUSDT", priority=80.0, liquidity=80.0, volume=90.0),
    )
    result = InPlayTargetBridge().sync(candidates, observed_at=NOW)
    assert [item.symbol for item in result.watched] == [
        "CCCUSDT",
        "BBBUSDT",
        "AAAUSDT",
        "DDDUSDT",
    ]


def test_cooldown_suppresses_then_allows_rediscovery() -> None:
    bridge = InPlayTargetBridge(InPlayTargetBridgeConfig(cooldown_seconds=300))
    bridge.discover(target(), observed_at=NOW)
    bridge.remove("BTCUSDT", changed_at=NOW + timedelta(seconds=1))

    early = bridge.discover(
        target(discovered_at=NOW + timedelta(seconds=100)),
        observed_at=NOW + timedelta(seconds=100),
    )
    assert early.accepted is False
    assert early.reason == "Target cooldown is active."

    later = bridge.discover(
        target(discovered_at=NOW + timedelta(seconds=301)),
        observed_at=NOW + timedelta(seconds=301),
    )
    assert later.accepted is True
    assert later.target is not None
    assert later.target.lifecycle is ScalperTargetLifecycle.DISCOVERED


def test_expiration_removes_stale_target_at_boundary() -> None:
    bridge = InPlayTargetBridge(InPlayTargetBridgeConfig(expiration_seconds=60))
    bridge.discover(target(), observed_at=NOW)
    assert bridge.expire(observed_at=NOW + timedelta(seconds=59)) == ()
    expired = bridge.expire(observed_at=NOW + timedelta(seconds=60))
    assert len(expired) == 1
    assert expired[0].lifecycle is ScalperTargetLifecycle.REMOVED
    assert "expired" in expired[0].reason


def test_duplicate_observation_refreshes_expiration_clock() -> None:
    bridge = InPlayTargetBridge(InPlayTargetBridgeConfig(expiration_seconds=60))
    item = target()
    bridge.discover(item, observed_at=NOW)
    bridge.discover(item, observed_at=NOW + timedelta(seconds=50))
    assert bridge.expire(observed_at=NOW + timedelta(seconds=100)) == ()
    assert len(bridge.expire(observed_at=NOW + timedelta(seconds=110))) == 1


def test_fresh_observation_at_expiration_boundary_is_not_removed() -> None:
    bridge = InPlayTargetBridge(InPlayTargetBridgeConfig(expiration_seconds=60))
    item = target()
    bridge.discover(item, observed_at=NOW)
    refreshed = bridge.discover(item, observed_at=NOW + timedelta(seconds=60))
    assert refreshed.accepted is True
    assert refreshed.target is not None
    assert refreshed.target.lifecycle is ScalperTargetLifecycle.DISCOVERED
    assert len(bridge.watched_targets()) == 1


def test_sync_reports_expired_targets_separately() -> None:
    bridge = InPlayTargetBridge(InPlayTargetBridgeConfig(expiration_seconds=60))
    bridge.sync((target("BTCUSDT"),), observed_at=NOW)
    result = bridge.sync(
        (target("ETHUSDT", discovered_at=NOW + timedelta(seconds=60)),),
        observed_at=NOW + timedelta(seconds=60),
    )
    assert [item.symbol for item in result.removed] == ["BTCUSDT"]
    assert [item.symbol for item in result.discovered] == ["ETHUSDT"]


def test_same_input_produces_same_ordered_output() -> None:
    candidates = (
        target("SOLUSDT", priority=80.0),
        target("BTCUSDT", priority=90.0),
        target("ETHUSDT", priority=85.0),
    )
    forward = InPlayTargetBridge().sync(candidates, observed_at=NOW)
    reversed_result = InPlayTargetBridge().sync(
        tuple(reversed(candidates)),
        observed_at=NOW,
    )
    assert forward == reversed_result


def test_non_discovered_scanner_input_is_safely_rejected() -> None:
    item = replace(target(), lifecycle=ScalperTargetLifecycle.WATCHING)
    decision = InPlayTargetBridge().discover(item, observed_at=NOW)
    assert decision.accepted is False
    assert decision.target is None


def test_timestamp_and_score_validation() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        target(discovered_at=datetime(2026, 8, 16, 12))
    with pytest.raises(ValueError, match="between 0 and 100"):
        target(priority=101.0)
    with pytest.raises(ValueError, match="future"):
        InPlayTargetBridge().discover(
            target(discovered_at=NOW + timedelta(seconds=1)),
            observed_at=NOW,
        )


def test_observations_must_be_chronological() -> None:
    bridge = InPlayTargetBridge()
    bridge.discover(target(), observed_at=NOW + timedelta(seconds=10))
    with pytest.raises(ValueError, match="chronological"):
        bridge.expire(observed_at=NOW)


def test_concurrent_duplicate_discovery_remains_single_target() -> None:
    bridge = InPlayTargetBridge()
    threads = [
        Thread(target=bridge.discover, args=(target(),), kwargs={"observed_at": NOW})
        for _ in range(10)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(bridge.watched_targets()) == 1


def test_config_validation() -> None:
    with pytest.raises(ValueError, match="max watched"):
        InPlayTargetBridgeConfig(max_watched_symbols=0)
    with pytest.raises(ValueError, match="expiration"):
        InPlayTargetBridgeConfig(expiration_seconds=0)
    with pytest.raises(ValueError, match="cooldown"):
        InPlayTargetBridgeConfig(cooldown_seconds=-1)
