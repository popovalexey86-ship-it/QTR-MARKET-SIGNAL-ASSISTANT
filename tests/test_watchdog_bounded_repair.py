from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_signal_assistant.watchdog.baselines import (
    BaselineObservation,
    JsonBaselineStore,
    RollingBaselineEngine,
)
from market_signal_assistant.watchdog.runtime.retention import (
    InactiveRetention,
    InactiveSymbolArchive,
)
from market_signal_assistant.watchdog.runtime.schedule import (
    BucketCursorError,
    JsonBucketCursorStore,
)
from market_signal_assistant.watchdog.runtime.universe_evidence import (
    UniverseTransition,
    UniverseTransitionJournal,
)
from market_signal_assistant.watchdog.state_machine import (
    StateEvaluation,
    WatchdogStateMachine,
    WatchdogSymbolState,
)
from market_signal_assistant.watchdog.state_store import (
    JsonWatchdogStateStore,
    WatchdogRuntimeState,
    WatchdogStateRepository,
)

NOW = datetime(2026, 9, 24, tzinfo=UTC)


def _parts(
    root: Path, *, maximum_samples: int = 3
) -> tuple[
    RollingBaselineEngine,
    WatchdogStateRepository,
    JsonBucketCursorStore,
    InactiveSymbolArchive,
    UniverseTransitionJournal,
    InactiveRetention,
]:
    baselines = RollingBaselineEngine(
        JsonBaselineStore(root / "baselines.json"),
        minimum_samples=2,
        maximum_samples=maximum_samples,
    )
    states = WatchdogStateRepository(JsonWatchdogStateStore(root / "states.json"))
    cursors = JsonBucketCursorStore(root / "cursors.json")
    archive = InactiveSymbolArchive(root / "inactive.sqlite3")
    transitions = UniverseTransitionJournal(root / "transitions.jsonl")
    retention = InactiveRetention(
        baselines, states, cursors, archive, transitions, window=timedelta(hours=1)
    )
    return baselines, states, cursors, archive, transitions, retention


def _transition(
    symbol: str,
    when: datetime,
    previous: str | None,
    new: str | None,
    reasons: tuple[str, ...] = (),
) -> UniverseTransition:
    return UniverseTransition(
        symbol=symbol,
        previous_eligible=previous is not None,
        new_eligible=new is not None,
        previous_tier=previous,
        new_tier=new,
        previous_state="NORMAL",
        new_state="NORMAL",
        rejection_reasons=reasons,
        observed_at=when,
        available_at=when + timedelta(seconds=1),
        recorded_at=when + timedelta(seconds=2),
    )


def test_universe_transition_is_append_only_pit_and_recoverable(tmp_path: Path) -> None:
    journal = UniverseTransitionJournal(tmp_path / "transitions.jsonl")
    added = _transition("abcusdt", NOW, None, "COLD_START")
    promoted = _transition(
        "ABCUSDT", NOW + timedelta(minutes=15), "COLD_START", "ACTIVE"
    )
    removed = _transition(
        "ABCUSDT",
        NOW + timedelta(minutes=30),
        "ACTIVE",
        None,
        ("turnover_below_minimum",),
    )
    assert journal.append(added)
    assert not journal.append(added)
    assert journal.append(promoted)
    assert journal.append(removed)
    assert removed.payload()["rejection_reasons"] == ["turnover_below_minimum"]
    assert (
        removed.payload()["recorded_at"]
        == (NOW + timedelta(minutes=30, seconds=2)).isoformat()
    )
    assert journal.current() == {}
    assert journal.last_removals({"ABCUSDT"}) == {
        "ABCUSDT": NOW + timedelta(minutes=30)
    }
    assert journal.retained_index_entries == 0
    restarted = UniverseTransitionJournal(tmp_path / "transitions.jsonl")
    assert restarted.current() == {}
    assert restarted.retained_index_entries == 0
    assert restarted.append(
        _transition("ABCUSDT", NOW + timedelta(hours=2), None, "COLD_START")
    )
    assert restarted.current() == {"ABCUSDT": ("COLD_START", "NORMAL")}
    assert restarted.last_removals({"ABCUSDT"}) == {}
    with pytest.raises(ValueError, match="PIT ordered"):
        UniverseTransition(
            "ABCUSDT",
            False,
            True,
            None,
            "COLD_START",
            None,
            None,
            (),
            NOW,
            NOW + timedelta(seconds=2),
            NOW + timedelta(seconds=1),
        )


def test_inactive_expiry_eviction_cold_reentry_and_durable_chronology(
    tmp_path: Path,
) -> None:
    baselines, states, cursors, archive, transitions, retention = _parts(tmp_path)
    for index in range(5):
        available = NOW + timedelta(minutes=index)
        baselines.observe(
            BaselineObservation(
                "ABCUSDT", "volume", float(index), available, available
            ),
            detected_at=available,
        )
    assert baselines.retained_counts == (1, 3, 3)
    previous_state = WatchdogRuntimeState(
        WatchdogSymbolState.initial("ABCUSDT", detected_at=NOW)
    )
    states.save(previous_state)
    boundary = NOW + timedelta(minutes=15)
    cursors.save("ABCUSDT", "15", boundary)
    removed_at = NOW + timedelta(minutes=20)
    transitions.append(_transition("ABCUSDT", removed_at, "ACTIVE", None))
    retention.removed("ABCUSDT", removed_at)
    assert retention.expire(removed_at + timedelta(minutes=59), set()) == ()
    assert baselines.retained_counts == (1, 3, 3)
    assert retention.expire(removed_at + timedelta(hours=1), set()) == ("ABCUSDT",)
    assert baselines.retained_counts == (0, 0, 3)
    assert states.persisted("ABCUSDT") is None
    assert cursors.get("ABCUSDT", "15") is None
    tombstone = archive.get("ABCUSDT")
    assert tombstone is not None
    assert tombstone.chronology_floor == NOW
    assert tombstone.cursors == {"15": boundary}
    assert archive.record_count == 1

    # Simulate process restart from durable snapshots and tombstone.
    restored, restarted_states, restarted_cursors, restarted_archive, _, restarted = (
        _parts(tmp_path)
    )
    assert restored.retained_counts == (0, 0, 3)
    assert restarted.activated("ABCUSDT")
    assert restarted_archive.record_count == 0
    assert restarted_states.persisted("ABCUSDT") == previous_state
    assert restarted_cursors.get("ABCUSDT", "15") == boundary
    assert restored.snapshot(
        "ABCUSDT", "volume", detected_at=removed_at + timedelta(hours=2)
    ).cold_start
    with pytest.raises(BucketCursorError, match="backwards"):
        restarted_cursors.save("ABCUSDT", "15", NOW)
    persisted = restarted_states.persisted("ABCUSDT")
    assert persisted is not None
    with pytest.raises(ValueError, match="State evaluations must be chronological"):
        WatchdogStateMachine().evaluate(
            persisted.symbol_state,
            StateEvaluation(
                0.0, NOW - timedelta(minutes=1), NOW - timedelta(minutes=1), ("test",)
            ),
        )


def test_reentry_before_retention_expiry_also_starts_cold(tmp_path: Path) -> None:
    baselines, states, cursors, archive, transitions, retention = _parts(tmp_path)
    for index in range(3):
        available = NOW + timedelta(minutes=index)
        baselines.observe(
            BaselineObservation("ABCUSDT", "volume", 1.0, available, available),
            detected_at=available,
        )
    cursors.save("ABCUSDT", "15m", NOW)
    states.save(
        WatchdogRuntimeState(WatchdogSymbolState.initial("ABCUSDT", detected_at=NOW))
    )
    removed_at = NOW + timedelta(minutes=15)
    transitions.append(_transition("ABCUSDT", removed_at, "ACTIVE", None))
    retention.removed("ABCUSDT", removed_at)
    assert baselines.retained_counts[1] == 3
    assert retention.activated("ABCUSDT")
    assert archive.record_count == 0
    assert baselines.retained_counts[1] == 0
    assert states.persisted("ABCUSDT") is not None
    assert cursors.get("ABCUSDT", "15m") == NOW


def test_bounded_stress_many_transient_keys_and_eviction_plateau(
    tmp_path: Path,
) -> None:
    # 1,200 transient symbols x 3 keys x 5 samples exceeds 3,000 rings and
    # forces every 3-sample ring past its eviction boundary.
    count, features = 1200, 3
    observations = tuple(
        BaselineObservation(
            f"S{symbol:04d}USDT",
            f"feature_{feature}",
            float(sample),
            NOW + timedelta(minutes=sample),
            NOW + timedelta(minutes=sample),
        )
        for symbol in range(count)
        for feature in range(features)
        for sample in range(5)
    )
    JsonBaselineStore(tmp_path / "baselines.json").save(observations)
    baselines, states, cursors, archive, transitions, retention = _parts(tmp_path)
    assert baselines.retained_counts == (count * features, count * features * 3, 3)
    for symbol in range(count):
        name = f"S{symbol:04d}USDT"
        states.save(
            WatchdogRuntimeState(WatchdogSymbolState.initial(name, detected_at=NOW))
        )
        cursors.save(name, "15m", NOW)
    assert states.retained_count == count
    assert cursors.retained_count == count
    active = {f"S{symbol:04d}USDT" for symbol in range(20)}
    for symbol in range(20, count):
        name = f"S{symbol:04d}USDT"
        transitions.append(
            _transition(name, NOW + timedelta(hours=1), "STANDARD", None)
        )
        retention.removed(name, NOW + timedelta(hours=1))
    evicted = retention.expire(NOW + timedelta(hours=2), active)
    assert len(evicted) == count - len(active)
    assert baselines.retained_counts == (
        len(active) * features,
        len(active) * features * 3,
        3,
    )
    assert retention.expire(NOW + timedelta(hours=3), active) == ()
    assert baselines.retained_counts[1] == len(active) * features * 3
    assert archive.record_count == len(evicted)
    assert states.retained_count == len(active)
    assert cursors.retained_count == len(active)
    assert transitions.retained_index_entries == 0
