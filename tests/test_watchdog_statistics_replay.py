from datetime import timedelta
from pathlib import Path

import pytest
from watchdog_evidence_helpers import NOW, evidence

from market_signal_assistant.watchdog.events.journal import WatchdogEventJournal
from market_signal_assistant.watchdog.events.replay import WatchdogJournalReplay
from market_signal_assistant.watchdog.outcomes.journal import WatchdogOutcomeJournal
from market_signal_assistant.watchdog.outcomes.models import PriceObservation
from market_signal_assistant.watchdog.outcomes.scheduler import (
    ForwardOutcomeScheduler,
    JsonOutcomeCheckpointStore,
)
from market_signal_assistant.watchdog.outcomes.statistics import (
    WatchdogDescriptiveStatistics,
)


def test_descriptive_statistics_and_offline_replay_are_deterministic(
    tmp_path: Path,
) -> None:
    event_path = tmp_path / "watchdog" / "events" / "events.jsonl"
    outcome_path = tmp_path / "watchdog" / "outcomes" / "outcomes.jsonl"
    events = WatchdogEventJournal(event_path)
    event = evidence()
    events.append(event)
    outcomes = WatchdogOutcomeJournal(outcome_path)
    scheduler = ForwardOutcomeScheduler(
        events,
        outcomes,
        JsonOutcomeCheckpointStore(tmp_path / "watchdog" / "state" / "pending.json"),
    )
    scheduler.register(event)
    for minutes, price in ((1, 101.0), (5, 98.0), (15, 103.0), (30, 99.0), (60, 104.0)):
        scheduler.observe(
            PriceObservation(
                "ABCUSDT",
                NOW + timedelta(minutes=minutes),
                NOW + timedelta(minutes=minutes),
                price,
            )
        )

    first = WatchdogJournalReplay(
        WatchdogEventJournal(event_path), WatchdogOutcomeJournal(outcome_path)
    ).replay()
    second = WatchdogJournalReplay(
        WatchdogEventJournal(event_path), WatchdogOutcomeJournal(outcome_path)
    ).replay()

    assert first == second
    assert first.statistics.event_count == 1
    assert dict(first.statistics.events_by_anomaly_type) == {"COMPRESSION": 1}
    assert dict(first.statistics.events_by_state) == {"WATCH": 1}
    assert dict(first.statistics.events_by_score_bucket) == {"60-079": 1}
    assert dict(first.statistics.median_abs_return_by_horizon)[1] == pytest.approx(0.01)
    assert first.statistics.missing_outcome_rate == 0.0
    assert first.statistics.late_outcome_rate == 0.0
    assert first.statistics.expansion_rate == 1.0
    assert first.statistics.median_time_to_expansion_seconds == 60.0
    assert {item.dimension for item in first.statistics.breakdowns} == {
        "anomaly_type",
        "anomaly_combination",
        "score_bucket",
        "symbol",
        "universe_tier",
    }
    assert first.transitions[0].state_before.value == "NORMAL"
    assert first.transitions[0].state_after.value == "WATCH"


def test_statistics_report_missing_and_late_rates(tmp_path: Path) -> None:
    events = WatchdogEventJournal(tmp_path / "events.jsonl")
    event = evidence()
    events.append(event)
    outcomes = WatchdogOutcomeJournal(tmp_path / "outcomes.jsonl")
    scheduler = ForwardOutcomeScheduler(
        events,
        outcomes,
        JsonOutcomeCheckpointStore(tmp_path / "pending.json"),
    )
    scheduler.register(event)
    scheduler.observe(
        PriceObservation(
            "ABCUSDT",
            NOW + timedelta(minutes=2),
            NOW + timedelta(minutes=3),
            101.0,
        )
    )
    scheduler.mark_missing(
        as_of=NOW + timedelta(minutes=70), grace=timedelta(minutes=5)
    )

    report = WatchdogDescriptiveStatistics().analyze(
        events.records(), outcomes.records()
    )

    assert report.late_outcome_rate == pytest.approx(0.2)
    assert report.missing_outcome_rate == pytest.approx(0.8)
