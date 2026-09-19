import json
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
from watchdog_evidence_helpers import NOW, evidence

from market_signal_assistant.watchdog.events.journal import WatchdogEventJournal
from market_signal_assistant.watchdog.journal import JournalConflictError
from market_signal_assistant.watchdog.outcomes.journal import WatchdogOutcomeJournal
from market_signal_assistant.watchdog.outcomes.models import (
    BreakoutSide,
    OutcomeDataQuality,
    PriceObservation,
)
from market_signal_assistant.watchdog.outcomes.price_journal import (
    WatchdogPriceJournal,
)
from market_signal_assistant.watchdog.outcomes.scheduler import (
    ForwardOutcomeScheduler,
    JsonOutcomeCheckpointStore,
    OutcomeCheckpointError,
)


def scheduler(
    root: Path,
) -> tuple[ForwardOutcomeScheduler, WatchdogOutcomeJournal]:
    events = WatchdogEventJournal(root / "watchdog" / "events" / "events.jsonl")
    event = evidence()
    events.append(event)
    outcomes = WatchdogOutcomeJournal(
        root / "watchdog" / "outcomes" / "outcomes.jsonl"
    )
    result = ForwardOutcomeScheduler(
        events,
        outcomes,
        JsonOutcomeCheckpointStore(root / "watchdog" / "state" / "pending.json"),
    )
    result.register(event)
    return result, outcomes


def point(
    minutes: int,
    price: float,
    *,
    available_minutes: int | None = None,
    high: float | None = None,
    low: float | None = None,
) -> PriceObservation:
    return PriceObservation(
        "ABCUSDT",
        NOW + timedelta(minutes=minutes),
        NOW + timedelta(
            minutes=available_minutes if available_minutes is not None else minutes
        ),
        price,
        high,
        low,
    )


def test_direction_neutral_outcome_metrics_and_lateness(tmp_path: Path) -> None:
    tracker, outcomes = scheduler(tmp_path)

    one = tracker.observe(point(1, 101.0, high=102.0, low=99.0))[0]
    five = tracker.observe(point(6, 98.0, available_minutes=7))[0]

    assert one.horizon_minutes == 1
    assert one.signed_return == pytest.approx(0.01)
    assert one.abs_return == pytest.approx(0.01)
    assert one.mfe_up == pytest.approx(0.02)
    assert one.mfe_down == pytest.approx(0.01)
    assert one.max_abs_excursion == pytest.approx(0.02)
    assert one.realized_range_after_event == pytest.approx(0.03)
    assert one.did_expansion_occur is True
    assert one.breakout_side is BreakoutSide.BOTH
    assert five.horizon_minutes == 5
    assert five.data_quality is OutcomeDataQuality.LATE
    assert five.lateness_seconds == 120.0
    assert five.observed_at == NOW + timedelta(minutes=6)
    assert five.available_at == NOW + timedelta(minutes=7)
    payload = json.loads(outcomes.path.read_text(encoding="utf-8").splitlines()[0])
    assert payload["horizon"] == "1m"
    assert payload["return"] == pytest.approx(0.01)
    assert payload["mfe"] == pytest.approx(0.02)
    assert payload["mae"] == pytest.approx(0.01)
    assert payload["max_abs_move"] == pytest.approx(0.02)
    raw = WatchdogPriceJournal(
        tmp_path / "watchdog" / "outcomes" / "price_observations.jsonl"
    ).records()
    assert [item.price for item in raw] == [101.0, 98.0]


def test_all_horizons_and_duplicate_outcome_attempt_are_idempotent(
    tmp_path: Path,
) -> None:
    tracker, outcomes = scheduler(tmp_path)
    created = []
    for minutes in (1, 5, 15, 30, 60):
        created.extend(tracker.observe(point(minutes, 100.0 + minutes / 10)))

    assert [item.horizon_minutes for item in created] == [1, 5, 15, 30, 60]
    assert tracker.pending_horizons() == ()
    assert outcomes.append(created[0]) is False
    with pytest.raises(JournalConflictError, match="Conflicting"):
        outcomes.append(replace(created[0], reference_price=99.0))


def test_missing_outcomes_are_explicit_immutable_records(tmp_path: Path) -> None:
    tracker, _ = scheduler(tmp_path)

    missing = tracker.mark_missing(
        as_of=NOW + timedelta(minutes=70), grace=timedelta(minutes=5)
    )

    assert [item.horizon_minutes for item in missing] == [1, 5, 15, 30, 60]
    assert all(item.data_quality is OutcomeDataQuality.MISSING for item in missing)
    assert all(item.horizon_price is None for item in missing)
    assert tracker.pending_horizons() == ()


class FailingCheckpoint(JsonOutcomeCheckpointStore):
    fail = False

    def save(self, points: dict[str, tuple[PriceObservation, ...]]) -> None:
        if self.fail:
            raise OutcomeCheckpointError("simulated crash after outcome append")
        super().save(points)


def test_crash_after_outcome_append_recovers_without_duplicate(tmp_path: Path) -> None:
    events = WatchdogEventJournal(tmp_path / "events.jsonl")
    event = evidence()
    events.append(event)
    outcomes = WatchdogOutcomeJournal(tmp_path / "outcomes.jsonl")
    checkpoint = FailingCheckpoint(tmp_path / "pending.json")
    tracker = ForwardOutcomeScheduler(events, outcomes, checkpoint)
    tracker.register(event)
    checkpoint.fail = True

    with pytest.raises(OutcomeCheckpointError, match="simulated crash"):
        tracker.observe(point(1, 101.0))
    assert len(outcomes.records()) == 1

    restarted = ForwardOutcomeScheduler(
        WatchdogEventJournal(tmp_path / "events.jsonl"),
        WatchdogOutcomeJournal(tmp_path / "outcomes.jsonl"),
        JsonOutcomeCheckpointStore(tmp_path / "pending.json"),
    )
    assert [item.horizon_minutes for item in restarted.pending_horizons()] == [
        5,
        15,
        30,
        60,
    ]
    assert restarted.observe(point(1, 101.0)) == ()
    assert len(WatchdogOutcomeJournal(tmp_path / "outcomes.jsonl").records()) == 1


def test_crash_before_outcome_append_is_replayable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tracker, outcomes = scheduler(tmp_path)

    def crash_before_append(record: object) -> bool:
        del record
        raise OSError("simulated crash before outcome append")

    monkeypatch.setattr(outcomes, "append", crash_before_append)
    with pytest.raises(OSError, match="before outcome append"):
        tracker.observe(point(1, 101.0))
    assert outcomes.records() == ()

    restarted, recovered_outcomes = scheduler(tmp_path)
    created = restarted.recover_pending()
    assert len(created) == 1
    assert len(recovered_outcomes.records()) == 1


def test_restart_with_pending_horizons_restores_price_path(tmp_path: Path) -> None:
    tracker, _ = scheduler(tmp_path)
    tracker.observe(point(1, 101.0, high=102.0, low=99.0))

    events = WatchdogEventJournal(
        tmp_path / "watchdog" / "events" / "events.jsonl"
    )
    outcomes = WatchdogOutcomeJournal(
        tmp_path / "watchdog" / "outcomes" / "outcomes.jsonl"
    )
    restarted = ForwardOutcomeScheduler(
        events,
        outcomes,
        JsonOutcomeCheckpointStore(
            tmp_path / "watchdog" / "state" / "pending.json"
        ),
    )
    five = restarted.observe(point(5, 98.0))[0]

    assert five.horizon_minutes == 5
    assert five.mfe_up == pytest.approx(0.02)
    assert five.mfe_down == pytest.approx(0.02)


def test_restart_after_event_append_before_registration_recovers_pending(
    tmp_path: Path,
) -> None:
    events = WatchdogEventJournal(tmp_path / "events.jsonl")
    events.append(evidence())

    restarted = ForwardOutcomeScheduler(
        WatchdogEventJournal(tmp_path / "events.jsonl"),
        WatchdogOutcomeJournal(tmp_path / "outcomes.jsonl"),
        JsonOutcomeCheckpointStore(tmp_path / "pending.json"),
    )

    assert [item.horizon_minutes for item in restarted.pending_horizons()] == [
        1,
        5,
        15,
        30,
        60,
    ]


def test_outcome_partial_tail_is_reported_without_losing_complete_record(
    tmp_path: Path,
) -> None:
    tracker, outcomes = scheduler(tmp_path)
    tracker.observe(point(1, 101.0))
    with outcomes.path.open("ab") as stream:
        stream.write(b"{partial-outcome")

    restarted = WatchdogOutcomeJournal(outcomes.path)

    assert len(restarted.records()) == 1
    assert restarted.recovery.partial_tail is True
