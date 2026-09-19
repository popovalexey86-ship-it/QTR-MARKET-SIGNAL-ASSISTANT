from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from market_signal_assistant.watchdog.events.journal import WatchdogEventJournal
from market_signal_assistant.watchdog.events.models import WatchdogEventEvidence
from market_signal_assistant.watchdog.models import WatchdogState
from market_signal_assistant.watchdog.outcomes.journal import WatchdogOutcomeJournal
from market_signal_assistant.watchdog.outcomes.models import ForwardOutcome
from market_signal_assistant.watchdog.outcomes.statistics import (
    WatchdogDescriptiveStatistics,
    WatchdogStatisticsReport,
)


@dataclass(frozen=True, slots=True)
class ReplayedTransition:
    event_id: str
    symbol: str
    detected_at: datetime
    state_before: WatchdogState
    state_after: WatchdogState


@dataclass(frozen=True, slots=True)
class WatchdogReplayResult:
    events: tuple[WatchdogEventEvidence, ...]
    outcomes: tuple[ForwardOutcome, ...]
    transitions: tuple[ReplayedTransition, ...]
    statistics: WatchdogStatisticsReport


class WatchdogJournalReplay:
    """Deterministic offline replay with no provider/network dependency."""

    def __init__(
        self,
        events: WatchdogEventJournal,
        outcomes: WatchdogOutcomeJournal,
    ) -> None:
        self._events = events
        self._outcomes = outcomes

    def replay(self) -> WatchdogReplayResult:
        events = tuple(
            sorted(
                self._events.records(),
                key=lambda item: (item.detected_at, item.event_id),
            )
        )
        outcomes = tuple(
            sorted(
                self._outcomes.records(),
                key=lambda item: (
                    item.target_time,
                    item.event_id,
                    item.horizon_minutes,
                ),
            )
        )
        transitions = tuple(
            ReplayedTransition(
                item.event_id,
                item.symbol,
                item.detected_at,
                item.state_before,
                item.state_after,
            )
            for item in events
        )
        statistics = WatchdogDescriptiveStatistics().analyze(events, outcomes)
        return WatchdogReplayResult(events, outcomes, transitions, statistics)
