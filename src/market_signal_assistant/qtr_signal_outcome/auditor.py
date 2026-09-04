from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from market_signal_assistant.qtr_signal_outcome.bybit_provider import (
    MarketDataProvider,
    MarketDataProviderError,
)
from market_signal_assistant.qtr_signal_outcome.engine import (
    OutcomeEngine,
    failed_market_data_outcome,
)
from market_signal_assistant.qtr_signal_outcome.journal import JsonlOutcomeJournal
from market_signal_assistant.qtr_signal_outcome.source import SignalSourceReader


@dataclass(frozen=True, slots=True)
class AuditRunStats:
    delivered_signals: int
    outcomes_written: int
    skipped_complete: int
    failed_market_data: int
    invalid_source_records: int


class SignalOutcomeAuditor:
    def __init__(
        self,
        source: SignalSourceReader,
        provider: MarketDataProvider,
        engine: OutcomeEngine,
        journal: JsonlOutcomeJournal,
    ) -> None:
        self._source = source
        self._provider = provider
        self._engine = engine
        self._journal = journal

    def run(
        self,
        *,
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> AuditRunStats:
        delivered = written = skipped = failed = 0
        for signal in self._source.iter_signals(since=since, until=until):
            delivered += 1
            if self._journal.is_complete(
                signal.signal_id, self._engine.maximum_horizon_minutes
            ):
                skipped += 1
                continue
            end = signal.signal_timestamp + timedelta(
                minutes=self._engine.maximum_horizon_minutes
            )
            try:
                candles = self._provider.fetch(
                    signal.symbol, signal.signal_timestamp, end
                )
                outcome = self._engine.analyze(signal, candles)
            except MarketDataProviderError as error:
                failed += 1
                outcome = failed_market_data_outcome(
                    signal,
                    str(error),
                    self._engine.maximum_horizon_minutes,
                )
            if self._journal.append(outcome):
                written += 1
        return AuditRunStats(
            delivered,
            written,
            skipped,
            failed,
            self._source.stats.invalid_source_records,
        )
