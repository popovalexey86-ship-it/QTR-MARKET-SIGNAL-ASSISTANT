from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

from market_signal_assistant.qtr_signal_outcome.auditor import SignalOutcomeAuditor
from market_signal_assistant.qtr_signal_outcome.bybit_provider import (
    MarketDataProviderError,
)
from market_signal_assistant.qtr_signal_outcome.engine import OutcomeEngine
from market_signal_assistant.qtr_signal_outcome.journal import JsonlOutcomeJournal
from market_signal_assistant.qtr_signal_outcome.models import (
    MarketCandle,
    OutcomeStatus,
)
from market_signal_assistant.qtr_signal_outcome.source import SignalSourceReader
from qtr_signal_outcome.helpers import NOW, candle, source_record


class Provider:
    def __init__(self, *, fail_symbol: str | None = None) -> None:
        self.fail_symbol = fail_symbol
        self.calls: list[str] = []

    def fetch(
        self, symbol: str, start: datetime, end: datetime
    ) -> tuple[MarketCandle, ...]:
        del start, end
        self.calls.append(symbol)
        if symbol == self.fail_symbol:
            raise MarketDataProviderError("offline")
        return tuple(candle(i) for i in range(1, 241))


def write_source(path: Path, records: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(json.dumps(item) + "\n" for item in records), encoding="utf-8"
    )


def test_journal_recovery_and_idempotent_second_run(tmp_path: Path) -> None:
    source_path = tmp_path / "source.jsonl"
    output = tmp_path / "outcomes.jsonl"
    write_source(source_path, [source_record()])
    provider = Provider()
    first = SignalOutcomeAuditor(
        SignalSourceReader(source_path),
        provider,
        OutcomeEngine(),
        JsonlOutcomeJournal(output),
    ).run()
    second = SignalOutcomeAuditor(
        SignalSourceReader(source_path),
        provider,
        OutcomeEngine(),
        JsonlOutcomeJournal(output),
    ).run()
    assert first.outcomes_written == 1
    assert second.outcomes_written == 0
    assert second.skipped_complete == 1
    assert len(output.read_text(encoding="utf-8").splitlines()) == 1


def test_malformed_legacy_output_line_does_not_break_recovery(tmp_path: Path) -> None:
    path = tmp_path / "outcomes.jsonl"
    path.write_text("{bad}\n", encoding="utf-8")
    journal = JsonlOutcomeJournal(path)
    assert journal.recovery_malformed_lines == 1


def test_provider_failure_is_isolated_per_signal(tmp_path: Path) -> None:
    source_path = tmp_path / "source.jsonl"
    output = tmp_path / "outcomes.jsonl"
    eth = source_record(
        symbol="ETHUSDT",
        timestamp=(NOW + timedelta(minutes=1)).isoformat(),
        semantic_fingerprint="eth",
    )
    eth["price_context"]["market_price"] = 2000.0
    write_source(source_path, [source_record(), eth])
    stats = SignalOutcomeAuditor(
        SignalSourceReader(source_path),
        Provider(fail_symbol="ETHUSDT"),
        OutcomeEngine(),
        JsonlOutcomeJournal(output),
    ).run()
    records = tuple(JsonlOutcomeJournal(output).iter_latest())
    assert stats.failed_market_data == 1
    assert {item.status for item in records} == {
        OutcomeStatus.COMPLETE,
        OutcomeStatus.FAILED_MARKET_DATA,
    }


def test_partial_outcome_same_revision_is_not_duplicated(tmp_path: Path) -> None:
    output = tmp_path / "outcomes.jsonl"
    journal = JsonlOutcomeJournal(output)
    source_path = tmp_path / "source.jsonl"
    write_source(source_path, [source_record()])

    class PartialProvider:
        def fetch(
            self, symbol: str, start: datetime, end: datetime
        ) -> tuple[MarketCandle, ...]:
            del symbol, start, end
            return tuple(candle(i) for i in range(1, 10))

    auditor = SignalOutcomeAuditor(
        SignalSourceReader(source_path), PartialProvider(), OutcomeEngine(), journal
    )
    assert auditor.run().outcomes_written == 1
    assert auditor.run().outcomes_written == 0
    assert tuple(journal.iter_latest())[0].status is OutcomeStatus.PARTIAL
