from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from market_signal_assistant.qtr_signal_outcome.auditor import SignalOutcomeAuditor
from market_signal_assistant.qtr_signal_outcome.bybit_provider import (
    BybitKlineProvider,
    CachingMarketDataProvider,
    MarketDataProvider,
)
from market_signal_assistant.qtr_signal_outcome.engine import OutcomeEngine
from market_signal_assistant.qtr_signal_outcome.journal import (
    DEFAULT_OUTCOME_JOURNAL_PATH,
    JsonlOutcomeJournal,
)
from market_signal_assistant.qtr_signal_outcome.metrics import (
    build_summary,
    format_summary,
)
from market_signal_assistant.qtr_signal_outcome.source import SignalSourceReader

DEFAULT_SOURCE_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "qtr_setup_telegram_pilot_audit.jsonl"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="market-signal-outcome-audit",
        description="Read-only outcome audit of delivered QTR Setup Pilot signals.",
    )
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTCOME_JOURNAL_PATH)
    parser.add_argument("--since", type=_timestamp)
    parser.add_argument("--until", type=_timestamp)
    parser.add_argument("--horizon-minutes", type=int, default=240)
    parser.add_argument("--summary", action="store_true")
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    provider: MarketDataProvider | None = None,
) -> int:
    args = build_parser().parse_args(argv)
    if args.since is not None and args.until is not None and args.until <= args.since:
        raise SystemExit("--until must be after --since")
    source = SignalSourceReader(args.source)
    journal = JsonlOutcomeJournal(args.output)
    auditor = SignalOutcomeAuditor(
        source,
        CachingMarketDataProvider(provider or BybitKlineProvider()),
        OutcomeEngine(args.horizon_minutes),
        journal,
    )
    stats = auditor.run(since=args.since, until=args.until)
    if args.summary:
        summary = build_summary(
            journal.iter_latest(),
            invalid_source_records=stats.invalid_source_records,
        )
        print(format_summary(summary))
    return 0


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("Timestamp must include a timezone.")
    return parsed.astimezone(UTC)


if __name__ == "__main__":
    raise SystemExit(main())
