from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from dataclasses import asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from market_signal_assistant.engine import SignalEngine
from market_signal_assistant.models import AssetClass, Instrument, ScreeningResult
from market_signal_assistant.providers import (
    CsvMarketDataProvider,
    RoutingMarketDataProvider,
)
from market_signal_assistant.screening import MarketScreener


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Explainable multi-asset market signal screener."
    )
    parser.add_argument(
        "--instrument",
        action="append",
        required=True,
        type=_instrument,
        help="SYMBOL:crypto|stock|fund|forex; repeat for a watchlist.",
    )
    parser.add_argument(
        "--interval",
        choices=("5m", "15m", "1h", "4h", "1d"),
        default="1h",
    )
    parser.add_argument("--limit", type=_positive_int, default=250)
    parser.add_argument("--min-score", type=_score, default=45.0)
    parser.add_argument("--min-confirmations", type=_positive_int, default=2)
    parser.add_argument(
        "--csv",
        action="append",
        default=[],
        type=_csv_mapping,
        help="Optional offline SYMBOL=path mapping; repeat as needed.",
    )
    parser.add_argument("--json-output", type=Path)
    return parser


def run(args: argparse.Namespace) -> ScreeningResult:
    csv_paths = dict(args.csv)
    provider = RoutingMarketDataProvider(
        csv_provider=(
            CsvMarketDataProvider(csv_paths) if csv_paths else None
        )
    )
    screener = MarketScreener(
        provider=provider,
        engine=SignalEngine(
            min_score=args.min_score,
            min_confirmations=args.min_confirmations,
        ),
    )
    result = screener.screen(
        args.instrument,
        interval=args.interval,
        limit=args.limit,
    )
    _print_result(result)
    if args.json_output is not None:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(
            json.dumps(
                _json_value(result),
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run(args)
    return 1 if result.failures and not result.signals else 0


def _print_result(result: ScreeningResult) -> None:
    print("Market Signal Assistant")
    print(f"Generated: {result.generated_at.isoformat()}")
    print(f"Signals: {len(result.signals)}")
    for index, signal in enumerate(result.signals, start=1):
        print(
            f"{index}. {signal.instrument.symbol} "
            f"{signal.direction.value} "
            f"score={signal.score:.1f} "
            f"confidence={signal.confidence:.1f}% "
            f"price={signal.price:.8g}"
        )
        for evidence in signal.evidence:
            marker = "+" if evidence.direction is signal.direction else "!"
            print(f"   {marker} {evidence.name}: {evidence.detail}")
    print(f"No signal: {len(result.no_signal)}")
    print(f"Failures: {len(result.failures)}")
    for failure in result.failures:
        print(
            f"   {failure.instrument.symbol}: "
            f"{failure.error_type}: {failure.message}",
            file=sys.stderr,
        )


def _instrument(value: str) -> Instrument:
    try:
        symbol, asset_class = value.rsplit(":", 1)
        return Instrument(symbol, AssetClass(asset_class.lower()))
    except (ValueError, TypeError):
        raise argparse.ArgumentTypeError(
            "instrument must be SYMBOL:crypto|stock|fund|forex"
        ) from None


def _csv_mapping(value: str) -> tuple[str, Path]:
    try:
        symbol, path = value.split("=", 1)
    except ValueError:
        raise argparse.ArgumentTypeError("CSV mapping must be SYMBOL=path") from None
    if not symbol.strip() or not path.strip():
        raise argparse.ArgumentTypeError("CSV mapping must be SYMBOL=path")
    return symbol, Path(path)


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("expected a positive integer") from None
    if parsed <= 0:
        raise argparse.ArgumentTypeError("expected a positive integer")
    return parsed


def _score(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("score must be between 0 and 100") from None
    if not 0 < parsed <= 100:
        raise argparse.ArgumentTypeError("score must be between 0 and 100")
    return parsed


def _json_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if hasattr(value, "__dataclass_fields__"):
        return {
            key: _json_value(item)
            for key, item in asdict(value).items()
        }
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    return value


if __name__ == "__main__":
    raise SystemExit(main())
