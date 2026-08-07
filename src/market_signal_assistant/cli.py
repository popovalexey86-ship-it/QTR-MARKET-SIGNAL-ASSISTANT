from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Protocol

from market_signal_assistant.application.models import (
    ScreeningReport,
    ScreeningRequest,
)
from market_signal_assistant.application.presentation import (
    format_number,
    present_report,
)
from market_signal_assistant.localized_argparse import RussianArgumentParser
from market_signal_assistant.models import AssetClass, Instrument


class ScreeningService(Protocol):
    def screen(self, request: ScreeningRequest) -> ScreeningReport: ...


def build_parser() -> argparse.ArgumentParser:
    parser = RussianArgumentParser(
        description="Объяснимый информационный скринер рыночных сигналов."
    )
    parser.add_argument(
        "--instrument",
        action="append",
        required=True,
        type=_instrument,
        help=(
            "SYMBOL:crypto|stock|fund|forex; повторите параметр для списка "
            "наблюдения."
        ),
    )
    parser.add_argument(
        "--interval",
        choices=("5m", "15m", "1h", "4h", "1d"),
        default="1h",
    )
    parser.add_argument("--limit", type=_positive_int, default=250)
    parser.add_argument("--min-score", type=_score, default=45.0)
    parser.add_argument("--min-confirmations", type=_positive_int, default=2)
    parser.add_argument("--min-confidence", type=_score, default=0.0)
    parser.add_argument("--maximum-results", type=_positive_int, default=10)
    parser.add_argument("--include-derivatives", action="store_true")
    parser.add_argument(
        "--csv",
        action="append",
        default=[],
        type=_csv_mapping,
        help="Необязательное offline-сопоставление SYMBOL=path.",
    )
    parser.add_argument("--json-output", type=Path)
    return parser


def run(
    args: argparse.Namespace,
    service: ScreeningService | None = None,
) -> ScreeningReport:
    selected_service = service or _build_service(args)
    request = ScreeningRequest(
        instruments=tuple(args.instrument),
        interval=args.interval,
        minimum_score=args.min_score,
        minimum_confidence=args.min_confidence,
        include_derivatives=args.include_derivatives,
        maximum_results=args.maximum_results,
    )
    result = selected_service.screen(request)
    view = present_report(result)
    _print_view(view)
    if args.json_output is not None:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(
            json.dumps(view.as_dict(), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = run(args)
    return 1 if report.failed_instruments and not report.ranked_signals else 0


def _build_service(args: argparse.Namespace) -> ScreeningService:
    # Lazy import keeps --help and module import free of composition/provider setup.
    from market_signal_assistant.composition import build_screening_service
    from market_signal_assistant.engine import SignalEngine
    from market_signal_assistant.providers import (
        CsvMarketDataProvider,
        RoutingMarketDataProvider,
    )

    csv_paths = dict(args.csv)
    provider = RoutingMarketDataProvider(
        csv_provider=CsvMarketDataProvider(csv_paths) if csv_paths else None
    )
    service, _ = build_screening_service(
        technical_provider=provider,
        technical_analyzer=SignalEngine(
            min_score=1.0,
            min_confirmations=args.min_confirmations,
        ),
        candle_limit=args.limit,
    )
    return service


def _print_view(view: object) -> None:
    from market_signal_assistant.application.presentation import ReportView

    if not isinstance(view, ReportView):
        raise TypeError("Expected ReportView.")
    print("Информационный помощник по рынку")
    print(f"Сформировано: {view.generated_at}")
    print(f"Сигналы: {len(view.ranked_signals)}")
    if not view.ranked_signals:
        print("Подходящих сигналов не найдено.")
    for index, signal in enumerate(view.ranked_signals, start=1):
        print(
            f"{index}. {signal.symbol} — {signal.direction}\n"
            f"   Итоговый балл: {format_number(signal.combined_score)}\n"
            "   Техническая сила сигнала: "
            f"{format_number(signal.technical_score)}\n"
            f"   Уверенность: {format_number(signal.confidence)}%\n"
            f"   Подтверждения: {signal.confirmations}\n"
            f"   Контекст деривативов: {signal.derivatives_context}"
        )
        for reason in signal.explanations:
            print(f"   Причина: {reason}")
        if signal.conflicts:
            print(f"   Противоречия: {signal.conflicts}")
        for warning in signal.warnings:
            print(f"   Предупреждение: {warning}")
    for failure in view.failed_instruments:
        print(
            f"   Ошибка анализа {failure.symbol} "
            f"({failure.stage}): {failure.message}",
            file=sys.stderr,
        )
    print(view.disclaimer)


def _instrument(value: str) -> Instrument:
    try:
        symbol, asset_class = value.rsplit(":", 1)
        return Instrument(symbol, AssetClass(asset_class.lower()))
    except (ValueError, TypeError):
        raise argparse.ArgumentTypeError(
            "инструмент должен иметь формат SYMBOL:crypto|stock|fund|forex"
        ) from None


def _csv_mapping(value: str) -> tuple[str, Path]:
    try:
        symbol, path = value.split("=", 1)
    except ValueError:
        raise argparse.ArgumentTypeError(
            "CSV-сопоставление должно иметь формат SYMBOL=path"
        ) from None
    if not symbol.strip() or not path.strip():
        raise argparse.ArgumentTypeError(
            "CSV-сопоставление должно иметь формат SYMBOL=path"
        )
    return symbol.strip().upper(), Path(path)


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError:
        message = "ожидается положительное целое число"
        raise argparse.ArgumentTypeError(message) from None
    if parsed <= 0:
        raise argparse.ArgumentTypeError("ожидается положительное целое число")
    return parsed


def _score(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("балл должен быть от 0 до 100") from None
    if not 0 <= parsed <= 100:
        raise argparse.ArgumentTypeError("балл должен быть от 0 до 100")
    return parsed


if __name__ == "__main__":
    raise SystemExit(main())
