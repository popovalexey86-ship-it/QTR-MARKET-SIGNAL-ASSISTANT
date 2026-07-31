from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

import pytest

from market_signal_assistant import cli
from market_signal_assistant.engine import SignalEngine
from market_signal_assistant.models import (
    AssetClass,
    Instrument,
    MarketSeries,
    MarketSignal,
    ScreeningResult,
)
from market_signal_assistant.screening import MarketScreener


class FakeProvider:
    def __init__(
        self,
        results: dict[str, MarketSeries | Exception],
    ) -> None:
        self.results = results

    def load(
        self,
        instrument: Instrument,
        interval: str,
        limit: int,
    ) -> MarketSeries:
        del interval, limit
        value = self.results[instrument.symbol]
        if isinstance(value, Exception):
            raise value
        return value


class FakeEngine(SignalEngine):
    def analyze(self, series: MarketSeries) -> MarketSignal | None:
        del series
        return None


def test_screener_isolates_provider_failure() -> None:
    first = Instrument("A", AssetClass.STOCK)
    second = Instrument("B", AssetClass.FUND)
    empty_result = object.__new__(MarketSeries)
    provider = FakeProvider(
        {
            "A": empty_result,
            "B": RuntimeError("unavailable"),
        }
    )

    result = MarketScreener(
        provider=provider,
        engine=FakeEngine(),
    ).screen((first, second), interval="1h")

    assert result.no_signal == (first,)
    assert len(result.failures) == 1
    assert result.failures[0].instrument is second


def test_cli_parses_multi_asset_watchlist() -> None:
    args = cli.build_parser().parse_args(
        [
            "--instrument",
            "BTCUSDT:crypto",
            "--instrument",
            "AAPL:stock",
            "--instrument",
            "SPY:fund",
            "--instrument",
            "EURUSD=X:forex",
        ]
    )

    assert tuple(item.asset_class for item in args.instrument) == (
        AssetClass.CRYPTO,
        AssetClass.STOCK,
        AssetClass.FUND,
        AssetClass.FOREX,
    )


def test_cli_json_output_is_optional(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    result = ScreeningResult(
        generated_at=datetime(2026, 1, 1, tzinfo=UTC),
        signals=(),
        no_signal=(),
        failures=(),
    )

    class FakeScreener:
        def __init__(self, **kwargs: object) -> None:
            del kwargs

        def screen(
            self,
            instruments: Iterable[Instrument],
            *,
            interval: str,
            limit: int = 250,
        ) -> ScreeningResult:
            del instruments, interval, limit
            return result

    monkeypatch.setattr(cli, "MarketScreener", FakeScreener)
    without_output = cli.build_parser().parse_args(
        ["--instrument", "BTCUSDT:crypto"]
    )
    cli.run(without_output)
    assert tuple(tmp_path.iterdir()) == ()

    output = tmp_path / "report.json"
    with_output = cli.build_parser().parse_args(
        [
            "--instrument",
            "BTCUSDT:crypto",
            "--json-output",
            str(output),
        ]
    )
    cli.run(with_output)
    assert output.exists()
