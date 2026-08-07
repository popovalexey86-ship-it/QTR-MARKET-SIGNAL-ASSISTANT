from datetime import UTC, datetime
from pathlib import Path

import pytest

from market_signal_assistant import cli
from market_signal_assistant.application.models import (
    MarketSummary,
    ScreeningReport,
)
from market_signal_assistant.models import (
    AssetClass,
)


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


def test_cli_preserves_csv_limit_and_confirmation_contract(tmp_path: Path) -> None:
    csv_path = tmp_path / "BTCUSDT.csv"
    args = cli.build_parser().parse_args(
        [
            "--instrument",
            "BTCUSDT:crypto",
            "--csv",
            f"BTCUSDT={csv_path}",
            "--limit",
            "125",
            "--min-confirmations",
            "3",
        ]
    )
    assert args.csv == [("BTCUSDT", csv_path)]
    assert args.limit == 125
    assert args.min_confirmations == 3


def test_cli_json_output_is_optional(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = ScreeningReport(
        generated_at=datetime(2026, 1, 1, tzinfo=UTC),
        successful_results=(),
        failed_instruments=(),
        ranked_signals=(),
        market_summary=MarketSummary(0, 0, 0, 0, 0, 0),
    )

    class FakeService:
        def screen(self, request: object) -> ScreeningReport:
            del request
            return result
    without_output = cli.build_parser().parse_args(
        ["--instrument", "BTCUSDT:crypto"]
    )
    cli.run(without_output, FakeService())
    output_text = capsys.readouterr().out
    assert "Сформировано:" in output_text
    assert "Сигналы: 0" in output_text
    assert "Подходящих сигналов не найдено." in output_text
    assert "Информационный анализ, не торговая рекомендация." in output_text
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
    cli.run(with_output, FakeService())
    assert output.exists()


def test_cli_help_does_not_build_composition(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        cli,
        "_build_service",
        lambda *args: (_ for _ in ()).throw(AssertionError("composition built")),
    )
    with pytest.raises(SystemExit) as exit_info:
        cli.main(["--help"])
    assert exit_info.value.code == 0
    help_text = capsys.readouterr().out
    assert "информационный скринер" in help_text
    assert "наблюдения" in help_text
    assert "использование:" in help_text
    assert "параметры:" in help_text
    assert "показать справку и выйти" in help_text
