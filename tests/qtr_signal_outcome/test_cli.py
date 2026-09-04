from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from market_signal_assistant.qtr_signal_outcome.cli import build_parser, main
from market_signal_assistant.qtr_signal_outcome.models import MarketCandle
from qtr_signal_outcome.helpers import candle, source_record


def test_cli_help_builds_without_network() -> None:
    assert "--horizon-minutes" in build_parser().format_help()


def test_cli_uses_injected_provider_and_prints_summary(
    tmp_path: Path, capsys: object
) -> None:
    source = tmp_path / "source.jsonl"
    output = tmp_path / "outcomes.jsonl"
    source.write_text(json.dumps(source_record()) + "\n", encoding="utf-8")

    class Provider:
        def fetch(
            self, symbol: str, start: datetime, end: datetime
        ) -> tuple[MarketCandle, ...]:
            del symbol, start, end
            return tuple(candle(i) for i in range(1, 241))

    code = main(
        [
            "--source",
            str(source),
            "--output",
            str(output),
            "--since",
            "2026-09-03T00:00:00Z",
            "--until",
            "2026-09-04T00:00:00Z",
            "--summary",
        ],
        provider=Provider(),
    )
    assert code == 0
    assert "Total delivered signals: 1" in capsys.readouterr().out  # type: ignore[attr-defined]
