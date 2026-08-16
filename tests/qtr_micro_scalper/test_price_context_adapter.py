from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_signal_assistant.qtr_micro_scalper.price_context_adapter import (
    JsonlVerifiedSetupProvider,
    VerifiedPriceContextAdapter,
)
from market_signal_assistant.qtr_micro_scalper.service import main
from market_signal_assistant.qtr_micro_scalper.setup_context import ShadowDirection

NOW = datetime(2026, 8, 16, 12, 0, tzinfo=UTC)


def _payload(
    *,
    symbol: str = "BTCUSDT",
    observed_at: datetime = NOW,
    source_direction: str = "UP",
    setup_direction: str = "UP",
    atr: float | None = 100.0,
    invalidation_price: float | None = 59_800.0,
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "price_context": {
            "observed_at": observed_at.isoformat(),
            "source_direction": source_direction,
            "setup_direction": setup_direction,
            "market_price": 60_050.0,
            "atr": atr,
            "trigger_price": 60_000.0,
            "invalidation_price": invalidation_price,
            "local_range_low": 59_500.0,
            "local_range_high": 60_000.0,
            "setup_state": "CONFIRMING",
            "setup_confidence": 75.0,
            "volume_confirmation": True,
            "volatility_confirmation": True,
            "liquidity_ok": True,
            "confirmations": ["Пробой подтверждён."],
            "warnings": [],
        },
    }


def _adapter(path: Path, payload: dict[str, object]) -> VerifiedPriceContextAdapter:
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")
    return VerifiedPriceContextAdapter(JsonlVerifiedSetupProvider(path))


def test_valid_long_price_context(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path / "setup.jsonl", _payload())

    result = adapter("BTCUSDT", NOW + timedelta(minutes=1), 60_060.0)

    assert result is not None
    assert result.direction is ShadowDirection.LONG
    assert result.market_price == 60_060.0
    assert result.atr == 100.0
    assert result.trigger_price == 60_000.0
    assert result.invalidation_price == 59_800.0
    assert result.local_range_low == 59_500.0
    assert result.local_range_high == 60_000.0


def test_valid_short_price_context(tmp_path: Path) -> None:
    payload = _payload(
        source_direction="DOWN",
        setup_direction="DOWN",
        invalidation_price=60_200.0,
    )
    context = payload["price_context"]
    assert isinstance(context, dict)
    context["trigger_price"] = 59_500.0
    adapter = _adapter(tmp_path / "setup.jsonl", payload)

    result = adapter("BTCUSDT", NOW, 59_450.0)

    assert result is not None
    assert result.direction is ShadowDirection.SHORT
    assert result.structure_valid


@pytest.mark.parametrize(
    ("field", "value"),
    (("atr", None), ("invalidation_price", None)),
)
def test_missing_required_numeric_field_returns_none(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    payload = _payload()
    context = payload["price_context"]
    assert isinstance(context, dict)
    context[field] = value
    adapter = _adapter(tmp_path / "setup.jsonl", payload)

    assert adapter("BTCUSDT", NOW, 60_050.0) is None


def test_stale_source_returns_none(tmp_path: Path) -> None:
    adapter = _adapter(
        tmp_path / "setup.jsonl",
        _payload(observed_at=NOW - timedelta(minutes=16)),
    )

    assert adapter("BTCUSDT", NOW, 60_050.0) is None


def test_conflicting_direction_returns_none(tmp_path: Path) -> None:
    adapter = _adapter(
        tmp_path / "setup.jsonl",
        _payload(source_direction="UP", setup_direction="DOWN"),
    )

    assert adapter("BTCUSDT", NOW, 60_050.0) is None


def test_wrong_symbol_returns_none(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path / "setup.jsonl", _payload(symbol="ETHUSDT"))

    assert adapter("BTCUSDT", NOW, 60_050.0) is None


@pytest.mark.parametrize(
    "content",
    (
        '{"symbol":"BTCUSDT","direction":"UP"}\n',
        "not-json\n",
    ),
)
def test_legacy_or_malformed_record_fails_closed(
    tmp_path: Path,
    content: str,
) -> None:
    path = tmp_path / "setup.jsonl"
    path.write_text(content, encoding="utf-8")
    adapter = VerifiedPriceContextAdapter(JsonlVerifiedSetupProvider(path))

    assert adapter("BTCUSDT", NOW, 60_050.0) is None


def test_missing_audit_file_fails_closed(tmp_path: Path) -> None:
    adapter = VerifiedPriceContextAdapter(
        JsonlVerifiedSetupProvider(tmp_path / "missing.jsonl")
    )

    assert adapter("BTCUSDT", NOW, 60_050.0) is None


def test_output_is_deterministic(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path / "setup.jsonl", _payload())

    first = adapter("BTCUSDT", NOW, 60_050.0)
    second = adapter("BTCUSDT", NOW, 60_050.0)

    assert first == second


def test_verified_target_uses_recorded_setup_evidence(tmp_path: Path) -> None:
    adapter = _adapter(tmp_path / "setup.jsonl", _payload())

    target = adapter.target("BTCUSDT", NOW)

    assert target is not None
    assert target.priority == 75.0
    assert target.volatility_score == 100.0
    assert target.volume_score == 100.0
    assert target.liquidity_score == 100.0


def test_clean_import_and_cli_help(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as error:
        main(["--help"])

    assert error.value.code == 0
    assert "QTR Micro Scalper V2" in capsys.readouterr().out
