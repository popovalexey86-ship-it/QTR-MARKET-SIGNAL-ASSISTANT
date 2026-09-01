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
    setup_state: str = "CONFIRMING",
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
            "setup_state": setup_state,
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


def _append_payload(path: Path, payload: dict[str, object]) -> int:
    line = (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")
    with path.open("ab") as stream:
        stream.write(line)
    return len(line)


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
    assert result.verified_setup_state == "CONFIRMING"
    assert result.verified_setup_confidence == 75.0
    assert result.volume_confirmation is True
    assert result.volatility_confirmation is True
    assert result.liquidity_confirmation is True
    assert result.source_observed_at == NOW


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


def test_large_existing_jsonl_is_bootstrapped_once(tmp_path: Path) -> None:
    path = tmp_path / "setup.jsonl"
    legacy = json.dumps({"symbol": "LEGACYUSDT", "decision": "send"}) + "\n"
    path.write_text(legacy * 20_000, encoding="utf-8")
    _append_payload(path, _payload(symbol="BTCUSDT"))
    _append_payload(path, _payload(symbol="ETHUSDT"))

    provider = JsonlVerifiedSetupProvider(path)

    bitcoin = provider.latest("BTCUSDT")
    assert bitcoin is not None
    assert bitcoin.confirmations == ("Пробой подтверждён.",)
    assert provider.latest("ETHUSDT") is not None
    assert provider.metrics.bootstrap_scans == 1
    assert provider.metrics.incremental_reads == 0
    assert provider.metrics.bytes_read == path.stat().st_size
    assert provider.metrics.cached_symbols == 2


def test_repeated_latest_calls_do_not_rescan_file(tmp_path: Path) -> None:
    path = tmp_path / "setup.jsonl"
    path.write_text(json.dumps(_payload()) + "\n", encoding="utf-8")
    provider = JsonlVerifiedSetupProvider(path)
    after_bootstrap = provider.metrics

    for _ in range(1_000):
        assert provider.latest("BTCUSDT") is not None

    assert provider.metrics == after_bootstrap
    assert provider.metrics.bootstrap_scans == 1


def test_appended_record_is_read_from_saved_offset(tmp_path: Path) -> None:
    path = tmp_path / "setup.jsonl"
    path.write_text(json.dumps(_payload()) + "\n", encoding="utf-8")
    provider = JsonlVerifiedSetupProvider(path)
    initial = provider.metrics
    appended = _append_payload(
        path,
        _payload(observed_at=NOW + timedelta(minutes=2), atr=125.0),
    )

    record = provider.latest("BTCUSDT")

    assert record is not None
    assert record.atr == 125.0
    assert provider.metrics.bootstrap_scans == 1
    assert provider.metrics.incremental_reads == 1
    assert provider.metrics.bytes_read == initial.bytes_read + appended


def test_file_that_appears_after_provider_creation_is_bootstrapped(
    tmp_path: Path,
) -> None:
    path = tmp_path / "later.jsonl"
    provider = JsonlVerifiedSetupProvider(path)
    assert provider.latest("BTCUSDT") is None
    assert provider.metrics.bootstrap_scans == 0

    path.write_text(json.dumps(_payload()) + "\n", encoding="utf-8")

    assert provider.latest("BTCUSDT") is not None
    assert provider.metrics.bootstrap_scans == 1


def test_multiple_symbols_and_malformed_utf8_are_isolated(tmp_path: Path) -> None:
    path = tmp_path / "setup.jsonl"
    path.write_bytes(b"not-json\n\xff\xfe\n")
    _append_payload(path, _payload(symbol="BTCUSDT"))
    _append_payload(path, _payload(symbol="ETHUSDT"))
    provider = JsonlVerifiedSetupProvider(path)

    assert provider.latest("BTCUSDT") is not None
    assert provider.latest("ETHUSDT") is not None
    assert provider.latest("SOLUSDT") is None
    assert provider.metrics.cached_symbols == 2
    assert provider.metrics.malformed_lines == 2


def test_partial_trailing_line_waits_for_completion(tmp_path: Path) -> None:
    path = tmp_path / "setup.jsonl"
    line = json.dumps(_payload(), ensure_ascii=False).encode("utf-8")
    path.write_bytes(line)
    provider = JsonlVerifiedSetupProvider(path)

    assert provider.latest("BTCUSDT") is None
    assert provider.metrics.malformed_lines == 0

    with path.open("ab") as stream:
        stream.write(b"\n")

    assert provider.latest("BTCUSDT") is not None
    assert provider.metrics.incremental_reads == 1


def test_truncation_resets_cache_and_bootstraps_new_file(tmp_path: Path) -> None:
    path = tmp_path / "setup.jsonl"
    path.write_text((json.dumps(_payload()) + "\n") * 3, encoding="utf-8")
    provider = JsonlVerifiedSetupProvider(path)

    path.write_text(json.dumps(_payload(symbol="ETHUSDT")) + "\n", encoding="utf-8")

    assert provider.latest("BTCUSDT") is None
    assert provider.latest("ETHUSDT") is not None
    assert provider.metrics.bootstrap_scans == 2
    assert provider.metrics.resets_rotations == 1


def test_rotated_file_replaces_cached_generation(tmp_path: Path) -> None:
    path = tmp_path / "setup.jsonl"
    path.write_text(json.dumps(_payload()) + "\n", encoding="utf-8")
    provider = JsonlVerifiedSetupProvider(path)
    replacement = tmp_path / "replacement.jsonl"
    replacement.write_text(
        json.dumps(_payload(symbol="ETHUSDT")) + "\n",
        encoding="utf-8",
    )
    replacement.replace(path)

    assert provider.latest("BTCUSDT") is None
    assert provider.latest("ETHUSDT") is not None
    assert provider.metrics.bootstrap_scans == 2
    assert provider.metrics.resets_rotations == 1


@pytest.mark.parametrize("setup_state", ("CANCELLED", "LATE"))
def test_rejected_setup_states_remain_fail_closed(
    tmp_path: Path,
    setup_state: str,
) -> None:
    adapter = _adapter(
        tmp_path / "setup.jsonl",
        _payload(setup_state=setup_state),
    )

    assert adapter("BTCUSDT", NOW, 60_050.0) is None
