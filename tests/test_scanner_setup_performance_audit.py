from __future__ import annotations

import csv
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_signal_assistant.setup_engine.performance_audit import (
    HORIZONS,
    analyze,
    atr_bucket,
    confidence_bucket,
    outcome_for,
    read_audit,
    signal_age_bucket,
    sources_unchanged,
    too_far_bucket,
    write_outputs,
)

ROOT = Path(__file__).resolve().parents[1]
REAL_AUDIT = ROOT / "data" / "inplay_early_discovery_v2_audit.jsonl"


def _fixture_snapshot(tmp_path: Path) -> Path:
    directory = tmp_path / "snapshot"
    directory.mkdir()
    raw = json.loads(REAL_AUDIT.read_text(encoding="utf-8-sig").splitlines()[0])
    start = datetime(2026, 8, 10, 8, 0, tzinfo=UTC)
    rows: list[dict[str, object]] = []
    for minutes, price in (
        (0, 100.0),
        (5, 99.0),
        (15, 98.0),
        (30, 97.0),
        (60, 96.0),
        (180, 95.0),
    ):
        row = dict(raw)
        at = start + timedelta(minutes=minutes)
        row.update(
            {
                "scan_id": f"scan-{minutes}",
                "scanned_at": at.isoformat(),
                "symbol": "TESTUSDT",
                "market_direction": "DOWN",
                "direction_v1": "DOWN",
                "direction_v2": "DOWN",
                "current_price": price,
                "breakout_level": 100.0,
                "first_detected_at": start.isoformat(),
                "first_ready_at": start.isoformat(),
                "absolute_distance_atr": abs(price - 100.0) / 10,
                "signed_distance_atr": (price - 100.0) / 10,
                "technical_error": None,
            }
        )
        rows.append(row)
    (directory / "inplay_early_discovery_v2_audit.jsonl").write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n",
        encoding="utf-8",
    )
    decision = {
        "decided_at": (start + timedelta(seconds=10)).isoformat(),
        "symbol": "TESTUSDT",
        "episode_id": start.isoformat(),
        "skip_reason": "too_far",
        "skip_detail": None,
        "instrument_status": None,
    }
    (directory / "qtr_micro_decisions.jsonl").write_text(
        json.dumps(decision) + "\n", encoding="utf-8"
    )
    events = (
        "ENTRY_REVALIDATION_STARTED",
        "FRESH_PRICE_LOADED",
        "ENTRY_REVALIDATION_REJECTED",
    )
    runtime = [
        {
            "occurred_at": (start + timedelta(seconds=20)).isoformat(),
            "event": event,
            "symbol": "TESTUSDT",
            "trade_id": "QTRM-test",
            "stage": "PREPARED",
            "reason": None,
            "detail": "too_far" if event.endswith("REJECTED") else None,
            "ret_code": None,
        }
        for event in events
    ]
    (directory / "qtr_micro_runtime_audit.jsonl").write_text(
        "\n".join(json.dumps(row) for row in runtime) + "\n", encoding="utf-8"
    )
    trade = {
        "trade_id": "QTRM-cap",
        "symbol": "CAPUSDT",
        "direction": "LONG",
        "setup_type": "RETEST",
        "pre_submit_price": 1.0,
        "planned_notional": 100.0,
        "actual_risk_at_fill": 5.0,
        "actual_risk_pct": 0.5,
        "gross_pnl": 2.0,
        "net_pnl": 1.5,
        "total_fees": 0.5,
        "gross_r": 0.4,
        "net_r": 0.3,
        "hold_duration_seconds": 60,
        "exit_reason": "STRUCTURE_EXIT",
        "average_fill": 1.0,
    }
    (directory / "qtr_micro_trades.jsonl").write_text(
        json.dumps(trade) + "\n", encoding="utf-8"
    )
    (directory / "qtr_micro_state.json").write_text(
        '{"schema_version":2,"positions":{}}', encoding="utf-8"
    )
    return directory


def test_bucket_boundaries_are_explicit() -> None:
    assert confidence_bucket(59.99) == "<60"
    assert confidence_bucket(60) == "60-69"
    assert confidence_bucket(95) == "95-100"
    assert atr_bucket(None) == "null"
    assert atr_bucket(0.10) == "0-0.10"
    assert atr_bucket(-0.25) == "0.10-0.25"
    assert too_far_bucket(0.25) == "0.25-0.35"
    assert too_far_bucket(1.01) == ">1.0"
    assert signal_age_bucket(None) == "null"
    assert signal_age_bucket(60) == "31-60s"


def test_outcomes_use_direction_and_do_not_invent_missing_horizons(
    tmp_path: Path,
) -> None:
    data = read_audit(_fixture_snapshot(tmp_path))
    by_symbol = {"TESTUSDT": data.snapshots}
    times = {"TESTUSDT": tuple(row.scanned_at for row in data.snapshots)}
    outcome = outcome_for(data.snapshots[0], by_symbol, times, 5)
    assert outcome is not None
    assert outcome.value_pct == pytest.approx(1.0)
    assert outcome_for(data.snapshots[-1], by_symbol, times, 5) is None
    assert HORIZONS == (5, 15, 30, 60, 180)


def test_analysis_outputs_are_offline_bom_semicolon_and_immutable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import socket

    directory = _fixture_snapshot(tmp_path)
    before = {path.name: path.read_bytes() for path in directory.iterdir()}

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError((args, kwargs))

    monkeypatch.setattr(socket, "create_connection", forbidden)
    result = analyze(directory)
    outputs = write_outputs(result, tmp_path / "output")
    assert sources_unchanged(result)
    assert len(outputs) == 14
    assert result.metrics["source"]["snapshot_rows"] == 6  # type: ignore[index]
    assert result.metrics["trades"]["repair_v2"] == 1  # type: ignore[index]
    assert all(path.read_bytes() == before[path.name] for path in directory.iterdir())
    csv_path = tmp_path / "output" / "missed_moves.csv"
    assert csv_path.read_bytes().startswith(b"\xef\xbb\xbf")
    with csv_path.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.reader(stream, delimiter=";"))
    assert len(rows) == 3
    assert "too_far" in rows[1]


def test_diagnostic_module_has_no_network_telegram_or_bybit_imports() -> None:
    source = (
        ROOT
        / "src"
        / "market_signal_assistant"
        / "setup_engine"
        / "performance_audit.py"
    ).read_text(encoding="utf-8")
    assert "market_signal_assistant.telegram" not in source
    assert "market_signal_assistant.providers" not in source
    assert "urllib" not in source
