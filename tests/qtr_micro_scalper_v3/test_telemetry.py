from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

from market_signal_assistant.qtr_micro_scalper_v3.engine import CashScalperEngine
from market_signal_assistant.qtr_micro_scalper_v3.models import (
    ImpulseDirection,
    V3ForwardOutcome,
)
from market_signal_assistant.qtr_micro_scalper_v3.telemetry import (
    ForwardOutcomeTracker,
    JsonlTelemetryJournal,
    build_entry_record,
)
from qtr_micro_scalper_v3.helpers import NOW, snapshot


def test_entry_record_contains_only_causal_snapshot_features(tmp_path: Path) -> None:
    source = snapshot()
    decision = CashScalperEngine().evaluate(source, notional=1_000.0)
    assert decision.entry_price is not None
    assert decision.target_price is not None
    assert decision.stop_price is not None
    record = build_entry_record(
        source,
        recorded_at=NOW,
        notional=1_000.0,
        entry_price=decision.entry_price,
        target_price=decision.target_price,
        stop_price=decision.stop_price,
        cost=decision.cost,
    )
    journal = JsonlTelemetryJournal(tmp_path / "entries.jsonl")
    assert journal.append(record) is True

    payload = json.loads((tmp_path / "entries.jsonl").read_text(encoding="utf-8"))
    assert payload["symbol"] == "BTCUSDT"
    assert payload["direction"] == "LONG"
    assert payload["impulse_id"] == "BTC-LONG-1"
    assert payload["price_displacement_5s_bps"] == 8.0
    assert payload["source_age_ms"] == 100.0
    assert payload["impulse_age_seconds"] == 6.0
    assert payload["estimated_round_trip_cost_bps"] == 15.0
    assert "future_outcome" not in payload


def test_forward_outcomes_continue_after_strategy_exit_and_cover_all_windows() -> None:
    tracker = ForwardOutcomeTracker(
        entry_id="entry-1",
        symbol="BTCUSDT",
        direction=ImpulseDirection.LONG,
        entry_at=NOW,
        entry_price=100.0,
        round_trip_cost_pct=0.15,
    )
    prices = (
        (20, 100.20),
        (40, 100.30),
        (70, 100.55),
        (190, 100.10),
        (310, 99.80),
        (610, 100.40),
    )
    outcomes: list[V3ForwardOutcome] = []
    for seconds, price in prices:
        outcomes.extend(tracker.observe(NOW + timedelta(seconds=seconds), price))

    assert [item.window_seconds for item in outcomes] == [30, 60, 180, 300, 600]
    final = outcomes[-1]
    assert final.mfe_pct == 0.55
    assert final.mae_pct == 0.2
    assert final.reached_025 is True
    assert final.reached_050 is True
    assert final.time_to_025_seconds == 40.0
    assert final.time_to_050_seconds == 70.0
    assert final.net_hypothetical_pct == -0.35


def test_short_forward_outcomes_keep_directional_mfe_and_mae() -> None:
    tracker = ForwardOutcomeTracker(
        entry_id="entry-short",
        symbol="BTCUSDT",
        direction=ImpulseDirection.SHORT,
        entry_at=NOW,
        entry_price=100.0,
        round_trip_cost_pct=0.1,
    )
    tracker.observe(NOW + timedelta(seconds=20), 99.5)
    tracker.observe(NOW + timedelta(seconds=25), 100.2)
    outcome = tracker.observe(NOW + timedelta(seconds=31), 100.1)[0]
    assert outcome.mfe_pct == 0.5
    assert outcome.mae_pct == 0.2


def test_jsonl_is_deterministic_and_duplicate_safe(tmp_path: Path) -> None:
    path = tmp_path / "entries.jsonl"
    journal = JsonlTelemetryJournal(path)
    source = snapshot()
    decision = CashScalperEngine().evaluate(source)
    assert decision.entry_price is not None
    assert decision.target_price is not None
    assert decision.stop_price is not None
    record = build_entry_record(
        source,
        recorded_at=NOW,
        notional=1_000.0,
        entry_price=decision.entry_price,
        target_price=decision.target_price,
        stop_price=decision.stop_price,
        cost=decision.cost,
    )
    assert journal.append(record) is True
    assert journal.append(record) is False
    assert len(path.read_text(encoding="utf-8").splitlines()) == 1
