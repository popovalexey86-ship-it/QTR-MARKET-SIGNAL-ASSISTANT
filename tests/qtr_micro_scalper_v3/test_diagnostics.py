from __future__ import annotations

import json
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

from market_signal_assistant.qtr_micro_scalper_v3.diagnostics import (
    DecisionDiagnostics,
)
from market_signal_assistant.qtr_micro_scalper_v3.engine import CashScalperEngine
from market_signal_assistant.qtr_micro_scalper_v3.models import ImpulseDirection
from qtr_micro_scalper_v3.helpers import NOW, snapshot


def test_decision_diagnostics_count_accept_reject_and_reasons(tmp_path: Path) -> None:
    diagnostics = DecisionDiagnostics(tmp_path / "decisions.jsonl")
    engine = CashScalperEngine()
    accepted = engine.evaluate(snapshot())
    rejected = engine.evaluate(replace(snapshot(), price_displacement_5s_bps=0.0))

    diagnostics.observe(accepted)
    diagnostics.observe(rejected)
    current = diagnostics.snapshot()

    assert current.snapshots_evaluated == 2
    assert current.accepted == 1
    assert current.rejected == 1
    assert current.long_evaluated == 2
    assert current.short_evaluated == 0
    assert current.blocking_reasons["insufficient_directional_displacement"] == 1
    assert current.spread_min_bps == 2.0
    assert current.spread_max_bps == 2.0
    assert current.spread_mean_bps == 2.0


def test_diagnostics_write_compact_periodic_summary_not_per_tick(
    tmp_path: Path,
) -> None:
    path = tmp_path / "decisions.jsonl"
    diagnostics = DecisionDiagnostics(path)
    engine = CashScalperEngine()
    for seconds in range(50):
        decision = engine.evaluate(
            replace(
                snapshot(ImpulseDirection.SHORT),
                observed_at=NOW + timedelta(seconds=seconds),
                source_at=NOW + timedelta(seconds=seconds),
                impulse_started_at=NOW,
            )
        )
        diagnostics.observe(decision)

    assert not path.exists()
    diagnostics.flush()
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["snapshots_evaluated"] == 50
    assert payload["short_evaluated"] == 50


def test_diagnostics_roll_over_at_60_seconds(tmp_path: Path) -> None:
    path = tmp_path / "decisions.jsonl"
    diagnostics = DecisionDiagnostics(path)
    engine = CashScalperEngine()
    diagnostics.observe(engine.evaluate(snapshot()))
    diagnostics.observe(
        engine.evaluate(
            replace(
                snapshot(),
                observed_at=NOW + timedelta(seconds=60),
                source_at=NOW + timedelta(seconds=60),
                impulse_started_at=NOW + timedelta(seconds=55),
            )
        )
    )

    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["snapshots_evaluated"] == 1
