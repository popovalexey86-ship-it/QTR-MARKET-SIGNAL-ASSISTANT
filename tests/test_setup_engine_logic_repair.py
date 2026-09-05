from __future__ import annotations

import csv
import json
from pathlib import Path

from market_signal_assistant.setup_engine.logic_repair import (
    compare_audit,
    write_comparison,
)
from market_signal_assistant.setup_engine.models import SetupType

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_AUDIT = (
    ROOT / "tests" / "fixtures" / "early_discovery_v2_recovered_retest.jsonl"
)


def test_replay_reclassifies_recovered_historical_false_breakout(
    tmp_path: Path,
) -> None:
    first = FIXTURE_AUDIT.read_text(encoding="utf-8").splitlines()[0]
    source = tmp_path / "audit.jsonl"
    source.write_text(first + "\n", encoding="utf-8")
    before = source.read_bytes()

    comparison = compare_audit(source)

    assert comparison.before[0].setup_type is SetupType.FALSE_BREAKOUT
    assert comparison.after[0].setup_type is SetupType.RETEST
    assert comparison.snapshots[0].result.historical_breakout_failure is True
    assert comparison.snapshots[0].result.current_breakout_failure is False
    assert comparison.snapshots[0].result.structure_recovered is True
    assert comparison.source_sha256_before == comparison.source_sha256_after
    assert source.read_bytes() == before


def test_logic_repair_writes_four_offline_artifacts(tmp_path: Path) -> None:
    first = FIXTURE_AUDIT.read_text(encoding="utf-8").splitlines()[0]
    source = tmp_path / "audit.jsonl"
    source.write_text(first + "\n", encoding="utf-8")
    outputs = write_comparison(compare_audit(source), tmp_path / "output")
    assert {item.name for item in outputs} == {
        "сравнение_до_после.md",
        "метрики_до_после.json",
        "изменённые_классификации.csv",
        "проверка_переходов.csv",
    }
    metrics = json.loads(outputs[1].read_text(encoding="utf-8"))
    assert metrics["source"]["unchanged"] is True
    with outputs[2].open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.reader(stream, delimiter=";"))
    assert rows[0][0] == "Строка"
    assert rows[1][3:5] == ["ЛОЖНЫЙ ПРОБОЙ", "РЕТЕСТ"]
