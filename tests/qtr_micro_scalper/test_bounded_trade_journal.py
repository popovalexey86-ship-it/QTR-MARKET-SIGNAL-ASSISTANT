from __future__ import annotations

import gc
import random
import subprocess
import sys
import tracemalloc
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
from test_analytics import decision
from test_metrics import NOW, record, runtime_event, sample_records

from market_signal_assistant.qtr_micro_scalper.analytics import (
    IncrementalDecisionAnalytics,
    analyze_shadow_journals,
)
from market_signal_assistant.qtr_micro_scalper.cli import JournalObservationSource
from market_signal_assistant.qtr_micro_scalper.decision_journal import (
    ShadowDecisionEventType,
)
from market_signal_assistant.qtr_micro_scalper.metrics import (
    ShadowMetricsAggregator,
    TradeMetricDimensions,
    aggregate_shadow_metrics,
)
from market_signal_assistant.qtr_micro_scalper.service import (
    build_shadow_service_from_environment,
)
from market_signal_assistant.qtr_micro_scalper.shadow_decision import (
    ShadowOutcomeStatus,
    ShadowTradeStage,
)
from market_signal_assistant.qtr_micro_scalper.shadow_journal import (
    ShadowTradeJournal,
    ShadowTradeRecord,
    recover_shadow_journal,
    serialize_shadow_trade_record,
)
from market_signal_assistant.qtr_micro_scalper.shadow_journal_index import (
    ShadowTradeJournalIndex,
)
from market_signal_assistant.qtr_micro_scalper.shadow_runtime import (
    ShadowRuntimeEventType,
)


def write(path: Path, records: list[ShadowTradeRecord]) -> None:
    path.write_text(
        "".join(serialize_shadow_trade_record(r) + "\n" for r in records),
        encoding="utf8",
    )


def test_append_tails_once_and_deduplicates_all_history(tmp_path: Path) -> None:
    path = tmp_path / "trades.jsonl"
    first = record("first")
    write(path, [first])
    journal = ShadowTradeJournal(path)
    initial = journal.index_metrics
    for i in range(200):
        assert journal.append(record(str(i)))
    size = path.stat().st_size
    assert journal.record_count == 201
    assert not journal.append(replace(first, recorded_at=NOW + timedelta(days=1)))
    assert path.stat().st_size == size
    for _ in range(30):
        journal.refresh()
    assert journal.index_metrics.bootstrap_scans == initial.bootstrap_scans == 1
    assert journal.index_metrics.records_processed == 201
    assert journal.index_metrics.bytes_read == size
    restarted = ShadowTradeJournal(path)
    assert not restarted.append(first)
    assert restarted.record_count == 201


@pytest.mark.parametrize("mode", ["replace", "truncate", "regrow", "delete"])
def test_reset_on_rotation_or_truncation(tmp_path: Path, mode: str) -> None:
    path = tmp_path / "trades.jsonl"
    write(path, [record("old")])
    journal = ShadowTradeJournal(path)
    if mode == "replace":
        replacement = tmp_path / "replacement.jsonl"
        write(replacement, [record("new")])
        replacement.replace(path)
    elif mode == "truncate":
        path.write_bytes(b"")
    elif mode == "regrow":
        write(path, [record("new"), record("another")])
    else:
        path.unlink()
    journal.refresh()
    assert journal.latest("old") is None
    assert journal.index_metrics.resets_rotations == 1
    expected = {"replace": 1, "regrow": 2, "truncate": 0, "delete": 0}[mode]
    assert journal.record_count == expected


def test_partial_line_is_deferred_and_malformed_diagnostics_are_bounded(
    tmp_path: Path,
) -> None:
    path = tmp_path / "trades.jsonl"
    payload = serialize_shadow_trade_record(record("partial")).encode()
    path.write_bytes(b"bad\n" * 150 + payload[:-5])
    index = ShadowTradeJournalIndex(path)
    index.refresh()
    assert index.record_count == 0
    assert index.metrics().malformed_lines == 150
    assert len(index.recovery.warnings) == 100
    with path.open("ab") as stream:
        stream.write(payload[-5:] + b"\n")
    index.refresh()
    assert index.record_count == 1
    assert index.metrics().bootstrap_scans == 1
    assert index.metrics().file_offset == path.stat().st_size


@pytest.mark.parametrize("valid_tail", [True, False])
def test_writer_preserves_crash_tail_and_duplicate_identity(
    tmp_path: Path, valid_tail: bool
) -> None:
    path = tmp_path / "trades.jsonl"
    first = record("first")
    original = (
        serialize_shadow_trade_record(first).encode() if valid_tail else b'{"partial":'
    )
    path.write_bytes(original)
    journal = ShadowTradeJournal(path)
    assert journal.append(first) is (not valid_tail)
    assert path.read_bytes().startswith(original)
    assert journal.record_count == 1
    assert journal.latest("first") == first


def test_recovery_retains_five_waiting_and_one_open_without_fabrication(
    tmp_path: Path,
) -> None:
    path = tmp_path / "trades.jsonl"
    waiting = [
        record(
            str(i),
            stage=ShadowTradeStage.WAITING_ENTRY,
            entered=False,
            outcome=ShadowOutcomeStatus.PENDING,
            result_r=0.0,
        )
        for i in range(5)
    ]
    opened = record(
        "open", stage=ShadowTradeStage.OPEN, outcome=ShadowOutcomeStatus.PENDING
    )
    write(path, [*sample_records(), *waiting, opened])
    journal = ShadowTradeJournal(path)
    before = path.read_bytes()
    assert set(journal.active_records) == set([*waiting, opened])
    assert path.read_bytes() == before
    assert len(journal.snapshot().states) == 10


def test_live_start_refuses_to_discard_unresumable_legacy_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = ShadowTradeJournal(tmp_path / "trades.jsonl")
    assert journal.append(record("open", stage=ShadowTradeStage.OPEN))
    monkeypatch.setenv("QTR_SCALPER_V2_ENABLED", "true")
    monkeypatch.setenv("QTR_SCALPER_V2_SHADOW_MODE", "true")
    with pytest.raises(RuntimeError, match="unfinished trades"):
        build_shadow_service_from_environment(journal=journal)
    assert len(journal.active_records) == 1


@pytest.mark.parametrize("seed", range(4))
def test_compact_metrics_and_analytics_match_full_history(
    tmp_path: Path, seed: int
) -> None:
    path = tmp_path / "trades.jsonl"
    rows = list(sample_records())
    rows.extend(
        [
            replace(
                record("win", seconds=1),
                events=(
                    runtime_event("win", "BTCUSDT", ShadowRuntimeEventType.TP1_REACHED),
                ),
                warnings=("historical stop warning",),
            ),
            record("win", seconds=30, events=()),
            record(
                "pending",
                stage=ShadowTradeStage.WAITING_ENTRY,
                entered=False,
                outcome=ShadowOutcomeStatus.PENDING,
            ),
        ]
    )
    random.Random(seed).shuffle(rows)
    write(path, rows)
    journal = ShadowTradeJournal(path)
    history = recover_shadow_journal(path).records
    dimensions = {"win": TradeMetricDimensions("TEST", "RETEST")}
    assert ShadowMetricsAggregator(journal).snapshot(
        generated_at=NOW, dimensions=dimensions
    ) == aggregate_shadow_metrics(history, generated_at=NOW, dimensions=dimensions)
    decisions = [
        decision(ShadowDecisionEventType.SHADOW_ENTRY_CREATED, seconds=20),
        decision(ShadowDecisionEventType.SHADOW_ENTRY_CREATED),
        decision(ShadowDecisionEventType.DECISION_BLOCKED, reasons=("risk",)),
    ]
    cache = IncrementalDecisionAnalytics()
    for row in decisions:
        cache.consume(row)
    assert cache.snapshot(
        journal.snapshot(), generated_at=NOW
    ) == analyze_shadow_journals(decisions, history, generated_at=NOW)
    assert journal.latest("win") == next(
        r for r in reversed(rows) if r.trade_id == "win"
    )


def test_observation_never_recreates_journal_or_calls_offline_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = ShadowTradeJournal(tmp_path / "trades.jsonl")
    source = JournalObservationSource(
        decision_journal_path=tmp_path / "decisions.jsonl",
        trade_journal_path=journal.path,
        trade_journal=journal,
    )

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("Live observation attempted full-history recovery")

    monkeypatch.setattr(ShadowTradeJournal, "records", forbidden)
    monkeypatch.setattr(
        "market_signal_assistant.qtr_micro_scalper.shadow_journal.recover_shadow_journal",
        forbidden,
    )
    for i in range(60):
        journal.append(record(str(i)))
        result = source.observe(generated_at=NOW)
        assert result.metrics.journal_records == i + 1
    after = journal.index_metrics
    for _ in range(30):
        source.observe(generated_at=NOW)
    assert journal.index_metrics == after
    assert after.bootstrap_scans == 1
    assert after.retained_trades == 60


def test_many_updates_for_fixed_trade_cardinality_have_bounded_python_memory(
    tmp_path: Path,
) -> None:
    peaks = []
    for count in (500, 10_000):
        path = tmp_path / f"{count}.jsonl"
        template = record("fixed")
        with path.open("w", encoding="utf8") as stream:
            for i in range(count):
                row = replace(
                    template, mfe=float(i), recorded_at=NOW + timedelta(seconds=i)
                )
                stream.write(serialize_shadow_trade_record(row) + "\n")
        gc.collect()
        tracemalloc.start()
        journal = ShadowTradeJournal(path)
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        peaks.append(peak)
        assert journal.record_count == count
        assert journal.index_metrics.retained_trades == 1
        assert not hasattr(journal, "_records")
        journal.close()
    assert peaks[1] - peaks[0] < 2_000_000


@pytest.mark.parametrize("collision", ["decisions", "trades", "snapshot", "growth"])
def test_offline_validator_cannot_overwrite_source_journals(
    tmp_path: Path, collision: str
) -> None:
    output = tmp_path / "report.json"
    decisions = tmp_path / (
        "report.snapshot.json" if collision == "snapshot" else "decisions.jsonl"
    )
    trades = tmp_path / (
        "report.benchmark-trades.jsonl" if collision == "growth" else "trades.jsonl"
    )
    decisions.write_bytes(b"immutable decisions\n")
    trades.write_bytes(b"immutable trades\n")
    if collision == "decisions":
        output = decisions
    elif collision == "trades":
        output = trades
    script = (
        Path(__file__).resolve().parents[2]
        / "scripts/validate_scalper_bounded_memory.py"
    )
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            str(script),
            "--decisions",
            str(decisions),
            "--trades",
            str(trades),
            "--audit",
            "--output",
            str(output),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert "never overwrite" in result.stderr
    assert decisions.read_bytes() == b"immutable decisions\n"
    assert trades.read_bytes() == b"immutable trades\n"
