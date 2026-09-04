from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

from market_signal_assistant.qtr_signal_outcome.source import SignalSourceReader
from qtr_signal_outcome.helpers import NOW, source_record


def write_rows(path: Path, rows: list[object]) -> None:
    path.write_text(
        "\n".join(row if isinstance(row, str) else json.dumps(row) for row in rows)
        + "\n",
        encoding="utf-8",
    )


def test_source_reader_selects_only_committed_delivery(tmp_path: Path) -> None:
    path = tmp_path / "source.jsonl"
    write_rows(
        path,
        [
            source_record(),
            source_record(sent=False),
            source_record(decision="suppress"),
            source_record(delivery_committed=False),
        ],
    )
    reader = SignalSourceReader(path)
    signals = tuple(reader.iter_signals())
    assert len(signals) == 1
    assert signals[0].signal_price == 100.0
    assert reader.stats.delivered_records == 1


def test_malformed_and_damaged_last_line_are_isolated(tmp_path: Path) -> None:
    path = tmp_path / "source.jsonl"
    path.write_text(
        json.dumps(source_record()) + "\n{malformed}\n{partial",
        encoding="utf-8",
    )
    reader = SignalSourceReader(path)
    assert len(tuple(reader.iter_signals())) == 1
    assert reader.stats.invalid_source_records == 2


def test_duplicates_are_removed_but_two_real_sends_are_distinct(tmp_path: Path) -> None:
    path = tmp_path / "source.jsonl"
    first = source_record()
    second = source_record(
        timestamp=(NOW + timedelta(minutes=10)).isoformat(),
        semantic_fingerprint="fingerprint-2",
    )
    write_rows(path, [first, first, second])
    reader = SignalSourceReader(path)
    signals = tuple(reader.iter_signals())
    assert len(signals) == 2
    assert signals[0].signal_id != signals[1].signal_id
    assert reader.stats.duplicate_records == 1


def test_missing_price_or_invalid_atr_is_invalid_source(tmp_path: Path) -> None:
    path = tmp_path / "source.jsonl"
    missing_price = source_record()
    missing_price["price_context"]["market_price"] = None
    bad_atr = source_record(timestamp=(NOW + timedelta(minutes=1)).isoformat())
    bad_atr["price_context"]["atr"] = 0
    write_rows(path, [missing_price, bad_atr])
    reader = SignalSourceReader(path)
    assert tuple(reader.iter_signals()) == ()
    assert reader.stats.invalid_source_records == 2


def test_legacy_quality_is_unknown_and_not_inferred(tmp_path: Path) -> None:
    path = tmp_path / "source.jsonl"
    row = source_record()
    row.pop("telegram_quality_score")
    row.pop("quality_components")
    write_rows(path, [row])
    item = tuple(SignalSourceReader(path).iter_signals())[0]
    assert item.telegram_quality_score is None
    assert item.quality_components == ()


def test_source_filter_uses_timezone_aware_signal_timestamp(tmp_path: Path) -> None:
    path = tmp_path / "source.jsonl"
    write_rows(path, [source_record()])
    reader = SignalSourceReader(path)
    items = tuple(
        reader.iter_signals(
            since=NOW - timedelta(minutes=1),
            until=NOW + timedelta(minutes=1),
        )
    )
    assert items[0].signal_timestamp.tzinfo is not None


def test_large_source_is_read_streaming(tmp_path: Path, monkeypatch: object) -> None:
    path = tmp_path / "source.jsonl"
    write_rows(path, [source_record()] * 1000)

    def forbidden(*args: object, **kwargs: object) -> object:
        raise AssertionError((args, kwargs))

    monkeypatch.setattr(Path, "read_text", forbidden)  # type: ignore[attr-defined]
    monkeypatch.setattr(Path, "read_bytes", forbidden)  # type: ignore[attr-defined]

    reader = SignalSourceReader(path)
    assert len(tuple(reader.iter_signals())) == 1
    assert reader.stats.lines_read == 1000
