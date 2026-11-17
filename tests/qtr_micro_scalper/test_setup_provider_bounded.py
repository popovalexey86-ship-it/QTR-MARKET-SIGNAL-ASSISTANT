from __future__ import annotations

import gc
import json
import tracemalloc
from datetime import timedelta
from pathlib import Path

import pytest
from test_price_context_adapter import NOW, _payload

from market_signal_assistant.qtr_micro_scalper.price_context_adapter import (
    _SETUP_MAX_LINE_BYTES,
    _SETUP_READ_CHUNK_BYTES,
    JsonlVerifiedSetupProvider,
    _parse_record_line,
)


def line(symbol: str = "BTCUSDT", seconds: int = 0) -> bytes:
    return (
        json.dumps(
            _payload(symbol=symbol, observed_at=NOW + timedelta(seconds=seconds)),
            ensure_ascii=False,
        )
        + "\n"
    ).encode()


def test_bootstrap_latest_semantics_match_independent_streaming_reference(
    tmp_path: Path,
) -> None:
    path = tmp_path / "audit.jsonl"
    expected = {}
    with path.open("wb") as stream:
        for i in range(5_000):
            # Out-of-order timestamps and equal-time ties both occur.
            raw = line(f"SYMBOL{i % 7}", (i * 13) % 39)
            stream.write(raw)
            row, malformed = _parse_record_line(raw.decode())
            assert row is not None and not malformed
            old = expected.get(row.symbol)
            if old is None or row.observed_at >= old.observed_at:
                expected[row.symbol] = row
    provider = JsonlVerifiedSetupProvider(path)
    assert provider.latest_records() == tuple(expected[s] for s in sorted(expected))
    assert provider._offset == path.stat().st_size
    assert provider.metrics.bytes_read == path.stat().st_size
    assert provider.metrics.bootstrap_scans == 1
    before = provider.metrics
    for _ in range(30):
        provider.latest_records()
    assert provider.metrics == before


@pytest.mark.parametrize("chunk_size", [1, 7, 64, 65536])
def test_partial_unicode_crlf_and_multiple_append_cycles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    chunk_size: int,
) -> None:
    monkeypatch.setattr(
        "market_signal_assistant.qtr_micro_scalper.price_context_adapter._SETUP_READ_CHUNK_BYTES",
        chunk_size,
    )
    path = tmp_path / "audit.jsonl"
    raw = line().replace(b"\n", b"\r\n")
    # Deliberately split a multibyte Cyrillic codepoint.
    cut = raw.index("П".encode()) + 1
    path.write_bytes(raw[:cut])
    provider = JsonlVerifiedSetupProvider(path)
    assert provider.latest_records() == ()
    assert provider._offset == cut and provider._pending == raw[:cut]
    for fragment in (raw[cut:-2], raw[-2:-1], raw[-1:]):
        with path.open("ab") as stream:
            stream.write(fragment)
        rows = provider.latest_records()
        assert len(rows) == (1 if fragment == raw[-1:] else 0)
        assert provider.metrics.malformed_lines == 0
    for i in range(1, 6):
        appended = line(seconds=i)
        before = provider.metrics
        with path.open("ab") as stream:
            stream.write(appended)
        assert provider.latest("BTCUSDT").observed_at == NOW + timedelta(seconds=i)
        assert provider.metrics.bytes_read - before.bytes_read == len(appended)
        assert provider.metrics.incremental_reads - before.incremental_reads == 1
    assert provider.metrics.incremental_reads == 8
    assert provider.metrics.bytes_read == path.stat().st_size == provider._offset
    assert provider._pending == b""


def test_oversized_line_counted_once_across_refreshes_and_next_line_survives(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    path = tmp_path / "audit.jsonl"
    path.write_bytes(b"x" * _SETUP_MAX_LINE_BYTES)
    provider = JsonlVerifiedSetupProvider(path)
    assert len(provider._pending) == _SETUP_MAX_LINE_BYTES
    assert provider.metrics.malformed_lines == 0
    for _ in range(5):
        with path.open("ab") as stream:
            stream.write(b"x" * _SETUP_READ_CHUNK_BYTES)
        provider.latest_records()
        assert provider._pending == b""
        assert provider.metrics.malformed_lines == 1
    with path.open("ab") as stream:
        stream.write(b"\n" + line() + b"y" * (_SETUP_MAX_LINE_BYTES + 1) + b"\n")
    assert provider.latest("BTCUSDT") is not None
    assert provider.metrics.malformed_lines == 2
    assert not provider._discarding_oversized_line
    assert len([r for r in caplog.records if "line exceeds" in r.message]) == 1


def test_malformed_utf8_blank_and_incomplete_lines_are_isolated(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    path.write_bytes(b"\n\r\nnot-json\n\xff\xfe\n" + line() + b'{"pending":')
    provider = JsonlVerifiedSetupProvider(path)
    assert provider.latest("BTCUSDT") is not None
    assert provider.metrics.malformed_lines == 2
    assert provider._pending == b'{"pending":'
    with path.open("ab") as stream:
        stream.write(b"true}\n")
    provider.latest_records()
    assert provider.metrics.malformed_lines == 2
    assert provider._pending == b""


@pytest.mark.parametrize(
    "change",
    ["bootstrap", "append", "replace", "truncate", "open_race", "truncate_race"],
)
def test_all_rebuild_paths_use_bounded_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    path = tmp_path / "audit.jsonl"
    path.write_bytes(line() * 10)
    provider = JsonlVerifiedSetupProvider(path)
    old_identity = provider._file_identity
    if change == "append":
        with path.open("ab") as stream:
            for i in range(500):
                stream.write(line("ETHUSDT", i))
    elif "truncate" in change:
        path.write_bytes(line("ETHUSDT"))
    else:
        replacement = tmp_path / "replacement.jsonl"
        with replacement.open("wb") as stream:
            for _ in range(500):
                stream.write(line("ETHUSDT"))
        replacement.replace(path)
    original_open = Path.open
    sizes = []

    class Guard:
        def __init__(self, stream):
            self.stream = stream

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.stream.close()

        def fileno(self):
            return self.stream.fileno()

        def seek(self, offset):
            return self.stream.seek(offset)

        def read(self, size=-1):
            assert 0 < size <= _SETUP_READ_CHUNK_BYTES
            sizes.append(size)
            return self.stream.read(size)

        def readlines(self, *args):
            raise AssertionError("full readlines forbidden")

        def __iter__(self):
            raise AssertionError("unbounded line iteration forbidden")

    def guarded_open(p, mode="r", *args, **kwargs):
        stream = original_open(p, mode, *args, **kwargs)
        return Guard(stream) if p == path and mode == "rb" else stream

    monkeypatch.setattr(Path, "open", guarded_open)
    if change == "bootstrap":
        provider = JsonlVerifiedSetupProvider(path)
    elif change.endswith("race"):
        provider._read_increment_locked(old_identity)
    assert (provider.latest("BTCUSDT") is not None) == (change == "append")
    assert provider.latest("ETHUSDT") is not None
    assert sizes
    assert provider.metrics.bootstrap_scans == (
        1 if change in {"bootstrap", "append"} else 2
    )
    assert provider.metrics.resets_rotations == (
        0 if change in {"bootstrap", "append"} else 1
    )
    assert provider._offset == path.stat().st_size


def test_large_bootstrap_and_rotation_have_bounded_python_memory(
    tmp_path: Path,
) -> None:
    peaks = []
    payload = _payload()
    payload["padding"] = "x" * 32_000
    raw = (json.dumps(payload) + "\n").encode()
    path = tmp_path / "audit.jsonl"
    for size in (2 * 1024 * 1024, 32 * 1024 * 1024):
        replacement = tmp_path / "replacement.jsonl"
        with replacement.open("wb") as stream:
            for _ in range(size // len(raw)):
                stream.write(raw)
        if not path.exists():
            replacement.replace(path)
            provider = None
        else:
            # Bootstrap small generation before measuring a large replacement.
            provider = JsonlVerifiedSetupProvider(path)
            replacement.replace(path)
        gc.collect()
        tracemalloc.start()
        if provider is None:
            provider = JsonlVerifiedSetupProvider(path)
        assert len(provider.latest_records()) == 1
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        peaks.append(peak)
        assert provider.metrics.bytes_read >= path.stat().st_size
    # Previous read()+split() retains tens of MiB for this 32 MiB file.
    assert max(peaks) < 3 * 1024 * 1024
    assert peaks[1] - peaks[0] < 1024 * 1024
