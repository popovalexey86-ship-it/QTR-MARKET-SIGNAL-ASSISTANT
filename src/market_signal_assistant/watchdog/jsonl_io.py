"""Bounded physical-line reads for append-only Watchdog JSONL evidence."""

from __future__ import annotations

from typing import BinaryIO

READ_CHUNK_BYTES = 64 * 1024
MAX_RECORD_BYTES = 1024 * 1024


class JsonlLineTooLong(ValueError):
    """A physical record exceeds the supported evidence size."""


def read_bounded_line(
    stream: BinaryIO, *, maximum_bytes: int = MAX_RECORD_BYTES
) -> bytes:
    """Read one line in small blocks, retaining at most one bounded record."""
    parts: list[bytes] = []
    size = 0
    while True:
        part = stream.readline(min(READ_CHUNK_BYTES, maximum_bytes - size + 1))
        if not part:
            return b"".join(parts)
        size += len(part)
        if size > maximum_bytes:
            raise JsonlLineTooLong("JSONL physical record exceeds size limit")
        parts.append(part)
        if part.endswith(b"\n"):
            return b"".join(parts)
