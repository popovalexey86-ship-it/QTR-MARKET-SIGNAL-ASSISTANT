from __future__ import annotations

import importlib
import os
from pathlib import Path
from typing import Any, BinaryIO


class DuplicateInstanceError(RuntimeError):
    """Another process owns the Watchdog storage writer lock."""


class SingleInstanceLock:
    """Cross-platform advisory process lock held for the writer lifetime."""

    def __init__(self, path: Path) -> None:
        self._path = path.resolve()
        self._stream: BinaryIO | None = None

    @property
    def held(self) -> bool:
        return self._stream is not None

    def acquire(self) -> None:
        if self._stream is not None:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        stream = self._path.open("a+b")
        try:
            stream.seek(0)
            if stream.tell() == 0 and self._path.stat().st_size == 0:
                stream.write(b"0")
                stream.flush()
                os.fsync(stream.fileno())
            stream.seek(0)
            _lock_nonblocking(stream)
            stream.seek(0)
            stream.truncate()
            stream.write(f"pid={os.getpid()}\n".encode())
            stream.flush()
            os.fsync(stream.fileno())
        except OSError as error:
            stream.close()
            raise DuplicateInstanceError(
                f"Watchdog storage is already locked: {self._path}."
            ) from error
        self._stream = stream

    def release(self) -> None:
        stream = self._stream
        if stream is None:
            return
        try:
            stream.seek(0)
            _unlock(stream)
        finally:
            stream.close()
            self._stream = None

    def __enter__(self) -> SingleInstanceLock:
        self.acquire()
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()


def _lock_nonblocking(stream: BinaryIO) -> None:
    if os.name == "nt":
        msvcrt: Any = importlib.import_module("msvcrt")
        msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        return
    fcntl: Any = importlib.import_module("fcntl")
    fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock(stream: BinaryIO) -> None:
    if os.name == "nt":
        msvcrt: Any = importlib.import_module("msvcrt")
        msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
        return
    fcntl: Any = importlib.import_module("fcntl")
    fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
