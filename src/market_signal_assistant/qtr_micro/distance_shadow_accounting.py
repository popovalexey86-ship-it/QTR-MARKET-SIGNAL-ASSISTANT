"""Shadow-only checkpoints and an immutable final seal; no production state I/O."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

_LOGGER = logging.getLogger(__name__)
ACCOUNTING_SCHEMA_VERSION = 1
SEAL_SCHEMA_VERSION = 1
EXPERIMENT_SCHEMA_VERSION = 3
COUNTERS = (
    "accepted_count",
    "completed_count",
    "failed_invalid_count",
    "cancelled_invalid_count",
    "queue_overflow_count",
    "queue_dropped_count",
    "worker_exception_count",
    "enqueue_failure_count",
    "enqueue_dropped_count",
    "journal_failure_count",
    "fallback_failure_count",
)


def _current_invocation_id() -> str:
    value = os.getenv("INVOCATION_ID", "")
    if not value.strip():
        raise ValueError("Shadow requires a non-empty InvocationID.")
    return value


def _run_created_at() -> str:
    return datetime.now(UTC).isoformat()


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("Missing Shadow lifecycle timestamp.")
    result = datetime.fromisoformat(value)
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("Shadow lifecycle timestamps must be offset-aware.")
    return result


def _validate_final(state: dict[str, Any]) -> None:
    if (
        type(state.get("seal_schema_version")) is not int
        or state["seal_schema_version"] != SEAL_SCHEMA_VERSION
    ):
        raise ValueError("Unknown Shadow seal schema.")
    created = _timestamp(state.get("run_created_at"))
    cutoff = _timestamp(state.get("admission_closed_at"))
    finalized = _timestamp(state.get("finalized_at"))
    if not created <= cutoff <= finalized <= _timestamp(state["checkpoint_at"]):
        raise ValueError("Impossible Shadow lifecycle ordering.")
    for key in (*COUNTERS, "invalid_observations", "persistence_failures", "revision"):
        if type(state.get(key)) is not int or state[key] < 0:
            raise ValueError("Invalid final Shadow counter.")
    for key in ("evidence_record_count", "unique_observation_count"):
        if type(state.get(key)) is not int or state[key] < 0:
            raise ValueError("Invalid Shadow evidence count.")
    if state["unique_observation_count"] != state["evidence_record_count"]:
        raise ValueError("Duplicate Shadow observations cannot be sealed.")
    if state["evidence_record_count"] != state["completed_count"]:
        raise ValueError("Shadow evidence/count mismatch.")
    digest = state.get("evidence_sha256")
    if not isinstance(digest, str) or len(digest) != 64:
        raise ValueError("Missing Shadow evidence digest.")


async def drain_thread_task(task: asyncio.Task[Any]) -> Any:
    """Cancellation of an awaiter must not orphan already-running thread work."""
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    return task.result()


class ShadowAccounting:
    def __init__(self, path: Path, run_id: str) -> None:
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError("Shadow requires a non-empty run identity.")
        self.path = path
        self.run_id = run_id
        self.invocation_id = _current_invocation_id()
        if not isinstance(self.invocation_id, str) or not self.invocation_id.strip():
            raise ValueError("Shadow requires a non-empty InvocationID.")
        self.owner_id = uuid4().hex
        self.run_created_at = _run_created_at()
        self._final_state: dict[str, Any] = {}
        self.counts = dict.fromkeys(COUNTERS, 0)
        # No adoption/resumption: even the same invocation must use new files.
        owner_path = path.with_suffix(".owner.json")
        evidence_path = path.with_name(
            path.name.removesuffix(".accounting.jsonl") + ".jsonl"
        )
        if any(
            item.exists()
            for item in (
                path,
                evidence_path,
                owner_path,
                path.with_suffix(".seal.json"),
                path.with_suffix(".seal.tmp"),
            )
        ):
            raise ValueError("Existing Shadow run cannot acquire a new writer.")
        path.parent.mkdir(parents=True, exist_ok=True)
        with owner_path.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(json.dumps(self._identity(), sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        self._revision = 0
        self._persisted_revision = -1
        self._persisted_snapshot: dict[str, Any] | None = None
        self._persistence_failures = 0
        self._writer: asyncio.Task[None] | None = None
        self._sealed = False
        self._sealing = False

    def _identity(self) -> dict[str, Any]:
        return {
            "experiment_run_id": self.run_id,
            "invocation_id": self.invocation_id,
            "owner_id": self.owner_id,
            "accounting_schema_version": ACCOUNTING_SCHEMA_VERSION,
            "seal_schema_version": SEAL_SCHEMA_VERSION,
            "experiment_schema_version": EXPERIMENT_SCHEMA_VERSION,
            "run_created_at": self.run_created_at,
        }

    def admission_failed(
        self, *, accepted_before: int, dropped_before: int, dropped: int
    ) -> None:
        """Synchronous rollback accounting, independent of the failed scheduler."""
        admitted = self.counts["accepted_count"] - accepted_before
        self.counts["failed_invalid_count"] += admitted
        self.counts["enqueue_failure_count"] += 1
        already_dropped = (
            self.counts["queue_dropped_count"]
            + self.counts["enqueue_dropped_count"]
            - dropped_before
        )
        self.counts["enqueue_dropped_count"] += max(0, dropped - already_dropped)
        self._persistence_failures += int(admitted > 0)
        self._revision += 1
        with suppress(Exception):
            self._schedule()

    def snapshot(self) -> dict[str, Any]:
        invalid = (
            self.counts["queue_dropped_count"]
            + self.counts["worker_exception_count"]
            + self.counts["enqueue_dropped_count"]
            + self.counts["cancelled_invalid_count"]
        )
        terminal = (
            self.counts["completed_count"]
            + self.counts["failed_invalid_count"]
            + self.counts["cancelled_invalid_count"]
        )
        return {
            **self._identity(),
            **self._final_state,
            "checkpoint_at": datetime.now(UTC).isoformat(),
            "revision": self._revision,
            **self.counts,
            "invalid_observations": invalid,
            "pending_count": self.counts["accepted_count"] - terminal,
            "persistence_failures": self._persistence_failures,
            "authoritative": self._persistence_failures == 0,
            "sealed": self._sealed,
        }

    def change(self, **increments: int) -> None:
        if self._sealing or self._sealed:
            raise RuntimeError("Shadow accounting admission is closed.")
        if any(value < 0 for value in increments.values()):
            raise ValueError("Shadow counters cannot regress.")
        if any(key not in self.counts for key in increments):
            raise ValueError("Unknown Shadow counter.")
        for key, value in increments.items():
            self.counts[key] += value
        self._revision += 1
        self._schedule()

    def _schedule(self) -> None:
        if self._writer is None or self._writer.done():
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                return
            self._writer = loop.create_task(self._flush())

    async def wait(self) -> None:
        if (
            self._persisted_revision != self._revision
            and not self._persistence_failures
        ):
            self._schedule()
        if self._writer is not None:
            await asyncio.shield(self._writer)

    async def _flush(self) -> None:
        while self._persisted_revision != self._revision:
            snapshot = self.snapshot()
            io = asyncio.create_task(asyncio.to_thread(self._append, snapshot))
            try:
                await asyncio.shield(io)
            except asyncio.CancelledError:
                try:
                    await drain_thread_task(io)
                finally:
                    self._persistence_failures += 1
                raise
            except Exception as error:
                self._persistence_failures += 1
                _LOGGER.warning(
                    "QTR distance Shadow accounting unavailable (%s).",
                    type(error).__name__,
                )
                return
            self._persisted_revision = snapshot["revision"]
            self._persisted_snapshot = snapshot

    def _append(self, snapshot: dict[str, Any]) -> None:
        payload = json.dumps(snapshot, sort_keys=True) + "\n"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())

    async def seal(
        self,
        *,
        evidence_record_count: int,
        evidence_sha256: str,
        admission_closed_at: str,
        unique_observation_count: int | None = None,
    ) -> dict[str, Any]:
        if self._sealing or self._sealed:
            raise RuntimeError("Shadow seal already requested.")
        self._sealing = True
        await self.wait()
        if self._persistence_failures or self.snapshot()["pending_count"] != 0:
            raise RuntimeError("Cannot seal unavailable/pending Shadow accounting.")
        if evidence_record_count != self.counts["completed_count"]:
            raise RuntimeError("Cannot seal a Shadow journal/accounting mismatch.")
        # Every final field is persisted inside the hashed final checkpoint.
        self._final_state = {
            "seal_schema_version": SEAL_SCHEMA_VERSION,
            "admission_closed_at": admission_closed_at,
            "finalized_at": datetime.now(UTC).isoformat(),
            "evidence_record_count": evidence_record_count,
            "unique_observation_count": (
                evidence_record_count
                if unique_observation_count is None
                else unique_observation_count
            ),
            "evidence_sha256": evidence_sha256,
        }
        _validate_final(self.snapshot())
        self._revision += 1
        await self.wait()
        if self._persistence_failures or self._persisted_revision != self._revision:
            raise RuntimeError("Final Shadow checkpoint was not persisted.")
        assert self._persisted_snapshot is not None
        seal = {**self._persisted_snapshot, "sealed": True}
        io = asyncio.create_task(asyncio.to_thread(self._publish_seal, seal))
        try:
            await asyncio.shield(io)
        except asyncio.CancelledError:
            await drain_thread_task(io)
            self._sealed = True
            raise
        except Exception:
            self._persistence_failures += 1
            raise
        self._sealed = True
        return seal

    def _publish_seal(self, seal: dict[str, Any]) -> None:
        # A failed write/fsync must NEVER publish a readable final seal.
        final = self.path.with_suffix(".seal.json")
        temporary = self.path.with_suffix(".seal.tmp")
        if final.exists():
            raise ValueError("Existing Shadow seal cannot be overwritten.")
        seal["accounting_sha256"] = hashlib.sha256(self.path.read_bytes()).hexdigest()
        with temporary.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(json.dumps(seal, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        # Only Shadow metadata is renamed, never production state. No fallible
        # post-publication I/O: loss of the directory entry fails closed as absent.
        temporary.replace(final)


def read_shadow_accounting(
    path: Path,
    *,
    run_id: str,
    require_sealed: bool = True,
) -> dict[str, Any]:
    raw = path.read_bytes()
    lines = raw.splitlines(keepends=True)
    if not lines or not lines[-1].endswith(b"\n"):
        raise ValueError("Incomplete Shadow accounting journal.")
    snapshots = [json.loads(line) for line in lines]
    owner = json.loads(path.with_suffix(".owner.json").read_bytes())
    if not isinstance(owner, dict) or any(
        not isinstance(item, dict) for item in snapshots
    ):
        raise ValueError("Malformed Shadow accounting ownership/checkpoints.")
    identity_keys = (
        "experiment_run_id",
        "invocation_id",
        "owner_id",
        "accounting_schema_version",
        "seal_schema_version",
        "experiment_schema_version",
        "run_created_at",
    )
    if owner.get("experiment_run_id") != run_id:
        raise ValueError("Mixed Shadow accounting runs.")
    if (
        not isinstance(owner.get("invocation_id"), str)
        or not owner["invocation_id"].strip()
    ):
        raise ValueError("Missing Shadow InvocationID.")
    if not owner.get("owner_id"):
        raise ValueError("Missing Shadow writer ownership.")
    if (
        type(owner.get("accounting_schema_version")) is not int
        or type(owner.get("seal_schema_version")) is not int
        or type(owner.get("experiment_schema_version")) is not int
        or owner.get("accounting_schema_version") != ACCOUNTING_SCHEMA_VERSION
        or owner.get("seal_schema_version") != SEAL_SCHEMA_VERSION
        or owner.get("experiment_schema_version") != EXPERIMENT_SCHEMA_VERSION
    ):
        raise ValueError("Unknown Shadow accounting/experiment schema.")
    previous: dict[str, Any] | None = None
    for item in snapshots:
        if any(item.get(key) != owner.get(key) for key in identity_keys):
            raise ValueError("Shadow accounting ownership differs.")
        _timestamp(item["checkpoint_at"])
        for key in (
            *COUNTERS,
            "invalid_observations",
            "persistence_failures",
            "revision",
        ):
            if type(item.get(key)) is not int or item[key] < 0:
                raise ValueError("Invalid cumulative Shadow counter.")
            if previous is not None and item[key] < previous[key]:
                raise ValueError("Shadow cumulative counter regression.")
        if previous is not None and item["revision"] <= previous["revision"]:
            raise ValueError("Shadow accounting revision regression.")
        if previous is not None and _timestamp(item["checkpoint_at"]) < _timestamp(
            previous["checkpoint_at"]
        ):
            raise ValueError("Shadow checkpoint time regression.")
        terminal = (
            item["completed_count"]
            + item["failed_invalid_count"]
            + item["cancelled_invalid_count"]
        )
        invalid = (
            item["queue_dropped_count"]
            + item["worker_exception_count"]
            + item["enqueue_dropped_count"]
            + item["cancelled_invalid_count"]
        )
        if (
            item["pending_count"] != item["accepted_count"] - terminal
            or item["pending_count"] < 0
            or item["invalid_observations"] != invalid
            or item["authoritative"] != (item["persistence_failures"] == 0)
        ):
            raise ValueError("Inconsistent Shadow accounting checkpoint.")
        previous = item
    last: dict[str, Any] = snapshots[-1]
    terminal = (
        last["completed_count"]
        + last["failed_invalid_count"]
        + last["cancelled_invalid_count"]
    )
    if not last["authoritative"] or last["pending_count"] != 0:
        raise ValueError("Shadow checkpoint is incomplete or unavailable.")
    if (
        terminal != last["accepted_count"]
        or last["invalid_observations"]
        != last["queue_dropped_count"]
        + last["worker_exception_count"]
        + last["enqueue_dropped_count"]
        + last["cancelled_invalid_count"]
    ):
        raise ValueError("Inconsistent Shadow accounting checkpoint.")
    if not require_sealed:
        return last  # Explicit provisional inspection; never a final verdict.
    seal: dict[str, Any] = json.loads(path.with_suffix(".seal.json").read_bytes())
    if not isinstance(seal, dict):
        raise ValueError("Malformed Shadow seal.")
    _validate_final(last)
    _validate_final(seal)
    if seal.get("sealed") is not True:
        raise ValueError("Missing final Shadow seal.")
    for key, value in last.items():
        if key != "sealed" and seal.get(key) != value:
            raise ValueError("Shadow seal does not bind the final checkpoint.")
    if seal.get("accounting_sha256") != hashlib.sha256(raw).hexdigest():
        raise ValueError("Shadow accounting hash differs from its seal.")
    return seal
