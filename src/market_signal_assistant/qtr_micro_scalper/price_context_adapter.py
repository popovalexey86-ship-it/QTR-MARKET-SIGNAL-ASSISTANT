from __future__ import annotations

import json
import math
import os
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import RLock
from typing import Protocol

from market_signal_assistant.qtr_micro_scalper.inplay_bridge import ScalperTarget
from market_signal_assistant.qtr_micro_scalper.setup_context import (
    PriceContext,
    ShadowDirection,
)

DEFAULT_VERIFIED_SETUP_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "qtr_setup_telegram_pilot_audit.jsonl"
)


@dataclass(frozen=True, slots=True)
class VerifiedSetupRecord:
    symbol: str
    observed_at: datetime
    source_direction: str
    setup_direction: str
    market_price: float
    atr: float
    trigger_price: float
    invalidation_price: float
    local_range_low: float
    local_range_high: float
    setup_state: str
    setup_confidence: float
    volume_confirmation: bool
    volatility_confirmation: bool
    liquidity_ok: bool
    confirmations: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(self, "observed_at", _utc(self.observed_at))


class VerifiedSetupProvider(Protocol):
    def latest(self, symbol: str) -> VerifiedSetupRecord | None: ...


@dataclass(frozen=True, slots=True)
class VerifiedSetupProviderMetrics:
    bootstrap_scans: int
    incremental_reads: int
    bytes_read: int
    cached_symbols: int
    malformed_lines: int
    resets_rotations: int


class JsonlVerifiedSetupProvider:
    """Incrementally tail and cache verified Setup Pilot JSONL records."""

    def __init__(self, path: Path = DEFAULT_VERIFIED_SETUP_PATH) -> None:
        self._path = path.resolve()
        self._lock = RLock()
        self._file_identity: tuple[int, int] | None = None
        self._offset = 0
        self._pending = b""
        self._latest_by_symbol: dict[str, VerifiedSetupRecord] = {}
        self._bootstrap_scans = 0
        self._incremental_reads = 0
        self._bytes_read = 0
        self._malformed_lines = 0
        self._resets_rotations = 0
        with self._lock:
            self._refresh_locked()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def metrics(self) -> VerifiedSetupProviderMetrics:
        with self._lock:
            return VerifiedSetupProviderMetrics(
                bootstrap_scans=self._bootstrap_scans,
                incremental_reads=self._incremental_reads,
                bytes_read=self._bytes_read,
                cached_symbols=len(self._latest_by_symbol),
                malformed_lines=self._malformed_lines,
                resets_rotations=self._resets_rotations,
            )

    def latest(self, symbol: str) -> VerifiedSetupRecord | None:
        normalized = symbol.strip().upper()
        if not normalized:
            return None
        with self._lock:
            self._refresh_locked()
            return self._latest_by_symbol.get(normalized)

    def _refresh_locked(self) -> None:
        try:
            metadata = self._path.stat()
        except OSError:
            self._reset_missing_locked()
            return
        if not stat.S_ISREG(metadata.st_mode):
            self._reset_missing_locked()
            return
        identity = _file_identity(metadata)
        if self._file_identity is None:
            self._bootstrap_locked()
            return
        if identity != self._file_identity or metadata.st_size < self._offset:
            self._reset_locked()
            self._bootstrap_locked()
            return
        if metadata.st_size > self._offset:
            self._read_increment_locked(identity)

    def _bootstrap_locked(self) -> None:
        try:
            with self._path.open("rb") as stream:
                metadata = os.fstat(stream.fileno())
                if not stat.S_ISREG(metadata.st_mode):
                    self._reset_missing_locked()
                    return
                data = stream.read()
        except OSError:
            self._reset_missing_locked()
            return
        self._file_identity = _file_identity(metadata)
        self._offset = len(data)
        self._pending = b""
        self._latest_by_symbol.clear()
        self._bootstrap_scans += 1
        self._bytes_read += len(data)
        self._consume_locked(data)

    def _read_increment_locked(self, expected_identity: tuple[int, int]) -> None:
        try:
            with self._path.open("rb") as stream:
                metadata = os.fstat(stream.fileno())
                identity = _file_identity(metadata)
                if identity != expected_identity or metadata.st_size < self._offset:
                    self._reset_locked()
                    data = stream.read()
                    self._file_identity = identity
                    self._offset = len(data)
                    self._bootstrap_scans += 1
                    self._bytes_read += len(data)
                    self._consume_locked(data)
                    return
                stream.seek(self._offset)
                data = stream.read()
        except OSError:
            return
        if not data:
            return
        self._offset += len(data)
        self._incremental_reads += 1
        self._bytes_read += len(data)
        self._consume_locked(data)

    def _consume_locked(self, data: bytes) -> None:
        chunks = (self._pending + data).split(b"\n")
        self._pending = chunks.pop()
        for raw_line in chunks:
            raw_line = raw_line.removesuffix(b"\r")
            if not raw_line:
                continue
            try:
                line = raw_line.decode("utf-8")
            except UnicodeDecodeError:
                self._malformed_lines += 1
                continue
            record, malformed = _parse_record_line(line)
            if malformed:
                self._malformed_lines += 1
            if record is None:
                continue
            current = self._latest_by_symbol.get(record.symbol)
            if current is None or record.observed_at >= current.observed_at:
                self._latest_by_symbol[record.symbol] = record

    def _reset_missing_locked(self) -> None:
        if (
            self._file_identity is not None
            or self._offset
            or self._pending
            or self._latest_by_symbol
        ):
            self._reset_locked()

    def _reset_locked(self) -> None:
        self._file_identity = None
        self._offset = 0
        self._pending = b""
        self._latest_by_symbol.clear()
        self._resets_rotations += 1


class VerifiedPriceContextAdapter:
    """Map only complete, fresh Setup Pilot evidence into PriceContext."""

    def __init__(
        self,
        provider: VerifiedSetupProvider,
        *,
        maximum_age: timedelta = timedelta(minutes=15),
    ) -> None:
        if maximum_age <= timedelta(0):
            raise ValueError("Verified price-context maximum age must be positive.")
        self._provider = provider
        self._maximum_age = maximum_age

    def __call__(
        self,
        symbol: str,
        assessed_at: datetime,
        market_price: float,
    ) -> PriceContext | None:
        normalized = symbol.strip().upper()
        now = _utc(assessed_at)
        source = self._provider.latest(normalized)
        if not _valid_source(source, normalized, now, market_price, self._maximum_age):
            return None
        assert source is not None
        direction = _direction(source.setup_direction)
        if direction is None:
            return None
        context = PriceContext(
            symbol=normalized,
            assessed_at=now,
            direction=direction,
            market_price=market_price,
            atr=source.atr,
            trigger_price=source.trigger_price,
            invalidation_price=source.invalidation_price,
            local_range_low=source.local_range_low,
            local_range_high=source.local_range_high,
            confirmations=source.confirmations,
            warnings=source.warnings,
        )
        return context if context.structure_valid else None

    def target(self, symbol: str, observed_at: datetime) -> ScalperTarget | None:
        normalized = symbol.strip().upper()
        now = _utc(observed_at)
        source = self._provider.latest(normalized)
        if not _valid_source(source, normalized, now, None, self._maximum_age):
            return None
        assert source is not None
        return ScalperTarget(
            symbol=source.symbol,
            discovered_at=source.observed_at,
            source="qtr_setup_pilot_verified_state",
            reason=f"Verified Setup Engine state: {source.setup_state}.",
            priority=source.setup_confidence,
            volatility_score=_confirmation_score(source.volatility_confirmation),
            volume_score=_confirmation_score(source.volume_confirmation),
            liquidity_score=_confirmation_score(source.liquidity_ok),
        )


def _parse_record_line(
    line: str,
) -> tuple[VerifiedSetupRecord | None, bool]:
    try:
        payload = json.loads(line)
    except (json.JSONDecodeError, TypeError):
        return None, True
    if not isinstance(payload, Mapping):
        return None, True
    symbol = str(payload.get("symbol", "")).strip().upper()
    if not symbol:
        return None, False
    context = payload.get("price_context")
    if not isinstance(context, Mapping):
        return None, False
    try:
        record = VerifiedSetupRecord(
            symbol=symbol,
            observed_at=datetime.fromisoformat(str(context["observed_at"])),
            source_direction=str(context["source_direction"]),
            setup_direction=str(context["setup_direction"]),
            market_price=_number(context["market_price"]),
            atr=_number(context["atr"]),
            trigger_price=_number(context["trigger_price"]),
            invalidation_price=_number(context["invalidation_price"]),
            local_range_low=_number(context["local_range_low"]),
            local_range_high=_number(context["local_range_high"]),
            setup_state=str(context["setup_state"]),
            setup_confidence=_number(context["setup_confidence"]),
            volume_confirmation=_boolean(context["volume_confirmation"]),
            volatility_confirmation=_boolean(context["volatility_confirmation"]),
            liquidity_ok=_boolean(context["liquidity_ok"]),
            confirmations=_strings(context.get("confirmations", ())),
            warnings=_strings(context.get("warnings", ())),
        )
    except (KeyError, TypeError, ValueError):
        return None, False
    return record, False


def _file_identity(metadata: os.stat_result) -> tuple[int, int]:
    return metadata.st_dev, metadata.st_ino


def _valid_source(
    source: VerifiedSetupRecord | None,
    symbol: str,
    assessed_at: datetime,
    market_price: float | None,
    maximum_age: timedelta,
) -> bool:
    if source is None or source.symbol != symbol:
        return False
    age = assessed_at - source.observed_at
    if age < timedelta(0) or age > maximum_age:
        return False
    if source.source_direction != source.setup_direction:
        return False
    if _direction(source.setup_direction) is None:
        return False
    if source.setup_state in {"CANCELLED", "LATE"}:
        return False
    values = (
        source.market_price,
        source.atr,
        source.trigger_price,
        source.invalidation_price,
        source.local_range_low,
        source.local_range_high,
    )
    if any(not _positive(value) for value in values):
        return False
    if source.local_range_low >= source.local_range_high:
        return False
    if not source.local_range_low <= source.trigger_price <= source.local_range_high:
        return False
    if not 0.0 <= source.setup_confidence <= 100.0:
        return False
    return market_price is None or _positive(market_price)


def _direction(value: str) -> ShadowDirection | None:
    return {"UP": ShadowDirection.LONG, "DOWN": ShadowDirection.SHORT}.get(value)


def _confirmation_score(value: bool) -> float:
    return 100.0 if value else 0.0


def _number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("Expected a JSON number.")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("Expected a finite JSON number.")
    return number


def _boolean(value: object) -> bool:
    if not isinstance(value, bool):
        raise TypeError("Expected a JSON boolean.")
    return value


def _strings(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    strings = tuple(
        item.strip()
        for item in value
        if isinstance(item, str) and item.strip()
    )
    return tuple(dict.fromkeys(strings))


def _positive(value: float) -> bool:
    return math.isfinite(value) and value > 0


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Verified price-context timestamp must be timezone-aware.")
    return value.astimezone(UTC)
