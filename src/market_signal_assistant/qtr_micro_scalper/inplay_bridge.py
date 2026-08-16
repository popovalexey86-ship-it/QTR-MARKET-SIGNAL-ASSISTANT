from __future__ import annotations

import math
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from threading import Lock


class ScalperTargetLifecycle(StrEnum):
    DISCOVERED = "DISCOVERED"
    WATCHING = "WATCHING"
    ACTIVE = "ACTIVE"
    REMOVED = "REMOVED"


@dataclass(frozen=True, slots=True)
class ScalperTarget:
    symbol: str
    discovered_at: datetime
    source: str
    reason: str
    priority: float
    volatility_score: float
    volume_score: float
    liquidity_score: float
    lifecycle: ScalperTargetLifecycle = ScalperTargetLifecycle.DISCOVERED

    def __post_init__(self) -> None:
        symbol = self.symbol.strip().upper()
        if not symbol:
            raise ValueError("Scalper target symbol cannot be empty.")
        source = self.source.strip()
        reason = self.reason.strip()
        if not source or not reason:
            raise ValueError("Scalper target source and reason cannot be empty.")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "reason", reason)
        object.__setattr__(self, "discovered_at", _utc(self.discovered_at))
        for name, value in (
            ("priority", self.priority),
            ("volatility_score", self.volatility_score),
            ("volume_score", self.volume_score),
            ("liquidity_score", self.liquidity_score),
        ):
            if not _finite(value) or not 0.0 <= value <= 100.0:
                raise ValueError(f"Scalper target {name} must be between 0 and 100.")
        if not isinstance(self.lifecycle, ScalperTargetLifecycle):
            raise ValueError("Scalper target lifecycle is invalid.")


@dataclass(frozen=True, slots=True)
class InPlayTargetBridgeConfig:
    max_watched_symbols: int = 20
    expiration_seconds: int = 900
    cooldown_seconds: int = 300

    def __post_init__(self) -> None:
        for name, value in (
            ("max watched symbols", self.max_watched_symbols),
            ("expiration", self.expiration_seconds),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"InPlay bridge {name} must be positive.")
        if (
            isinstance(self.cooldown_seconds, bool)
            or not isinstance(self.cooldown_seconds, int)
            or self.cooldown_seconds < 0
        ):
            raise ValueError("InPlay bridge cooldown cannot be negative.")


@dataclass(frozen=True, slots=True)
class TargetBridgeDecision:
    accepted: bool
    target: ScalperTarget | None
    reason: str

    def __post_init__(self) -> None:
        if not self.reason.strip():
            raise ValueError("Target bridge decision requires a reason.")
        if self.accepted != (self.target is not None):
            raise ValueError("Target bridge decision acceptance is inconsistent.")


@dataclass(frozen=True, slots=True)
class TargetSyncResult:
    watched: tuple[ScalperTarget, ...]
    discovered: tuple[ScalperTarget, ...]
    removed: tuple[ScalperTarget, ...]
    suppressed_symbols: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _TargetEntry:
    target: ScalperTarget
    last_seen_at: datetime
    status_changed_at: datetime
    removed_at: datetime | None = None


class InPlayTargetBridge:
    """Offline lifecycle bridge between scanner candidates and Scalper V2."""

    def __init__(self, config: InPlayTargetBridgeConfig | None = None) -> None:
        self._config = config or InPlayTargetBridgeConfig()
        self._entries: dict[str, _TargetEntry] = {}
        self._last_observed_at: datetime | None = None
        self._lock = Lock()

    def discover(
        self,
        target: ScalperTarget,
        *,
        observed_at: datetime,
    ) -> TargetBridgeDecision:
        normalized_at = _utc(observed_at)
        with self._lock:
            self._validate_chronology(normalized_at)
            self._expire_locked(normalized_at, preserve_symbols={target.symbol})
            existing = self._entries.get(target.symbol)
            decision = self._discover_locked(target, normalized_at, existing)
            self._last_observed_at = normalized_at
            return decision

    def sync(
        self,
        targets: tuple[ScalperTarget, ...],
        *,
        observed_at: datetime,
    ) -> TargetSyncResult:
        normalized_at = _utc(observed_at)
        deduplicated = _deduplicate(targets)
        with self._lock:
            self._validate_chronology(normalized_at)
            removed = list(
                self._expire_locked(
                    normalized_at,
                    preserve_symbols={target.symbol for target in deduplicated},
                )
            )
            discovered: list[ScalperTarget] = []
            suppressed: list[str] = []
            for target in deduplicated:
                existing = self._entries.get(target.symbol)
                decision = self._discover_locked(target, normalized_at, existing)
                if decision.accepted and decision.target is not None:
                    if existing is None or existing.target.lifecycle is (
                        ScalperTargetLifecycle.REMOVED
                    ):
                        discovered.append(decision.target)
                else:
                    suppressed.append(target.symbol)
            self._last_observed_at = normalized_at
            return TargetSyncResult(
                watched=self._watched_locked(),
                discovered=tuple(sorted(discovered, key=_sort_key)),
                removed=tuple(sorted(removed, key=lambda item: item.symbol)),
                suppressed_symbols=tuple(dict.fromkeys(suppressed)),
            )

    def begin_watching(
        self,
        symbol: str,
        *,
        changed_at: datetime,
    ) -> ScalperTarget:
        return self._transition(
            symbol,
            changed_at,
            expected=ScalperTargetLifecycle.DISCOVERED,
            target=ScalperTargetLifecycle.WATCHING,
        )

    def activate(
        self,
        symbol: str,
        *,
        changed_at: datetime,
    ) -> ScalperTarget:
        return self._transition(
            symbol,
            changed_at,
            expected=ScalperTargetLifecycle.WATCHING,
            target=ScalperTargetLifecycle.ACTIVE,
        )

    def remove(
        self,
        symbol: str,
        *,
        changed_at: datetime,
        reason: str | None = None,
    ) -> ScalperTarget:
        normalized_symbol = _symbol(symbol)
        normalized_at = _utc(changed_at)
        with self._lock:
            self._validate_chronology(normalized_at)
            entry = self._required_entry(normalized_symbol)
            if entry.target.lifecycle is ScalperTargetLifecycle.REMOVED:
                self._last_observed_at = normalized_at
                return entry.target
            removal_reason = (
                reason.strip() if reason is not None else entry.target.reason
            )
            if not removal_reason:
                raise ValueError("Removed target requires a reason.")
            removed = replace(
                entry.target,
                lifecycle=ScalperTargetLifecycle.REMOVED,
                reason=removal_reason,
            )
            self._entries[normalized_symbol] = _TargetEntry(
                target=removed,
                last_seen_at=entry.last_seen_at,
                status_changed_at=normalized_at,
                removed_at=normalized_at,
            )
            self._last_observed_at = normalized_at
            return removed

    def expire(self, *, observed_at: datetime) -> tuple[ScalperTarget, ...]:
        normalized_at = _utc(observed_at)
        with self._lock:
            self._validate_chronology(normalized_at)
            removed = self._expire_locked(normalized_at)
            self._last_observed_at = normalized_at
            return removed

    def watched_targets(self) -> tuple[ScalperTarget, ...]:
        with self._lock:
            return self._watched_locked()

    def all_targets(self) -> tuple[ScalperTarget, ...]:
        with self._lock:
            return tuple(
                sorted(
                    (entry.target for entry in self._entries.values()),
                    key=lambda item: item.symbol,
                )
            )

    def _discover_locked(
        self,
        target: ScalperTarget,
        observed_at: datetime,
        existing: _TargetEntry | None,
    ) -> TargetBridgeDecision:
        if target.lifecycle is not ScalperTargetLifecycle.DISCOVERED:
            return TargetBridgeDecision(
                accepted=False,
                target=None,
                reason="Scanner input must use DISCOVERED lifecycle.",
            )
        if target.discovered_at > observed_at:
            raise ValueError("Scalper target cannot be discovered in the future.")
        if existing is not None and existing.target.lifecycle is not (
            ScalperTargetLifecycle.REMOVED
        ):
            refreshed = replace(
                _preferred(existing.target, target),
                discovered_at=existing.target.discovered_at,
                lifecycle=existing.target.lifecycle,
            )
            self._entries[target.symbol] = replace(
                existing,
                target=refreshed,
                last_seen_at=observed_at,
            )
            return TargetBridgeDecision(
                accepted=True,
                target=refreshed,
                reason="Existing target observation refreshed without duplication.",
            )
        if existing is not None and existing.removed_at is not None:
            cooldown_ends = existing.removed_at + timedelta(
                seconds=self._config.cooldown_seconds
            )
            if observed_at < cooldown_ends:
                return TargetBridgeDecision(
                    accepted=False,
                    target=None,
                    reason="Target cooldown is active.",
                )
        if len(self._watched_locked()) >= self._config.max_watched_symbols:
            return TargetBridgeDecision(
                accepted=False,
                target=None,
                reason="Maximum watched symbol limit reached.",
            )
        discovered = replace(target, lifecycle=ScalperTargetLifecycle.DISCOVERED)
        self._entries[target.symbol] = _TargetEntry(
            target=discovered,
            last_seen_at=observed_at,
            status_changed_at=observed_at,
        )
        return TargetBridgeDecision(
            accepted=True,
            target=discovered,
            reason="🔥 Scanner target accepted for deep analysis.",
        )

    def _transition(
        self,
        symbol: str,
        changed_at: datetime,
        *,
        expected: ScalperTargetLifecycle,
        target: ScalperTargetLifecycle,
    ) -> ScalperTarget:
        normalized_symbol = _symbol(symbol)
        normalized_at = _utc(changed_at)
        with self._lock:
            self._validate_chronology(normalized_at)
            entry = self._required_entry(normalized_symbol)
            if entry.target.lifecycle is not expected:
                raise ValueError(
                    f"Target transition requires {expected.value} lifecycle."
                )
            transitioned = replace(entry.target, lifecycle=target)
            self._entries[normalized_symbol] = replace(
                entry,
                target=transitioned,
                status_changed_at=normalized_at,
            )
            self._last_observed_at = normalized_at
            return transitioned

    def _expire_locked(
        self,
        observed_at: datetime,
        *,
        preserve_symbols: set[str] | None = None,
    ) -> tuple[ScalperTarget, ...]:
        expired: list[ScalperTarget] = []
        lifetime = timedelta(seconds=self._config.expiration_seconds)
        preserved = preserve_symbols or set()
        for symbol, entry in tuple(self._entries.items()):
            if symbol in preserved:
                continue
            if entry.target.lifecycle is ScalperTargetLifecycle.REMOVED:
                continue
            if observed_at < entry.last_seen_at + lifetime:
                continue
            removed = replace(
                entry.target,
                lifecycle=ScalperTargetLifecycle.REMOVED,
                reason="Target expired without a fresh scanner observation.",
            )
            self._entries[symbol] = _TargetEntry(
                target=removed,
                last_seen_at=entry.last_seen_at,
                status_changed_at=observed_at,
                removed_at=observed_at,
            )
            expired.append(removed)
        return tuple(sorted(expired, key=lambda item: item.symbol))

    def _watched_locked(self) -> tuple[ScalperTarget, ...]:
        return tuple(
            sorted(
                (
                    entry.target
                    for entry in self._entries.values()
                    if entry.target.lifecycle is not ScalperTargetLifecycle.REMOVED
                ),
                key=_sort_key,
            )
        )

    def _required_entry(self, symbol: str) -> _TargetEntry:
        entry = self._entries.get(symbol)
        if entry is None:
            raise KeyError(f"Unknown scalper target: {symbol}.")
        return entry

    def _validate_chronology(self, observed_at: datetime) -> None:
        if self._last_observed_at is not None and observed_at < self._last_observed_at:
            raise ValueError("InPlay bridge observations must be chronological.")


def _deduplicate(targets: tuple[ScalperTarget, ...]) -> tuple[ScalperTarget, ...]:
    unique: dict[str, ScalperTarget] = {}
    for target in targets:
        existing = unique.get(target.symbol)
        unique[target.symbol] = (
            target if existing is None else _preferred(existing, target)
        )
    return tuple(sorted(unique.values(), key=_sort_key))


def _preferred(first: ScalperTarget, second: ScalperTarget) -> ScalperTarget:
    return min((first, second), key=_sort_key)


def _sort_key(target: ScalperTarget) -> tuple[float, float, float, float, str]:
    return (
        -target.priority,
        -target.liquidity_score,
        -target.volume_score,
        -target.volatility_score,
        target.symbol,
    )


def _symbol(value: str) -> str:
    normalized = value.strip().upper()
    if not normalized:
        raise ValueError("Scalper target symbol cannot be empty.")
    return normalized


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("InPlay bridge timestamp must be timezone-aware.")
    if value.utcoffset() is None:
        raise ValueError("InPlay bridge timestamp must be timezone-aware.")
    return value.astimezone(UTC)


def _finite(value: float) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(value)
    )
