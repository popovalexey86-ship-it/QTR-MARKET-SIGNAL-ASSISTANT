"""Versioned immutable operator projections; no journals or runtime dependencies."""

from __future__ import annotations

import json
import math
import re
import types
from dataclasses import asdict, dataclass, fields, is_dataclass
from datetime import UTC, datetime
from functools import cache
from typing import cast, get_args, get_origin, get_type_hints

SCHEMA_VERSION = 1
MAX_SNAPSHOT_BYTES = 128 * 1024
MAX_ACTIVE_TRADES = 20
MAX_SYMBOLS = 50
MAX_BLOCKERS = 10
MAX_TEXT = 300


@dataclass(frozen=True, slots=True)
class ActiveTrade:
    trade_id: str
    symbol: str
    direction: str
    stage: str
    score: float
    entry: float
    stop: float
    tp1: float
    tp2: float
    entry_time: datetime | None
    result_r: float


@dataclass(frozen=True, slots=True)
class RuntimeDetails:
    """Current in-process state only; absence means unavailable, never zero."""

    active_count: int | None = None
    active: tuple[ActiveTrade, ...] = ()
    symbols: tuple[str, ...] = ()
    holding_enabled: bool | None = None
    micro_profit_enabled: bool | None = None
    protected_runner_enabled: bool | None = None
    live_enabled: bool | None = None


@dataclass(frozen=True, slots=True)
class RuntimeSection:
    status: str
    ready: bool
    shadow_mode: bool
    live_enabled: bool | None
    started_at: datetime | None
    uptime_seconds: float
    reconnect_attempts: int
    pipeline_errors: int
    last_error: str | None
    quarantine_count: int
    quarantine_warning: str | None


@dataclass(frozen=True, slots=True)
class MarketSection:
    snapshot_count: int
    active_symbol_count: int
    symbols: tuple[str, ...]
    last_target_refresh_at: datetime | None
    last_market_activity_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class DecisionsSection:
    total: int
    blocked: int
    entries: int
    dominant_market_state: str | None
    dominant_score_bucket: str | None


@dataclass(frozen=True, slots=True)
class TradesSection:
    active_count: int | None
    completed: int
    win_rate: float
    average_r: float
    total_r: float
    expired_rate: float
    tp1_rate: float
    tp2_rate: float
    stop_rate: float
    active: tuple[ActiveTrade, ...]


@dataclass(frozen=True, slots=True)
class Blocker:
    reason: str
    count: int


@dataclass(frozen=True, slots=True)
class Experiment:
    name: str
    enabled: bool | None
    warning: str | None
    errors: int


@dataclass(frozen=True, slots=True)
class ResourcesSection:
    queue_depth: int
    retained_events: int
    stale_data_suppressed: int
    previous_publish_error: str | None


@dataclass(frozen=True, slots=True)
class VersionSection:
    schema_version: int
    code_sha: str


@dataclass(frozen=True, slots=True)
class ScalperControlSnapshot:
    schema_version: int
    generated_at: datetime
    observed_at: datetime
    stale_after_seconds: float
    runtime: RuntimeSection
    market: MarketSection
    decisions: DecisionsSection
    trades: TradesSection
    blockers: tuple[Blocker, ...]
    experiments: tuple[Experiment, ...]
    resources: ResourcesSection
    version: VersionSection


class UnsupportedControlSchema(ValueError):
    pass


def snapshot_to_bytes(snapshot: ScalperControlSnapshot) -> bytes:
    payload = json.dumps(
        asdict(snapshot),
        default=_datetime_json,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf8")
    # Apply identical validation at the publisher and reader boundary.
    snapshot_from_bytes(payload)
    return payload + b"\n"


def snapshot_from_bytes(data: bytes) -> ScalperControlSnapshot:
    if len(data) > MAX_SNAPSHOT_BYTES:
        raise ValueError("Control snapshot exceeds size limit")
    payload: object = json.loads(data.decode("utf8"))
    if not isinstance(payload, dict):
        raise ValueError("Control snapshot must be an object")
    if type(payload.get("schema_version")) is not int:
        raise ValueError("Missing/malformed control schema")
    if payload["schema_version"] != SCHEMA_VERSION:
        raise UnsupportedControlSchema("Unsupported control schema")
    result = cast(ScalperControlSnapshot, _decode(ScalperControlSnapshot, payload))
    if result.version.schema_version != SCHEMA_VERSION:
        raise UnsupportedControlSchema("Inconsistent control schema")
    if not 1 <= result.stale_after_seconds <= 3600:
        raise ValueError("Invalid control freshness threshold")
    if (
        len(result.trades.active) > MAX_ACTIVE_TRADES
        or len(result.market.symbols) > MAX_SYMBOLS
        or len(result.blockers) > MAX_BLOCKERS
        or len(result.experiments) != 3
    ):
        raise ValueError("Control collection exceeds bounds")
    if result.trades.active_count is not None and result.trades.active_count < len(
        result.trades.active
    ):
        raise ValueError("Inconsistent active count")
    if any(
        t.stage not in {"WAITING_ENTRY", "OPEN", "TP1_HIT"}
        for t in result.trades.active
    ):
        raise ValueError("Non-active runtime trade")
    if len({t.trade_id for t in result.trades.active}) != len(result.trades.active):
        raise ValueError("Duplicate runtime trade")
    if (
        result.runtime.status
        not in {
            "RUNNING",
            "DEGRADED",
            "STOPPED",
            "STOPPING",
            "STARTING",
            "DISABLED",
            "BLOCKED",
        }
        or result.runtime.uptime_seconds < 0
    ):
        raise ValueError("Invalid runtime status")
    if result.version.code_sha != "unknown" and not re.fullmatch(
        r"[0-9a-f]{40}", result.version.code_sha
    ):
        raise ValueError("Invalid code SHA")
    if any(
        not 0 <= rate <= 100
        for rate in (
            result.trades.win_rate,
            result.trades.expired_rate,
            result.trades.tp1_rate,
            result.trades.tp2_rate,
            result.trades.stop_rate,
        )
    ):
        raise ValueError("Invalid trade rate")
    for trade in result.trades.active:
        if (
            not trade.trade_id
            or not trade.symbol
            or trade.direction not in {"LONG", "SHORT"}
            or not 0 <= trade.score <= 100
            or min(trade.entry, trade.stop, trade.tp1, trade.tp2) <= 0
        ):
            raise ValueError("Invalid active trade summary")
    if {e.name for e in result.experiments} != {
        "Holding Experiment",
        "Micro Profit",
        "Protected Runner",
    }:
        raise ValueError("Invalid experiment identities")
    return result


def _datetime_json(value: object) -> str:
    if not isinstance(value, datetime):
        raise TypeError("Unsupported control value")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Control timestamps must include timezone")
    return value.astimezone(UTC).isoformat()


@cache
def _field_types(kind: type) -> dict[str, object]:
    return cast(dict[str, object], get_type_hints(kind))


def _decode(kind: object, value: object) -> object:
    if get_origin(kind) is types.UnionType:
        for choice in get_args(kind):
            try:
                return _decode(choice, value)
            except (ValueError, TypeError):
                pass
        raise ValueError("Invalid optional control field")
    if kind is type(None) and value is None:
        return None
    if kind is datetime and isinstance(value, str):
        result = datetime.fromisoformat(value)
        if result.tzinfo is None or result.utcoffset() is None:
            raise ValueError("Naive control timestamp")
        return result.astimezone(UTC)
    if get_origin(kind) is tuple and isinstance(value, list):
        if len(value) > MAX_SYMBOLS:
            raise ValueError("Oversized control collection")
        return tuple(_decode(get_args(kind)[0], item) for item in value)
    if isinstance(kind, type) and is_dataclass(kind) and isinstance(value, dict):
        hints = _field_types(kind)
        if set(value) != {f.name for f in fields(kind)}:
            raise ValueError("Missing/unknown control fields")
        return kind(**{key: _decode(hints[key], item) for key, item in value.items()})
    if kind is bool and type(value) is bool:
        return value
    if kind is int and type(value) is int and value >= 0:
        return value
    if kind is float and type(value) in {int, float}:
        number = float(cast(float, value))
        if math.isfinite(number):
            return number
    if kind is str and isinstance(value, str) and len(value) <= MAX_TEXT:
        return value
    raise ValueError("Invalid control field")
