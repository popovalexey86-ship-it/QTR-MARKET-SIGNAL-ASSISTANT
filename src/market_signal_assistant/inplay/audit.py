from __future__ import annotations

import json
import logging
import math
import threading
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

from market_signal_assistant.indicators import mean, true_ranges
from market_signal_assistant.inplay.models import (
    CatalogInstrument,
    InPlayResult,
    ListingStatus,
)
from market_signal_assistant.models import MarketSeries, MarketSignal

DEFAULT_INPLAY_TIMING_AUDIT_PATH = Path("data/inplay_timing_audit.jsonl")
DEFAULT_INPLAY_DETECTION_STATE_PATH = Path("data/inplay_detection_state.json")
AUDIT_RETENTION = timedelta(days=7)
_RETENTION_CHECK_INTERVAL = timedelta(days=1)
_BREAKOUT_LOOKBACK_BARS = 20

ScanSource = Literal["manual", "inplay_auto", "timing_audit_auto"]
_SCAN_SOURCES = frozenset(("manual", "inplay_auto", "timing_audit_auto"))

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class InPlayAuditCandidate:
    catalog: CatalogInstrument
    listing: ListingStatus
    series: MarketSeries
    intraday_series: MarketSeries | None
    technical: MarketSignal | None
    result: InPlayResult


@dataclass(frozen=True, slots=True)
class DetectionRecord:
    first_observed_at: datetime
    first_observed_price: float | None
    episode_id: str | None
    episode_started_at: datetime | None
    episode_started_price: float | None
    first_qualified_at: datetime | None
    first_qualified_price: float | None
    last_seen_at: datetime
    peak_price_since_episode_start: float | None
    trough_price_since_episode_start: float | None
    last_score: float | None = None
    last_direction: str | None = None
    episode_direction: str | None = None
    price_change_24h_at_episode_start_pct: float | None = None

    def __post_init__(self) -> None:
        for name in ("first_observed_at", "last_seen_at"):
            value = getattr(self, name)
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"Detection {name} must be timezone-aware.")
            object.__setattr__(self, name, value.astimezone(UTC))
        for name in ("episode_started_at", "first_qualified_at"):
            value = getattr(self, name)
            if value is not None:
                if value.tzinfo is None or value.utcoffset() is None:
                    raise ValueError(f"Detection {name} must be timezone-aware.")
                object.__setattr__(self, name, value.astimezone(UTC))
        for value in (
            self.first_observed_price,
            self.episode_started_price,
            self.first_qualified_price,
            self.peak_price_since_episode_start,
            self.trough_price_since_episode_start,
        ):
            if value is not None and (not math.isfinite(value) or value <= 0):
                raise ValueError("Detection prices must be finite and positive.")
        if self.last_score is not None and not 0.0 <= self.last_score <= 100.0:
            raise ValueError("Detection last score must be between 0 and 100.")


@dataclass(frozen=True, slots=True)
class DetectionState:
    records: Mapping[str, DetectionRecord]


@dataclass(frozen=True, slots=True)
class InPlayAuditSnapshot:
    scanned_at: datetime
    symbol: str
    internal_direction: str | None
    display_status: str | None
    inplay_score: float | None
    last_price: float | None
    spread_pct: float | None
    price_change_5m_pct: float | None
    price_change_15m_pct: float | None
    price_change_1h_pct: float | None
    price_change_24h_pct: float | None
    relative_volume: float | None
    atr_pct: float | None
    breakout_direction: str | None
    breakout_age_bars: int | None
    breakout_level: float | None
    distance_from_breakout_pct: float | None
    distance_from_breakout_atr: float | None
    confirmations: int | None
    warnings: tuple[str, ...]
    is_new_listing: bool
    instrument_status: str
    symbol_type: str
    turnover_24h: float
    first_detected_at: datetime
    first_detected_price: float | None
    move_before_first_detection_pct: float | None
    first_observed_at: datetime
    episode_id: str | None
    episode_started_at: datetime | None
    episode_started_price: float | None
    first_qualified_at: datetime | None
    first_qualified_price: float | None
    price_change_24h_at_episode_start_pct: float | None
    move_from_episode_start_to_qualification_pct: float | None
    move_from_episode_start_to_current_pct: float | None
    bars_from_episode_start_to_qualification: int | None
    scan_source: ScanSource

    def __post_init__(self) -> None:
        for name in ("scanned_at", "first_detected_at", "first_observed_at"):
            value = getattr(self, name)
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"Audit {name} must be timezone-aware.")
            object.__setattr__(self, name, value.astimezone(UTC))
        if self.scan_source not in _SCAN_SOURCES:
            raise ValueError("Unsupported IN PLAY audit scan source.")


@dataclass(frozen=True, slots=True)
class _Breakout:
    direction: str
    age_bars: int
    level: float
    distance_pct: float
    distance_atr: float | None


class JsonInPlayDetectionStore:
    """Atomic durable state independent from notification and listing state."""

    def __init__(
        self,
        path: Path = DEFAULT_INPLAY_DETECTION_STATE_PATH,
    ) -> None:
        self._path = path
        self._lock = threading.Lock()

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> DetectionState:
        with self._lock:
            return self._load_unlocked()

    def save(self, state: DetectionState) -> None:
        with self._lock:
            self._save_unlocked(state)

    def _load_unlocked(self) -> DetectionState:
        if not self._path.exists():
            return DetectionState({})
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            return _detection_state_from_json(raw)
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            _LOGGER.warning(
                "Файл диагностического состояния IN PLAY повреждён; "
                "используется безопасное пустое состояние."
            )
            return DetectionState({})

    def _save_unlocked(self, state: DetectionState) -> None:
        payload = {
            "version": 2,
            "records": {
                symbol: _detection_record_to_json(record)
                for symbol, record in sorted(state.records.items())
            },
        }
        temporary = self._path.with_suffix(f"{self._path.suffix}.tmp")
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            temporary.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            temporary.replace(self._path)
        except OSError:
            _LOGGER.warning(
                "Не удалось атомарно сохранить диагностическое состояние "
                "IN PLAY."
            )


class JsonlInPlayTimingAuditStore:
    """Append JSONL snapshots and occasionally prune entries older than 7 days."""

    def __init__(
        self,
        path: Path = DEFAULT_INPLAY_TIMING_AUDIT_PATH,
        *,
        retention: timedelta = AUDIT_RETENTION,
    ) -> None:
        if retention <= timedelta():
            raise ValueError("Audit retention must be positive.")
        self._path = path
        self._retention = retention
        self._last_retention_check: datetime | None = None
        self._lock = threading.Lock()

    @property
    def path(self) -> Path:
        return self._path

    def append(
        self,
        snapshots: tuple[InPlayAuditSnapshot, ...],
        observed_at: datetime,
    ) -> None:
        if not snapshots:
            return
        now = _utc(observed_at)
        with self._lock:
            self._prune_if_due(now)
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._ensure_line_boundary()
            with self._path.open("a", encoding="utf-8", newline="\n") as stream:
                for snapshot in snapshots:
                    stream.write(
                        json.dumps(
                            _snapshot_to_json(snapshot),
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                    )
                    stream.write("\n")

    def _ensure_line_boundary(self) -> None:
        if not self._path.exists() or self._path.stat().st_size == 0:
            return
        with self._path.open("rb") as stream:
            stream.seek(-1, 2)
            terminated = stream.read(1) in {b"\n", b"\r"}
        if not terminated:
            with self._path.open("ab") as stream:
                stream.write(b"\n")

    def _prune_if_due(self, now: datetime) -> None:
        if (
            self._last_retention_check is not None
            and now - self._last_retention_check < _RETENTION_CHECK_INTERVAL
        ):
            return
        self._last_retention_check = now
        if not self._path.exists():
            return
        cutoff = now - self._retention
        removed = False
        temporary = self._path.with_suffix(f"{self._path.suffix}.tmp")
        with (
            self._path.open("r", encoding="utf-8") as source,
            temporary.open("w", encoding="utf-8", newline="\n") as destination,
        ):
            for raw_line in source:
                line = raw_line.rstrip("\r\n")
                if not line.strip():
                    continue
                try:
                    raw = json.loads(line)
                    scanned_at = _datetime(raw["scanned_at"])
                except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                    _LOGGER.warning(
                        "Повреждённая строка IN PLAY timing audit сохранена "
                        "при retention-проверке."
                    )
                    destination.write(f"{line}\n")
                    continue
                if scanned_at < cutoff:
                    removed = True
                else:
                    destination.write(f"{line}\n")
        if removed:
            temporary.replace(self._path)
        else:
            temporary.unlink(missing_ok=True)


class InPlayTimingAuditor:
    """Best-effort diagnostic collector with no notification dependency."""

    def __init__(
        self,
        audit_store: JsonlInPlayTimingAuditStore,
        detection_store: JsonInPlayDetectionStore,
        *,
        episode_score: float = 40.0,
        qualification_score: float = 50.0,
        episode_reset: timedelta = timedelta(minutes=60),
    ) -> None:
        if not 0.0 <= episode_score <= 100.0:
            raise ValueError("Audit episode score must be between 0 and 100.")
        if not 0.0 <= qualification_score <= 100.0:
            raise ValueError("Audit qualification score must be between 0 and 100.")
        if episode_reset <= timedelta():
            raise ValueError("Audit episode reset must be positive.")
        self._audit_store = audit_store
        self._detection_store = detection_store
        self._episode_score = episode_score
        self._qualification_score = qualification_score
        self._episode_reset = episode_reset
        self._lock = threading.Lock()

    def record(
        self,
        candidates: tuple[InPlayAuditCandidate, ...],
        scanned_at: datetime,
        *,
        scan_source: ScanSource = "manual",
    ) -> tuple[InPlayAuditSnapshot, ...]:
        now = _utc(scanned_at)
        if scan_source not in _SCAN_SOURCES:
            raise ValueError("Unsupported IN PLAY audit scan source.")
        if not candidates:
            return ()
        with self._lock:
            state = self._detection_store.load()
            records = dict(state.records)
            snapshots: list[InPlayAuditSnapshot] = []
            for candidate in candidates:
                price = _last_price(candidate.series)
                price_change_24h = _price_change(candidate.series, 24)
                previous = records.get(candidate.catalog.symbol)
                record = _updated_detection(
                    previous,
                    candidate,
                    observed_at=now,
                    price=price,
                    price_change_24h=price_change_24h,
                    episode_score=self._episode_score,
                    qualification_score=self._qualification_score,
                    episode_reset=self._episode_reset,
                )
                records[candidate.catalog.symbol] = record
                snapshots.append(
                    _snapshot(
                        candidate,
                        record,
                        scanned_at=now,
                        first_detection=previous is None,
                        scan_source=scan_source,
                    )
                )
            self._detection_store.save(DetectionState(records))
            result = tuple(snapshots)
            try:
                self._audit_store.append(result, now)
            except OSError:
                _LOGGER.warning(
                    "Не удалось записать диагностический IN PLAY timing audit; "
                    "сканирование продолжено."
                )
            return result


def _snapshot(
    candidate: InPlayAuditCandidate,
    detection: DetectionRecord,
    *,
    scanned_at: datetime,
    first_detection: bool,
    scan_source: ScanSource,
) -> InPlayAuditSnapshot:
    series = candidate.series
    intraday = candidate.intraday_series
    last_price = _last_price(series)
    atr = _atr(series)
    breakout = _latest_breakout(series, atr)
    price_change_24h = _price_change(series, 24)
    return InPlayAuditSnapshot(
        scanned_at=scanned_at,
        symbol=candidate.catalog.symbol,
        internal_direction=candidate.result.direction.name,
        display_status=candidate.result.direction.value,
        inplay_score=candidate.result.inplay_score,
        last_price=last_price,
        spread_pct=candidate.catalog.spread_ratio * 100.0,
        price_change_5m_pct=(
            _price_change(intraday, 1) if intraday is not None else None
        ),
        price_change_15m_pct=(
            _price_change(intraday, 3) if intraday is not None else None
        ),
        price_change_1h_pct=_price_change(series, 1),
        price_change_24h_pct=price_change_24h,
        relative_volume=_relative_volume(series),
        atr_pct=(
            atr / last_price * 100.0
            if atr is not None and last_price is not None and last_price > 0
            else None
        ),
        breakout_direction=breakout.direction if breakout is not None else None,
        breakout_age_bars=breakout.age_bars if breakout is not None else None,
        breakout_level=breakout.level if breakout is not None else None,
        distance_from_breakout_pct=(
            breakout.distance_pct if breakout is not None else None
        ),
        distance_from_breakout_atr=(
            breakout.distance_atr if breakout is not None else None
        ),
        confirmations=(
            candidate.technical.confirmations
            if candidate.technical is not None
            else None
        ),
        warnings=candidate.result.warnings,
        is_new_listing=candidate.listing.is_new_listing,
        instrument_status=candidate.catalog.status,
        symbol_type=candidate.catalog.symbol_type,
        turnover_24h=candidate.catalog.turnover_24h,
        first_detected_at=detection.first_observed_at,
        first_detected_price=detection.first_observed_price,
        move_before_first_detection_pct=(
            price_change_24h if first_detection else None
        ),
        first_observed_at=detection.first_observed_at,
        episode_id=detection.episode_id,
        episode_started_at=detection.episode_started_at,
        episode_started_price=detection.episode_started_price,
        first_qualified_at=detection.first_qualified_at,
        first_qualified_price=detection.first_qualified_price,
        price_change_24h_at_episode_start_pct=(
            detection.price_change_24h_at_episode_start_pct
        ),
        move_from_episode_start_to_qualification_pct=_percentage_move(
            detection.episode_started_price,
            detection.first_qualified_price,
        ),
        move_from_episode_start_to_current_pct=_percentage_move(
            detection.episode_started_price,
            last_price,
        ),
        bars_from_episode_start_to_qualification=_elapsed_hour_bars(
            detection.episode_started_at,
            detection.first_qualified_at,
        ),
        scan_source=scan_source,
    )


def _updated_detection(
    previous: DetectionRecord | None,
    candidate: InPlayAuditCandidate,
    *,
    observed_at: datetime,
    price: float | None,
    price_change_24h: float | None,
    episode_score: float,
    qualification_score: float,
    episode_reset: timedelta,
) -> DetectionRecord:
    score = candidate.result.inplay_score
    direction = candidate.result.direction.name
    directional = direction in {"LONG", "SHORT"}
    first_observed_at = (
        previous.first_observed_at if previous is not None else observed_at
    )
    first_observed_price = (
        previous.first_observed_price if previous is not None else price
    )
    absent_reset = (
        previous is not None
        and observed_at - previous.last_seen_at >= episode_reset
    )
    threshold_crossed = (
        previous is not None
        and previous.last_score is not None
        and previous.last_score < episode_score <= score
    )
    first_visible_episode = (
        score >= episode_score
        and (
            previous is None
            or (
                previous.episode_started_at is None
                and previous.last_score is None
            )
        )
    )
    independent_direction = (
        previous is not None
        and previous.episode_started_at is not None
        and previous.episode_direction in {"LONG", "SHORT"}
        and previous.last_direction == "WATCH"
        and directional
        and direction != previous.episode_direction
    )
    starts_episode = (
        (absent_reset and score >= episode_score)
        or threshold_crossed
        or first_visible_episode
        or independent_direction
    )
    clears_episode = absent_reset and score < episode_score

    if starts_episode:
        episode_id = uuid.uuid4().hex
        episode_started_at = observed_at
        episode_started_price = price
        first_qualified_at = (
            observed_at if score >= qualification_score else None
        )
        first_qualified_price = price if first_qualified_at is not None else None
        peak = price
        trough = price
        episode_direction = direction if directional else None
        episode_change_24h = price_change_24h
    elif clears_episode or previous is None:
        episode_id = None
        episode_started_at = None
        episode_started_price = None
        first_qualified_at = None
        first_qualified_price = None
        peak = None
        trough = None
        episode_direction = None
        episode_change_24h = None
    else:
        episode_id = previous.episode_id
        episode_started_at = previous.episode_started_at
        episode_started_price = previous.episode_started_price
        first_qualified_at = previous.first_qualified_at
        first_qualified_price = previous.first_qualified_price
        peak = _maximum(previous.peak_price_since_episode_start, price)
        trough = _minimum(previous.trough_price_since_episode_start, price)
        episode_direction = previous.episode_direction
        if episode_direction is None and directional:
            episode_direction = direction
        episode_change_24h = previous.price_change_24h_at_episode_start_pct
        if (
            episode_started_at is not None
            and first_qualified_at is None
            and score >= qualification_score
        ):
            first_qualified_at = observed_at
            first_qualified_price = price

    return DetectionRecord(
        first_observed_at=first_observed_at,
        first_observed_price=first_observed_price,
        episode_id=episode_id,
        episode_started_at=episode_started_at,
        episode_started_price=episode_started_price,
        first_qualified_at=first_qualified_at,
        first_qualified_price=first_qualified_price,
        last_seen_at=observed_at,
        peak_price_since_episode_start=peak,
        trough_price_since_episode_start=trough,
        last_score=score,
        last_direction=direction,
        episode_direction=episode_direction,
        price_change_24h_at_episode_start_pct=episode_change_24h,
    )


def _maximum(left: float | None, right: float | None) -> float | None:
    values = tuple(value for value in (left, right) if value is not None)
    return max(values) if values else None


def _minimum(left: float | None, right: float | None) -> float | None:
    values = tuple(value for value in (left, right) if value is not None)
    return min(values) if values else None


def _percentage_move(start: float | None, end: float | None) -> float | None:
    if start is None or end is None or start <= 0:
        return None
    return (end / start - 1.0) * 100.0


def _elapsed_hour_bars(
    start: datetime | None,
    end: datetime | None,
) -> int | None:
    if start is None or end is None or end < start:
        return None
    return int((end - start).total_seconds() // 3600)


def _last_price(series: MarketSeries) -> float | None:
    return series.candles[-1].close if series.candles else None


def _price_change(series: MarketSeries, bars: int) -> float | None:
    if bars <= 0 or len(series.candles) <= bars:
        return None
    current = series.candles[-1].close
    reference = series.candles[-bars - 1].close
    return (current / reference - 1.0) * 100.0 if reference > 0 else None


def _relative_volume(series: MarketSeries) -> float | None:
    if len(series.candles) < 2:
        return None
    prior = tuple(candle.volume for candle in series.candles[-21:-1])
    average = mean(prior)
    return series.candles[-1].volume / average if average > 0 else None


def _atr(series: MarketSeries) -> float | None:
    if not series.candles:
        return None
    highs = tuple(candle.high for candle in series.candles)
    lows = tuple(candle.low for candle in series.candles)
    closes = tuple(candle.close for candle in series.candles)
    ranges = true_ranges(highs, lows, closes)
    return mean(ranges[-min(14, len(ranges)) :]) if ranges else None


def _latest_breakout(
    series: MarketSeries,
    atr: float | None,
) -> _Breakout | None:
    candles = series.candles
    if len(candles) <= _BREAKOUT_LOOKBACK_BARS:
        return None
    latest: tuple[int, str, float] | None = None
    for index in range(_BREAKOUT_LOOKBACK_BARS, len(candles)):
        history = candles[index - _BREAKOUT_LOOKBACK_BARS : index]
        upper = max(candle.high for candle in history)
        lower = min(candle.low for candle in history)
        close = candles[index].close
        if close > upper:
            latest = (index, "UP", upper)
        elif close < lower:
            latest = (index, "DOWN", lower)
    if latest is None:
        return None
    index, direction, level = latest
    current = candles[-1].close
    distance = current - level
    return _Breakout(
        direction=direction,
        age_bars=len(candles) - 1 - index,
        level=level,
        distance_pct=distance / level * 100.0,
        distance_atr=distance / atr if atr is not None and atr > 0 else None,
    )


def _snapshot_to_json(snapshot: InPlayAuditSnapshot) -> dict[str, Any]:
    return {
        "scanned_at": snapshot.scanned_at.isoformat(),
        "symbol": snapshot.symbol,
        "internal_direction": snapshot.internal_direction,
        "display_status": snapshot.display_status,
        "inplay_score": snapshot.inplay_score,
        "last_price": snapshot.last_price,
        "spread_pct": snapshot.spread_pct,
        "price_change_5m_pct": snapshot.price_change_5m_pct,
        "price_change_15m_pct": snapshot.price_change_15m_pct,
        "price_change_1h_pct": snapshot.price_change_1h_pct,
        "price_change_24h_pct": snapshot.price_change_24h_pct,
        "relative_volume": snapshot.relative_volume,
        "atr_pct": snapshot.atr_pct,
        "breakout_direction": snapshot.breakout_direction,
        "breakout_age_bars": snapshot.breakout_age_bars,
        "breakout_level": snapshot.breakout_level,
        "distance_from_breakout_pct": snapshot.distance_from_breakout_pct,
        "distance_from_breakout_atr": snapshot.distance_from_breakout_atr,
        "confirmations": snapshot.confirmations,
        "warnings": list(snapshot.warnings),
        "is_new_listing": snapshot.is_new_listing,
        "instrument_status": snapshot.instrument_status,
        "symbol_type": snapshot.symbol_type,
        "turnover_24h": snapshot.turnover_24h,
        "first_detected_at": snapshot.first_detected_at.isoformat(),
        "first_detected_price": snapshot.first_detected_price,
        "move_before_first_detection_pct": (
            snapshot.move_before_first_detection_pct
        ),
        "first_observed_at": snapshot.first_observed_at.isoformat(),
        "episode_id": snapshot.episode_id,
        "episode_started_at": (
            snapshot.episode_started_at.isoformat()
            if snapshot.episode_started_at is not None
            else None
        ),
        "episode_started_price": snapshot.episode_started_price,
        "first_qualified_at": (
            snapshot.first_qualified_at.isoformat()
            if snapshot.first_qualified_at is not None
            else None
        ),
        "first_qualified_price": snapshot.first_qualified_price,
        "price_change_24h_at_episode_start_pct": (
            snapshot.price_change_24h_at_episode_start_pct
        ),
        "move_from_episode_start_to_qualification_pct": (
            snapshot.move_from_episode_start_to_qualification_pct
        ),
        "move_from_episode_start_to_current_pct": (
            snapshot.move_from_episode_start_to_current_pct
        ),
        "bars_from_episode_start_to_qualification": (
            snapshot.bars_from_episode_start_to_qualification
        ),
        "scan_source": snapshot.scan_source,
    }


def _detection_record_to_json(record: DetectionRecord) -> dict[str, Any]:
    return {
        "first_observed_at": record.first_observed_at.isoformat(),
        "first_observed_price": record.first_observed_price,
        "episode_id": record.episode_id,
        "episode_started_at": (
            record.episode_started_at.isoformat()
            if record.episode_started_at is not None
            else None
        ),
        "episode_started_price": record.episode_started_price,
        "first_qualified_at": (
            record.first_qualified_at.isoformat()
            if record.first_qualified_at is not None
            else None
        ),
        "first_qualified_price": record.first_qualified_price,
        "last_seen_at": record.last_seen_at.isoformat(),
        "peak_price_since_episode_start": record.peak_price_since_episode_start,
        "trough_price_since_episode_start": record.trough_price_since_episode_start,
        "last_score": record.last_score,
        "last_direction": record.last_direction,
        "episode_direction": record.episode_direction,
        "price_change_24h_at_episode_start_pct": (
            record.price_change_24h_at_episode_start_pct
        ),
    }


def _detection_state_from_json(value: Any) -> DetectionState:
    if not isinstance(value, dict) or value.get("version") not in {1, 2}:
        raise ValueError("Unsupported IN PLAY detection state.")
    version = value["version"]
    raw_records = value.get("records")
    if not isinstance(raw_records, dict):
        raise ValueError("Detection records must be an object.")
    records: dict[str, DetectionRecord] = {}
    for symbol, raw in raw_records.items():
        if not isinstance(symbol, str) or not isinstance(raw, dict):
            raise ValueError("Invalid detection record.")
        records[symbol] = (
            _legacy_detection_record(raw)
            if version == 1
            else _current_detection_record(raw)
        )
    return DetectionState(records)


def _legacy_detection_record(raw: dict[str, Any]) -> DetectionRecord:
    return DetectionRecord(
        first_observed_at=_datetime(raw["first_detected_at"]),
        first_observed_price=_optional_float(raw["first_detected_price"]),
        episode_id=None,
        episode_started_at=None,
        episode_started_price=None,
        first_qualified_at=None,
        first_qualified_price=None,
        last_seen_at=_datetime(raw["last_seen_at"]),
        peak_price_since_episode_start=None,
        trough_price_since_episode_start=None,
    )


def _current_detection_record(raw: dict[str, Any]) -> DetectionRecord:
    return DetectionRecord(
        first_observed_at=_datetime(raw["first_observed_at"]),
        first_observed_price=_optional_float(raw["first_observed_price"]),
        episode_id=_optional_string(raw.get("episode_id")),
        episode_started_at=_optional_datetime(raw.get("episode_started_at")),
        episode_started_price=_optional_float(raw.get("episode_started_price")),
        first_qualified_at=_optional_datetime(raw.get("first_qualified_at")),
        first_qualified_price=_optional_float(raw.get("first_qualified_price")),
        last_seen_at=_datetime(raw["last_seen_at"]),
        peak_price_since_episode_start=_optional_float(
            raw.get("peak_price_since_episode_start")
        ),
        trough_price_since_episode_start=_optional_float(
            raw.get("trough_price_since_episode_start")
        ),
        last_score=_optional_float(raw.get("last_score")),
        last_direction=_optional_string(raw.get("last_direction")),
        episode_direction=_optional_string(raw.get("episode_direction")),
        price_change_24h_at_episode_start_pct=_optional_float(
            raw.get("price_change_24h_at_episode_start_pct")
        ),
    )


def _datetime(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("Expected ISO timestamp.")
    parsed = datetime.fromisoformat(value)
    return _utc(parsed)


def _optional_datetime(value: Any) -> datetime | None:
    return None if value is None else _datetime(value)


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Expected optional non-empty string.")
    return value


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Expected timezone-aware timestamp.")
    return value.astimezone(UTC)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("Expected optional number.")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("Expected finite number.")
    return result
