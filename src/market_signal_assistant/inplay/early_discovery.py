from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Protocol

from market_signal_assistant.indicators import mean, true_ranges
from market_signal_assistant.inplay.models import CatalogInstrument, InPlayResult
from market_signal_assistant.inplay.safety import automatic_semantics
from market_signal_assistant.models import AssetClass, Instrument, MarketSeries
from market_signal_assistant.providers import MarketDataError, MarketDataProvider

DEFAULT_EARLY_DISCOVERY_AUDIT_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "inplay_early_discovery_audit.jsonl"
)
EARLY_DISCOVERY_RETENTION = timedelta(days=7)
RETENTION_CHECK_INTERVAL = timedelta(days=1)
MINIMUM_TURNOVER_24H = 5_000_000.0
MAXIMUM_SPREAD_RATIO = 0.005
READY_MAXIMUM_SPREAD_PCT = 0.2
BREAKOUT_LOOKBACK = 20

# Discovery score weights total 100. Price change over 24h is deliberately absent.
VOLUME_ACCELERATION_WEIGHT = 20.0
ATR_EXPANSION_WEIGHT = 15.0
RANGE_PROXIMITY_WEIGHT = 15.0
BREAKOUT_FRESHNESS_WEIGHT = 20.0
COMPRESSION_WEIGHT = 10.0
LIQUIDITY_WEIGHT = 10.0
SPREAD_QUALITY_WEIGHT = 10.0

_LOGGER = logging.getLogger(__name__)


class EarlyDiscoveryDataError(RuntimeError):
    """Controlled per-symbol market-data failure."""


class MarketDirection(Enum):
    UP = "UP"
    DOWN = "DOWN"
    NEUTRAL = "NEUTRAL"


class DiscoveryStage(Enum):
    QUIET = "QUIET"
    EARLY_ATTENTION = "EARLY_ATTENTION"
    SETUP_FORMING = "SETUP_FORMING"
    READY_CANDIDATE = "READY_CANDIDATE"
    LATE = "LATE"
    DO_NOT_CHASE = "DO_NOT_CHASE"


@dataclass(frozen=True, slots=True)
class EarlyDiscoveryResult:
    symbol: str
    scanned_at: datetime
    market_direction: MarketDirection
    discovery_stage: DiscoveryStage
    discovery_score: float
    entry_readiness_score: float
    last_price: float | None
    spread_pct: float | None
    turnover_24h: float | None
    relative_volume_5m: float | None
    relative_volume_15m: float | None
    volume_acceleration: float | None
    price_change_5m_pct: float | None
    price_change_15m_pct: float | None
    price_change_1h_pct: float | None
    price_change_24h_pct: float | None
    atr_5m_pct: float | None
    atr_15m_pct: float | None
    atr_expansion_ratio: float | None
    range_position: float | None
    breakout_direction: MarketDirection | None
    breakout_level: float | None
    breakout_age_5m_bars: int | None
    distance_from_breakout_pct: float | None
    distance_from_breakout_atr: float | None
    compression_score: float | None
    confirmations: tuple[str, ...]
    warnings: tuple[str, ...]
    current_inplay_score: float | None
    current_inplay_direction: str | None
    current_inplay_display_status: str | None
    is_in_current_top20: bool
    rank_in_current_inplay_universe: int | None

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValueError("Early Discovery symbol cannot be empty.")
        if self.scanned_at.tzinfo is None or self.scanned_at.utcoffset() is None:
            raise ValueError("Early Discovery timestamp must be timezone-aware.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(self, "scanned_at", self.scanned_at.astimezone(UTC))
        for value in (self.discovery_score, self.entry_readiness_score):
            if not math.isfinite(value) or not 0.0 <= value <= 100.0:
                raise ValueError("Early Discovery scores must be between 0 and 100.")


@dataclass(frozen=True, slots=True)
class EarlyDiscoveryScanReport:
    started_at: datetime
    finished_at: datetime
    universe_size: int
    successfully_analyzed: int
    skipped: int
    errors: int
    results: tuple[EarlyDiscoveryResult, ...]


class CatalogProvider(Protocol):
    def list_instruments(self) -> tuple[CatalogInstrument, ...]: ...


class InPlayEvaluator(Protocol):
    def evaluate_existing_series(
        self,
        catalog: CatalogInstrument,
        series: MarketSeries,
        observed_at: datetime,
    ) -> InPlayResult | None: ...


@dataclass(frozen=True, slots=True)
class _LoadedCandidate:
    catalog: CatalogInstrument
    series_5m: MarketSeries
    series_15m: MarketSeries
    series_1h: MarketSeries
    current_inplay: InPlayResult | None


@dataclass(frozen=True, slots=True)
class _Breakout:
    direction: MarketDirection
    level: float
    age: int
    distance_pct: float
    distance_atr: float | None
    confirmed: bool
    retest: bool
    volume_ratio: float | None


class JsonlEarlyDiscoveryAuditStore:
    """Append-only seven-day diagnostic store independent from notifications."""

    def __init__(
        self,
        path: Path = DEFAULT_EARLY_DISCOVERY_AUDIT_PATH,
        *,
        retention: timedelta = EARLY_DISCOVERY_RETENTION,
    ) -> None:
        self._path = path
        self._retention = retention
        self._last_retention_check: datetime | None = None
        self._lock = threading.Lock()

    @property
    def path(self) -> Path:
        return self._path

    def append(
        self,
        results: tuple[EarlyDiscoveryResult, ...],
        observed_at: datetime,
    ) -> None:
        if not results:
            return
        now = _utc(observed_at)
        with self._lock:
            self._prune_if_due(now)
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._ensure_line_boundary()
            with self._path.open("a", encoding="utf-8", newline="\n") as stream:
                for result in results:
                    stream.write(
                        json.dumps(
                            _result_to_json(result),
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
            and now - self._last_retention_check < RETENTION_CHECK_INTERVAL
        ):
            return
        self._last_retention_check = now
        if not self._path.exists():
            return
        cutoff = now - self._retention
        retained: list[str] = []
        removed = False
        for line in self._path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                raw = json.loads(line)
                scanned_at = datetime.fromisoformat(str(raw["scanned_at"]))
                if scanned_at.tzinfo is None:
                    raise ValueError
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                retained.append(line)
                continue
            if scanned_at.astimezone(UTC) < cutoff:
                removed = True
            else:
                retained.append(line)
        if not removed:
            return
        temporary = self._path.with_suffix(f"{self._path.suffix}.tmp")
        temporary.write_text(
            "".join(f"{line}\n" for line in retained),
            encoding="utf-8",
        )
        temporary.replace(self._path)


class EarlyDiscoveryService:
    """Analyze the complete eligible crypto universe without user output."""

    def __init__(
        self,
        *,
        catalog_provider: CatalogProvider,
        market_provider: MarketDataProvider,
        audit_store: JsonlEarlyDiscoveryAuditStore,
        inplay_evaluator: InPlayEvaluator | None = None,
        clock: Callable[[], datetime] | None = None,
        maximum_workers: int = 4,
    ) -> None:
        if maximum_workers <= 0:
            raise ValueError("Early Discovery concurrency must be positive.")
        self._catalog = catalog_provider
        self._market = market_provider
        self._audit = audit_store
        self._inplay_evaluator = inplay_evaluator
        self._clock = clock or (lambda: datetime.now(UTC))
        self._maximum_workers = maximum_workers
        self._scan_lock = threading.Lock()

    def scan(self) -> EarlyDiscoveryScanReport:
        if not self._scan_lock.acquire(blocking=False):
            raise RuntimeError("Early Discovery scan is already running.")
        try:
            return self._scan_unlocked()
        finally:
            self._scan_lock.release()

    def _scan_unlocked(self) -> EarlyDiscoveryScanReport:
        started = _utc(self._clock())
        monotonic_started = time.monotonic()
        catalog = _eligible_catalog(self._catalog.list_instruments())
        loaded: list[_LoadedCandidate] = []
        errors = 0
        skipped = 0
        with ThreadPoolExecutor(max_workers=self._maximum_workers) as executor:
            futures = {
                executor.submit(self._load_candidate, item, started): item.symbol
                for item in catalog
            }
            for future in as_completed(futures):
                try:
                    candidate = future.result()
                except Exception:
                    errors += 1
                    continue
                if candidate is None:
                    skipped += 1
                else:
                    loaded.append(candidate)
        ranks = _inplay_ranks(loaded)
        results = tuple(
            _analyze(candidate, started, ranks)
            for candidate in sorted(loaded, key=lambda item: item.catalog.symbol)
        )
        try:
            self._audit.append(results, started)
        except OSError:
            _LOGGER.warning(
                "Не удалось записать Early Discovery audit; scan продолжен."
            )
        finished = _utc(self._clock())
        duration = time.monotonic() - monotonic_started
        _LOGGER.info(
            "Early Discovery scan started_at=%s finished_at=%s duration=%.3fs "
            "universe=%d analyzed=%d skipped=%d errors=%d",
            started.isoformat(),
            finished.isoformat(),
            duration,
            len(catalog),
            len(results),
            skipped,
            errors,
        )
        return EarlyDiscoveryScanReport(
            started_at=started,
            finished_at=finished,
            universe_size=len(catalog),
            successfully_analyzed=len(results),
            skipped=skipped,
            errors=errors,
            results=results,
        )

    def _load_candidate(
        self,
        catalog: CatalogInstrument,
        observed_at: datetime,
    ) -> _LoadedCandidate | None:
        instrument = Instrument(catalog.symbol, AssetClass.CRYPTO)
        try:
            series_5m = self._market.load(instrument, "5m", 80)
            series_15m = self._market.load(instrument, "15m", 80)
            series_1h = self._market.load(instrument, "1h", 100)
        except (MarketDataError, ValueError) as error:
            raise EarlyDiscoveryDataError from error
        if (
            len(series_5m.candles) < 30
            or len(series_15m.candles) < 30
            or len(series_1h.candles) < 25
        ):
            return None
        current = (
            self._inplay_evaluator.evaluate_existing_series(
                catalog,
                series_1h,
                observed_at,
            )
            if self._inplay_evaluator is not None
            else None
        )
        return _LoadedCandidate(catalog, series_5m, series_15m, series_1h, current)


def _eligible_catalog(
    items: tuple[CatalogInstrument, ...],
) -> tuple[CatalogInstrument, ...]:
    selected: dict[str, CatalogInstrument] = {}
    for item in items:
        symbol = item.symbol.strip().upper()
        if (
            not item.is_crypto_linear_usdt
            or item.status != "Trading"
            or item.turnover_24h < MINIMUM_TURNOVER_24H
            or item.bid <= 0
            or item.ask <= 0
            or item.spread_ratio > MAXIMUM_SPREAD_RATIO
        ):
            continue
        previous = selected.get(symbol)
        if previous is None or item.turnover_24h > previous.turnover_24h:
            selected[symbol] = item
    return tuple(selected.values())


def _analyze(
    candidate: _LoadedCandidate,
    scanned_at: datetime,
    ranks: Mapping[str, int],
) -> EarlyDiscoveryResult:
    catalog = candidate.catalog
    five = candidate.series_5m
    fifteen = candidate.series_15m
    hourly = candidate.series_1h
    last_price = five.candles[-1].close
    relative_5m = _relative_volume(five)
    relative_15m = _relative_volume(fifteen)
    acceleration = _ratio(relative_5m, relative_15m)
    atr_5m = _atr_pct(five)
    atr_15m = _atr_pct(fifteen)
    atr_expansion = _atr_expansion(five)
    range_position = _range_position(fifteen)
    breakout_5m = _latest_breakout(five)
    breakout_15m = _latest_breakout(fifteen)
    breakout = breakout_5m or _in_five_minute_bars(breakout_15m)
    compression = _compression_score(five)
    change_5m = _price_change(five, 1)
    change_15m = _price_change(fifteen, 1)
    change_1h = _price_change(hourly, 1)
    change_24h = _price_change(hourly, 24)
    direction = _market_direction(breakout, change_15m, change_1h)
    discovery_score = _discovery_score(
        catalog=catalog,
        volume_acceleration=acceleration,
        atr_expansion=atr_expansion,
        range_position=range_position,
        breakout_5m=breakout_5m,
        breakout_15m=breakout_15m,
        compression=compression,
    )
    readiness = _entry_readiness(
        catalog=catalog,
        relative_volume=relative_5m,
        breakout=breakout,
        breakout_15m=breakout_15m,
        price_change_24h=change_24h,
    )
    stage = _stage(
        discovery_score=discovery_score,
        readiness=readiness,
        direction=direction,
        breakout=breakout,
        spread_pct=catalog.spread_ratio * 100.0,
        price_change_24h=change_24h,
    )
    confirmations = _confirmations(
        relative_5m,
        atr_expansion,
        breakout_5m,
        breakout_15m,
    )
    warnings = _warnings(catalog, breakout, change_24h)
    current = candidate.current_inplay
    current_semantics = automatic_semantics(current) if current is not None else None
    rank = ranks.get(catalog.symbol)
    return EarlyDiscoveryResult(
        symbol=catalog.symbol,
        scanned_at=scanned_at,
        market_direction=direction,
        discovery_stage=stage,
        discovery_score=discovery_score,
        entry_readiness_score=readiness,
        last_price=last_price,
        spread_pct=catalog.spread_ratio * 100.0,
        turnover_24h=catalog.turnover_24h,
        relative_volume_5m=relative_5m,
        relative_volume_15m=relative_15m,
        volume_acceleration=acceleration,
        price_change_5m_pct=change_5m,
        price_change_15m_pct=change_15m,
        price_change_1h_pct=change_1h,
        price_change_24h_pct=change_24h,
        atr_5m_pct=atr_5m,
        atr_15m_pct=atr_15m,
        atr_expansion_ratio=atr_expansion,
        range_position=range_position,
        breakout_direction=breakout.direction if breakout is not None else None,
        breakout_level=breakout.level if breakout is not None else None,
        breakout_age_5m_bars=breakout.age if breakout is not None else None,
        distance_from_breakout_pct=(
            breakout.distance_pct if breakout is not None else None
        ),
        distance_from_breakout_atr=(
            breakout.distance_atr if breakout is not None else None
        ),
        compression_score=compression,
        confirmations=confirmations,
        warnings=warnings,
        current_inplay_score=(current.inplay_score if current is not None else None),
        current_inplay_direction=(
            current.direction.value if current is not None else None
        ),
        current_inplay_display_status=(
            current_semantics.display_status.value
            if current_semantics is not None
            else None
        ),
        is_in_current_top20=rank is not None and rank <= 20,
        rank_in_current_inplay_universe=rank,
    )


def _discovery_score(
    *,
    catalog: CatalogInstrument,
    volume_acceleration: float | None,
    atr_expansion: float | None,
    range_position: float | None,
    breakout_5m: _Breakout | None,
    breakout_15m: _Breakout | None,
    compression: float | None,
) -> float:
    volume_points = _scaled(volume_acceleration, 1.0, 2.0) * VOLUME_ACCELERATION_WEIGHT
    atr_points = _scaled(atr_expansion, 1.0, 2.0) * ATR_EXPANSION_WEIGHT
    proximity = (
        min(1.0, abs((range_position or 0.5) - 0.5) * 2.0)
        if range_position is not None
        else 0.0
    )
    breakout_points = 0.0
    freshness_values = [0.0]
    if breakout_5m is not None:
        freshness_values.append(max(0.0, 1.0 - breakout_5m.age / 8.0))
    if breakout_15m is not None:
        freshness_values.append(max(0.0, 1.0 - breakout_15m.age / 4.0))
    breakout_points = max(freshness_values)
    compression_points = (compression or 0.0) / 100.0
    liquidity_points = min(1.0, catalog.turnover_24h / 50_000_000.0)
    spread_points = max(0.0, 1.0 - catalog.spread_ratio / MAXIMUM_SPREAD_RATIO)
    return min(
        100.0,
        volume_points
        + atr_points
        + proximity * RANGE_PROXIMITY_WEIGHT
        + breakout_points * BREAKOUT_FRESHNESS_WEIGHT
        + compression_points * COMPRESSION_WEIGHT
        + liquidity_points * LIQUIDITY_WEIGHT
        + spread_points * SPREAD_QUALITY_WEIGHT,
    )


def _entry_readiness(
    *,
    catalog: CatalogInstrument,
    relative_volume: float | None,
    breakout: _Breakout | None,
    breakout_15m: _Breakout | None,
    price_change_24h: float | None,
) -> float:
    if breakout is None:
        freshness = 0.0
        distance = 0.0
    else:
        freshness = max(0.0, 30.0 - breakout.age * 5.0)
        distance_atr = abs(breakout.distance_atr or math.inf)
        if distance_atr <= 0.5:
            distance = 20.0
        elif distance_atr <= 1.0:
            distance = 15.0
        elif distance_atr <= 2.0:
            distance = 8.0
        else:
            distance = 0.0
    spread = max(
        0.0,
        15.0 * (1.0 - catalog.spread_ratio * 100.0 / READY_MAXIMUM_SPREAD_PCT),
    )
    liquidity = min(10.0, catalog.turnover_24h / 5_000_000.0)
    volume = _scaled(relative_volume, 1.0, 2.0) * 10.0
    structural_breakout = breakout or breakout_15m
    structure = 0.0
    breakout_volume = volume
    if structural_breakout is not None:
        structure = 7.5
        if structural_breakout.confirmed or structural_breakout.retest:
            structure = 15.0
        breakout_volume = _scaled(
            structural_breakout.volume_ratio,
            1.0,
            2.0,
        ) * 10.0
    score = freshness + distance + spread + liquidity + breakout_volume + structure
    if price_change_24h is not None and abs(price_change_24h) >= 15.0 - 1e-9:
        score = min(score, 49.0)
    return min(100.0, score)


def _stage(
    *,
    discovery_score: float,
    readiness: float,
    direction: MarketDirection,
    breakout: _Breakout | None,
    spread_pct: float,
    price_change_24h: float | None,
) -> DiscoveryStage:
    absolute_change = abs(price_change_24h or 0.0)
    if absolute_change >= 30.0 - 1e-9:
        return DiscoveryStage.DO_NOT_CHASE
    if absolute_change >= 15.0 - 1e-9:
        return DiscoveryStage.LATE
    ready = (
        discovery_score >= 60.0
        and readiness >= 60.0
        and direction is not MarketDirection.NEUTRAL
        and breakout is not None
        and breakout.age <= 3
        and abs(breakout.distance_atr or math.inf) <= 2.0
        and spread_pct <= READY_MAXIMUM_SPREAD_PCT
    )
    if ready:
        return DiscoveryStage.READY_CANDIDATE
    if discovery_score >= 40.0:
        return DiscoveryStage.SETUP_FORMING
    if discovery_score >= 25.0:
        return DiscoveryStage.EARLY_ATTENTION
    return DiscoveryStage.QUIET


def _latest_breakout(series: MarketSeries) -> _Breakout | None:
    candles = series.candles
    if len(candles) <= BREAKOUT_LOOKBACK:
        return None
    latest: tuple[int, MarketDirection, float] | None = None
    for index in range(BREAKOUT_LOOKBACK, len(candles)):
        history = candles[index - BREAKOUT_LOOKBACK : index]
        upper = max(item.high for item in history)
        lower = min(item.low for item in history)
        if candles[index].close > upper:
            latest = (index, MarketDirection.UP, upper)
        elif candles[index].close < lower:
            latest = (index, MarketDirection.DOWN, lower)
    if latest is None:
        return None
    index, direction, level = latest
    current = candles[-1].close
    atr = _atr_value(series)
    distance = current - level
    later = candles[index + 1 :]
    if direction is MarketDirection.UP:
        confirmed = any(item.close > level for item in later)
        retest = any(
            item.low <= level + (atr or 0.0) * 0.25 and item.close >= level
            for item in later
        )
    else:
        confirmed = any(item.close < level for item in later)
        retest = any(
            item.high >= level - (atr or 0.0) * 0.25 and item.close <= level
            for item in later
        )
    prior_volume = mean(tuple(item.volume for item in candles[index - 20 : index]))
    volume_ratio = candles[index].volume / prior_volume if prior_volume > 0 else None
    return _Breakout(
        direction=direction,
        level=level,
        age=len(candles) - 1 - index,
        distance_pct=distance / level * 100.0 if level > 0 else 0.0,
        distance_atr=distance / atr if atr is not None and atr > 0 else None,
        confirmed=confirmed,
        retest=retest,
        volume_ratio=volume_ratio,
    )


def _in_five_minute_bars(breakout: _Breakout | None) -> _Breakout | None:
    if breakout is None:
        return None
    return _Breakout(
        direction=breakout.direction,
        level=breakout.level,
        age=breakout.age * 3,
        distance_pct=breakout.distance_pct,
        distance_atr=breakout.distance_atr,
        confirmed=breakout.confirmed,
        retest=breakout.retest,
        volume_ratio=breakout.volume_ratio,
    )


def _market_direction(
    breakout: _Breakout | None,
    change_15m: float | None,
    change_1h: float | None,
) -> MarketDirection:
    if breakout is not None and breakout.age <= 6:
        return breakout.direction
    combined = (change_15m or 0.0) + (change_1h or 0.0)
    if combined > 0.3:
        return MarketDirection.UP
    if combined < -0.3:
        return MarketDirection.DOWN
    return MarketDirection.NEUTRAL


def _relative_volume(series: MarketSeries) -> float | None:
    candles = series.candles
    if len(candles) < 21:
        return None
    baseline = mean(tuple(item.volume for item in candles[-21:-1]))
    return candles[-1].volume / baseline if baseline > 0 else None


def _atr_value(series: MarketSeries) -> float | None:
    candles = series.candles
    if len(candles) < 2:
        return None
    ranges = true_ranges(
        tuple(item.high for item in candles),
        tuple(item.low for item in candles),
        tuple(item.close for item in candles),
    )
    return mean(ranges[-min(14, len(ranges)) :])


def _atr_pct(series: MarketSeries) -> float | None:
    atr = _atr_value(series)
    close = series.candles[-1].close
    return atr / close * 100.0 if atr is not None and close > 0 else None


def _atr_expansion(series: MarketSeries) -> float | None:
    candles = series.candles
    if len(candles) < 30:
        return None
    ranges = true_ranges(
        tuple(item.high for item in candles),
        tuple(item.low for item in candles),
        tuple(item.close for item in candles),
    )
    current = mean(ranges[-7:])
    baseline = mean(ranges[-28:-7])
    return current / baseline if baseline > 0 else None


def _compression_score(series: MarketSeries) -> float | None:
    candles = series.candles
    if len(candles) < 30:
        return None
    ranges = tuple(item.high - item.low for item in candles)
    recent = mean(ranges[-5:])
    baseline = mean(ranges[-25:-5])
    if baseline <= 0:
        return None
    return max(0.0, min(100.0, (1.0 - recent / baseline) * 200.0))


def _range_position(series: MarketSeries) -> float | None:
    candles = series.candles
    if len(candles) < 21:
        return None
    history = candles[-21:-1]
    lower = min(item.low for item in history)
    upper = max(item.high for item in history)
    if upper <= lower:
        return None
    return max(0.0, min(1.0, (candles[-1].close - lower) / (upper - lower)))


def _price_change(series: MarketSeries, bars: int) -> float | None:
    if bars <= 0 or len(series.candles) <= bars:
        return None
    current = series.candles[-1].close
    reference = series.candles[-bars - 1].close
    return (current / reference - 1.0) * 100.0 if reference > 0 else None


def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return numerator / denominator


def _scaled(value: float | None, lower: float, upper: float) -> float:
    if value is None or upper <= lower:
        return 0.0
    return max(0.0, min(1.0, (value - lower) / (upper - lower)))


def _inplay_ranks(candidates: list[_LoadedCandidate]) -> dict[str, int]:
    ranked = sorted(
        (
            (item.catalog.symbol, item.current_inplay.inplay_score)
            for item in candidates
            if item.current_inplay is not None
        ),
        key=lambda item: item[1],
        reverse=True,
    )
    return {symbol: index for index, (symbol, _) in enumerate(ranked, start=1)}


def _confirmations(
    relative_volume: float | None,
    atr_expansion: float | None,
    breakout_5m: _Breakout | None,
    breakout_15m: _Breakout | None,
) -> tuple[str, ...]:
    values: list[str] = []
    if relative_volume is not None and relative_volume >= 1.5:
        values.append("5m volume expansion")
    if atr_expansion is not None and atr_expansion >= 1.3:
        values.append("5m ATR expansion")
    if breakout_5m is not None and breakout_5m.age <= 3:
        values.append("fresh 5m breakout")
    if breakout_15m is not None and breakout_15m.age <= 2:
        values.append("fresh 15m breakout")
    structural = breakout_5m or breakout_15m
    if structural is not None and structural.retest:
        values.append("breakout retest")
    elif structural is not None and structural.confirmed:
        values.append("breakout hold")
    return tuple(values)


def _warnings(
    catalog: CatalogInstrument,
    breakout: _Breakout | None,
    price_change_24h: float | None,
) -> tuple[str, ...]:
    values: list[str] = []
    absolute_change = abs(price_change_24h or 0.0)
    if absolute_change >= 30.0 - 1e-9:
        values.append("24h move is extreme; do not chase")
    elif absolute_change >= 15.0 - 1e-9:
        values.append("24h move is significantly realized")
    if catalog.spread_ratio * 100.0 > READY_MAXIMUM_SPREAD_PCT:
        values.append("spread blocks entry readiness")
    if breakout is not None and abs(breakout.distance_atr or 0.0) > 2.0:
        values.append("distance from breakout exceeds 2 ATR")
    return tuple(values)


def _result_to_json(result: EarlyDiscoveryResult) -> dict[str, Any]:
    payload = asdict(result)
    payload["scanned_at"] = result.scanned_at.isoformat()
    payload["market_direction"] = result.market_direction.value
    payload["discovery_stage"] = result.discovery_stage.value
    payload["breakout_direction"] = (
        result.breakout_direction.value
        if result.breakout_direction is not None
        else None
    )
    payload["confirmations"] = list(result.confirmations)
    payload["warnings"] = list(result.warnings)
    return payload


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Early Discovery timestamp must be timezone-aware.")
    return value.astimezone(UTC)
