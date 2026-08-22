from __future__ import annotations

import argparse
import json
import logging
import shutil
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

from market_signal_assistant.indicators import mean, true_ranges
from market_signal_assistant.inplay.early_discovery import (
    READY_MAXIMUM_SPREAD_PCT,
    CatalogProvider,
    DiscoveryStage,
    EarlyDiscoveryDataError,
    EarlyDiscoveryResult,
    InPlayEvaluator,
    JsonlEarlyDiscoveryAuditStore,
    MarketDirection,
    _analyze,
    _eligible_catalog,
    _inplay_ranks,
    _LoadedCandidate,
)
from market_signal_assistant.inplay.models import CatalogInstrument
from market_signal_assistant.localized_argparse import RussianArgumentParser
from market_signal_assistant.models import AssetClass, Candle, Instrument, MarketSeries
from market_signal_assistant.providers import MarketDataError, MarketDataProvider

DEFAULT_V2_AUDIT_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "inplay_early_discovery_v2_audit.jsonl"
)
DEFAULT_V2_STATE_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "inplay_early_discovery_v2_state.json"
)
V2_SCHEMA_VERSION = 1
V2_STATE_VERSION = 1
V2_RETENTION = timedelta(days=7)
V2_RETENTION_CHECK_INTERVAL = timedelta(days=1)
V2_READY_SCORE = 60.0
V2_EARLY_SCORE = 25.0
V2_FORMING_SCORE = 40.0

DISCOVERY_WEIGHTS = {
    "volume_acceleration": 20.0,
    "atr_expansion": 15.0,
    "range_proximity": 15.0,
    "breakout_freshness": 20.0,
    "compression": 10.0,
    "liquidity": 10.0,
    "spread_quality": 10.0,
}
READINESS_WEIGHTS = {
    "breakout_freshness": 15.0,
    "distance": 10.0,
    "correct_side": 15.0,
    "hold": 10.0,
    "retest": 10.0,
    "direction_stability": 10.0,
    "ready_stability": 10.0,
    "spread_quality": 5.0,
    "liquidity": 5.0,
    "breakout_volume": 10.0,
}

DISPLAY_STAGE_RU = {
    DiscoveryStage.QUIET: "БЕЗ СИГНАЛА",
    DiscoveryStage.EARLY_ATTENTION: "РАННЕЕ ВНИМАНИЕ",
    DiscoveryStage.SETUP_FORMING: "ФОРМИРУЕТСЯ",
    DiscoveryStage.READY_CANDIDATE: "ПОДТВЕРЖДЁННОЕ НАБЛЮДЕНИЕ",
    DiscoveryStage.LATE: "ПОЗДНО",
    DiscoveryStage.DO_NOT_CHASE: "НЕ ДОГОНЯТЬ",
}

_LOGGER = logging.getLogger(__name__)


class RetestState(Enum):
    UNCONFIRMED = "UNCONFIRMED"
    HOLDING = "HOLDING"
    RETEST_IN_PROGRESS = "RETEST_IN_PROGRESS"
    RETEST_HELD = "RETEST_HELD"
    FAILED = "FAILED"


RETEST_STATE_RU = {
    RetestState.UNCONFIRMED: "ПРОБОЙ НЕ ПОДТВЕРЖДЁН",
    RetestState.HOLDING: "ПРОБОЙ УДЕРЖИВАЕТСЯ",
    RetestState.RETEST_IN_PROGRESS: "РЕТЕСТ ИДЁТ",
    RetestState.RETEST_HELD: "РЕТЕСТ УДЕРЖАН",
    RetestState.FAILED: "ПРОБОЙ ПРОВАЛЕН",
}


@dataclass(frozen=True, slots=True)
class EarlyDiscoveryV2Config:
    required_ready_scans: int = 3
    forming_scans: int = 2
    episode_gap_minutes: int = 30

    def __post_init__(self) -> None:
        if self.forming_scans < 1:
            raise ValueError(
                "Количество формирующих сканирований должно быть положительным."
            )
        if self.required_ready_scans <= self.forming_scans:
            raise ValueError(
                "Количество подтверждающих сканирований должно быть больше "
                "формирующего."
            )
        if not 5 <= self.episode_gap_minutes <= 1440:
            raise ValueError("Пауза эпизода должна быть от 5 до 1440 минут.")

    @property
    def episode_gap(self) -> timedelta:
        return timedelta(minutes=self.episode_gap_minutes)


ComponentRawValue = float | int | bool | str | None


@dataclass(frozen=True, slots=True)
class ScoreComponent:
    score_kind: str
    score_name_ru: str
    component_id: str
    raw_value: ComponentRawValue
    points: float
    maximum_points: float
    reason: str
    explanation_ru: str


@dataclass(frozen=True, slots=True)
class BreakoutAssessment:
    direction: MarketDirection
    interval: str
    level: float
    current_price: float
    absolute_distance: float
    signed_distance_atr: float | None
    absolute_distance_atr: float | None
    distance_sign: int
    is_correct_side_of_level: bool
    breakout_hold_candles: int
    returned_to_level: bool
    retest_state: RetestState
    breakout_failure: bool
    returned_inside_range: bool
    breakout_age_bars: int
    volume_ratio: float | None


@dataclass(frozen=True, slots=True)
class SequenceSnapshot:
    consecutive_active_scans: int
    consecutive_ready_scans: int
    first_detected_at: datetime | None
    first_ready_at: datetime | None
    second_confirmation_at: datetime | None
    third_confirmation_at: datetime | None
    last_stage: DiscoveryStage
    last_direction: MarketDirection
    last_seen_at: datetime | None
    reset_reason: str | None


@dataclass(frozen=True, slots=True)
class EarlyDiscoveryV2Result:
    schema_version: int
    scan_id: str
    scanned_at: datetime
    symbol: str
    market_direction: MarketDirection
    direction_v1: MarketDirection | None
    direction_v2: MarketDirection | None
    stage_v1: DiscoveryStage | None
    stage_v2: DiscoveryStage
    display_stage_v2_ru: str
    discovery_score_v1: float | None
    discovery_score_v2: float | None
    readiness_score_v1: float | None
    readiness_score_v2: float | None
    consecutive_active_scans: int
    consecutive_ready_scans: int
    first_detected_at: datetime | None
    first_ready_at: datetime | None
    second_confirmation_at: datetime | None
    third_confirmation_at: datetime | None
    reset_reason: str | None
    breakout_level: float | None
    current_price: float | None
    absolute_distance: float | None
    signed_distance_atr: float | None
    absolute_distance_atr: float | None
    distance_sign: int | None
    is_correct_side_of_level: bool | None
    breakout_hold_candles: int | None
    returned_to_level: bool | None
    retest_state: RetestState | None
    breakout_failure: bool | None
    returned_inside_range: bool | None
    breakout_age_bars: int | None
    spread_pct: float | None
    price_change_24h_pct: float | None
    production_rank: int | None
    is_in_production_top20: bool
    component_scores: tuple[ScoreComponent, ...]
    confirmations: tuple[str, ...]
    warnings: tuple[str, ...]
    technical_error: str | None
    reason_v2_ru: str


@dataclass(frozen=True, slots=True)
class EarlyDiscoveryV2ScanReport:
    started_at: datetime
    finished_at: datetime
    universe_size: int
    successfully_analyzed: int
    skipped: int
    errors: int
    confirmed_observations: int
    forming_observations: int
    results: tuple[EarlyDiscoveryV2Result, ...]


@dataclass(slots=True)
class V2SymbolState:
    symbol: str
    consecutive_active_scans: int = 0
    consecutive_ready_scans: int = 0
    quiet_scans: int = 0
    first_detected_at: datetime | None = None
    first_ready_at: datetime | None = None
    second_confirmation_at: datetime | None = None
    third_confirmation_at: datetime | None = None
    last_stage: DiscoveryStage = DiscoveryStage.QUIET
    last_direction: MarketDirection = MarketDirection.NEUTRAL
    last_seen_at: datetime | None = None
    reset_reason: str | None = None

    def snapshot(self) -> SequenceSnapshot:
        return SequenceSnapshot(
            self.consecutive_active_scans,
            self.consecutive_ready_scans,
            self.first_detected_at,
            self.first_ready_at,
            self.second_confirmation_at,
            self.third_confirmation_at,
            self.last_stage,
            self.last_direction,
            self.last_seen_at,
            self.reset_reason,
        )


class JsonEarlyDiscoveryV2StateStore:
    def __init__(self, path: Path = DEFAULT_V2_STATE_PATH) -> None:
        self._path = path
        self._lock = threading.Lock()

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> dict[str, V2SymbolState]:
        with self._lock:
            if not self._path.exists():
                return {}
            try:
                raw = json.loads(self._path.read_text(encoding="utf-8"))
                records = raw.get("records", {})
                if not isinstance(records, dict):
                    raise ValueError("Некорректная структура записей.")
                return {
                    str(symbol).upper(): _state_from_json(str(symbol), value)
                    for symbol, value in records.items()
                    if isinstance(value, dict)
                }
            except (OSError, ValueError, TypeError, json.JSONDecodeError) as error:
                backup = self._backup_corrupt_file()
                _LOGGER.warning(
                    "Состояние раннего обнаружения V2 повреждено (%s); "
                    "резервная копия: %s. Начато пустое состояние.",
                    type(error).__name__,
                    backup,
                )
                return {}

    def save(self, records: Mapping[str, V2SymbolState]) -> None:
        payload = {
            "version": V2_STATE_VERSION,
            "records": {
                symbol: _state_to_json(state)
                for symbol, state in sorted(records.items())
            },
        }
        with self._lock:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self._path.with_suffix(f"{self._path.suffix}.tmp")
            temporary.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            temporary.replace(self._path)

    def _backup_corrupt_file(self) -> Path | None:
        if not self._path.exists():
            return None
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        backup = self._path.with_name(f"{self._path.name}.corrupt-{timestamp}")
        try:
            shutil.copy2(self._path, backup)
        except OSError:
            return None
        return backup


class EarlyDiscoveryV2SequenceTracker:
    def __init__(
        self,
        store: JsonEarlyDiscoveryV2StateStore,
        config: EarlyDiscoveryV2Config,
    ) -> None:
        self._store = store
        self._config = config
        self._records = store.load()

    @property
    def records(self) -> Mapping[str, V2SymbolState]:
        return self._records

    def technical_error(self, symbol: str) -> SequenceSnapshot:
        return self._records.get(symbol, V2SymbolState(symbol)).snapshot()

    def update(
        self,
        *,
        symbol: str,
        direction: MarketDirection,
        active: bool,
        ready: bool,
        observed_at: datetime,
    ) -> SequenceSnapshot:
        now = _utc(observed_at)
        state = self._records.setdefault(symbol, V2SymbolState(symbol))
        if (
            state.last_seen_at is not None
            and now - state.last_seen_at >= self._config.episode_gap
        ):
            _reset_sequence(
                state, "инструмент отсутствовал не менее заданного интервала"
            )
        if (
            active
            and direction is not MarketDirection.NEUTRAL
            and state.last_direction is not MarketDirection.NEUTRAL
            and direction is not state.last_direction
        ):
            _reset_sequence(state, "направление изменилось")
        if not active:
            state.quiet_scans += 1
            state.consecutive_ready_scans = 0
            if state.quiet_scans >= 2:
                _reset_sequence(state, "два последовательных состояния без сигнала")
            state.last_stage = DiscoveryStage.QUIET
            state.last_seen_at = now
            return state.snapshot()

        state.quiet_scans = 0
        state.consecutive_active_scans += 1
        if state.first_detected_at is None:
            state.first_detected_at = now
        if direction is not MarketDirection.NEUTRAL:
            state.last_direction = direction
        if ready:
            state.consecutive_ready_scans += 1
            if state.consecutive_ready_scans == 1:
                state.first_ready_at = now
            elif state.consecutive_ready_scans == 2:
                state.second_confirmation_at = now
            elif state.consecutive_ready_scans == 3:
                state.third_confirmation_at = now
        else:
            state.consecutive_ready_scans = 0
            state.first_ready_at = None
            state.second_confirmation_at = None
            state.third_confirmation_at = None
        state.last_seen_at = now
        return state.snapshot()

    def set_stage(self, symbol: str, stage: DiscoveryStage) -> None:
        self._records.setdefault(symbol, V2SymbolState(symbol)).last_stage = stage

    def save(self) -> None:
        self._store.save(self._records)


class JsonlEarlyDiscoveryV2AuditStore:
    def __init__(
        self,
        path: Path = DEFAULT_V2_AUDIT_PATH,
        *,
        retention: timedelta = V2_RETENTION,
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
        results: tuple[EarlyDiscoveryV2Result, ...],
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
            and now - self._last_retention_check < V2_RETENTION_CHECK_INTERVAL
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
                    scanned_at = datetime.fromisoformat(str(raw["scanned_at"]))
                    if scanned_at.tzinfo is None:
                        raise ValueError
                except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                    destination.write(f"{line}\n")
                    continue
                if scanned_at.astimezone(UTC) < cutoff:
                    removed = True
                else:
                    destination.write(f"{line}\n")
        if removed:
            temporary.replace(self._path)
        else:
            temporary.unlink(missing_ok=True)


class EarlyDiscoveryV2Service:
    """Silent V1/V2 comparison over one shared market-data snapshot."""

    def __init__(
        self,
        *,
        catalog_provider: CatalogProvider,
        market_provider: MarketDataProvider,
        audit_store: JsonlEarlyDiscoveryV2AuditStore,
        state_store: JsonEarlyDiscoveryV2StateStore,
        config: EarlyDiscoveryV2Config,
        inplay_evaluator: InPlayEvaluator | None = None,
        v1_audit_store: JsonlEarlyDiscoveryAuditStore | None = None,
        clock: Callable[[], datetime] | None = None,
        maximum_workers: int = 4,
    ) -> None:
        if maximum_workers <= 0:
            raise ValueError("Количество рабочих потоков должно быть положительным.")
        self._catalog = catalog_provider
        self._market = market_provider
        self._audit = audit_store
        self._tracker = EarlyDiscoveryV2SequenceTracker(state_store, config)
        self._config = config
        self._inplay_evaluator = inplay_evaluator
        self._v1_audit = v1_audit_store
        self._clock = clock or (lambda: datetime.now(UTC))
        self._maximum_workers = maximum_workers
        self._scan_lock = threading.Lock()

    def scan(self) -> EarlyDiscoveryV2ScanReport:
        if not self._scan_lock.acquire(blocking=False):
            raise RuntimeError("Сканирование раннего обнаружения V2 уже выполняется.")
        try:
            return self._scan_unlocked()
        finally:
            self._scan_lock.release()

    def _scan_unlocked(self) -> EarlyDiscoveryV2ScanReport:
        started = _utc(self._clock())
        catalog = _eligible_catalog(self._catalog.list_instruments())
        loaded: list[_LoadedCandidate] = []
        failures: dict[str, str] = {}
        skipped: dict[str, str] = {}
        with ThreadPoolExecutor(max_workers=self._maximum_workers) as executor:
            futures = {
                executor.submit(self._load_candidate, item, started): item
                for item in catalog
            }
            for future in as_completed(futures):
                item = futures[future]
                try:
                    candidate = future.result()
                except Exception as error:
                    failures[item.symbol] = type(error).__name__
                    continue
                if candidate is None:
                    skipped[item.symbol] = "недостаточно завершённых свечей"
                else:
                    loaded.append(candidate)
        ranks = _inplay_ranks(loaded)
        scan_id = started.strftime("%Y%m%dT%H%M%S.%fZ")
        v1_results = {
            item.catalog.symbol: _analyze(item, started, ranks)
            for item in sorted(loaded, key=lambda value: value.catalog.symbol)
        }
        if self._v1_audit is not None:
            self._v1_audit.append(tuple(v1_results.values()), started)
        results = [
            _analyze_v2(
                candidate=item,
                v1=v1_results[item.catalog.symbol],
                scan_id=scan_id,
                scanned_at=started,
                tracker=self._tracker,
                config=self._config,
            )
            for item in sorted(loaded, key=lambda value: value.catalog.symbol)
        ]
        catalog_by_symbol = {item.symbol: item for item in catalog}
        for symbol, message in sorted({**skipped, **failures}.items()):
            results.append(
                _technical_error_result(
                    catalog_by_symbol[symbol],
                    scan_id,
                    started,
                    self._tracker.technical_error(symbol),
                    message,
                )
            )
        ordered = tuple(sorted(results, key=lambda item: item.symbol))
        try:
            self._tracker.save()
        except OSError:
            _LOGGER.warning(
                "Не удалось сохранить состояние раннего обнаружения V2; "
                "сканирование продолжено."
            )
        try:
            self._audit.append(ordered, started)
        except OSError:
            _LOGGER.warning(
                "Не удалось записать аудит раннего обнаружения V2; "
                "сканирование продолжено."
            )
        finished = _utc(self._clock())
        confirmed = sum(
            item.stage_v2 is DiscoveryStage.READY_CANDIDATE for item in ordered
        )
        forming = sum(item.stage_v2 is DiscoveryStage.SETUP_FORMING for item in ordered)
        return EarlyDiscoveryV2ScanReport(
            started,
            finished,
            len(catalog),
            len(loaded),
            len(skipped),
            len(failures),
            confirmed,
            forming,
            ordered,
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
        completed_5m = _completed_series(series_5m, observed_at, 5)
        completed_15m = _completed_series(series_15m, observed_at, 15)
        completed_1h = _completed_series(series_1h, observed_at, 60)
        if (
            completed_5m is None
            or completed_15m is None
            or completed_1h is None
            or len(completed_5m.candles) < 30
            or len(completed_15m.candles) < 30
            or len(completed_1h.candles) < 25
        ):
            return None
        current = (
            self._inplay_evaluator.evaluate_existing_series(
                catalog,
                completed_1h,
                observed_at,
            )
            if self._inplay_evaluator is not None
            else None
        )
        return _LoadedCandidate(
            catalog,
            completed_5m,
            completed_15m,
            completed_1h,
            current,
        )


class EarlyDiscoveryV2FixedSchedule:
    def __init__(
        self,
        service: EarlyDiscoveryV2Service,
        *,
        interval_seconds: float,
        monotonic: Callable[[], float] | None = None,
        sleeper: Callable[[float], None] | None = None,
        reporter: Callable[[EarlyDiscoveryV2ScanReport], None] | None = None,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("Интервал V2 должен быть положительным.")
        self._service = service
        self._interval = interval_seconds
        self._monotonic = monotonic or time.monotonic
        self._sleeper = sleeper or time.sleep
        self._reporter = reporter or print_scan_report

    def run(self, *, maximum_scans: int | None = None) -> None:
        next_run = self._monotonic()
        scans = 0
        while maximum_scans is None or scans < maximum_scans:
            delay = max(0.0, next_run - self._monotonic())
            if delay > 0:
                self._sleeper(delay)
            next_run += self._interval
            try:
                report = self._service.scan()
            except Exception as error:
                _LOGGER.warning(
                    "Сканирование раннего обнаружения V2 завершилось ошибкой (%s).",
                    type(error).__name__,
                )
            else:
                self._reporter(report)
            scans += 1
            now = self._monotonic()
            if now >= next_run:
                missed = int((now - next_run) // self._interval) + 1
                next_run += missed * self._interval


def print_scan_report(report: EarlyDiscoveryV2ScanReport) -> None:
    print("Модуль раннего обнаружения V2: сканирование завершено.")
    print(f"Инструментов: {report.successfully_analyzed}.")
    print(f"Подтверждённых наблюдений: {report.confirmed_observations}.")
    print(f"Формирующихся ситуаций: {report.forming_observations}.")
    print(f"Ошибок отдельных инструментов: {report.errors}.")


def _analyze_v2(
    *,
    candidate: _LoadedCandidate,
    v1: EarlyDiscoveryResult,
    scan_id: str,
    scanned_at: datetime,
    tracker: EarlyDiscoveryV2SequenceTracker,
    config: EarlyDiscoveryV2Config,
) -> EarlyDiscoveryV2Result:
    assessment = _breakout_assessment(
        candidate.series_5m,
        candidate.series_15m,
    )
    direction_v2 = (
        assessment.direction if assessment is not None else v1.market_direction
    )
    discovery_components = _discovery_components(candidate, assessment)
    discovery_score = sum(item.points for item in discovery_components)
    preliminary_active = (
        discovery_score >= V2_EARLY_SCORE
        and direction_v2 is not MarketDirection.NEUTRAL
    )
    preliminary_ready = _preliminary_ready(
        candidate.catalog,
        direction_v2,
        assessment,
        discovery_score,
        v1.price_change_24h_pct,
    )
    sequence = tracker.update(
        symbol=v1.symbol,
        direction=direction_v2,
        active=preliminary_active,
        ready=preliminary_ready,
        observed_at=scanned_at,
    )
    readiness_components = _readiness_components(
        candidate,
        assessment,
        sequence,
        config,
    )
    readiness_score = sum(item.points for item in readiness_components)
    stage = _v2_stage(
        direction=direction_v2,
        discovery_score=discovery_score,
        readiness_score=readiness_score,
        sequence=sequence,
        assessment=assessment,
        spread_pct=v1.spread_pct,
        price_change_24h=v1.price_change_24h_pct,
        config=config,
    )
    tracker.set_stage(v1.symbol, stage)
    components = discovery_components + readiness_components
    confirmations = _v2_confirmations(assessment, sequence)
    warnings = _v2_warnings(
        candidate.catalog,
        assessment,
        v1.price_change_24h_pct,
    )
    return EarlyDiscoveryV2Result(
        schema_version=V2_SCHEMA_VERSION,
        scan_id=scan_id,
        scanned_at=scanned_at,
        symbol=v1.symbol,
        market_direction=direction_v2,
        direction_v1=v1.market_direction,
        direction_v2=direction_v2,
        stage_v1=v1.discovery_stage,
        stage_v2=stage,
        display_stage_v2_ru=DISPLAY_STAGE_RU[stage],
        discovery_score_v1=v1.discovery_score,
        discovery_score_v2=min(100.0, discovery_score),
        readiness_score_v1=v1.entry_readiness_score,
        readiness_score_v2=min(100.0, readiness_score),
        consecutive_active_scans=sequence.consecutive_active_scans,
        consecutive_ready_scans=sequence.consecutive_ready_scans,
        first_detected_at=sequence.first_detected_at,
        first_ready_at=sequence.first_ready_at,
        second_confirmation_at=sequence.second_confirmation_at,
        third_confirmation_at=sequence.third_confirmation_at,
        reset_reason=sequence.reset_reason,
        breakout_level=assessment.level if assessment else None,
        current_price=assessment.current_price if assessment else v1.last_price,
        absolute_distance=assessment.absolute_distance if assessment else None,
        signed_distance_atr=assessment.signed_distance_atr if assessment else None,
        absolute_distance_atr=assessment.absolute_distance_atr if assessment else None,
        distance_sign=assessment.distance_sign if assessment else None,
        is_correct_side_of_level=(
            assessment.is_correct_side_of_level if assessment else None
        ),
        breakout_hold_candles=(
            assessment.breakout_hold_candles if assessment else None
        ),
        returned_to_level=assessment.returned_to_level if assessment else None,
        retest_state=assessment.retest_state if assessment else None,
        breakout_failure=assessment.breakout_failure if assessment else None,
        returned_inside_range=(
            assessment.returned_inside_range if assessment else None
        ),
        breakout_age_bars=assessment.breakout_age_bars if assessment else None,
        spread_pct=v1.spread_pct,
        price_change_24h_pct=v1.price_change_24h_pct,
        production_rank=v1.rank_in_current_inplay_universe,
        is_in_production_top20=v1.is_in_current_top20,
        component_scores=components,
        confirmations=confirmations,
        warnings=warnings,
        technical_error=None,
        reason_v2_ru=_stage_reason(v1.discovery_stage, stage, assessment, sequence),
    )


def _technical_error_result(
    catalog: CatalogInstrument,
    scan_id: str,
    scanned_at: datetime,
    sequence: SequenceSnapshot,
    error: str,
) -> EarlyDiscoveryV2Result:
    return EarlyDiscoveryV2Result(
        schema_version=V2_SCHEMA_VERSION,
        scan_id=scan_id,
        scanned_at=scanned_at,
        symbol=catalog.symbol,
        market_direction=sequence.last_direction,
        direction_v1=None,
        direction_v2=None,
        stage_v1=None,
        stage_v2=DiscoveryStage.QUIET,
        display_stage_v2_ru=DISPLAY_STAGE_RU[DiscoveryStage.QUIET],
        discovery_score_v1=None,
        discovery_score_v2=None,
        readiness_score_v1=None,
        readiness_score_v2=None,
        consecutive_active_scans=sequence.consecutive_active_scans,
        consecutive_ready_scans=sequence.consecutive_ready_scans,
        first_detected_at=sequence.first_detected_at,
        first_ready_at=sequence.first_ready_at,
        second_confirmation_at=sequence.second_confirmation_at,
        third_confirmation_at=sequence.third_confirmation_at,
        reset_reason=sequence.reset_reason,
        breakout_level=None,
        current_price=None,
        absolute_distance=None,
        signed_distance_atr=None,
        absolute_distance_atr=None,
        distance_sign=None,
        is_correct_side_of_level=None,
        breakout_hold_candles=None,
        returned_to_level=None,
        retest_state=None,
        breakout_failure=None,
        returned_inside_range=None,
        breakout_age_bars=None,
        spread_pct=catalog.spread_ratio * 100.0,
        price_change_24h_pct=None,
        production_rank=None,
        is_in_production_top20=False,
        component_scores=(),
        confirmations=(),
        warnings=("Технические данные недоступны.",),
        technical_error=error,
        reason_v2_ru=(
            "Инструмент пропущен из-за технической ошибки; "
            "последовательность не сброшена."
        ),
    )


@dataclass(frozen=True, slots=True)
class _BreakoutEvent:
    direction: MarketDirection
    interval: str
    level: float
    index: int
    candles: tuple[Candle, ...]
    atr: float | None
    volume_ratio: float | None


def _breakout_assessment(
    series_5m: MarketSeries,
    series_15m: MarketSeries,
) -> BreakoutAssessment | None:
    events = tuple(
        item
        for item in (
            _latest_breakout_event(series_5m, "5m"),
            _latest_breakout_event(series_15m, "15m"),
        )
        if item is not None
    )
    if not events:
        return None
    event = max(events, key=lambda item: item.candles[item.index].timestamp)
    current = series_5m.candles[-1].close
    distance = current - event.level
    signed_atr = (
        distance / event.atr if event.atr is not None and event.atr > 0 else None
    )
    correct = _is_correct_side(current, event.level, event.direction)
    later = event.candles[event.index + 1 :]
    tolerance = (event.atr or 0.0) * 0.25
    touch_indexes = [
        index
        for index, candle in enumerate(later)
        if _touches_level(candle, event.level, event.direction, tolerance)
    ]
    returned = bool(touch_indexes)
    returned_inside = any(
        not _is_correct_side(candle.close, event.level, event.direction)
        for candle in later
    )
    failure = not correct
    hold_candles = _consecutive_correct_closes(
        event.candles[event.index :],
        event.level,
        event.direction,
    )
    if failure:
        retest_state = RetestState.FAILED
    elif returned:
        first_touch = touch_indexes[0]
        held = any(
            _is_correct_side(candle.close, event.level, event.direction)
            for candle in later[first_touch:]
        )
        retest_state = (
            RetestState.RETEST_HELD if held else RetestState.RETEST_IN_PROGRESS
        )
    elif hold_candles >= 1:
        retest_state = RetestState.HOLDING
    else:
        retest_state = RetestState.UNCONFIRMED
    return BreakoutAssessment(
        direction=event.direction,
        interval=event.interval,
        level=event.level,
        current_price=current,
        absolute_distance=abs(distance),
        signed_distance_atr=signed_atr,
        absolute_distance_atr=abs(signed_atr) if signed_atr is not None else None,
        distance_sign=1 if distance > 0 else -1 if distance < 0 else 0,
        is_correct_side_of_level=correct,
        breakout_hold_candles=hold_candles,
        returned_to_level=returned,
        retest_state=retest_state,
        breakout_failure=failure,
        returned_inside_range=returned_inside,
        breakout_age_bars=len(event.candles) - 1 - event.index,
        volume_ratio=event.volume_ratio,
    )


def _latest_breakout_event(
    series: MarketSeries,
    interval: str,
    lookback: int = 20,
) -> _BreakoutEvent | None:
    candles = series.candles
    latest: tuple[int, MarketDirection, float] | None = None
    for index in range(lookback, len(candles)):
        history = candles[index - lookback : index]
        upper = max(item.high for item in history)
        lower = min(item.low for item in history)
        if candles[index].close > upper:
            latest = index, MarketDirection.UP, upper
        elif candles[index].close < lower:
            latest = index, MarketDirection.DOWN, lower
    if latest is None:
        return None
    index, direction, level = latest
    atr = _atr(candles)
    prior_volume = mean(
        tuple(item.volume for item in candles[index - lookback : index])
    )
    volume_ratio = candles[index].volume / prior_volume if prior_volume > 0 else None
    return _BreakoutEvent(direction, interval, level, index, candles, atr, volume_ratio)


def _completed_series(
    series: MarketSeries,
    observed_at: datetime,
    interval_minutes: int,
) -> MarketSeries | None:
    now = _utc(observed_at)
    candles = tuple(
        candle
        for candle in series.candles
        if candle.timestamp + timedelta(minutes=interval_minutes) <= now
    )
    return replace(series, candles=candles) if candles else None


def _discovery_components(
    candidate: _LoadedCandidate,
    assessment: BreakoutAssessment | None,
) -> tuple[ScoreComponent, ...]:
    catalog = candidate.catalog
    relative_5m = _relative_volume(candidate.series_5m)
    relative_15m = _relative_volume(candidate.series_15m)
    acceleration = _ratio(relative_5m, relative_15m)
    atr_expansion = _atr_expansion(candidate.series_5m)
    range_position = _range_position(candidate.series_15m)
    compression = _compression(candidate.series_5m)
    proximity = (
        min(1.0, abs(range_position - 0.5) * 2.0) if range_position is not None else 0.0
    )
    freshness = (
        max(0.0, 1.0 - assessment.breakout_age_bars / 8.0)
        if assessment is not None
        else 0.0
    )
    liquidity = min(1.0, catalog.turnover_24h / 50_000_000.0)
    spread = max(0.0, 1.0 - catalog.spread_ratio / 0.005)
    return (
        _component(
            "volume_acceleration",
            acceleration,
            _scaled(acceleration, 1.0, 2.0),
            "volume_acceleration",
            "Ускорение относительного объёма.",
            DISCOVERY_WEIGHTS,
        ),
        _component(
            "atr_expansion",
            atr_expansion,
            _scaled(atr_expansion, 1.0, 2.0),
            "atr_expansion",
            "Расширение среднего истинного диапазона.",
            DISCOVERY_WEIGHTS,
        ),
        _component(
            "range_proximity",
            range_position,
            proximity,
            "range_proximity",
            "Близость к границе локального диапазона.",
            DISCOVERY_WEIGHTS,
        ),
        _component(
            "breakout_freshness",
            assessment.breakout_age_bars if assessment else None,
            freshness,
            "breakout_freshness",
            "Свежесть подтверждённого завершённой свечой пробоя.",
            DISCOVERY_WEIGHTS,
        ),
        _component(
            "compression",
            compression,
            (compression or 0.0) / 100.0,
            "compression",
            "Предшествующее сжатие диапазона.",
            DISCOVERY_WEIGHTS,
        ),
        _component(
            "liquidity",
            catalog.turnover_24h,
            liquidity,
            "liquidity",
            "Качество ликвидности по обороту.",
            DISCOVERY_WEIGHTS,
        ),
        _component(
            "spread_quality",
            catalog.spread_ratio * 100.0,
            spread,
            "spread_quality",
            "Качество текущего спреда.",
            DISCOVERY_WEIGHTS,
        ),
    )


def _readiness_components(
    candidate: _LoadedCandidate,
    assessment: BreakoutAssessment | None,
    sequence: SequenceSnapshot,
    config: EarlyDiscoveryV2Config,
) -> tuple[ScoreComponent, ...]:
    catalog = candidate.catalog
    age = assessment.breakout_age_bars if assessment else None
    freshness = max(0.0, 1.0 - (age or 99) / 6.0) if age is not None else 0.0
    distance = assessment.absolute_distance_atr if assessment else None
    distance_factor = 0.0 if distance is None else max(0.0, 1.0 - distance / 2.0)
    correct_side = bool(assessment and assessment.is_correct_side_of_level)
    hold_factor = (
        min(1.0, assessment.breakout_hold_candles / 2.0) if assessment else 0.0
    )
    retest_factor = (
        1.0
        if assessment and assessment.retest_state is RetestState.RETEST_HELD
        else 0.0
    )
    direction_stability = min(
        1.0,
        sequence.consecutive_active_scans / config.required_ready_scans,
    )
    ready_stability = min(
        1.0,
        sequence.consecutive_ready_scans / config.required_ready_scans,
    )
    spread_factor = max(
        0.0,
        1.0 - catalog.spread_ratio * 100.0 / READY_MAXIMUM_SPREAD_PCT,
    )
    liquidity_factor = min(1.0, catalog.turnover_24h / 50_000_000.0)
    volume_factor = _scaled(
        assessment.volume_ratio if assessment else None,
        1.0,
        2.0,
    )
    return (
        _component(
            "breakout_freshness",
            age,
            freshness,
            "breakout_freshness",
            "Свежесть пробоя.",
            READINESS_WEIGHTS,
        ),
        _component(
            "distance",
            distance,
            distance_factor,
            "distance",
            "Абсолютное расстояние от уровня.",
            READINESS_WEIGHTS,
        ),
        _component(
            "correct_side",
            correct_side,
            float(correct_side),
            "correct_side",
            "Цена находится на правильной стороне уровня.",
            READINESS_WEIGHTS,
        ),
        _component(
            "hold",
            assessment.breakout_hold_candles if assessment else None,
            hold_factor,
            "hold",
            "Удержание уровня завершёнными свечами.",
            READINESS_WEIGHTS,
        ),
        _component(
            "retest",
            assessment.retest_state.value if assessment else None,
            retest_factor,
            "retest",
            "Подтверждённый ретест уровня.",
            READINESS_WEIGHTS,
        ),
        _component(
            "direction_stability",
            sequence.consecutive_active_scans,
            direction_stability,
            "direction_stability",
            "Устойчивость направления между сканированиями.",
            READINESS_WEIGHTS,
        ),
        _component(
            "ready_stability",
            sequence.consecutive_ready_scans,
            ready_stability,
            "ready_stability",
            "Устойчивость готового состояния.",
            READINESS_WEIGHTS,
        ),
        _component(
            "spread_quality",
            catalog.spread_ratio * 100.0,
            spread_factor,
            "spread_quality",
            "Качество спреда.",
            READINESS_WEIGHTS,
        ),
        _component(
            "liquidity",
            catalog.turnover_24h,
            liquidity_factor,
            "liquidity",
            "Ликвидность по обороту.",
            READINESS_WEIGHTS,
        ),
        _component(
            "breakout_volume",
            assessment.volume_ratio if assessment else None,
            volume_factor,
            "breakout_volume",
            "Объём на свече пробоя.",
            READINESS_WEIGHTS,
        ),
    )


def _component(
    component_id: str,
    raw_value: ComponentRawValue,
    factor: float,
    weight_id: str,
    explanation_ru: str,
    weights: Mapping[str, float],
) -> ScoreComponent:
    maximum = weights[weight_id]
    points = max(0.0, min(maximum, factor * maximum))
    is_discovery = weights is DISCOVERY_WEIGHTS
    return ScoreComponent(
        "discovery" if is_discovery else "readiness",
        "Оценка раннего обнаружения" if is_discovery else "Готовность",
        component_id,
        raw_value,
        points,
        maximum,
        f"{component_id}:{points:.4f}/{maximum:.4f}",
        explanation_ru,
    )


def _preliminary_ready(
    catalog: CatalogInstrument,
    direction: MarketDirection,
    assessment: BreakoutAssessment | None,
    discovery_score: float,
    price_change_24h: float | None,
) -> bool:
    return bool(
        discovery_score >= V2_READY_SCORE
        and direction is not MarketDirection.NEUTRAL
        and assessment is not None
        and not assessment.breakout_failure
        and (
            assessment.is_correct_side_of_level
            or assessment.retest_state is RetestState.RETEST_HELD
        )
        and assessment.absolute_distance_atr is not None
        and assessment.absolute_distance_atr <= 2.0
        and catalog.spread_ratio * 100.0 <= READY_MAXIMUM_SPREAD_PCT
        and abs(price_change_24h or 0.0) < 15.0 - 1e-9
    )


def _v2_stage(
    *,
    direction: MarketDirection,
    discovery_score: float,
    readiness_score: float,
    sequence: SequenceSnapshot,
    assessment: BreakoutAssessment | None,
    spread_pct: float | None,
    price_change_24h: float | None,
    config: EarlyDiscoveryV2Config,
) -> DiscoveryStage:
    absolute_change = abs(price_change_24h or 0.0)
    if absolute_change >= 30.0 - 1e-9:
        return DiscoveryStage.DO_NOT_CHASE
    if absolute_change >= 15.0 - 1e-9:
        return DiscoveryStage.LATE
    confirmed = bool(
        direction is not MarketDirection.NEUTRAL
        and sequence.consecutive_active_scans >= config.required_ready_scans
        and sequence.consecutive_ready_scans >= config.required_ready_scans
        and assessment is not None
        and not assessment.breakout_failure
        and (
            assessment.is_correct_side_of_level
            or assessment.retest_state is RetestState.RETEST_HELD
        )
        and assessment.absolute_distance_atr is not None
        and assessment.absolute_distance_atr <= 2.0
        and spread_pct is not None
        and spread_pct <= READY_MAXIMUM_SPREAD_PCT
        and discovery_score >= V2_READY_SCORE
        and readiness_score >= V2_READY_SCORE
    )
    if confirmed:
        return DiscoveryStage.READY_CANDIDATE
    if sequence.consecutive_ready_scans >= config.forming_scans or (
        sequence.consecutive_active_scans >= config.forming_scans
        and discovery_score >= V2_FORMING_SCORE
    ):
        return DiscoveryStage.SETUP_FORMING
    if discovery_score >= V2_EARLY_SCORE:
        return DiscoveryStage.EARLY_ATTENTION
    return DiscoveryStage.QUIET


def _v2_confirmations(
    assessment: BreakoutAssessment | None,
    sequence: SequenceSnapshot,
) -> tuple[str, ...]:
    values: list[str] = []
    if assessment is not None and assessment.is_correct_side_of_level:
        values.append("Цена на правильной стороне уровня.")
    if assessment is not None and assessment.breakout_hold_candles >= 2:
        values.append("Две завершённые свечи удерживают уровень.")
    if assessment is not None and assessment.retest_state is RetestState.RETEST_HELD:
        values.append("Ретест уровня подтверждён.")
    if sequence.consecutive_ready_scans >= 2:
        values.append("Готовое состояние устойчиво между сканированиями.")
    return tuple(values)


def _v2_warnings(
    catalog: CatalogInstrument,
    assessment: BreakoutAssessment | None,
    price_change_24h: float | None,
) -> tuple[str, ...]:
    values: list[str] = []
    if assessment is None:
        values.append("Подтверждённый пробой не найден.")
    elif assessment.breakout_failure:
        values.append("Цена вернулась на неправильную сторону: пробой провален.")
    if assessment is not None and (
        assessment.absolute_distance_atr is None
        or assessment.absolute_distance_atr > 2.0
    ):
        values.append("Расстояние от уровня превышает два средних истинных диапазона.")
    if catalog.spread_ratio * 100.0 > READY_MAXIMUM_SPREAD_PCT:
        values.append("Спред превышает 0,2 процента.")
    if abs(price_change_24h or 0.0) >= 15.0 - 1e-9:
        values.append("Движение за 24 часа уже значительно реализовано.")
    return tuple(values)


def _stage_reason(
    stage_v1: DiscoveryStage,
    stage_v2: DiscoveryStage,
    assessment: BreakoutAssessment | None,
    sequence: SequenceSnapshot,
) -> str:
    if stage_v2 is DiscoveryStage.READY_CANDIDATE:
        return (
            "Стадия подтверждена устойчивостью, правильной стороной уровня "
            "и удержанием."
        )
    if assessment is not None and assessment.breakout_failure:
        return "Стадия понижена: пробой провален."
    if sequence.consecutive_ready_scans < 3:
        return (
            "Стадия ожидает устойчивости: последовательных готовых сканирований "
            f"{sequence.consecutive_ready_scans}."
        )
    if stage_v1 is not stage_v2:
        return "Стадия изменена проверками уровня и защитными ограничениями V2."
    return "Стадия сохранена после проверок V2."


def _state_to_json(state: V2SymbolState) -> dict[str, Any]:
    return {
        "symbol": state.symbol,
        "consecutive_active_scans": state.consecutive_active_scans,
        "consecutive_ready_scans": state.consecutive_ready_scans,
        "quiet_scans": state.quiet_scans,
        "first_detected_at": _iso(state.first_detected_at),
        "first_ready_at": _iso(state.first_ready_at),
        "second_confirmation_at": _iso(state.second_confirmation_at),
        "third_confirmation_at": _iso(state.third_confirmation_at),
        "last_stage": state.last_stage.value,
        "last_direction": state.last_direction.value,
        "last_seen_at": _iso(state.last_seen_at),
        "reset_reason": state.reset_reason,
    }


def _state_from_json(symbol: str, raw: Mapping[str, Any]) -> V2SymbolState:
    return V2SymbolState(
        symbol=symbol.upper(),
        consecutive_active_scans=_safe_int(raw.get("consecutive_active_scans")),
        consecutive_ready_scans=_safe_int(raw.get("consecutive_ready_scans")),
        quiet_scans=_safe_int(raw.get("quiet_scans")),
        first_detected_at=_optional_datetime(raw.get("first_detected_at")),
        first_ready_at=_optional_datetime(raw.get("first_ready_at")),
        second_confirmation_at=_optional_datetime(raw.get("second_confirmation_at")),
        third_confirmation_at=_optional_datetime(raw.get("third_confirmation_at")),
        last_stage=_safe_stage(raw.get("last_stage")),
        last_direction=_safe_direction(raw.get("last_direction")),
        last_seen_at=_optional_datetime(raw.get("last_seen_at")),
        reset_reason=(
            str(raw["reset_reason"]) if raw.get("reset_reason") is not None else None
        ),
    )


def _result_to_json(result: EarlyDiscoveryV2Result) -> dict[str, Any]:
    payload = asdict(result)
    payload["scanned_at"] = result.scanned_at.isoformat()
    payload["market_direction"] = result.market_direction.value
    payload["direction_v1"] = (
        result.direction_v1.value if result.direction_v1 is not None else None
    )
    payload["direction_v2"] = (
        result.direction_v2.value if result.direction_v2 is not None else None
    )
    payload["stage_v1"] = result.stage_v1.value if result.stage_v1 else None
    payload["stage_v2"] = result.stage_v2.value
    payload["first_detected_at"] = _iso(result.first_detected_at)
    payload["first_ready_at"] = _iso(result.first_ready_at)
    payload["second_confirmation_at"] = _iso(result.second_confirmation_at)
    payload["third_confirmation_at"] = _iso(result.third_confirmation_at)
    payload["retest_state"] = (
        result.retest_state.value if result.retest_state is not None else None
    )
    payload["component_scores"] = [asdict(item) for item in result.component_scores]
    payload["confirmations"] = list(result.confirmations)
    payload["warnings"] = list(result.warnings)
    return payload


def _reset_sequence(state: V2SymbolState, reason: str) -> None:
    state.consecutive_active_scans = 0
    state.consecutive_ready_scans = 0
    state.quiet_scans = 0
    state.first_detected_at = None
    state.first_ready_at = None
    state.second_confirmation_at = None
    state.third_confirmation_at = None
    state.last_stage = DiscoveryStage.QUIET
    state.last_direction = MarketDirection.NEUTRAL
    state.reset_reason = reason


def _is_correct_side(price: float, level: float, direction: MarketDirection) -> bool:
    if direction is MarketDirection.UP:
        return price >= level
    if direction is MarketDirection.DOWN:
        return price <= level
    return False


def _touches_level(
    candle: Candle,
    level: float,
    direction: MarketDirection,
    tolerance: float,
) -> bool:
    if direction is MarketDirection.UP:
        return candle.low <= level + tolerance
    return candle.high >= level - tolerance


def _consecutive_correct_closes(
    candles: Sequence[Candle],
    level: float,
    direction: MarketDirection,
) -> int:
    count = 0
    for candle in reversed(candles):
        if not _is_correct_side(candle.close, level, direction):
            break
        count += 1
    return count


def _atr(candles: Sequence[Candle]) -> float | None:
    if len(candles) < 2:
        return None
    ranges = true_ranges(
        tuple(item.high for item in candles),
        tuple(item.low for item in candles),
        tuple(item.close for item in candles),
    )
    return mean(ranges[-min(14, len(ranges)) :])


def _relative_volume(series: MarketSeries) -> float | None:
    if len(series.candles) < 21:
        return None
    baseline = mean(tuple(item.volume for item in series.candles[-21:-1]))
    return series.candles[-1].volume / baseline if baseline > 0 else None


def _atr_expansion(series: MarketSeries) -> float | None:
    if len(series.candles) < 30:
        return None
    ranges = true_ranges(
        tuple(item.high for item in series.candles),
        tuple(item.low for item in series.candles),
        tuple(item.close for item in series.candles),
    )
    current = mean(ranges[-7:])
    baseline = mean(ranges[-28:-7])
    return current / baseline if baseline > 0 else None


def _range_position(series: MarketSeries) -> float | None:
    if len(series.candles) < 21:
        return None
    history = series.candles[-21:-1]
    lower = min(item.low for item in history)
    upper = max(item.high for item in history)
    if upper <= lower:
        return None
    return max(0.0, min(1.0, (series.candles[-1].close - lower) / (upper - lower)))


def _compression(series: MarketSeries) -> float | None:
    if len(series.candles) < 30:
        return None
    ranges = tuple(item.high - item.low for item in series.candles)
    recent = mean(ranges[-5:])
    baseline = mean(ranges[-25:-5])
    if baseline <= 0:
        return None
    return max(0.0, min(100.0, (1.0 - recent / baseline) * 200.0))


def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return numerator / denominator


def _scaled(value: float | None, lower: float, upper: float) -> float:
    if value is None or upper <= lower:
        return 0.0
    return max(0.0, min(1.0, (value - lower) / (upper - lower)))


def _safe_int(value: Any) -> int:
    return (
        value
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0
        else 0
    )


def _safe_stage(value: Any) -> DiscoveryStage:
    try:
        return DiscoveryStage(str(value))
    except ValueError:
        return DiscoveryStage.QUIET


def _safe_direction(value: Any) -> MarketDirection:
    try:
        return MarketDirection(str(value))
    except ValueError:
        return MarketDirection.NEUTRAL


def _optional_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    parsed = datetime.fromisoformat(value)
    return _utc(parsed)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Время раннего обнаружения V2 должно содержать часовой пояс.")
    return value.astimezone(UTC)


def build_parser() -> argparse.ArgumentParser:
    parser = RussianArgumentParser(
        description="Теневой модуль раннего обнаружения V2 без Telegram-уведомлений."
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Выполнить одно сканирование и завершить работу.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    from market_signal_assistant.composition import build_early_discovery_v2_service
    from market_signal_assistant.settings import EarlyDiscoveryV2Settings

    settings = EarlyDiscoveryV2Settings.from_environment()
    if not settings.enabled:
        print(
            "Модуль раннего обнаружения V2 выключен. "
            "Установите INPLAY_EARLY_DISCOVERY_V2_ENABLED=true."
        )
        return 0
    service = build_early_discovery_v2_service(settings)
    if args.once:
        print_scan_report(service.scan())
        return 0
    schedule = EarlyDiscoveryV2FixedSchedule(
        service,
        interval_seconds=settings.interval_minutes * 60,
    )
    try:
        schedule.run()
    except KeyboardInterrupt:
        print("Модуль раннего обнаружения V2 остановлен.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
