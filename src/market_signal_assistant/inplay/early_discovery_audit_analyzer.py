from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

EPISODE_GAP = timedelta(minutes=30)
QUIET_SCANS_TO_RESET = 2
HORIZONS_MINUTES = (15, 30, 60, 180)
HORIZON_TOLERANCE = timedelta(minutes=10)

STAGE_RU = {
    "QUIET": "БЕЗ СИГНАЛА",
    "EARLY_ATTENTION": "РАННЕЕ ВНИМАНИЕ",
    "SETUP_FORMING": "ФОРМИРУЕТСЯ",
    "READY_CANDIDATE": "ГОТОВ К НАБЛЮДЕНИЮ",
    "LATE": "ПОЗДНО",
    "DO_NOT_CHASE": "НЕ ДОГОНЯТЬ",
}
DIRECTION_RU = {"UP": "ВВЕРХ", "DOWN": "ВНИЗ", "NEUTRAL": "НЕЙТРАЛЬНО"}
STAGE_ORDER = {
    "QUIET": 0,
    "EARLY_ATTENTION": 1,
    "SETUP_FORMING": 2,
    "READY_CANDIDATE": 3,
    "LATE": 4,
    "DO_NOT_CHASE": 5,
}
ACTIVE_STAGES = frozenset(STAGE_ORDER).difference({"QUIET"})
REQUIRED_FIELDS = (
    "symbol",
    "scanned_at",
    "market_direction",
    "discovery_stage",
    "discovery_score",
    "entry_readiness_score",
    "last_price",
    "spread_pct",
    "price_change_24h_pct",
    "distance_from_breakout_atr",
    "is_in_current_top20",
    "rank_in_current_inplay_universe",
)
EXPECTED_COMPONENT_FIELDS = (
    "volume_acceleration_points",
    "atr_expansion_points",
    "range_proximity_points",
    "breakout_freshness_points",
    "compression_points",
    "liquidity_points",
    "spread_quality_points",
    "breakout_hold_points",
    "retest_points",
    "breakout_volume_points",
)
COMPONENT_FIELD_RU = {
    "volume_acceleration_points": "ускорение объёма",
    "atr_expansion_points": "расширение волатильности",
    "range_proximity_points": "близость к границе диапазона",
    "breakout_freshness_points": "свежесть пробоя",
    "compression_points": "сжатие диапазона",
    "liquidity_points": "ликвидность",
    "spread_quality_points": "качество спреда",
    "breakout_hold_points": "удержание пробоя",
    "retest_points": "повторная проверка уровня",
    "breakout_volume_points": "объём пробоя",
}


@dataclass(frozen=True, slots=True)
class AuditRecord:
    symbol: str
    scanned_at: datetime
    market_direction: str
    discovery_stage: str
    discovery_score: float | None
    entry_readiness_score: float | None
    last_price: float | None
    spread_pct: float | None
    price_change_24h_pct: float | None
    distance_from_breakout_atr: float | None
    is_in_current_top20: bool | None
    rank_in_current_inplay_universe: int | None
    confirmations: tuple[str, ...]
    warnings: tuple[str, ...]
    values: Mapping[str, Any]


@dataclass(slots=True)
class ReadResult:
    by_symbol: dict[str, list[AuditRecord]] = field(default_factory=dict)
    scan_symbols: dict[datetime, set[str]] = field(default_factory=dict)
    valid_lines: int = 0
    damaged_lines: int = 0
    empty_lines: int = 0
    missing_fields: Counter[str] = field(default_factory=Counter)
    observed_fields: set[str] = field(default_factory=set)


@dataclass(slots=True)
class Episode:
    episode_id: int
    symbol: str
    direction: str
    records: list[AuditRecord]
    first_stage: str
    maximum_stage: str
    started_at: datetime
    ended_at: datetime
    first_early_at: datetime | None
    first_forming_at: datetime | None
    first_ready_at: datetime | None
    ready_count: int
    maximum_ready_streak: int
    maximum_discovery_score: float | None
    maximum_entry_readiness: float | None
    ever_in_top20: bool
    best_production_rank: int | None
    episode_price_change_pct: float | None
    first_ready_record: AuditRecord | None
    horizon_results: dict[int, HorizonOutcome | None] = field(default_factory=dict)
    first_top20_at: datetime | None = None
    early_lead_minutes: float | None = None
    in_top20_at_early: bool | None = None
    in_top20_at_ready: bool | None = None
    appeared_in_top20_later: bool = False


@dataclass(frozen=True, slots=True)
class HorizonOutcome:
    minutes: int
    observed_at: datetime
    directed_return_pct: float
    mfe_pct: float
    mae_pct: float
    adverse_half_before_favorable_half: bool


@dataclass(frozen=True, slots=True)
class HorizonMetrics:
    available: int
    correct_share: float | None
    median_return_pct: float | None
    mean_return_pct: float | None
    percentile_25: float | None
    percentile_75: float | None
    maximum_favorable_pct: float | None
    maximum_adverse_pct: float | None
    share_at_least_025: float | None
    share_at_least_05: float | None
    share_at_least_1: float | None
    adverse_half_first_share: float | None


@dataclass(frozen=True, slots=True)
class AnalysisResult:
    read: ReadResult
    episodes: tuple[Episode, ...]
    horizon_metrics: Mapping[int, HorizonMetrics]
    direction_metrics: Mapping[str, Mapping[int, HorizonMetrics]]
    scan_statistics: Mapping[str, Any]
    distance_groups: Mapping[str, Mapping[str, Any]]
    stability_groups: Mapping[str, Mapping[str, Any]]
    slice_statistics: Mapping[str, Any]
    component_analysis: Mapping[str, Any]
    safety_violations: Mapping[str, int]
    production_rank_analysis: Mapping[str, int]


@dataclass(frozen=True, slots=True)
class OutputFiles:
    report: Path
    episodes: Path
    metrics: Path
    best: Path
    worst: Path
    recommendations: Path


def read_audit(path: Path) -> ReadResult:
    result = ReadResult()
    grouped: defaultdict[str, list[AuditRecord]] = defaultdict(list)
    scans: defaultdict[datetime, set[str]] = defaultdict(set)
    with path.open("r", encoding="utf-8") as stream:
        for line in stream:
            if not line.strip():
                result.empty_lines += 1
                continue
            try:
                raw = json.loads(line)
                record = _record_from_json(raw, result)
            except (json.JSONDecodeError, TypeError, ValueError, KeyError):
                result.damaged_lines += 1
                continue
            result.valid_lines += 1
            grouped[record.symbol].append(record)
            scans[record.scanned_at].add(record.symbol)
    for records in grouped.values():
        records.sort(key=lambda item: item.scanned_at)
    result.by_symbol = dict(grouped)
    result.scan_symbols = dict(scans)
    return result


def analyze(path: Path) -> AnalysisResult:
    read = read_audit(path)
    episodes = list(build_episodes(read.by_symbol))
    _attach_future_outcomes(episodes, read.by_symbol)
    _attach_top20_comparison(episodes, read.by_symbol)
    scan_statistics = _scan_statistics(read.scan_symbols)
    horizon_metrics = _metrics_by_horizon(episodes)
    direction_metrics = {
        direction: _metrics_by_horizon(
            tuple(item for item in episodes if item.direction == direction)
        )
        for direction in ("UP", "DOWN")
    }
    distance_groups = _distance_analysis(episodes, read.by_symbol)
    stability_groups = _stability_analysis(episodes)
    slices = _slice_analysis(episodes)
    components = _component_analysis(episodes)
    safety = _safety_violations(episodes)
    rank_analysis = _production_rank_analysis(read)
    return AnalysisResult(
        read=read,
        episodes=tuple(episodes),
        horizon_metrics=horizon_metrics,
        direction_metrics=direction_metrics,
        scan_statistics=scan_statistics,
        distance_groups=distance_groups,
        stability_groups=stability_groups,
        slice_statistics=slices,
        component_analysis=components,
        safety_violations=safety,
        production_rank_analysis=rank_analysis,
    )


def build_episodes(
    by_symbol: Mapping[str, Sequence[AuditRecord]],
) -> tuple[Episode, ...]:
    episodes: list[Episode] = []
    next_id = 1
    for symbol, records in sorted(by_symbol.items()):
        active: list[AuditRecord] = []
        quiet_count = 0
        episode_direction = "NEUTRAL"
        previous: AuditRecord | None = None
        for record in records:
            is_active = record.discovery_stage in ACTIVE_STAGES
            gap_break = (
                previous is not None
                and record.scanned_at - previous.scanned_at >= EPISODE_GAP
            )
            direction_break = (
                active
                and episode_direction in {"UP", "DOWN"}
                and record.market_direction in {"UP", "DOWN"}
                and record.market_direction != episode_direction
            )
            if active and (gap_break or direction_break):
                episodes.append(_episode(next_id, symbol, episode_direction, active))
                next_id += 1
                active = []
                quiet_count = 0
                episode_direction = "NEUTRAL"
            if not is_active:
                if active:
                    quiet_count += 1
                    if quiet_count >= QUIET_SCANS_TO_RESET:
                        episodes.append(
                            _episode(next_id, symbol, episode_direction, active)
                        )
                        next_id += 1
                        active = []
                        episode_direction = "NEUTRAL"
                previous = record
                continue
            quiet_count = 0
            if not active or (
                episode_direction == "NEUTRAL"
                and record.market_direction in {"UP", "DOWN"}
            ):
                episode_direction = record.market_direction
            active.append(record)
            previous = record
        if active:
            episodes.append(_episode(next_id, symbol, episode_direction, active))
            next_id += 1
    return tuple(episodes)


def _episode(
    episode_id: int,
    symbol: str,
    direction: str,
    records: list[AuditRecord],
) -> Episode:
    ready_records = [
        item for item in records if item.discovery_stage == "READY_CANDIDATE"
    ]
    ranks = [
        item.rank_in_current_inplay_universe
        for item in records
        if item.rank_in_current_inplay_universe is not None
    ]
    prices = [item.last_price for item in (records[0], records[-1])]
    price_change = (
        (prices[1] / prices[0] - 1.0) * 100.0
        if prices[0] is not None and prices[1] is not None and prices[0] > 0
        else None
    )
    first_top20 = next(
        (item.scanned_at for item in records if item.is_in_current_top20 is True),
        None,
    )
    first_active = records[0].scanned_at
    return Episode(
        episode_id=episode_id,
        symbol=symbol,
        direction=direction,
        records=list(records),
        first_stage=records[0].discovery_stage,
        maximum_stage=max(
            records, key=lambda item: STAGE_ORDER[item.discovery_stage]
        ).discovery_stage,
        started_at=records[0].scanned_at,
        ended_at=records[-1].scanned_at,
        first_early_at=_first_stage(records, "EARLY_ATTENTION"),
        first_forming_at=_first_stage(records, "SETUP_FORMING"),
        first_ready_at=_first_stage(records, "READY_CANDIDATE"),
        ready_count=len(ready_records),
        maximum_ready_streak=_maximum_ready_streak(records),
        maximum_discovery_score=_maximum(item.discovery_score for item in records),
        maximum_entry_readiness=_maximum(
            item.entry_readiness_score for item in records
        ),
        ever_in_top20=any(item.is_in_current_top20 is True for item in records),
        best_production_rank=min(ranks) if ranks else None,
        episode_price_change_pct=price_change,
        first_ready_record=ready_records[0] if ready_records else None,
        first_top20_at=first_top20,
        early_lead_minutes=(
            (first_top20 - first_active).total_seconds() / 60.0
            if first_top20 is not None
            else None
        ),
    )


def _attach_future_outcomes(
    episodes: Iterable[Episode],
    by_symbol: Mapping[str, Sequence[AuditRecord]],
) -> None:
    for episode in episodes:
        entry = episode.first_ready_record
        if entry is None or episode.direction not in {"UP", "DOWN"}:
            episode.horizon_results = {minutes: None for minutes in HORIZONS_MINUTES}
            continue
        records = by_symbol[episode.symbol]
        outcomes: dict[int, HorizonOutcome | None] = {}
        for minutes in HORIZONS_MINUTES:
            target = entry.scanned_at + timedelta(minutes=minutes)
            future = next(
                (
                    item
                    for item in records
                    if target <= item.scanned_at <= target + HORIZON_TOLERANCE
                    and item.last_price is not None
                ),
                None,
            )
            if future is None or entry.last_price is None or entry.last_price <= 0:
                outcomes[minutes] = None
                continue
            window = [
                item
                for item in records
                if entry.scanned_at < item.scanned_at <= future.scanned_at
                and item.last_price is not None
            ]
            returns = [
                _directed_return(entry.last_price, item.last_price, episode.direction)
                for item in window
                if item.last_price is not None
            ]
            directed = _directed_return(
                entry.last_price,
                _required_price(future.last_price),
                episode.direction,
            )
            first_favorable = next(
                (i for i, value in enumerate(returns) if value >= 0.5), None
            )
            first_adverse = next(
                (i for i, value in enumerate(returns) if value <= -0.5), None
            )
            outcomes[minutes] = HorizonOutcome(
                minutes=minutes,
                observed_at=future.scanned_at,
                directed_return_pct=directed,
                mfe_pct=max(returns, default=directed),
                mae_pct=min(returns, default=directed),
                adverse_half_before_favorable_half=(
                    first_adverse is not None
                    and (first_favorable is None or first_adverse < first_favorable)
                ),
            )
        episode.horizon_results = outcomes


def _attach_top20_comparison(
    episodes: Iterable[Episode],
    by_symbol: Mapping[str, Sequence[AuditRecord]],
) -> None:
    for episode in episodes:
        early_at = episode.first_early_at or episode.started_at
        early_record = next(
            (item for item in episode.records if item.scanned_at == early_at),
            episode.records[0],
        )
        episode.in_top20_at_early = early_record.is_in_current_top20
        if episode.first_ready_record is not None:
            episode.in_top20_at_ready = episode.first_ready_record.is_in_current_top20
        first_top20 = next(
            (
                item.scanned_at
                for item in by_symbol[episode.symbol]
                if item.scanned_at >= early_at and item.is_in_current_top20 is True
            ),
            None,
        )
        episode.first_top20_at = first_top20
        episode.appeared_in_top20_later = (
            first_top20 is not None and first_top20 > early_at
        )
        episode.early_lead_minutes = (
            (first_top20 - early_at).total_seconds() / 60.0
            if first_top20 is not None
            else None
        )


def horizon_metrics(
    outcomes: Iterable[HorizonOutcome | None],
) -> HorizonMetrics:
    available = [item for item in outcomes if item is not None]
    values = [item.directed_return_pct for item in available]
    if not values:
        return HorizonMetrics(
            0, None, None, None, None, None, None, None, None, None, None, None
        )
    return HorizonMetrics(
        available=len(values),
        correct_share=_share(value > 0 for value in values),
        median_return_pct=statistics.median(values),
        mean_return_pct=statistics.fmean(values),
        percentile_25=_percentile(values, 0.25),
        percentile_75=_percentile(values, 0.75),
        maximum_favorable_pct=max(item.mfe_pct for item in available),
        maximum_adverse_pct=min(item.mae_pct for item in available),
        share_at_least_025=_share(value >= 0.25 for value in values),
        share_at_least_05=_share(value >= 0.5 for value in values),
        share_at_least_1=_share(value >= 1.0 for value in values),
        adverse_half_first_share=_share(
            item.adverse_half_before_favorable_half for item in available
        ),
    )


def write_outputs(result: AnalysisResult, output_dir: Path) -> OutputFiles:
    output_dir.mkdir(parents=True, exist_ok=True)
    files = OutputFiles(
        report=output_dir / "итоговый_отчёт.md",
        episodes=output_dir / "эпизоды.csv",
        metrics=output_dir / "метрики.json",
        best=output_dir / "лучшие_эпизоды.csv",
        worst=output_dir / "худшие_эпизоды.csv",
        recommendations=output_dir / "рекомендации_для_калибровки.md",
    )
    files.report.write_text(_markdown_report(result), encoding="utf-8")
    _write_episode_csv(files.episodes, result.episodes)
    files.metrics.write_text(
        json.dumps(_metrics_json(result), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    ranked = sorted(
        (item for item in result.episodes if _objective_result(item) is not None),
        key=lambda item: _objective_result(item) or -math.inf,
        reverse=True,
    )
    _write_episode_csv(files.best, tuple(ranked[:20]))
    _write_episode_csv(files.worst, tuple(reversed(ranked[-20:])))
    files.recommendations.write_text(
        _calibration_recommendations(result),
        encoding="utf-8",
    )
    return files


def run(input_path: Path, output_dir: Path) -> tuple[AnalysisResult, OutputFiles]:
    if not input_path.is_file():
        raise FileNotFoundError(f"Файл аудита не найден: {input_path}")
    result = analyze(input_path)
    return result, write_outputs(result, output_dir)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Офлайн-анализ уже собранного аудита Early Discovery."
    )
    parser.add_argument("--input", type=Path, required=True, help="Входной JSONL-файл.")
    parser.add_argument("--output", type=Path, required=True, help="Каталог отчётов.")
    args = parser.parse_args(argv)
    try:
        result, files = run(args.input, args.output)
    except FileNotFoundError as error:
        parser.exit(2, f"Ошибка: {error}\n")
    print("Офлайн-анализ Early Discovery завершён.")
    print(f"Корректных строк: {result.read.valid_lines}")
    print(f"Повреждённых строк: {result.read.damaged_lines}")
    print(f"Сканирований: {len(result.read.scan_symbols)}")
    print(f"Инструментов: {len(result.read.by_symbol)}")
    print(f"Эпизодов: {len(result.episodes)}")
    for path in asdict(files).values():
        print(f"Создан файл: {path}")
    return 0


def _record_from_json(raw: Any, read: ReadResult) -> AuditRecord:
    if not isinstance(raw, dict):
        raise TypeError("JSONL row must be an object")
    read.observed_fields.update(str(key) for key in raw)
    for field_name in REQUIRED_FIELDS:
        if field_name not in raw:
            read.missing_fields[field_name] += 1
    symbol = _required_string(raw, "symbol").strip().upper()
    scanned_at = _datetime(_required_string(raw, "scanned_at"))
    direction = _required_string(raw, "market_direction")
    stage = _required_string(raw, "discovery_stage")
    if direction not in DIRECTION_RU or stage not in STAGE_RU:
        raise ValueError("Unsupported direction or stage")
    return AuditRecord(
        symbol=symbol,
        scanned_at=scanned_at,
        market_direction=direction,
        discovery_stage=stage,
        discovery_score=_optional_float(raw.get("discovery_score")),
        entry_readiness_score=_optional_float(raw.get("entry_readiness_score")),
        last_price=_optional_float(raw.get("last_price")),
        spread_pct=_optional_float(raw.get("spread_pct")),
        price_change_24h_pct=_optional_float(raw.get("price_change_24h_pct")),
        distance_from_breakout_atr=_optional_float(
            raw.get("distance_from_breakout_atr")
        ),
        is_in_current_top20=_optional_bool(raw.get("is_in_current_top20")),
        rank_in_current_inplay_universe=_optional_int(
            raw.get("rank_in_current_inplay_universe")
        ),
        confirmations=_string_tuple(raw.get("confirmations")),
        warnings=_string_tuple(raw.get("warnings")),
        values=raw,
    )


def _scan_statistics(scans: Mapping[datetime, set[str]]) -> dict[str, Any]:
    ordered = sorted(scans)
    sizes = [len(scans[item]) for item in ordered]
    intervals = [
        (current - previous).total_seconds() / 60.0
        for previous, current in zip(ordered, ordered[1:], strict=False)
    ]
    interval_buckets = {
        "около_5_минут": sum(3.0 <= item < 7.5 for item in intervals),
        "около_10_минут": sum(7.5 <= item < 12.5 for item in intervals),
        "около_15_минут": sum(12.5 <= item < 17.5 for item in intervals),
        "17_5_минут_и_более": sum(item >= 17.5 for item in intervals),
    }
    missing_cycles = sum(max(0, round(item / 5.0) - 1) for item in intervals)
    appearances: defaultdict[str, list[int]] = defaultdict(list)
    for index, scanned_at in enumerate(ordered):
        for symbol in scans[scanned_at]:
            appearances[symbol].append(index)
    disappearance_events = sum(
        sum(
            current - previous > 1
            for previous, current in zip(indexes, indexes[1:], strict=False)
        )
        for indexes in appearances.values()
    )
    reappeared_symbols = sum(
        any(
            current - previous > 1
            for previous, current in zip(indexes, indexes[1:], strict=False)
        )
        for indexes in appearances.values()
    )
    return {
        "started_at": ordered[0].isoformat() if ordered else None,
        "ended_at": ordered[-1].isoformat() if ordered else None,
        "duration_hours": (
            (ordered[-1] - ordered[0]).total_seconds() / 3600.0
            if len(ordered) >= 2
            else 0.0
        ),
        "scan_count": len(ordered),
        "complete_scan_count": len(ordered),
        "minimum_symbols": min(sizes) if sizes else 0,
        "maximum_symbols": max(sizes) if sizes else 0,
        "median_symbols": statistics.median(sizes) if sizes else 0,
        "median_interval_minutes": statistics.median(intervals) if intervals else None,
        "interval_buckets": interval_buckets,
        "estimated_missed_cycles": missing_cycles,
        "disappearance_events": disappearance_events,
        "symbols_reappeared": reappeared_symbols,
    }


def _metrics_by_horizon(
    episodes: Iterable[Episode],
) -> dict[int, HorizonMetrics]:
    values = tuple(episodes)
    return {
        minutes: horizon_metrics(item.horizon_results.get(minutes) for item in values)
        for minutes in HORIZONS_MINUTES
    }


def _distance_analysis(
    episodes: Sequence[Episode],
    by_symbol: Mapping[str, Sequence[AuditRecord]],
) -> dict[str, Mapping[str, Any]]:
    groups: dict[str, list[Episode]] = defaultdict(list)
    for episode in episodes:
        ready = episode.first_ready_record
        if ready is None or ready.distance_from_breakout_atr is None:
            continue
        sign = (
            "положительное"
            if ready.distance_from_breakout_atr >= 0
            else "отрицательное"
        )
        groups[f"{DIRECTION_RU[episode.direction]} — {sign} расстояние"].append(episode)
    result: dict[str, Mapping[str, Any]] = {}
    for name, items in groups.items():
        result[name] = {
            "episodes": len(items),
            "horizons": {
                str(minutes): asdict(metrics)
                for minutes, metrics in _metrics_by_horizon(items).items()
            },
            "returned_to_quiet_share": _share(
                _episode_followed_by_stage(item, by_symbol, "QUIET") for item in items
            ),
            "direction_changed_share": _share(
                _episode_direction_changed(item, by_symbol) for item in items
            ),
            "late_or_do_not_chase_share": _share(
                any(
                    record.discovery_stage in {"LATE", "DO_NOT_CHASE"}
                    for record in item.records
                )
                for item in items
            ),
        }
    return result


def _stability_analysis(episodes: Sequence[Episode]) -> dict[str, Mapping[str, Any]]:
    groups = {
        "одно_готовое_состояние": [
            item for item in episodes if item.maximum_ready_streak == 1
        ],
        "минимум_два_подряд": [
            item for item in episodes if item.maximum_ready_streak >= 2
        ],
        "минимум_три_подряд": [
            item for item in episodes if item.maximum_ready_streak >= 3
        ],
    }
    return {
        name: {
            "episodes": len(items),
            "hypothetical_notifications": len(items),
            "horizons": {
                str(minutes): asdict(metrics)
                for minutes, metrics in _metrics_by_horizon(items).items()
            },
        }
        for name, items in groups.items()
    }


def _slice_analysis(episodes: Sequence[Episode]) -> dict[str, Any]:
    ready = [item for item in episodes if item.first_ready_record is not None]
    definitions: dict[str, list[tuple[str, Any]]] = {
        "направление": [
            ("ВВЕРХ", lambda r: r.market_direction == "UP"),
            ("ВНИЗ", lambda r: r.market_direction == "DOWN"),
        ],
        "основной_список": [
            ("внутри top-20", lambda r: r.is_in_current_top20 is True),
            ("вне top-20", lambda r: r.is_in_current_top20 is False),
            ("rank недоступен", lambda r: r.rank_in_current_inplay_universe is None),
        ],
        "оценка_раннего_обнаружения": _numeric_bands(
            "discovery_score", ((50, 60), (60, 70), (70, 80), (80, math.inf))
        ),
        "готовность_к_входу": _numeric_bands(
            "entry_readiness_score",
            ((-math.inf, 60), (60, 70), (70, 80), (80, 90), (90, math.inf)),
        ),
        "расстояние_ATR": _absolute_bands(
            "distance_from_breakout_atr", ((0, 0.5), (0.5, 1), (1, 2))
        ),
        "спред": _numeric_bands(
            "spread_pct", ((0, 0.02), (0.02, 0.05), (0.05, 0.1), (0.1, 0.2))
        ),
        "изменение_24ч": _absolute_bands(
            "price_change_24h_pct", ((0, 3), (3, 7), (7, 15), (15, 30), (30, math.inf))
        ),
    }
    output: dict[str, Any] = {}
    for category, bands in definitions.items():
        output[category] = {}
        for label, predicate in bands:
            selected = [item for item in ready if predicate(item.first_ready_record)]
            output[category][label] = {
                "episodes": len(selected),
                "horizons": {
                    str(minutes): asdict(metrics)
                    for minutes, metrics in _metrics_by_horizon(selected).items()
                },
            }
    return output


def _component_analysis(episodes: Sequence[Episode]) -> dict[str, Any]:
    fields = {
        "volume_acceleration": "ускорение объёма",
        "atr_expansion_ratio": "расширение волатильности",
        "range_position": "положение в диапазоне",
        "breakout_age_5m_bars": "возраст пробоя в свечах 5 минут",
        "compression_score": "сжатие диапазона",
        "turnover_24h": "оборот за 24 часа",
        "spread_pct": "спред",
    }
    successful: list[AuditRecord] = []
    unsuccessful: list[AuditRecord] = []
    for episode in episodes:
        ready = episode.first_ready_record
        outcome = episode.horizon_results.get(60)
        if ready is None or outcome is None:
            continue
        target = successful if outcome.directed_return_pct > 0 else unsuccessful
        target.append(ready)

    factors: dict[str, Any] = {}
    for field_name, label in fields.items():
        good = [
            value
            for item in successful
            if (value := _raw_optional_float(item, field_name)) is not None
        ]
        bad = [
            value
            for item in unsuccessful
            if (value := _raw_optional_float(item, field_name)) is not None
        ]
        factors[field_name] = {
            "название": label,
            "успешные": _descriptive_values(good),
            "неуспешные": _descriptive_values(bad),
        }

    successful_confirmations = Counter(
        confirmation for item in successful for confirmation in set(item.confirmations)
    )
    unsuccessful_confirmations = Counter(
        confirmation
        for item in unsuccessful
        for confirmation in set(item.confirmations)
    )
    confirmations: dict[str, Any] = {}
    for confirmation in sorted(
        successful_confirmations.keys() | unsuccessful_confirmations.keys()
    ):
        confirmations[confirmation] = {
            "успешные_эпизоды": successful_confirmations[confirmation],
            "неуспешные_эпизоды": unsuccessful_confirmations[confirmation],
            "доля_среди_успешных": (
                successful_confirmations[confirmation] / len(successful)
                if successful
                else None
            ),
            "доля_среди_неуспешных": (
                unsuccessful_confirmations[confirmation] / len(unsuccessful)
                if unsuccessful
                else None
            ),
        }
    return {
        "критерий_успеха": "направленный результат через 60 минут выше нуля",
        "успешных_эпизодов": len(successful),
        "неуспешных_эпизодов": len(unsuccessful),
        "факторы": factors,
        "подтверждения": confirmations,
        "ограничение": (
            "В исходном аудите нет отдельных балльных вкладов компонентов; "
            "сравниваются только сохранённые исходные значения факторов."
        ),
    }


def _safety_violations(episodes: Sequence[Episode]) -> dict[str, int]:
    ready_records = [
        record
        for episode in episodes
        for record in episode.records
        if record.discovery_stage == "READY_CANDIDATE"
    ]
    return {
        "готовых_снимков": len(ready_records),
        "изменение_24ч_не_менее_15": sum(
            item.price_change_24h_pct is not None
            and abs(item.price_change_24h_pct) >= 15
            for item in ready_records
        ),
        "изменение_24ч_не_менее_30": sum(
            item.price_change_24h_pct is not None
            and abs(item.price_change_24h_pct) >= 30
            for item in ready_records
        ),
        "расстояние_более_2_ATR": sum(
            item.distance_from_breakout_atr is not None
            and abs(item.distance_from_breakout_atr) > 2
            for item in ready_records
        ),
        "спред_более_0_2": sum(
            item.spread_pct is not None and item.spread_pct > 0.2
            for item in ready_records
        ),
    }


def _production_rank_analysis(read: ReadResult) -> dict[str, int]:
    records = [item for values in read.by_symbol.values() for item in values]
    missing_rank = [
        item for item in records if item.rank_in_current_inplay_universe is None
    ]
    return {
        "всего_снимков": len(records),
        "место_недоступно": len(missing_rank),
        "место_недоступно_и_нет_оценки_IN_PLAY": sum(
            _raw_optional_float(item, "current_inplay_score") is None
            for item in missing_rank
        ),
        "место_недоступно_при_наличии_оценки_IN_PLAY": sum(
            _raw_optional_float(item, "current_inplay_score") is not None
            for item in missing_rank
        ),
    }


def _raw_optional_float(record: AuditRecord, field_name: str) -> float | None:
    value = record.values.get(field_name)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _descriptive_values(values: Sequence[float]) -> dict[str, float | int | None]:
    return {
        "наблюдений": len(values),
        "среднее": statistics.fmean(values) if values else None,
        "медиана": statistics.median(values) if values else None,
    }


def _markdown_report(result: AnalysisResult) -> str:
    stats = result.scan_statistics
    outside = [
        item
        for item in result.episodes
        if item.first_ready_record is not None
        and item.first_ready_record.is_in_current_top20 is False
    ]
    ready = [item for item in result.episodes if item.first_ready_record is not None]
    missing_components = [
        item
        for item in EXPECTED_COMPONENT_FIELDS
        if item not in result.read.observed_fields
    ]
    lines = [
        "# Итоговый отчёт офлайн-анализа раннего обнаружения V1",
        "",
        "## Краткий итог",
        "",
        f"Проанализировано {result.read.valid_lines} корректных строк, "
        f"{len(result.read.scan_symbols)} сканирований, "
        f"{len(result.read.by_symbol)} инструментов и {len(result.episodes)} эпизодов.",
        f"Повреждённых строк: {result.read.damaged_lines}.",
        "Формулы и пороги раннего обнаружения не изменялись.",
        "",
        "## Описание выборки",
        "",
        f"- Период: {stats['started_at']} — {stats['ended_at']}.",
        f"- Продолжительность: {_fmt(stats['duration_hours'])} ч.",
        f"- Инструментов за сканирование: минимум {stats['minimum_symbols']}, "
        f"медиана {_fmt(stats['median_symbols'])}, "
        f"максимум {stats['maximum_symbols']}.",
        f"- Медианный интервал: {_fmt(stats['median_interval_minutes'])} мин.",
        f"- Оценочно пропущено циклов: {stats['estimated_missed_cycles']}.",
        f"- Полных сканирований: {stats['complete_scan_count']}; событий исчезновения: "
        f"{stats['disappearance_events']}; повторно появившихся инструментов: "
        f"{stats['symbols_reappeared']}.",
        "",
        "## Качество направлений",
        "",
        *_horizon_markdown(result.horizon_metrics),
        "",
        "### ВВЕРХ",
        "",
        *_horizon_markdown(result.direction_metrics["UP"]),
        "",
        "### ВНИЗ",
        "",
        *_horizon_markdown(result.direction_metrics["DOWN"]),
        "",
        "## Статистика эпизодов",
        "",
        f"Готовых эпизодов: {len(ready)}; вне основного списка 20 лучших при первом "
        f"готовом состоянии: {len(outside)} ({_pct(len(outside), len(ready))}).",
        f"Правило эпизода: пауза ≥ {EPISODE_GAP.total_seconds() / 60:.0f} минут, "
        f"смена ВВЕРХ ↔ ВНИЗ или {QUIET_SCANS_TO_RESET} последовательных "
        "состояния БЕЗ СИГНАЛА.",
        "",
        "## Сравнение с основным IN PLAY",
        "",
        _top20_summary(result.episodes),
        "",
        "## Анализ расстояния от пробоя",
        "",
        *_distance_markdown(result.distance_groups),
        "",
        "## Устойчивость сигналов",
        "",
        *_stability_markdown(result.stability_groups),
        "",
        "## Факторы первого готового состояния",
        "",
        *_component_markdown(result.component_analysis),
        "",
        "## Проверка защитных ограничений",
        "",
        *_safety_markdown(result.safety_violations),
        "",
        "## Лучшие и худшие группы",
        "",
        "Подробные срезы по направлению, оценкам, расстоянию, спреду и "
        "изменению за 24 часа "
        "сохранены в `метрики.json`. Лучшие и худшие отдельные эпизоды вынесены "
        "в соответствующие CSV.",
        "",
        "## Ограничения данных",
        "",
        "- Отсутствующие обязательные поля: "
        f"{_counter_text(result.read.missing_fields)}.",
        "- Отдельные балльные вклады компонентов отсутствуют: "
        + (
            ", ".join(COMPONENT_FIELD_RU[item] for item in missing_components)
            if missing_components
            else "нет"
        )
        + ".",
        "- Максимальное движение в пользу и против направления рассчитано только "
        "по доступным снимкам аудита, не по внутрисвечным максимумам и минимумам.",
        "- Недоступное место в основном списке выделено отдельно и не считается "
        "ошибкой.",
        "- Причина недоступного места проверена по наличию текущей оценки IN PLAY; "
        "подробные количества сохранены в `метрики.json`.",
        f"- Место недоступно в {result.production_rank_analysis['место_недоступно']} "
        "снимках; во всех таких снимках отсутствует и текущая оценка IN PLAY: "
        f"{result.production_rank_analysis['место_недоступно_и_нет_оценки_IN_PLAY']}.",
        "",
        "## Факты для калибровки V2",
        "",
        "Использовать статистику горизонтов, устойчивости, основного списка "
        "20 лучших и знака расстояния "
        "как вход для отдельной калибровки. Автоматический подбор порогов "
        "не выполнялся.",
        "",
        "## Что нельзя заключить из этого аудита",
        "",
        "Нельзя оценить внутрисвечную последовательность движения, реальную цену "
        "исполнения, проскальзывание или причинный вклад отдельных компонентов оценки. "
        "Аудит не является торговой рекомендацией.",
    ]
    return "\n".join(lines) + "\n"


def _metrics_json(result: AnalysisResult) -> dict[str, Any]:
    return {
        "labels_ru": {
            "discovery_score": "оценка раннего обнаружения",
            "entry_readiness_score": "готовность к входу",
            "mfe": "максимальное движение в пользу направления",
            "mae": "максимальное движение против направления",
        },
        "sample": {
            "valid_lines": result.read.valid_lines,
            "damaged_lines": result.read.damaged_lines,
            "empty_lines": result.read.empty_lines,
            "unique_symbols": len(result.read.by_symbol),
            "episodes": len(result.episodes),
            **result.scan_statistics,
        },
        "missing_fields": dict(result.read.missing_fields),
        "missing_component_fields": [
            item
            for item in EXPECTED_COMPONENT_FIELDS
            if item not in result.read.observed_fields
        ],
        "horizons": {
            str(key): asdict(value) for key, value in result.horizon_metrics.items()
        },
        "directions": {
            DIRECTION_RU[key]: {str(h): asdict(metrics) for h, metrics in value.items()}
            for key, value in result.direction_metrics.items()
        },
        "distance_groups": result.distance_groups,
        "stability": result.stability_groups,
        "slices": result.slice_statistics,
        "component_analysis": result.component_analysis,
        "safety_violations": result.safety_violations,
        "production_rank_analysis": result.production_rank_analysis,
    }


def _write_episode_csv(path: Path, episodes: Sequence[Episode]) -> None:
    headers = (
        "Идентификатор эпизода",
        "Инструмент",
        "Направление",
        "Начало",
        "Конец",
        "Длительность, минут",
        "Первая стадия",
        "Максимальная стадия",
        "Первое раннее внимание",
        "Первое формирование",
        "Первое готовое состояние",
        "Повторных готовых состояний",
        "Максимальная серия готовых состояний",
        "Максимальная оценка раннего обнаружения",
        "Максимальная готовность к входу",
        "В основном списке 20 лучших",
        "Лучшее место",
        "Опережение основного списка, минут",
        "Изменение цены эпизода, %",
        "Расстояние от пробоя, ATR",
        "Спред, %",
        "Изменение за 24 часа, %",
        "Результат 15 минут, %",
        "Результат 30 минут, %",
        "Результат 60 минут, %",
        "Результат 180 минут, %",
        "Подтверждения",
        "Предупреждения",
    )
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.writer(stream, delimiter=";", lineterminator="\n")
        writer.writerow(headers)
        for item in episodes:
            ready = item.first_ready_record
            writer.writerow(
                _csv_values(
                    (
                        item.episode_id,
                        item.symbol,
                        DIRECTION_RU[item.direction],
                        item.started_at.isoformat(),
                        item.ended_at.isoformat(),
                        (item.ended_at - item.started_at).total_seconds() / 60.0,
                        STAGE_RU[item.first_stage],
                        STAGE_RU[item.maximum_stage],
                        _iso(item.first_early_at),
                        _iso(item.first_forming_at),
                        _iso(item.first_ready_at),
                        item.ready_count,
                        item.maximum_ready_streak,
                        item.maximum_discovery_score,
                        item.maximum_entry_readiness,
                        "да" if item.ever_in_top20 else "нет",
                        item.best_production_rank,
                        item.early_lead_minutes,
                        item.episode_price_change_pct,
                        ready.distance_from_breakout_atr if ready else None,
                        ready.spread_pct if ready else None,
                        ready.price_change_24h_pct if ready else None,
                        _outcome_value(item, 15),
                        _outcome_value(item, 30),
                        _outcome_value(item, 60),
                        _outcome_value(item, 180),
                        ", ".join(ready.confirmations) if ready else "",
                        ", ".join(ready.warnings) if ready else "",
                    )
                )
            )


def _calibration_recommendations(result: AnalysisResult) -> str:
    two = result.stability_groups["минимум_два_подряд"]
    three = result.stability_groups["минимум_три_подряд"]
    overall_60 = result.horizon_metrics[60]
    two_60 = two["horizons"]["60"]
    three_60 = three["horizons"]["60"]
    return "\n".join(
        (
            "# Рекомендации для будущей калибровки V2",
            "",
            "Формулы и пороги в этой задаче не изменялись.",
            "",
            "## 1. Перепроверить качество направления на более длинной выборке",
            "",
            "- Проблема: доля правильного направления пока не превышает "
            "случайный ориентир 50%.",
            f"- Подтверждение: через 60 минут наблюдений {overall_60.available}, "
            f"правильное направление {_percent(overall_60.correct_share)}, "
            "медианный результат "
            f"{_percent_value(overall_60.median_return_pct)}.",
            "- Предполагаемое изменение: не менять пороги сейчас; накопить "
            "независимый период "
            "и отдельно проверить направления вверх и вниз.",
            "- Ожидаемый эффект: отделение устойчивого свойства от шума "
            "короткого периода.",
            "- Риск: рыночный режим новой выборки может отличаться.",
            "- Нужные данные: несколько недель непрерывного аудита и больше доступных "
            "результатов на горизонтах 60 и 180 минут.",
            "",
            "## 2. Проверить требование устойчивости",
            "",
            "- Проблема: одиночное готовое состояние может быть шумом.",
            f"- Подтверждение: эпизодов с ≥2 подтверждениями — {two['episodes']}, "
            f"с ≥3 — {three['episodes']}; через 60 минут правильное направление "
            f"{_percent(two_60['correct_share'])} и "
            f"{_percent(three_60['correct_share'])} соответственно.",
            "- Предполагаемое изменение: сравнить одно, два и три "
            "последовательных состояния.",
            "- Ожидаемый эффект: уменьшение ложных ранних состояний.",
            "- Риск: дополнительная задержка обнаружения.",
            "- Нужные данные: более длинный период и внутрисвечные максимумы "
            "и минимумы.",
            "",
            "## 3. Проверить знак расстояния от пробоя",
            "",
            "- Проблема: отрицательное расстояние может означать возврат за уровень.",
            "- Подтверждение: группы и результаты приведены в итоговом отчёте "
            "и метриках.",
            "- Предполагаемое изменение: не менять формулу до накопления статистики.",
            "- Ожидаемый эффект: отделение удержанного пробоя от неудачного пробоя.",
            "- Риск: неверная интерпретация знака для направления ВНИЗ.",
            "- Нужные данные: удержание или повторная проверка уровня и свечные "
            "данные после уровня.",
            "",
            "## 4. Добавить вклады компонентов в будущий аудит",
            "",
            "- Проблема: текущий JSONL содержит факторы, но не отдельные "
            "балльные вклады.",
            "- Подтверждение: компонентный анализ причинности невозможен "
            "без реконструкции.",
            "- Предполагаемое изменение: сохранять каждый вклад формулы отдельно.",
            "- Ожидаемый эффект: измеримая полезность каждого компонента.",
            "- Риск: увеличение размера файла аудита.",
            "- Нужные данные: только диагностические балльные вклады компонентов.",
            "",
        )
    )


def _directed_return(entry: float, future: float, direction: str) -> float:
    raw = (future / entry - 1.0) * 100.0
    return raw if direction == "UP" else -raw


def _objective_result(episode: Episode) -> float | None:
    for minutes in (60, 180, 30, 15):
        outcome = episode.horizon_results.get(minutes)
        if outcome is not None:
            return outcome.directed_return_pct
    return None


def _maximum_ready_streak(records: Sequence[AuditRecord]) -> int:
    current = 0
    maximum = 0
    for record in records:
        current = current + 1 if record.discovery_stage == "READY_CANDIDATE" else 0
        maximum = max(maximum, current)
    return maximum


def _first_stage(records: Sequence[AuditRecord], stage: str) -> datetime | None:
    return next(
        (item.scanned_at for item in records if item.discovery_stage == stage), None
    )


def _maximum(values: Iterable[float | None]) -> float | None:
    available = [item for item in values if item is not None]
    return max(available) if available else None


def _share(values: Iterable[bool]) -> float | None:
    items = list(values)
    return sum(items) / len(items) if items else None


def _percentile(values: Sequence[float], fraction: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _episode_followed_by_stage(
    episode: Episode,
    by_symbol: Mapping[str, Sequence[AuditRecord]],
    stage: str,
) -> bool:
    return any(
        item.discovery_stage == stage
        for item in by_symbol[episode.symbol]
        if episode.ended_at < item.scanned_at <= episode.ended_at + EPISODE_GAP
    )


def _episode_direction_changed(
    episode: Episode,
    by_symbol: Mapping[str, Sequence[AuditRecord]],
) -> bool:
    if episode.direction not in {"UP", "DOWN"}:
        return False
    return any(
        item.market_direction in {"UP", "DOWN"}
        and item.market_direction != episode.direction
        for item in by_symbol[episode.symbol]
        if episode.ended_at < item.scanned_at <= episode.ended_at + EPISODE_GAP
    )


def _numeric_bands(
    field_name: str,
    bands: Sequence[tuple[float, float]],
) -> list[tuple[str, Any]]:
    return [
        (
            f"{_band_label(lower, upper)}",
            lambda record, low=lower, high=upper: (
                (value := getattr(record, field_name)) is not None
                and low <= value < high
            ),
        )
        for lower, upper in bands
    ]


def _absolute_bands(
    field_name: str,
    bands: Sequence[tuple[float, float]],
) -> list[tuple[str, Any]]:
    return [
        (
            f"{_band_label(lower, upper)}",
            lambda record, low=lower, high=upper: (
                (value := getattr(record, field_name)) is not None
                and low <= abs(value) < high
            ),
        )
        for lower, upper in bands
    ]


def _band_label(lower: float, upper: float) -> str:
    if math.isinf(lower):
        return f"ниже {_fmt(upper)}"
    if math.isinf(upper):
        return f"от {_fmt(lower)}"
    return f"{_fmt(lower)}–{_fmt(upper)}"


def _horizon_markdown(metrics: Mapping[int, HorizonMetrics]) -> list[str]:
    lines = [
        "| Горизонт | Наблюдений | Правильное направление | Медиана | Среднее |",
        "|---:|---:|---:|---:|---:|",
    ]
    for minutes, item in metrics.items():
        lines.append(
            f"| {minutes} мин | {item.available} | {_percent(item.correct_share)} | "
            f"{_percent_value(item.median_return_pct)} | "
            f"{_percent_value(item.mean_return_pct)} |"
        )
    return lines


def _mapping_markdown(values: Mapping[str, Mapping[str, Any]]) -> list[str]:
    if not values:
        return ["Доступных групп нет."]
    return [
        f"- **{key}**: эпизодов {value.get('episodes', 0)}."
        for key, value in values.items()
    ]


def _distance_markdown(values: Mapping[str, Mapping[str, Any]]) -> list[str]:
    if not values:
        return ["Доступных групп нет."]
    lines: list[str] = []
    for name, value in values.items():
        metrics = value.get("horizons", {}).get("60", {})
        lines.append(
            f"- **{name}**: эпизодов {value.get('episodes', 0)}; через 60 минут "
            f"правильное направление {_percent(metrics.get('correct_share'))}, "
            f"медиана {_percent_value(metrics.get('median_return_pct'))}."
        )
    lines.append(
        "Знак расстояния интерпретируется относительно направления: для движения вверх "
        "ожидаемо положительное расстояние, для движения вниз — отрицательное. Малые "
        "противоположные группы не позволяют делать устойчивый вывод без "
        "дополнительной выборки."
    )
    return lines


def _stability_markdown(values: Mapping[str, Mapping[str, Any]]) -> list[str]:
    if not values:
        return ["Доступных групп нет."]
    labels = {
        "одно_готовое_состояние": "одно готовое состояние",
        "минимум_два_подряд": "не менее двух подряд",
        "минимум_три_подряд": "не менее трёх подряд",
    }
    lines: list[str] = []
    for name, value in values.items():
        metrics = value.get("horizons", {}).get("60", {})
        lines.append(
            f"- **{labels.get(name, name)}**: эпизодов и гипотетических уведомлений "
            f"{value.get('episodes', 0)}; через 60 минут правильное направление "
            f"{_percent(metrics.get('correct_share'))}, медиана "
            f"{_percent_value(metrics.get('median_return_pct'))}."
        )
    return lines


def _component_markdown(values: Mapping[str, Any]) -> list[str]:
    lines = [
        f"Критерий: {values.get('критерий_успеха', 'н/д')}. Успешных эпизодов: "
        f"{values.get('успешных_эпизодов', 0)}, неуспешных: "
        f"{values.get('неуспешных_эпизодов', 0)}."
    ]
    factors = values.get("факторы", {})
    if isinstance(factors, Mapping):
        for details in factors.values():
            if not isinstance(details, Mapping):
                continue
            good = details.get("успешные", {})
            bad = details.get("неуспешные", {})
            if not isinstance(good, Mapping) or not isinstance(bad, Mapping):
                continue
            lines.append(
                f"- **{details.get('название', 'фактор')}**: медиана успешных "
                f"{_fmt(good.get('медиана'))}, неуспешных {_fmt(bad.get('медиана'))}."
            )
    lines.append(str(values.get("ограничение", "")))
    return lines


def _safety_markdown(values: Mapping[str, int]) -> list[str]:
    return [
        f"- Готовых снимков: {values.get('готовых_снимков', 0)}.",
        f"- Абсолютное изменение за 24 часа не менее 15%: "
        f"{values.get('изменение_24ч_не_менее_15', 0)}; не менее 30%: "
        f"{values.get('изменение_24ч_не_менее_30', 0)}.",
        f"- Расстояние от пробоя более 2 ATR: "
        f"{values.get('расстояние_более_2_ATR', 0)}; спред более 0,2%: "
        f"{values.get('спред_более_0_2', 0)}.",
    ]


def _top20_summary(episodes: Sequence[Episode]) -> str:
    ready = [item for item in episodes if item.first_ready_record is not None]
    inside = sum(
        item.first_ready_record is not None
        and item.first_ready_record.is_in_current_top20 is True
        for item in ready
    )
    outside = sum(
        item.first_ready_record is not None
        and item.first_ready_record.is_in_current_top20 is False
        for item in ready
    )
    missing = sum(
        item.first_ready_record is not None
        and item.first_ready_record.rank_in_current_inplay_universe is None
        for item in ready
    )
    never = sum(not item.ever_in_top20 for item in episodes)
    leads = [
        item.early_lead_minutes
        for item in episodes
        if item.early_lead_minutes is not None
    ]
    early_inside = sum(item.in_top20_at_early is True for item in episodes)
    early_outside = sum(item.in_top20_at_early is False for item in episodes)
    early_missing = sum(item.in_top20_at_early is None for item in episodes)
    appeared_later = sum(item.appeared_in_top20_later for item in episodes)
    return (
        f"При первом раннем состоянии внутри списка: {early_inside}, вне списка: "
        f"{early_outside}, признак недоступен: {early_missing}. При первом готовом "
        f"состоянии внутри списка: {inside}, вне списка: {outside}, место недоступно: "
        f"{missing}. Вошли в список позднее раннего обнаружения: {appeared_later}; "
        f"никогда не вошли в список: {never}. "
        f"Медианное опережение среди доступных наблюдений: "
        f"{_fmt(statistics.median(leads) if leads else None)} мин."
    )


def _counter_text(values: Counter[str]) -> str:
    return (
        ", ".join(f"{key}: {value}" for key, value in sorted(values.items())) or "нет"
    )


def _csv_values(values: Iterable[Any]) -> tuple[str, ...]:
    return tuple(_csv_value(item) for item in values)


def _csv_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.8f}".rstrip("0").rstrip(".").replace(".", ",")
    return str(value)


def _outcome_value(episode: Episode, minutes: int) -> float | None:
    outcome = episode.horizon_results.get(minutes)
    return outcome.directed_return_pct if outcome is not None else None


def _iso(value: datetime | None) -> str:
    return value.isoformat() if value is not None else ""


def _fmt(value: Any) -> str:
    if value is None:
        return "н/д"
    if isinstance(value, float):
        return f"{value:.2f}".replace(".", ",")
    return str(value)


def _pct(numerator: int, denominator: int) -> str:
    return _percent(numerator / denominator if denominator else None)


def _percent(value: float | None) -> str:
    return f"{value * 100:.1f}%".replace(".", ",") if value is not None else "н/д"


def _percent_value(value: float | None) -> str:
    return f"{value:.3f}%".replace(".", ",") if value is not None else "н/д"


def _required_string(value: Mapping[str, Any], key: str) -> str:
    item = value[key]
    if not isinstance(item, str) or not item:
        raise ValueError(f"Field {key} must be a non-empty string")
    return item


def _datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Timestamp must be timezone-aware")
    return parsed.astimezone(UTC)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("Expected optional number")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("Expected finite number")
    return number


def _required_price(value: float | None) -> float:
    if value is None:
        raise ValueError("Expected available future price")
    return value


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("Expected optional integer")
    return value


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if not isinstance(value, bool):
        raise TypeError("Expected optional boolean")
    return value


def _string_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise TypeError("Expected string list")
    return tuple(value)


if __name__ == "__main__":
    raise SystemExit(main())
