from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import statistics
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import cast

from market_signal_assistant.inplay.early_discovery import MarketDirection
from market_signal_assistant.inplay.early_discovery_v2 import RetestState
from market_signal_assistant.setup_engine.analyzer import classification_candidates
from market_signal_assistant.setup_engine.models import (
    SetupAnalysisInput,
    SetupDirection,
    SetupState,
    SetupType,
)
from market_signal_assistant.setup_engine.offline_analyzer import (
    HORIZONS,
    AuditReadResult,
    HorizonOutcome,
    ReplaySnapshot,
    build_episodes,
    outcome_for_snapshot,
    read_v2_audit,
)
from market_signal_assistant.setup_engine.offline_analyzer import (
    AnalyzerConfig as EpisodeAnalyzerConfig,
)

DEFAULT_V2_AUDIT = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "inplay_early_discovery_v2_audit.jsonl"
)
DEFAULT_STATE = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "inplay_early_discovery_v2_state.json"
)
DEFAULT_RUNTIME_LOG = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "inplay_early_discovery_v2_runtime.log"
)
DEFAULT_QTR_AUDIT = (
    Path(__file__).resolve().parents[3] / "data" / "qtr_setup_engine_audit.jsonl"
)
DEFAULT_OUTPUT = (
    Path(__file__).resolve().parents[3] / "data" / "setup_engine_diagnostics"
)

TYPE_RU = {
    SetupType.FALSE_BREAKOUT: "ЛОЖНЫЙ ПРОБОЙ",
    SetupType.REVERSAL: "РАЗВОРОТ",
    SetupType.RETEST: "РЕТЕСТ",
    SetupType.BREAKOUT: "ПРОБОЙ",
    SetupType.CONTINUATION: "ПРОДОЛЖЕНИЕ",
    SetupType.IMPULSE: "ИМПУЛЬС",
    SetupType.COMPRESSION: "СЖАТИЕ",
    SetupType.NO_TRADE: "НЕТ СДЕЛКИ",
}
STATE_RU = {
    SetupState.WATCHING: "НАБЛЮДАЕМ",
    SetupState.FORMING: "ФОРМИРУЕТСЯ",
    SetupState.CONFIRMING: "ПОДТВЕРЖДАЕТСЯ",
    SetupState.READY_TO_CONSIDER: "ГОТОВО К РАССМОТРЕНИЮ",
    SetupState.LATE: "ПОЗДНО",
    SetupState.CANCELLED: "ОТМЕНЕНО",
}
DIRECTION_RU = {
    SetupDirection.UP: "ВВЕРХ",
    SetupDirection.DOWN: "ВНИЗ",
    SetupDirection.NEUTRAL: "НЕЙТРАЛЬНО",
}
FALSE_GROUP_RU = {
    "A": "A. Цена закрылась обратно внутрь диапазона",
    "B": "B. Возврат к уровню без нарушения структуры",
    "C": "C. Цена восстановилась после временной неправильной стороны",
    "D": "D. Недостаточно данных",
    "E": "E. После MarketDataError или неполного scan",
    "F": "F. Конфликт signed_distance_atr и breakout_failure",
    "G": "G. Конфликт retest_state и breakout_failure",
}


@dataclass(frozen=True, slots=True)
class DiagnosticConfig:
    episode_gap_minutes: int = 30
    short_gap_minutes: int = 5
    outcome_tolerance_minutes: int = 10
    sharp_drop_share: float = 0.1

    def __post_init__(self) -> None:
        if self.episode_gap_minutes < 1:
            raise ValueError("episode_gap_minutes должен быть положительным")
        if self.short_gap_minutes < 1:
            raise ValueError("short_gap_minutes должен быть положительным")
        if self.outcome_tolerance_minutes < 0:
            raise ValueError("outcome_tolerance_minutes не может быть отрицательным")
        if not 0 < self.sharp_drop_share <= 1:
            raise ValueError("sharp_drop_share должен быть в диапазоне (0, 1]")


@dataclass(frozen=True, slots=True)
class ScanDiagnostic:
    index: int
    scan_id: str
    scanned_at: datetime
    snapshots: tuple[ReplaySnapshot, ...]
    symbol_count: int
    error_count: int
    previous_symbol_count: int | None
    count_delta: int | None
    gap_minutes: float | None
    missing_from_previous: tuple[str, ...]
    added_since_previous: tuple[str, ...]
    sharp_drop: bool

    @property
    def symbols(self) -> frozenset[str]:
        return frozenset(item.result.symbol for item in self.snapshots)

    @property
    def incomplete(self) -> bool:
        return self.error_count > 0 or self.sharp_drop


@dataclass(frozen=True, slots=True)
class RuntimeScanSummary:
    instruments: int
    ready: int
    forming: int
    errors: int


@dataclass(frozen=True, slots=True)
class RuntimeLogStats:
    exists: bool
    size_bytes: int
    line_count: int
    market_data_error_messages: int
    completed_scans: tuple[RuntimeScanSummary, ...]
    decode_note: str


@dataclass(frozen=True, slots=True)
class DiagnosticAnalysis:
    audit: AuditReadResult
    scans: tuple[ScanDiagnostic, ...]
    runtime: RuntimeLogStats
    metrics: Mapping[str, object]
    false_rows: tuple[Mapping[str, object], ...]
    transition_rows: tuple[Mapping[str, object], ...]
    no_trade_rows: tuple[Mapping[str, object], ...]
    scan_rows: tuple[Mapping[str, object], ...]
    adapter_rows: tuple[Mapping[str, object], ...]
    conflict_rows: tuple[Mapping[str, object], ...]
    source_hashes: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class OutputFiles:
    report: Path
    false_breakouts: Path
    transitions: Path
    no_trade: Path
    data_errors: Path
    adapter_map: Path
    conflicts: Path
    metrics: Path
    recommendations: Path


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _missing_data_from_input(data: SetupAnalysisInput) -> tuple[str, ...]:
    values = list(data.extra_missing_data)
    if not data.technical_data_complete:
        values.append("technical_data")
    for name, value in (
        ("current_price", data.current_price),
        ("trigger_level", data.trigger_level),
        ("distance_to_trigger_atr", data.distance_to_trigger_atr),
        ("breakout_age_bars", data.breakout_age_bars),
        ("spread_pct", data.spread_pct),
        ("correct_side_of_level", data.correct_side_of_level),
        ("volume_confirmation", data.volume_confirmation),
        ("volatility_confirmation", data.volatility_confirmation),
        ("structure_confirmation", data.structure_confirmation),
        ("liquidity_ok", data.liquidity_ok),
    ):
        if value is None:
            values.append(name)
    return tuple(dict.fromkeys(values))


def candidate_setup_types(data: SetupAnalysisInput) -> tuple[SetupType, ...]:
    """Return every simultaneously true production candidate, before priority."""
    return classification_candidates(data)


def diagnostic_no_trade_reasons(snapshot: ReplaySnapshot) -> tuple[str, ...]:
    return snapshot.result.no_trade_reasons


def build_scan_diagnostics(
    snapshots: Sequence[ReplaySnapshot],
    config: DiagnosticConfig | None = None,
) -> tuple[ScanDiagnostic, ...]:
    if config is None:
        config = DiagnosticConfig()
    grouped: dict[str, list[ReplaySnapshot]] = defaultdict(list)
    for snapshot in snapshots:
        grouped[snapshot.source.scan_id].append(snapshot)
    ordered = sorted(
        grouped.items(),
        key=lambda pair: min(item.at for item in pair[1]),
    )
    scans: list[ScanDiagnostic] = []
    previous: ScanDiagnostic | None = None
    for index, (scan_id, items) in enumerate(ordered, 1):
        current_items = tuple(sorted(items, key=lambda item: item.result.symbol))
        at = min(item.at for item in current_items)
        current_symbols = frozenset(item.result.symbol for item in current_items)
        previous_symbols = previous.symbols if previous is not None else frozenset()
        previous_count = previous.symbol_count if previous is not None else None
        delta = (
            len(current_symbols) - previous_count
            if previous_count is not None
            else None
        )
        sharp_drop = bool(
            previous_count
            and delta is not None
            and delta < 0
            and abs(delta) / previous_count >= config.sharp_drop_share
        )
        scan = ScanDiagnostic(
            index=index,
            scan_id=scan_id,
            scanned_at=at,
            snapshots=current_items,
            symbol_count=len(current_symbols),
            error_count=sum(item.source.technical_error is not None for item in items),
            previous_symbol_count=previous_count,
            count_delta=delta,
            gap_minutes=(
                (at - previous.scanned_at).total_seconds() / 60
                if previous is not None
                else None
            ),
            missing_from_previous=tuple(sorted(previous_symbols - current_symbols)),
            added_since_previous=tuple(sorted(current_symbols - previous_symbols)),
            sharp_drop=sharp_drop,
        )
        scans.append(scan)
        previous = scan
    return tuple(scans)


def symbol_missing_between(
    previous_scan: ScanDiagnostic | None,
    symbol: str,
) -> bool:
    return previous_scan is not None and symbol not in previous_scan.symbols


def _decode_runtime_log(raw: bytes) -> tuple[str, str]:
    if not raw:
        return "", "пустой файл"
    if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        text = raw.decode("utf-16", errors="replace")
        note = "UTF-16"
    else:
        text = raw.decode("utf-8-sig", errors="replace")
        note = "UTF-8"
    if "╨" in text or "╤" in text:
        repaired = text.encode("cp866", errors="replace").decode(
            "utf-8", errors="replace"
        )
        return repaired, note + "; исправлен OEM-mojibake"
    return text, note


def parse_runtime_log(path: Path) -> RuntimeLogStats:
    if not path.exists():
        return RuntimeLogStats(False, 0, 0, 0, (), "файл отсутствует")
    raw = path.read_bytes()
    text, note = _decode_runtime_log(raw)
    pattern = re.compile(
        r"Модуль раннего обнаружения V2: сканирование завершено\.\s*"
        r"Инструментов:\s*(\d+)\.\s*"
        r"Подтвержд[её]нных наблюдений:\s*(\d+)\.\s*"
        r"Формирующихся ситуаций:\s*(\d+)\.\s*"
        r"Ошибок отдельных инструментов:\s*(\d+)\.",
        re.IGNORECASE,
    )
    summaries = tuple(
        RuntimeScanSummary(*(int(value) for value in match.groups()))
        for match in pattern.finditer(text)
    )
    return RuntimeLogStats(
        True,
        len(raw),
        len(text.splitlines()),
        text.count("MarketDataError"),
        summaries,
        note,
    )


def _sign_failure_conflict(snapshot: ReplaySnapshot) -> bool:
    signed = snapshot.source.signed_distance_atr
    direction = snapshot.source.market_direction
    failure = snapshot.source.breakout_failure
    if signed is None or failure is None or direction is MarketDirection.NEUTRAL:
        return False
    correct_by_sign = signed >= 0 if direction is MarketDirection.UP else signed <= 0
    return failure is correct_by_sign


def _retest_failure_conflict(snapshot: ReplaySnapshot) -> bool:
    failure = snapshot.source.breakout_failure
    retest = snapshot.source.retest_state
    if failure is None or retest is None:
        return False
    return (failure and retest is not RetestState.FAILED) or (
        not failure and retest is RetestState.FAILED
    )


def primary_false_breakout_group(snapshot: ReplaySnapshot) -> str:
    source = snapshot.source
    if (
        source.breakout_failure is None
        or source.is_correct_side_of_level is None
        or source.signed_distance_atr is None
    ):
        return "D"
    if source.breakout_failure:
        return "A"
    if source.returned_inside_range and source.is_correct_side_of_level:
        return "C"
    if source.returned_to_level and not source.returned_inside_range:
        return "B"
    return "D"


def false_breakout_groups(
    snapshot: ReplaySnapshot,
    previous_scan: ScanDiagnostic | None,
    previous_symbol_snapshot: ReplaySnapshot | None,
    current_scan: ScanDiagnostic | None = None,
) -> tuple[str, ...]:
    groups = [primary_false_breakout_group(snapshot)]
    if (
        (current_scan is not None and current_scan.incomplete)
        or (previous_scan is not None and previous_scan.error_count > 0)
        or (
            previous_symbol_snapshot is not None
            and previous_symbol_snapshot.source.technical_error is not None
        )
        or symbol_missing_between(previous_scan, snapshot.result.symbol)
    ):
        groups.append("E")
    if _sign_failure_conflict(snapshot):
        groups.append("F")
    if _retest_failure_conflict(snapshot):
        groups.append("G")
    return tuple(groups)


def transition_matrix(
    snapshots: Sequence[ReplaySnapshot],
    gap_minutes: int = 30,
) -> Counter[tuple[SetupState, SetupState]]:
    grouped: dict[str, list[ReplaySnapshot]] = defaultdict(list)
    for snapshot in snapshots:
        grouped[snapshot.result.symbol].append(snapshot)
    counts: Counter[tuple[SetupState, SetupState]] = Counter()
    maximum_gap = timedelta(minutes=gap_minutes)
    for items in grouped.values():
        ordered = sorted(items, key=lambda item: item.at)
        for previous, current in zip(ordered, ordered[1:], strict=False):
            if current.at - previous.at <= maximum_gap:
                counts[(previous.result.setup_state, current.result.setup_state)] += 1
    return counts


def _transition_rows(
    snapshots: Sequence[ReplaySnapshot],
    gap_minutes: int,
) -> tuple[Mapping[str, object], ...]:
    counts = transition_matrix(snapshots, gap_minutes)
    return tuple(
        {
            "Предыдущее состояние": STATE_RU[previous],
            "Новое состояние": STATE_RU[current],
            "Количество": count,
            "Подозрительный переход": _suspicious_transition(previous, current),
        }
        for (previous, current), count in sorted(
            counts.items(), key=lambda item: (-item[1], item[0][0], item[0][1])
        )
    )


def _suspicious_transition(previous: SetupState, current: SetupState) -> bool:
    return (previous, current) in {
        (SetupState.FORMING, SetupState.READY_TO_CONSIDER),
        (SetupState.WATCHING, SetupState.READY_TO_CONSIDER),
        (SetupState.CANCELLED, SetupState.READY_TO_CONSIDER),
    }


def adapter_mapping() -> tuple[Mapping[str, object], ...]:
    rows = (
        ("snapshot_ids", "scan_id", "прямое отображение", "нет"),
        ("source", "константа early_discovery_v2", "константа", "всегда задано"),
        ("symbol", "symbol", "прямое отображение", "нет"),
        ("analyzed_at", "scanned_at", "прямое отображение", "нет"),
        ("direction", "market_direction", "enum mapping", "NEUTRAL сохраняется"),
        ("current_price", "current_price", "прямое отображение", "null при error"),
        ("trigger_level", "breakout_level", "прямое отображение", "null при error"),
        (
            "invalidation_level",
            "breakout_level, absolute_distance, absolute_distance_atr",
            "вычисление уровня ± ATR",
            "null при отсутствии/нулевом ATR или NEUTRAL",
        ),
        (
            "distance_to_trigger_pct",
            "current_price, breakout_level",
            "abs(price-level)/level*100",
            "null при отсутствии цены/уровня",
        ),
        (
            "price_change_24h_pct",
            "price_change_24h_pct",
            "прямое отображение",
            "null при technical error",
        ),
        ("distance_to_trigger_atr", "absolute_distance_atr", "прямое", "null"),
        ("breakout_age_bars", "breakout_age_bars", "прямое", "null"),
        ("hold_candles", "breakout_hold_candles", "прямое", "None; analyzer → 0"),
        (
            "breakout_confirmed",
            "level, price, correct_side, hold, failure",
            "bool: level+price, correct_side, hold>=1, not failure",
            "False",
        ),
        ("correct_side_of_level", "is_correct_side_of_level", "прямое", "null"),
        ("returned_inside_range", "returned_inside_range", "is True", "False"),
        (
            "retest_detected",
            "returned_to_level или retest_state",
            "bool для IN_PROGRESS/HELD",
            "False",
        ),
        ("retest_held", "retest_state == RETEST_HELD", "bool", "False"),
        ("breakout_failed", "breakout_failure is True", "bool", "False"),
        (
            "volume_confirmation",
            "volume_acceleration или breakout_volume component factor",
            "factor >= 0.3",
            "False при отсутствии component",
        ),
        (
            "volatility_confirmation",
            "atr_expansion component factor",
            "factor >= 0.3",
            "False при отсутствии component",
        ),
        (
            "structure_confirmation",
            "breakout_confirmed или retest_held или reversal_detected",
            "bool OR",
            "False",
        ),
        (
            "liquidity_ok",
            "liquidity component factor",
            "factor >= 0.5",
            "False при отсутствии component",
        ),
        ("spread_pct", "spread_pct", "прямое", "null"),
        (
            "compression_detected",
            "compression component factor",
            "factor >= 0.5",
            "False",
        ),
        (
            "continuation_detected",
            "breakout_confirmed, age>=2, hold>=2, not returned_to_level",
            "bool AND",
            "False",
        ),
        (
            "reversal_detected",
            "direction_v1 != direction_v2, обе направленные",
            "bool",
            "False",
        ),
        (
            "conflicting_confirmations",
            "не передаётся adapter",
            "dataclass default",
            "всегда False",
        ),
        (
            "completed_candles",
            "breakout_hold_candles, breakout_confirmed",
            "max(hold or 0, 1 if confirmed else 0)",
            "0",
        ),
        (
            "technical_data_complete",
            "technical_error",
            "technical_error is None",
            "False при error",
        ),
        (
            "extra_missing_data",
            "technical_error",
            "marker early_discovery_v2_technical_error",
            "пустой tuple",
        ),
        (
            "SetupAnalysisResult.spread_ok",
            "SetupAnalysisInput.spread_pct",
            "analyze_setup: spread задан и <= 0.2%",
            "False при null",
        ),
        (
            "SetupAnalysisResult.missing_data",
            "SetupAnalysisInput поля и extra_missing_data",
            "analyze_setup собирает список отсутствующих полей",
            "пустой tuple при полноте",
        ),
    )
    return tuple(
        {
            "Поле Setup Engine": field,
            "Источник V2 / audit": source,
            "Преобразование": transform,
            "Fallback / null": fallback,
        }
        for field, source, transform, fallback in rows
    )


def _previous_snapshots(
    snapshots: Sequence[ReplaySnapshot],
) -> Mapping[int, ReplaySnapshot | None]:
    grouped: dict[str, list[ReplaySnapshot]] = defaultdict(list)
    for snapshot in snapshots:
        grouped[snapshot.result.symbol].append(snapshot)
    result: dict[int, ReplaySnapshot | None] = {}
    for items in grouped.values():
        previous: ReplaySnapshot | None = None
        for current in sorted(items, key=lambda item: item.at):
            result[current.line_number] = previous
            previous = current
    return result


def _next_valid_snapshots(
    snapshots: Sequence[ReplaySnapshot],
) -> Mapping[int, ReplaySnapshot | None]:
    grouped: dict[str, list[ReplaySnapshot]] = defaultdict(list)
    for snapshot in snapshots:
        grouped[snapshot.result.symbol].append(snapshot)
    result: dict[int, ReplaySnapshot | None] = {}
    for items in grouped.values():
        next_valid: ReplaySnapshot | None = None
        for current in reversed(sorted(items, key=lambda item: item.at)):
            result[current.line_number] = next_valid
            if current.source.technical_error is None:
                next_valid = current
    return result


def _previous_valid_snapshots(
    snapshots: Sequence[ReplaySnapshot],
) -> Mapping[int, ReplaySnapshot | None]:
    grouped: dict[str, list[ReplaySnapshot]] = defaultdict(list)
    for snapshot in snapshots:
        grouped[snapshot.result.symbol].append(snapshot)
    result: dict[int, ReplaySnapshot | None] = {}
    for items in grouped.values():
        previous_valid: ReplaySnapshot | None = None
        for current in sorted(items, key=lambda item: item.at):
            result[current.line_number] = previous_valid
            if current.source.technical_error is None:
                previous_valid = current
    return result


def _scan_index(scans: Sequence[ScanDiagnostic]) -> Mapping[str, int]:
    return {scan.scan_id: index for index, scan in enumerate(scans)}


def _outcomes_for(
    snapshot: ReplaySnapshot,
    snapshots: Sequence[ReplaySnapshot],
    config: DiagnosticConfig,
) -> Mapping[int, HorizonOutcome]:
    outcomes: dict[int, HorizonOutcome] = {}
    for horizon in HORIZONS:
        outcome = outcome_for_snapshot(
            snapshot,
            snapshots,
            horizon,
            config.outcome_tolerance_minutes,
        )
        if outcome is not None:
            outcomes[horizon] = outcome
    return outcomes


def _false_rows(
    audit: AuditReadResult,
    scans: Sequence[ScanDiagnostic],
    config: DiagnosticConfig,
) -> tuple[Mapping[str, object], ...]:
    previous_by_line = _previous_snapshots(audit.snapshots)
    next_by_line = _next_valid_snapshots(audit.snapshots)
    index_by_scan = _scan_index(scans)
    rows: list[Mapping[str, object]] = []
    for snapshot in audit.snapshots:
        if snapshot.result.setup_type is not SetupType.FALSE_BREAKOUT:
            continue
        scan_index = index_by_scan[snapshot.source.scan_id]
        current_scan = scans[scan_index]
        previous_scan = scans[scan_index - 1] if scan_index > 0 else None
        previous_symbol = previous_by_line[snapshot.line_number]
        next_valid = next_by_line[snapshot.line_number]
        groups = false_breakout_groups(
            snapshot,
            previous_scan,
            previous_symbol,
            current_scan,
        )
        outcomes = _outcomes_for(snapshot, audit.snapshots, config)
        candidates = candidate_setup_types(snapshot.setup_input)
        rows.append(
            {
                "Строка": snapshot.line_number,
                "Символ": snapshot.result.symbol,
                "Время": snapshot.at.isoformat(),
                "Направление": DIRECTION_RU[snapshot.result.direction],
                "Основная группа": FALSE_GROUP_RU[groups[0]],
                "Все группы": " | ".join(FALSE_GROUP_RU[group] for group in groups),
                "breakout_level": snapshot.source.breakout_level,
                "current_price": snapshot.source.current_price,
                "signed_distance_atr": snapshot.source.signed_distance_atr,
                "absolute_distance_atr": snapshot.source.absolute_distance_atr,
                "is_correct_side_of_level": snapshot.source.is_correct_side_of_level,
                "breakout_hold_candles": snapshot.source.breakout_hold_candles,
                "retest_state": (
                    snapshot.source.retest_state.value
                    if snapshot.source.retest_state is not None
                    else None
                ),
                "breakout_failure": snapshot.source.breakout_failure,
                "returned_inside_range": snapshot.source.returned_inside_range,
                "consecutive_ready_scans": snapshot.source.consecutive_ready_scans,
                "stage_v1": (
                    snapshot.source.stage_v1.value
                    if snapshot.source.stage_v1 is not None
                    else None
                ),
                "stage_v2": snapshot.source.stage_v2.value,
                "Причина V2": snapshot.source.reason_v2_ru,
                "Причины Setup": " | ".join(snapshot.result.reasons),
                "Предупреждения Setup": " | ".join(snapshot.result.warnings),
                "Кандидаты": " + ".join(TYPE_RU[item] for item in candidates),
                "Предыдущий scan с ошибками": (
                    previous_scan.error_count > 0 if previous_scan else None
                ),
                "Текущий scan с ошибками": current_scan.error_count > 0,
                "Текущий scan неполный": current_scan.incomplete,
                "Symbol отсутствовал в предыдущем scan": symbol_missing_between(
                    previous_scan, snapshot.result.symbol
                ),
                "Пауза после предыдущего scan, мин": (
                    (snapshot.at - previous_scan.scanned_at).total_seconds() / 60
                    if previous_scan
                    else None
                ),
                "Следующий тип": (
                    TYPE_RU[next_valid.result.setup_type] if next_valid else None
                ),
                "Вернулся к исходному направлению": bool(
                    next_valid is not None
                    and next_valid.result.direction is snapshot.result.direction
                    and next_valid.source.is_correct_side_of_level is True
                ),
                "Следующий снимок без сигнала": bool(
                    next_valid is not None
                    and (
                        next_valid.result.setup_type is SetupType.NO_TRADE
                        or next_valid.source.stage_v2.value == "QUIET"
                    )
                ),
                **{
                    f"Outcome {horizon}м, %": (
                        outcomes[horizon].directed_return_pct
                        if horizon in outcomes
                        else None
                    )
                    for horizon in HORIZONS
                },
            }
        )
    return tuple(rows)


def _no_trade_rows(
    snapshots: Sequence[ReplaySnapshot],
) -> tuple[Mapping[str, object], ...]:
    rows: list[Mapping[str, object]] = []
    for snapshot in snapshots:
        reasons = diagnostic_no_trade_reasons(snapshot)
        candidates = candidate_setup_types(snapshot.setup_input)
        rows.append(
            {
                "Строка": snapshot.line_number,
                "Символ": snapshot.result.symbol,
                "Время": snapshot.at.isoformat(),
                "Фактический тип": TYPE_RU[snapshot.result.setup_type],
                "Фактическое состояние": STATE_RU[snapshot.result.setup_state],
                "Кандидаты": " + ".join(TYPE_RU[item] for item in candidates),
                "Диагностические причины НЕТ СДЕЛКИ": " | ".join(reasons),
                "Есть диагностическая причина": bool(reasons),
                "Production gate НЕТ СДЕЛКИ": candidates == (SetupType.NO_TRADE,),
                "Причина перехвачена другим типом": bool(reasons)
                and snapshot.result.setup_type is not SetupType.NO_TRADE,
                "technical_error": snapshot.source.technical_error,
                "missing_data": " | ".join(snapshot.result.missing_data),
            }
        )
    return tuple(rows)


def _scan_rows(scans: Sequence[ScanDiagnostic]) -> tuple[Mapping[str, object], ...]:
    return tuple(
        {
            "Номер scan": scan.index,
            "scan_id": scan.scan_id,
            "Время": scan.scanned_at.isoformat(),
            "Инструментов": scan.symbol_count,
            "Ошибок инструментов": scan.error_count,
            "Категория ошибок": _error_bucket(scan.error_count),
            "Предыдущее число инструментов": scan.previous_symbol_count,
            "Изменение числа инструментов": scan.count_delta,
            "Пауза, мин": scan.gap_minutes,
            "Резкое падение": scan.sharp_drop,
            "Неполный scan": scan.incomplete,
            "Исчезло symbol": len(scan.missing_from_previous),
            "Добавилось symbol": len(scan.added_since_previous),
            "Список исчезнувших": " | ".join(scan.missing_from_previous),
        }
        for scan in scans
    )


def _error_bucket(count: int) -> str:
    if count == 0:
        return "0"
    if count <= 5:
        return "1–5"
    if count <= 10:
        return "6–10"
    return ">10"


def _conflict_rows(
    snapshots: Sequence[ReplaySnapshot],
) -> tuple[Mapping[str, object], ...]:
    combinations: Counter[tuple[SetupType, ...]] = Counter(
        candidate_setup_types(snapshot.setup_input) for snapshot in snapshots
    )
    return tuple(
        {
            "Кандидаты одновременно": " + ".join(TYPE_RU[item] for item in combo),
            "Число кандидатов": len(combo),
            "Выбран по приоритету": TYPE_RU[combo[0]],
            "Количество строк": count,
            "Доля от всех строк": count / len(snapshots) if snapshots else None,
        }
        for combo, count in combinations.most_common()
    )


def _group_false_metrics(
    false_rows: Sequence[Mapping[str, object]],
) -> Mapping[str, object]:
    result: dict[str, object] = {}
    for group, label in FALSE_GROUP_RU.items():
        selected = [row for row in false_rows if label in cast(str, row["Все группы"])]
        result[group] = {
            "label": label,
            "count": len(selected),
            "share_of_false": len(selected) / len(false_rows) if false_rows else None,
            "returned_to_original_direction": sum(
                cast(bool, row["Вернулся к исходному направлению"]) for row in selected
            ),
            "next_retest": sum(row["Следующий тип"] == "РЕТЕСТ" for row in selected),
            "next_breakout": sum(row["Следующий тип"] == "ПРОБОЙ" for row in selected),
            "next_no_signal": sum(
                cast(bool, row["Следующий снимок без сигнала"]) for row in selected
            ),
            "outcomes": {
                str(horizon): _numeric_metrics(
                    cast(float, row[f"Outcome {horizon}м, %"])
                    for row in selected
                    if row[f"Outcome {horizon}м, %"] is not None
                )
                for horizon in HORIZONS
            },
        }
    return result


def _numeric_metrics(values: Iterable[float]) -> Mapping[str, object]:
    collected = list(values)
    return {
        "available": len(collected),
        "positive_share": (
            sum(value > 0 for value in collected) / len(collected)
            if collected
            else None
        ),
        "mean_pct": statistics.fmean(collected) if collected else None,
        "median_pct": statistics.median(collected) if collected else None,
    }


def _classification_metrics(
    snapshots: Sequence[ReplaySnapshot],
) -> Mapping[str, object]:
    actual = Counter(snapshot.result.setup_type for snapshot in snapshots)
    candidate_counts = Counter(
        len(candidate_setup_types(snapshot.setup_input)) for snapshot in snapshots
    )
    overlaps = Counter(
        candidate_setup_types(snapshot.setup_input) for snapshot in snapshots
    )
    special: dict[str, int] = {}
    for left, right in (
        (SetupType.FALSE_BREAKOUT, SetupType.RETEST),
        (SetupType.FALSE_BREAKOUT, SetupType.BREAKOUT),
        (SetupType.RETEST, SetupType.BREAKOUT),
        (SetupType.REVERSAL, SetupType.FALSE_BREAKOUT),
    ):
        special[f"{left.value}+{right.value}"] = sum(
            left in combo and right in combo for combo in overlaps.elements()
        )
    return {
        "actual_types": {item.value: actual[item] for item in SetupType},
        "candidate_count_distribution": {
            "1": candidate_counts[1],
            "2": candidate_counts[2],
            "3_plus": sum(
                count for length, count in candidate_counts.items() if length >= 3
            ),
        },
        "special_overlaps": special,
        "top_combinations": [
            {
                "types": [item.value for item in combo],
                "count": count,
                "selected": combo[0].value,
            }
            for combo, count in overlaps.most_common(15)
        ],
    }


def _state_metrics(
    snapshots: Sequence[ReplaySnapshot],
    transition_rows: Sequence[Mapping[str, object]],
) -> Mapping[str, object]:
    states = Counter(snapshot.result.setup_state for snapshot in snapshots)
    forming = [
        snapshot
        for snapshot in snapshots
        if snapshot.result.setup_state is SetupState.FORMING
    ]
    confirming = [
        snapshot
        for snapshot in snapshots
        if snapshot.result.setup_state is SetupState.CONFIRMING
    ]

    def flags(items: Sequence[ReplaySnapshot]) -> Mapping[str, int]:
        return {
            "rows": len(items),
            "structure_true": sum(
                item.setup_input.structure_confirmation is True for item in items
            ),
            "correct_side_true": sum(
                item.setup_input.correct_side_of_level is True for item in items
            ),
            "hold_ge_1": sum(
                (item.setup_input.hold_candles or 0) >= 1 for item in items
            ),
            "retest_not_held": sum(
                item.setup_input.retest_detected and not item.setup_input.retest_held
                for item in items
            ),
            "compression": sum(
                item.setup_input.compression_detected is True for item in items
            ),
        }

    return {
        "state_counts": {state.value: states[state] for state in SetupState},
        "forming_fields": flags(forming),
        "confirming_fields": flags(confirming),
        "transitions": list(transition_rows),
    }


def _scan_metrics(
    scans: Sequence[ScanDiagnostic],
    runtime: RuntimeLogStats,
    false_rows: Sequence[Mapping[str, object]],
    snapshots: Sequence[ReplaySnapshot],
    config: DiagnosticConfig,
) -> Mapping[str, object]:
    counts = [scan.symbol_count for scan in scans]
    error_buckets = Counter(_error_bucket(scan.error_count) for scan in scans)
    contexts: dict[str, Callable[[Mapping[str, object]], bool]] = {
        "полный scan": lambda row: not cast(bool, row["Текущий scan неполный"]),
        "неполный scan": lambda row: cast(bool, row["Текущий scan неполный"]),
        "после MarketDataError": lambda row: row["Предыдущий scan с ошибками"] is True,
        "после пропуска symbol": lambda row: cast(
            bool, row["Symbol отсутствовал в предыдущем scan"]
        ),
    }
    context_metrics: dict[str, object] = {}
    for label, predicate in contexts.items():
        selected = [row for row in false_rows if predicate(row)]
        context_metrics[label] = {
            "false_breakouts": len(selected),
            "share_of_all_setups": (
                len(selected) / sum(counts) if counts and sum(counts) else None
            ),
            "returned_to_original_direction_share": (
                sum(
                    cast(bool, row["Вернулся к исходному направлению"])
                    for row in selected
                )
                / len(selected)
                if selected
                else None
            ),
            "outcomes": {
                str(horizon): _numeric_metrics(
                    cast(float, row[f"Outcome {horizon}м, %"])
                    for row in selected
                    if row[f"Outcome {horizon}м, %"] is not None
                )
                for horizon in (15, 30, 60)
            },
        }
    error_rows = [
        snapshot
        for snapshot in snapshots
        if snapshot.source.technical_error is not None
    ]
    previous_valid = _previous_valid_snapshots(snapshots)
    next_valid = _next_valid_snapshots(snapshots)
    comparable = [
        (snapshot, previous_valid[snapshot.line_number]) for snapshot in error_rows
    ]
    future = [(snapshot, next_valid[snapshot.line_number]) for snapshot in error_rows]
    episode_config = EpisodeAnalyzerConfig(gap_minutes=config.episode_gap_minutes)
    episodes_with_errors = build_episodes(snapshots, episode_config)
    snapshots_without_errors = tuple(
        snapshot for snapshot in snapshots if snapshot.source.technical_error is None
    )
    episodes_without_errors = build_episodes(snapshots_without_errors, episode_config)
    technical_impact = {
        "error_rows": len(error_rows),
        "comparable_with_previous_valid": sum(
            previous is not None for _, previous in comparable
        ),
        "active_counter_preserved": sum(
            previous is not None
            and snapshot.source.consecutive_active_scans
            == previous.source.consecutive_active_scans
            for snapshot, previous in comparable
        ),
        "ready_counter_preserved": sum(
            previous is not None
            and snapshot.source.consecutive_ready_scans
            == previous.source.consecutive_ready_scans
            for snapshot, previous in comparable
        ),
        "next_valid_available": sum(item is not None for _, item in future),
        "next_gap_at_least_30_minutes": sum(
            item is not None and (item.at - snapshot.at).total_seconds() / 60 >= 30
            for snapshot, item in future
        ),
        "next_has_reset_reason": sum(
            item is not None and item.source.reset_reason is not None
            for _, item in future
        ),
        "next_direction_changed": sum(
            previous_valid[snapshot.line_number] is not None
            and item is not None
            and item.result.direction
            is not cast(
                ReplaySnapshot, previous_valid[snapshot.line_number]
            ).result.direction
            for snapshot, item in future
        ),
        "next_false_breakout": sum(
            item is not None and item.result.setup_type is SetupType.FALSE_BREAKOUT
            for _, item in future
        ),
        "next_ready": sum(
            item is not None and item.result.setup_state is SetupState.READY_TO_CONSIDER
            for _, item in future
        ),
        "next_retest": sum(
            item is not None and item.result.setup_type is SetupType.RETEST
            for _, item in future
        ),
        "episodes_with_error_rows": len(episodes_with_errors),
        "episodes_without_error_rows": len(episodes_without_errors),
        "episode_delta_from_error_rows": len(episodes_with_errors)
        - len(episodes_without_errors),
    }
    return {
        "scan_count": len(scans),
        "scans_without_recorded_errors": sum(scan.error_count == 0 for scan in scans),
        "scans_with_errors": sum(scan.error_count > 0 for scan in scans),
        "instrument_count_median": statistics.median(counts) if counts else None,
        "instrument_count_min": min(counts) if counts else None,
        "instrument_count_max": max(counts) if counts else None,
        "error_buckets": dict(error_buckets),
        "sharp_drops": sum(scan.sharp_drop for scan in scans),
        "gaps_over_5_minutes": sum((scan.gap_minutes or 0) > 5.01 for scan in scans),
        "gaps_at_least_30_minutes": sum(
            (scan.gap_minutes or 0) >= 30 for scan in scans
        ),
        "technical_error_rows": sum(scan.error_count for scan in scans),
        "runtime": asdict(runtime),
        "false_breakout_context": context_metrics,
        "technical_error_impact": technical_impact,
    }


def _state_file_metrics(path: Path) -> Mapping[str, object]:
    if not path.exists():
        return {"exists": False}
    loaded = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(loaded, dict) or not isinstance(loaded.get("records"), dict):
        return {"exists": True, "valid": False}
    records = cast(dict[str, object], loaded["records"])
    reset_reasons: Counter[str] = Counter()
    for raw in records.values():
        if isinstance(raw, dict):
            reason = raw.get("reset_reason")
            if isinstance(reason, str):
                reset_reasons[reason] += 1
    return {
        "exists": True,
        "valid": True,
        "records": len(records),
        "reset_reasons": dict(reset_reasons.most_common()),
    }


def analyze_diagnostics(
    v2_audit: Path,
    runtime_log: Path = DEFAULT_RUNTIME_LOG,
    state_path: Path = DEFAULT_STATE,
    qtr_audit: Path = DEFAULT_QTR_AUDIT,
    config: DiagnosticConfig | None = None,
) -> DiagnosticAnalysis:
    if config is None:
        config = DiagnosticConfig()
    source_paths = (v2_audit, runtime_log, state_path, qtr_audit)
    hashes = {str(path): _sha256(path) for path in source_paths if path.exists()}
    audit = read_v2_audit(v2_audit)
    scans = build_scan_diagnostics(audit.snapshots, config)
    runtime = parse_runtime_log(runtime_log)
    false_rows = _false_rows(audit, scans, config)
    transition_rows = _transition_rows(audit.snapshots, config.episode_gap_minutes)
    no_trade_rows = _no_trade_rows(audit.snapshots)
    scan_rows = _scan_rows(scans)
    adapter_rows = adapter_mapping()
    conflict_rows = _conflict_rows(audit.snapshots)
    metrics: dict[str, object] = {
        "source": {
            "rows": len(audit.snapshots),
            "rejected_rows": len(audit.rejected),
            "period_start": (
                audit.snapshots[0].at.isoformat() if audit.snapshots else None
            ),
            "period_end": (
                audit.snapshots[-1].at.isoformat() if audit.snapshots else None
            ),
            "qtr_setup_engine_audit_exists": qtr_audit.exists(),
        },
        "false_breakout": {
            "count": len(false_rows),
            "share": (
                len(false_rows) / len(audit.snapshots) if audit.snapshots else None
            ),
            "groups": _group_false_metrics(false_rows),
        },
        "states": _state_metrics(audit.snapshots, transition_rows),
        "classification": _classification_metrics(audit.snapshots),
        "no_trade": {
            "actual": sum(
                snapshot.result.setup_type is SetupType.NO_TRADE
                for snapshot in audit.snapshots
            ),
            "production_gate": sum(
                candidate_setup_types(snapshot.setup_input) == (SetupType.NO_TRADE,)
                for snapshot in audit.snapshots
            ),
            "with_diagnostic_reasons": sum(
                bool(diagnostic_no_trade_reasons(snapshot))
                for snapshot in audit.snapshots
            ),
            "intercepted_by_other_type": sum(
                bool(diagnostic_no_trade_reasons(snapshot))
                and snapshot.result.setup_type is not SetupType.NO_TRADE
                for snapshot in audit.snapshots
            ),
            "intercepted_type_counts": {
                setup_type.value: count
                for setup_type, count in Counter(
                    snapshot.result.setup_type
                    for snapshot in audit.snapshots
                    if diagnostic_no_trade_reasons(snapshot)
                    and snapshot.result.setup_type is not SetupType.NO_TRADE
                ).most_common()
            },
            "reason_counts": dict(
                Counter(
                    reason
                    for snapshot in audit.snapshots
                    for reason in diagnostic_no_trade_reasons(snapshot)
                ).most_common()
            ),
        },
        "scans": _scan_metrics(
            scans,
            runtime,
            false_rows,
            audit.snapshots,
            config,
        ),
        "state_file": _state_file_metrics(state_path),
        "adapter": {
            "mapping_rows": len(adapter_rows),
            "material_fallbacks": [
                "component отсутствует → confirmation/liquidity/compression = False",
                "hold None → analyzer использует 0; completed_candles fallback 0/1",
                "technical_error → technical_data_complete=False и missing marker",
                "structure_confirmation вычисляется OR и для "
                "breakout_confirmed сразу True",
                "conflicting_confirmations не заполняется adapter и всегда False",
            ],
        },
        "limitations": [
            "Runtime log не содержит timestamp для каждой строки MarketDataError, "
            "поэтому fatal errors нельзя точно привязать к audit scan.",
            "Размер universe меняется; scan без technical_error означает только "
            "отсутствие записанной ошибки, а не доказанную полноту внешних данных.",
            "Outcomes используют только последующие audit snapshots, без "
            "intrabar high/low.",
            "qtr_setup_engine_audit.jsonl отсутствует; replay выполнен напрямую "
            "через production adapter и analyze_setup().",
        ],
    }
    return DiagnosticAnalysis(
        audit,
        scans,
        runtime,
        metrics,
        false_rows,
        transition_rows,
        no_trade_rows,
        scan_rows,
        adapter_rows,
        conflict_rows,
        hashes,
    )


def _write_csv(path: Path, rows: Sequence[Mapping[str, object]]) -> None:
    payload = list(rows) or [{"Сообщение": "Данные отсутствуют"}]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(payload[0]),
            delimiter=";",
            lineterminator="\n",
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(payload)


def _fmt(value: object) -> str:
    if value is None:
        return "н/д"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def _transition_count(
    rows: Sequence[Mapping[str, object]], previous: str, current: str
) -> int:
    return sum(
        cast(int, row["Количество"])
        for row in rows
        if row["Предыдущее состояние"] == previous and row["Новое состояние"] == current
    )


def _report(analysis: DiagnosticAnalysis) -> str:
    metrics = analysis.metrics
    source = cast(dict[str, object], metrics["source"])
    false = cast(dict[str, object], metrics["false_breakout"])
    groups = cast(dict[str, object], false["groups"])
    states = cast(dict[str, object], metrics["states"])
    state_counts = cast(dict[str, int], states["state_counts"])
    forming = cast(dict[str, int], states["forming_fields"])
    confirming = cast(dict[str, int], states["confirming_fields"])
    classification = cast(dict[str, object], metrics["classification"])
    overlaps = cast(dict[str, int], classification["special_overlaps"])
    no_trade = cast(dict[str, object], metrics["no_trade"])
    intercepted_types = cast(dict[str, int], no_trade["intercepted_type_counts"])
    scans = cast(dict[str, object], metrics["scans"])
    runtime = cast(dict[str, object], scans["runtime"])
    error_impact = cast(dict[str, object], scans["technical_error_impact"])
    false_context = cast(dict[str, object], scans["false_breakout_context"])
    incomplete_false = cast(dict[str, object], false_context["неполный scan"])[
        "false_breakouts"
    ]
    after_error_false = cast(dict[str, object], false_context["после MarketDataError"])[
        "false_breakouts"
    ]
    after_missing_false = cast(
        dict[str, object], false_context["после пропуска symbol"]
    )["false_breakouts"]
    error_buckets_json = json.dumps(scans["error_buckets"], ensure_ascii=False)
    context_lines: list[str] = []
    for label in (
        "полный scan",
        "неполный scan",
        "после MarketDataError",
        "после пропуска symbol",
    ):
        context = cast(dict[str, object], false_context[label])
        outcomes = cast(dict[str, object], context["outcomes"])
        parts: list[str] = []
        for horizon in (15, 30, 60):
            outcome = cast(dict[str, object], outcomes[str(horizon)])
            parts.append(
                f"{horizon}м n={outcome['available']}, "
                f"медиана {_fmt(outcome['median_pct'])}%"
            )
        context_lines.append(
            f"- {label}: {context['false_breakouts']} false breakout; "
            + "; ".join(parts)
            + "."
        )
    adapter = cast(dict[str, object], metrics["adapter"])
    forming_to_confirming = _transition_count(
        analysis.transition_rows, "ФОРМИРУЕТСЯ", "ПОДТВЕРЖДАЕТСЯ"
    )
    confirming_to_ready = _transition_count(
        analysis.transition_rows,
        "ПОДТВЕРЖДАЕТСЯ",
        "ГОТОВО К РАССМОТРЕНИЮ",
    )
    forming_to_ready = _transition_count(
        analysis.transition_rows, "ФОРМИРУЕТСЯ", "ГОТОВО К РАССМОТРЕНИЮ"
    )
    forming_to_cancelled = _transition_count(
        analysis.transition_rows, "ФОРМИРУЕТСЯ", "ОТМЕНЕНО"
    )
    confirming_to_cancelled = _transition_count(
        analysis.transition_rows, "ПОДТВЕРЖДАЕТСЯ", "ОТМЕНЕНО"
    )
    lines = [
        "# Итоговая диагностика Early Discovery V2 → QTR Setup Engine V1",
        "",
        "Диагностика выполнена офлайн на неизменённой production-логике: каждая "
        "валидная V2-строка прошла через существующие adapter и `analyze_setup()`.",
        "",
        "## Покрытие",
        "",
        f"- Строк: {source['rows']}; отклонено: {source['rejected_rows']}.",
        f"- Период: {source['period_start']} — {source['period_end']}.",
        f"- Scan: {scans['scan_count']}; без записанных ошибок: "
        f"{scans['scans_without_recorded_errors']}; с ошибками: "
        f"{scans['scans_with_errors']}.",
        f"- Runtime log: {runtime['market_data_error_messages']} сообщений "
        f"MarketDataError; распознано успешных summary: "
        f"{len(cast(list[object], runtime['completed_scans']))}.",
        "",
        "## A. Почему доминирует ЛОЖНЫЙ ПРОБОЙ",
        "",
        f"ЛОЖНЫЙ ПРОБОЙ выбран в {false['count']} строках "
        f"({_fmt(false['share'])} от всех replay-строк).",
        "",
    ]
    for code in FALSE_GROUP_RU:
        group = cast(dict[str, object], groups[code])
        outcomes = cast(dict[str, object], group["outcomes"])
        outcome_15 = cast(dict[str, object], outcomes["15"])
        lines.append(
            f"- {group['label']}: {group['count']} "
            f"({_fmt(group['share_of_false'])}); 15м n={outcome_15['available']}, "
            f"медиана {_fmt(outcome_15['median_pct'])}%."
        )
    lines.extend(
        [
            "",
            "Доказанный механизм: V2 задаёт `breakout_failure = not correct_side` "
            "по текущему close. Adapter дополнительно позволяет false breakout при "
            "`returned_inside_range and breakout_confirmed`. Поэтому recovered "
            "RETEST_HELD остаётся кандидатом ЛОЖНЫЙ ПРОБОЙ и выигрывает приоритет.",
            "",
            "## B. ФОРМИРУЕТСЯ и ПОДТВЕРЖДАЕТСЯ",
            "",
            f"- ФОРМИРУЕТСЯ: {state_counts['FORMING']}; ПОДТВЕРЖДАЕТСЯ: "
            f"{state_counts['CONFIRMING']}; ГОТОВО: "
            f"{state_counts['READY_TO_CONSIDER']}.",
            f"- В FORMING retest без удержания: {forming['retest_not_held']}; "
            f"compression: {forming['compression']}.",
            f"- В FORMING structure=true: {forming['structure_true']}, "
            f"correct_side=true: {forming['correct_side_true']}, hold>=1: "
            f"{forming['hold_ge_1']}.",
            f"- В CONFIRMING structure=true: {confirming['structure_true']}, "
            f"correct_side=true: {confirming['correct_side_true']}, hold>=1: "
            f"{confirming['hold_ge_1']}.",
            "",
            "Adapter задаёт `structure_confirmation = breakout_confirmed OR "
            "retest_held OR reversal_detected`; сам `breakout_confirmed` уже требует "
            "correct_side и hold>=1. Поэтому обычный подтверждённый breakout сразу "
            "выполняет ветку CONFIRMING. FORMING доступен для RETEST без hold, "
            "COMPRESSION или fallback `any(structure, volume, volatility)`; на "
            "этой выборке единственная строка попала туда через fallback structure.",
            "",
            "### Ключевые переходы",
            "",
            f"- ФОРМИРУЕТСЯ → ПОДТВЕРЖДАЕТСЯ: {forming_to_confirming}.",
            f"- ПОДТВЕРЖДАЕТСЯ → ГОТОВО: {confirming_to_ready}.",
            f"- ФОРМИРУЕТСЯ → ГОТОВО: {forming_to_ready}.",
            f"- ФОРМИРУЕТСЯ → ОТМЕНЕНО: {forming_to_cancelled}.",
            f"- ПОДТВЕРЖДАЕТСЯ → ОТМЕНЕНО: {confirming_to_cancelled}.",
            "",
            "## C. Почему НЕТ СДЕЛКИ почти отсутствует",
            "",
            f"- Production gate теоретически достигнут: {no_trade['production_gate']}.",
            f"- Реально НЕТ СДЕЛКИ: {no_trade['actual']}.",
            f"- Строк с диагностическими причинами: "
            f"{no_trade['with_diagnostic_reasons']}; из них перехвачено другими "
            f"типами: {no_trade['intercepted_by_other_type']}.",
            "- Перехват по типам: "
            + ", ".join(
                f"{TYPE_RU[SetupType(name)]}: {count}"
                for name, count in intercepted_types.items()
            )
            + ".",
            "",
            "В текущем replay gate NO_TRADE работает: missing/technical/NEUTRAL "
            "строки доходят до него. Но для полной направленной строки почти всегда "
            "истинен breakout/retest/false-breakout кандидат; fallback NO_TRADE "
            "практически недостижим.",
            "",
            "## D. Влияние MarketDataError и неполных scan",
            "",
            f"- Инструментов: медиана {scans['instrument_count_median']}, минимум "
            f"{scans['instrument_count_min']}, максимум "
            f"{scans['instrument_count_max']}.",
            f"- Technical-error rows: {scans['technical_error_rows']}; scan с "
            f"резким падением universe: {scans['sharp_drops']}.",
            f"- Error buckets: {error_buckets_json}.",
            f"- Паузы >5 минут: {scans['gaps_over_5_minutes']}; паузы >=30 минут: "
            f"{scans['gaps_at_least_30_minutes']}.",
            f"- Error rows с предыдущим валидным снимком: "
            f"{error_impact['comparable_with_previous_valid']}; active counter "
            f"сохранён: {error_impact['active_counter_preserved']}; ready counter "
            f"сохранён: {error_impact['ready_counter_preserved']}.",
            f"- После error следующий валидный снимок стал ЛОЖНЫЙ ПРОБОЙ: "
            f"{error_impact['next_false_breakout']}; РЕТЕСТ: "
            f"{error_impact['next_retest']}; READY: {error_impact['next_ready']}.",
            f"- После error gap до следующего валидного снимка >=30 минут: "
            f"{error_impact['next_gap_at_least_30_minutes']}; смен направления: "
            f"{error_impact['next_direction_changed']}.",
            f"- Offline episodes с error rows: "
            f"{error_impact['episodes_with_error_rows']}; без error rows: "
            f"{error_impact['episodes_without_error_rows']}; разница: "
            f"{error_impact['episode_delta_from_error_rows']}.",
            f"- ЛОЖНЫЙ ПРОБОЙ в неполном текущем scan: "
            f"{incomplete_false}; после MarketDataError scan: "
            f"{after_error_false}; после пропуска symbol: {after_missing_false}.",
            *context_lines,
            "",
            "Technical-error row сохраняет counters и не обновляет tracker: это "
            "защищает от немедленного ложного reset. Однако длинная пауза до "
            "следующего валидного наблюдения может запустить обычный gap-reset. "
            "Fatal MarketDataError из runtime log не имеют timestamp, поэтому их "
            "точное причинное влияние на конкретный false breakout не доказуемо.",
            "",
            "## E. Adapter",
            "",
            f"Карта содержит {adapter['mapping_rows']} полей. Существенные fallback:",
        ]
    )
    lines.extend(
        f"- {item}." for item in cast(list[str], adapter["material_fallbacks"])
    )
    lines.extend(
        [
            "",
            "Главный adapter-эффект для состояний — derived "
            "`structure_confirmation`; главный эффект для ЛОЖНОГО ПРОБОЯ — "
            "передача `returned_inside_range` вместе с recovered "
            "`breakout_confirmed`.",
            "",
            "## F. Приоритет классификации",
            "",
            f"- ЛОЖНЫЙ ПРОБОЙ + РЕТЕСТ: {overlaps['FALSE_BREAKOUT+RETEST']}.",
            f"- ЛОЖНЫЙ ПРОБОЙ + ПРОБОЙ: {overlaps['FALSE_BREAKOUT+BREAKOUT']}.",
            f"- РЕТЕСТ + ПРОБОЙ: {overlaps['RETEST+BREAKOUT']}.",
            f"- РАЗВОРОТ + ЛОЖНЫЙ ПРОБОЙ: {overlaps['REVERSAL+FALSE_BREAKOUT']}.",
            "",
            "Высокий приоритет объясняет выбор типа при overlap, но не создаёт "
            "сам предикат: первопричина — широкое false-breakout условие, особенно "
            "исторический returned_inside_range после восстановления.",
            "",
            "## G. Что проверять первым после диагностики",
            "",
            "1. Отдельно валидировать recovered `returned_inside_range + "
            "RETEST_HELD` на более длинной выборке.",
            "2. Затем исследовать независимый критерий FORMING, не используя "
            "уже достаточный для CONFIRMING `breakout_confirmed`.",
            "3. После этого оценивать семантику diagnostic NO_TRADE и только затем "
            "калибровать. В этой задаче код и пороги не изменялись.",
            "",
            "## Что не доказано",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in cast(list[str], metrics["limitations"]))
    return "\n".join(lines) + "\n"


def _recommendations(analysis: DiagnosticAnalysis) -> str:
    false = cast(dict[str, object], analysis.metrics["false_breakout"])
    no_trade = cast(dict[str, object], analysis.metrics["no_trade"])
    return (
        "# Рекомендации без изменения production-кода\n\n"
        f"Диагностика зафиксировала {false['count']} классификаций ЛОЖНЫЙ ПРОБОЙ "
        f"и {no_trade['actual']} НЕТ СДЕЛКИ.\n\n"
        "1. Накопить несколько независимых рыночных периодов и повторить те же "
        "группы A–G.\n"
        "2. При последующей отдельной задаче сначала проверить recovered cases "
        "`returned_inside_range + RETEST_HELD`, не меняя другие ветки.\n"
        "3. Отдельным экспериментом исследовать разграничение FORMING/CONFIRMING.\n"
        "4. Добавить timestamped structured runtime telemetry до любых выводов о "
        "причинности MarketDataError.\n"
        "5. Не выполнять Calibration V3 по этой единственной выборке.\n"
    )


def write_outputs(analysis: DiagnosticAnalysis, output: Path) -> OutputFiles:
    output.mkdir(parents=True, exist_ok=True)
    files = OutputFiles(
        output / "итоговая_диагностика.md",
        output / "ложные_пробои.csv",
        output / "переходы_состояний.csv",
        output / "диагностика_нет_сделки.csv",
        output / "влияние_ошибок_данных.csv",
        output / "карта_adapter.csv",
        output / "конфликты_классификации.csv",
        output / "метрики.json",
        output / "рекомендации_без_изменения_кода.md",
    )
    _write_csv(files.false_breakouts, analysis.false_rows)
    _write_csv(files.transitions, analysis.transition_rows)
    _write_csv(files.no_trade, analysis.no_trade_rows)
    _write_csv(files.data_errors, analysis.scan_rows)
    _write_csv(files.adapter_map, analysis.adapter_rows)
    _write_csv(files.conflicts, analysis.conflict_rows)
    files.metrics.write_text(
        json.dumps(analysis.metrics, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    files.report.write_text(_report(analysis), encoding="utf-8")
    files.recommendations.write_text(_recommendations(analysis), encoding="utf-8")
    return files


def sources_unchanged(analysis: DiagnosticAnalysis) -> bool:
    return all(
        Path(path).exists() and _sha256(Path(path)) == digest
        for path, digest in analysis.source_hashes.items()
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Офлайн-диагностика Early Discovery V2 → Setup Engine V1."
    )
    parser.add_argument("--v2-audit", type=Path, default=DEFAULT_V2_AUDIT)
    parser.add_argument("--runtime-log", type=Path, default=DEFAULT_RUNTIME_LOG)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--qtr-audit", type=Path, default=DEFAULT_QTR_AUDIT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--episode-gap-minutes", type=int, default=30)
    parser.add_argument("--outcome-tolerance-minutes", type=int, default=10)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    analysis = analyze_diagnostics(
        args.v2_audit,
        args.runtime_log,
        args.state,
        args.qtr_audit,
        DiagnosticConfig(
            episode_gap_minutes=args.episode_gap_minutes,
            outcome_tolerance_minutes=args.outcome_tolerance_minutes,
        ),
    )
    files = write_outputs(analysis, args.output)
    false = cast(dict[str, object], analysis.metrics["false_breakout"])
    scans = cast(dict[str, object], analysis.metrics["scans"])
    print(
        f"Проанализировано строк: {len(analysis.audit.snapshots)}; "
        f"scan: {scans['scan_count']}; ЛОЖНЫЙ ПРОБОЙ: {false['count']}."
    )
    print(f"Итоговая диагностика: {files.report}")
    if not sources_unchanged(analysis):
        print(
            "Внимание: один из источников изменился внешним процессом во время анализа."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
