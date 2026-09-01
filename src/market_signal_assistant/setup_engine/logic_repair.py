from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

from market_signal_assistant.inplay.early_discovery import MarketDirection
from market_signal_assistant.inplay.early_discovery_v2 import (
    EarlyDiscoveryV2Result,
    RetestState,
)
from market_signal_assistant.setup_engine.adapters import (
    MINIMUM_COMPRESSION_FACTOR,
    MINIMUM_CONFIRMATION_FACTOR,
    MINIMUM_LIQUIDITY_FACTOR,
)
from market_signal_assistant.setup_engine.analyzer import (
    LATE_PRICE_CHANGE_PCT,
    MAXIMUM_FRESH_BREAKOUT_AGE_BARS,
    MAXIMUM_READY_DISTANCE_ATR,
    MAXIMUM_READY_SPREAD_PCT,
    MINIMUM_COMPLETED_CANDLES,
    MINIMUM_READY_HOLD_CANDLES,
)
from market_signal_assistant.setup_engine.models import (
    SETUP_CLASSIFICATION_PRIORITY,
    SetupAnalysisInput,
    SetupDirection,
    SetupState,
    SetupType,
)
from market_signal_assistant.setup_engine.offline_analyzer import (
    DEFAULT_INPUT,
    ReplaySnapshot,
    read_v2_audit,
)

DEFAULT_OUTPUT_DIR = (
    Path(__file__).resolve().parents[3] / "data" / "setup_engine_logic_repair"
)


@dataclass(frozen=True, slots=True)
class ClassificationProjection:
    setup_type: SetupType
    setup_state: SetupState
    trade_eligible: bool
    candidates: tuple[SetupType, ...]


@dataclass(frozen=True, slots=True)
class ReplayComparison:
    source_path: Path
    source_sha256_before: str
    source_sha256_after: str
    snapshots: tuple[ReplaySnapshot, ...]
    legacy_inputs: tuple[SetupAnalysisInput, ...]
    before: tuple[ClassificationProjection, ...]
    after: tuple[ClassificationProjection, ...]
    rejected_lines: int


def compare_audit(path: Path) -> ReplayComparison:
    audit = read_v2_audit(path)
    legacy_inputs = tuple(_legacy_input(item.source) for item in audit.snapshots)
    before = tuple(_legacy_projection(item) for item in legacy_inputs)
    after = tuple(
        ClassificationProjection(
            item.result.setup_type,
            item.result.setup_state,
            item.result.trade_eligible,
            item.result.classification_candidates,
        )
        for item in audit.snapshots
    )
    return ReplayComparison(
        path,
        audit.sha256_before,
        audit.sha256_after,
        audit.snapshots,
        legacy_inputs,
        before,
        after,
        len(audit.rejected),
    )


def write_comparison(
    comparison: ReplayComparison,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> tuple[Path, Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = output_dir / "метрики_до_после.json"
    changed_path = output_dir / "изменённые_классификации.csv"
    transitions_path = output_dir / "проверка_переходов.csv"
    report_path = output_dir / "сравнение_до_после.md"
    metrics = _metrics(comparison)
    metrics_path.write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _write_changed(comparison, changed_path)
    _write_transitions(comparison, transitions_path)
    report_path.write_text(_report(metrics), encoding="utf-8")
    return report_path, metrics_path, changed_path, transitions_path


def _legacy_input(result: EarlyDiscoveryV2Result) -> SetupAnalysisInput:
    direction = {
        MarketDirection.UP: SetupDirection.UP,
        MarketDirection.DOWN: SetupDirection.DOWN,
        MarketDirection.NEUTRAL: SetupDirection.NEUTRAL,
    }[result.market_direction]
    failure = result.breakout_failure is True
    hold = result.breakout_hold_candles
    correct = result.is_correct_side_of_level
    breakout = bool(
        result.breakout_level is not None
        and result.current_price is not None
        and correct is True
        and (hold or 0) >= 1
        and not failure
    )
    retest = bool(
        result.returned_to_level
        or result.retest_state
        in {RetestState.RETEST_IN_PROGRESS, RetestState.RETEST_HELD}
    )
    retest_held = result.retest_state is RetestState.RETEST_HELD
    volume = bool(
        _legacy_factor(result, "volume_acceleration") >= MINIMUM_CONFIRMATION_FACTOR
        or _legacy_factor(result, "breakout_volume") >= MINIMUM_CONFIRMATION_FACTOR
    )
    volatility = bool(
        _legacy_factor(result, "atr_expansion") >= MINIMUM_CONFIRMATION_FACTOR
    )
    liquidity = bool(_legacy_factor(result, "liquidity") >= MINIMUM_LIQUIDITY_FACTOR)
    compression = bool(
        _legacy_factor(result, "compression") >= MINIMUM_COMPRESSION_FACTOR
    )
    continuation = bool(
        breakout
        and (result.breakout_age_bars or 0) >= 2
        and (hold or 0) >= 2
        and not result.returned_to_level
    )
    reversal = bool(
        result.direction_v1 in {MarketDirection.UP, MarketDirection.DOWN}
        and result.direction_v2 in {MarketDirection.UP, MarketDirection.DOWN}
        and result.direction_v1 is not result.direction_v2
    )
    return SetupAnalysisInput(
        snapshot_ids=(result.scan_id,),
        source="early_discovery_v2",
        symbol=result.symbol,
        analyzed_at=result.scanned_at,
        direction=direction,
        current_price=result.current_price,
        trigger_level=result.breakout_level,
        price_change_24h_pct=result.price_change_24h_pct,
        distance_to_trigger_atr=result.absolute_distance_atr,
        breakout_age_bars=result.breakout_age_bars,
        hold_candles=hold,
        breakout_confirmed=breakout,
        correct_side_of_level=correct,
        returned_inside_range=result.returned_inside_range is True,
        retest_detected=retest,
        retest_held=retest_held,
        breakout_failed=failure,
        volume_confirmation=volume,
        volatility_confirmation=volatility,
        structure_confirmation=bool(breakout or retest_held or reversal),
        liquidity_ok=liquidity,
        spread_pct=result.spread_pct,
        compression_detected=compression,
        continuation_detected=continuation,
        reversal_detected=reversal,
        conflicting_confirmations=False,
        completed_candles=max(hold or 0, 1 if breakout else 0),
        technical_data_complete=result.technical_error is None,
        extra_missing_data=(
            ("early_discovery_v2_technical_error",)
            if result.technical_error is not None
            else ()
        ),
    )


def _legacy_factor(result: EarlyDiscoveryV2Result, component_id: str) -> float:
    return max(
        (
            item.points / item.maximum_points
            for item in result.component_scores
            if item.component_id == component_id and item.maximum_points > 0
        ),
        default=0.0,
    )


def _legacy_missing(data: SetupAnalysisInput) -> tuple[str, ...]:
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


def _legacy_candidates(data: SetupAnalysisInput) -> tuple[SetupType, ...]:
    missing = _legacy_missing(data)
    if (
        missing
        or data.direction is SetupDirection.NEUTRAL
        or data.conflicting_confirmations
    ):
        return (SetupType.NO_TRADE,)
    predicates = {
        SetupType.FALSE_BREAKOUT: bool(
            data.breakout_failed
            or (data.returned_inside_range and data.breakout_confirmed)
        ),
        SetupType.REVERSAL: data.reversal_detected,
        SetupType.RETEST: data.retest_detected,
        SetupType.BREAKOUT: data.breakout_confirmed,
        SetupType.CONTINUATION: data.continuation_detected,
        SetupType.IMPULSE: bool(
            data.volume_confirmation is True and data.volatility_confirmation is True
        ),
        SetupType.COMPRESSION: bool(
            data.compression_detected
            and not (
                data.volume_confirmation is True
                and data.volatility_confirmation is True
            )
        ),
    }
    candidates = tuple(
        item
        for item in SETUP_CLASSIFICATION_PRIORITY
        if item is not SetupType.NO_TRADE and predicates[item]
    )
    return candidates or (SetupType.NO_TRADE,)


def _legacy_projection(data: SetupAnalysisInput) -> ClassificationProjection:
    missing = _legacy_missing(data)
    candidates = _legacy_candidates(data)
    setup_type = candidates[0]
    hold = data.hold_candles or 0
    freshness = bool(
        data.breakout_age_bars is not None
        and data.breakout_age_bars <= MAXIMUM_FRESH_BREAKOUT_AGE_BARS
    )
    distance = abs(data.distance_to_trigger_atr or 0.0)
    late = bool(
        abs(data.price_change_24h_pct or 0.0) >= LATE_PRICE_CHANGE_PCT
        or distance > MAXIMUM_READY_DISTANCE_ATR
    )
    ready = bool(
        setup_type
        not in {SetupType.NO_TRADE, SetupType.FALSE_BREAKOUT, SetupType.COMPRESSION}
        and not missing
        and data.direction is not SetupDirection.NEUTRAL
        and not data.breakout_failed
        and not data.conflicting_confirmations
        and not late
        and data.spread_pct is not None
        and data.spread_pct <= MAXIMUM_READY_SPREAD_PCT
        and data.liquidity_ok is True
        and data.structure_confirmation is True
        and data.volume_confirmation is True
        and data.volatility_confirmation is True
        and data.correct_side_of_level is True
        and freshness
        and hold >= MINIMUM_READY_HOLD_CANDLES
        and data.completed_candles >= MINIMUM_COMPLETED_CANDLES
        and (setup_type is not SetupType.RETEST or data.retest_held)
        and (setup_type is not SetupType.BREAKOUT or data.breakout_confirmed)
    )
    if setup_type is SetupType.NO_TRADE:
        state = (
            SetupState.CANCELLED
            if missing or data.conflicting_confirmations
            else SetupState.WATCHING
        )
    elif setup_type is SetupType.FALSE_BREAKOUT:
        state = SetupState.CANCELLED
    elif late:
        state = SetupState.LATE
    elif ready:
        state = SetupState.READY_TO_CONSIDER
    elif setup_type is SetupType.COMPRESSION or (
        setup_type is SetupType.RETEST and not data.retest_held
    ):
        state = SetupState.FORMING
    elif (
        data.structure_confirmation is True
        and data.correct_side_of_level is True
        and hold >= 1
    ):
        state = SetupState.CONFIRMING
    elif any(
        (
            data.structure_confirmation is True,
            data.volume_confirmation is True,
            data.volatility_confirmation is True,
        )
    ):
        state = SetupState.FORMING
    else:
        state = SetupState.WATCHING
    return ClassificationProjection(setup_type, state, ready, candidates)


def _metrics(comparison: ReplayComparison) -> dict[str, Any]:
    before_types = Counter(item.setup_type.value for item in comparison.before)
    after_types = Counter(item.setup_type.value for item in comparison.after)
    before_states = Counter(item.setup_state.value for item in comparison.before)
    after_states = Counter(item.setup_state.value for item in comparison.after)
    total = len(comparison.snapshots)
    before_safety = _safety_count(comparison.before, comparison.legacy_inputs)
    after_inputs = tuple(item.setup_input for item in comparison.snapshots)
    after_safety = _safety_count(comparison.after, after_inputs)
    return {
        "source": {
            "path": str(comparison.source_path),
            "sha256_before": comparison.source_sha256_before,
            "sha256_after": comparison.source_sha256_after,
            "unchanged": comparison.source_sha256_before
            == comparison.source_sha256_after,
            "rows": total,
            "rejected_lines": comparison.rejected_lines,
        },
        "before": _metric_side(
            total, before_types, before_states, comparison.before, before_safety
        ),
        "after": _metric_side(
            total, after_types, after_states, comparison.after, after_safety
        ),
        "changed_rows": sum(
            old != new
            for old, new in zip(comparison.before, comparison.after, strict=True)
        ),
        "recovered_historical_false_breakouts": sum(
            item.result.historical_breakout_failure
            and item.result.structure_recovered
            and not item.result.current_breakout_failure
            for item in comparison.snapshots
        ),
        "technical_error_rows": sum(
            item.source.technical_error is not None for item in comparison.snapshots
        ),
    }


def _metric_side(
    total: int,
    types: Mapping[str, int],
    states: Mapping[str, int],
    projections: Sequence[ClassificationProjection],
    safety_violations: int,
) -> dict[str, Any]:
    return {
        "setup_type_counts": dict(sorted(types.items())),
        "setup_type_shares": {
            name: count / total if total else None
            for name, count in sorted(types.items())
        },
        "setup_state_counts": dict(sorted(states.items())),
        "trade_eligible_false": sum(not item.trade_eligible for item in projections),
        "trade_eligible_true": sum(item.trade_eligible for item in projections),
        "classification_conflicts": sum(
            len(item.candidates) > 1 for item in projections
        ),
        "ready_safety_violations": safety_violations,
    }


def _safety_count(
    projections: Sequence[ClassificationProjection],
    inputs: Sequence[SetupAnalysisInput],
) -> int:
    violations = 0
    for projection, data in zip(projections, inputs, strict=True):
        if not projection.trade_eligible:
            continue
        hold = data.hold_candles or 0
        freshness = bool(
            data.breakout_age_bars is not None
            and data.breakout_age_bars <= MAXIMUM_FRESH_BREAKOUT_AGE_BARS
        )
        unsafe = bool(
            data.direction is SetupDirection.NEUTRAL
            or data.breakout_failed
            or abs(data.price_change_24h_pct or 0.0) >= LATE_PRICE_CHANGE_PCT
            or abs(data.distance_to_trigger_atr or 0.0) > MAXIMUM_READY_DISTANCE_ATR
            or data.spread_pct is None
            or data.spread_pct > MAXIMUM_READY_SPREAD_PCT
            or data.liquidity_ok is not True
            or data.structure_confirmation is not True
            or data.volume_confirmation is not True
            or data.volatility_confirmation is not True
            or data.correct_side_of_level is not True
            or not freshness
            or hold < MINIMUM_READY_HOLD_CANDLES
            or data.completed_candles < MINIMUM_COMPLETED_CANDLES
            or not data.technical_data_complete
        )
        violations += unsafe
    return violations


def _write_changed(comparison: ReplayComparison, path: Path) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.writer(stream, delimiter=";")
        writer.writerow(
            (
                "Строка",
                "Символ",
                "Время UTC",
                "Тип до",
                "Тип после",
                "Состояние до",
                "Состояние после",
                "Допустимо до",
                "Допустимо после",
                "Текущий провал",
                "Исторический провал",
                "Структура восстановлена",
                "Кандидаты после",
            )
        )
        for snapshot, old, new in zip(
            comparison.snapshots, comparison.before, comparison.after, strict=True
        ):
            if old == new:
                continue
            writer.writerow(
                (
                    snapshot.line_number,
                    snapshot.result.symbol,
                    snapshot.at.isoformat(),
                    old.setup_type.name_ru,
                    new.setup_type.name_ru,
                    old.setup_state.name_ru,
                    new.setup_state.name_ru,
                    old.trade_eligible,
                    new.trade_eligible,
                    snapshot.result.current_breakout_failure,
                    snapshot.result.historical_breakout_failure,
                    snapshot.result.structure_recovered,
                    ", ".join(item.name_ru for item in new.candidates),
                )
            )


def _write_transitions(comparison: ReplayComparison, path: Path) -> None:
    before = _transition_counts(comparison, comparison.before, skip_errors=False)
    after = _transition_counts(comparison, comparison.after, skip_errors=True)
    keys = sorted(set(before) | set(after), key=lambda item: (item[0], item[1]))
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.writer(stream, delimiter=";")
        writer.writerow(("Предыдущее состояние", "Новое состояние", "До", "После"))
        for previous, current in keys:
            writer.writerow(
                (
                    previous.name_ru,
                    current.name_ru,
                    before[(previous, current)],
                    after[(previous, current)],
                )
            )


def _transition_counts(
    comparison: ReplayComparison,
    projections: Sequence[ClassificationProjection],
    *,
    skip_errors: bool,
) -> Counter[tuple[SetupState, SetupState]]:
    grouped: dict[str, list[tuple[ReplaySnapshot, ClassificationProjection]]] = (
        defaultdict(list)
    )
    for snapshot, projection in zip(comparison.snapshots, projections, strict=True):
        if skip_errors and snapshot.source.technical_error is not None:
            continue
        grouped[snapshot.result.symbol].append((snapshot, projection))
    counts: Counter[tuple[SetupState, SetupState]] = Counter()
    for items in grouped.values():
        ordered = sorted(items, key=lambda item: item[0].at)
        for previous, current in zip(ordered, ordered[1:], strict=False):
            if current[0].at - previous[0].at <= timedelta(minutes=30):
                counts[(previous[1].setup_state, current[1].setup_state)] += 1
    return counts


def _report(metrics: Mapping[str, Any]) -> str:
    before = metrics["before"]
    after = metrics["after"]
    assert isinstance(before, dict) and isinstance(after, dict)
    source = metrics["source"]
    assert isinstance(source, dict)
    before_types = cast(Mapping[str, int], before["setup_type_counts"])
    after_types = cast(Mapping[str, int], after["setup_type_counts"])
    before_states = cast(Mapping[str, int], before["setup_state_counts"])
    after_states = cast(Mapping[str, int], after["setup_state_counts"])
    return "\n".join(
        (
            "# Logic Repair V1: сравнение до/после",
            "",
            "Replay выполнен офлайн на одном неизменённом снимке V2 audit.",
            f"SHA-256: `{source['sha256_before']}`; строк: {source['rows']}.",
            "",
            "## Семантические исправления",
            "",
            "- ЛОЖНЫЙ ПРОБОЙ означает текущий подтверждённый провал; "
            "исторический провал хранится отдельно.",
            "- Правильная сторона уровня вместе с удержанным ретестом снимает "
            "исторический false breakout с текущей классификации.",
            "- Тип конструкции отделён от trade eligibility и явных причин НЕТ СДЕЛКИ.",
            "- UNKNOWN поля adapter сохраняются как null/missing, а не как False "
            "или нулевое удержание.",
            "- Technical-error строки остаются в audit, но не создают рыночные "
            "эпизоды и переходы.",
            "",
            "## Метрики",
            "",
            _count_line("ЛОЖНЫЙ ПРОБОЙ", before_types, after_types, "FALSE_BREAKOUT"),
            _count_line("РЕТЕСТ", before_types, after_types, "RETEST"),
            _count_line("ПРОБОЙ", before_types, after_types, "BREAKOUT"),
            _count_line("ФОРМИРУЕТСЯ", before_states, after_states, "FORMING"),
            _count_line("ПОДТВЕРЖДАЕТСЯ", before_states, after_states, "CONFIRMING"),
            _count_line("ГОТОВО", before_states, after_states, "READY_TO_CONSIDER"),
            _value_line("trade_eligible=false", before, after, "trade_eligible_false"),
            _value_line(
                "Конфликтные классификации",
                before,
                after,
                "classification_conflicts",
            ),
            _value_line(
                "Нарушения safety у READY",
                before,
                after,
                "ready_safety_violations",
            ),
            "",
            "Числовые веса и пороги V2, 15%/30%, spread 0,2% и distance "
            "2 ATR не изменялись.",
            "",
        )
    )


def _count_line(
    label: str,
    before: Mapping[str, int],
    after: Mapping[str, int],
    key: str,
) -> str:
    return f"- {label}: {before.get(key, 0)} → {after.get(key, 0)}."


def _value_line(
    label: str,
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    key: str,
) -> str:
    return f"- {label}: {before[key]} → {after[key]}."


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline Logic Repair V1 replay")
    parser.add_argument("--audit", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    write_comparison(compare_audit(args.audit), args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
