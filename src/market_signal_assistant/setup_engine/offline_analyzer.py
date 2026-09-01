from __future__ import annotations

import argparse
import csv
import hashlib
import json
import statistics
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, TypeVar, cast

from market_signal_assistant.inplay.early_discovery import (
    DiscoveryStage,
    MarketDirection,
)
from market_signal_assistant.inplay.early_discovery_v2 import (
    EarlyDiscoveryV2Result,
    RetestState,
    ScoreComponent,
)
from market_signal_assistant.setup_engine.adapters import (
    input_from_early_discovery_v2,
)
from market_signal_assistant.setup_engine.analyzer import analyze_setup
from market_signal_assistant.setup_engine.models import (
    SetupAnalysisInput,
    SetupAnalysisResult,
    SetupDirection,
    SetupState,
    SetupType,
)

DEFAULT_INPUT = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "inplay_early_discovery_v2_audit.jsonl"
)
DEFAULT_OUTPUT = Path(__file__).resolve().parents[3] / "data" / "setup_engine_analysis"
HORIZONS = (15, 30, 60, 180)
STATE_RANK = {
    SetupState.WATCHING: 0,
    SetupState.FORMING: 1,
    SetupState.CONFIRMING: 2,
    SetupState.READY_TO_CONSIDER: 3,
    SetupState.LATE: 1,
    SetupState.CANCELLED: 0,
}


@dataclass(frozen=True, slots=True)
class AnalyzerConfig:
    gap_minutes: int = 30
    no_trade_scans_to_close: int = 2
    outcome_tolerance_minutes: int = 10

    def __post_init__(self) -> None:
        if self.gap_minutes < 1:
            raise ValueError("gap_minutes должен быть положительным")
        if self.no_trade_scans_to_close < 1:
            raise ValueError("no_trade_scans_to_close должен быть положительным")
        if self.outcome_tolerance_minutes < 0:
            raise ValueError("outcome_tolerance_minutes не может быть отрицательным")


@dataclass(frozen=True, slots=True)
class RejectedLine:
    line_number: int
    reason: str


@dataclass(frozen=True, slots=True)
class ReplaySnapshot:
    line_number: int
    source: EarlyDiscoveryV2Result
    setup_input: SetupAnalysisInput
    result: SetupAnalysisResult

    @property
    def at(self) -> datetime:
        return self.source.scanned_at


@dataclass(frozen=True, slots=True)
class AuditReadResult:
    snapshots: tuple[ReplaySnapshot, ...]
    total_lines: int
    empty_lines: int
    rejected: tuple[RejectedLine, ...]
    sha256_before: str
    sha256_after: str
    observed_fields: tuple[str, ...]


@dataclass(slots=True)
class Episode:
    number: int
    snapshots: list[ReplaySnapshot] = field(default_factory=list)

    @property
    def first(self) -> ReplaySnapshot:
        return self.snapshots[0]

    @property
    def last(self) -> ReplaySnapshot:
        return self.snapshots[-1]

    @property
    def ready(self) -> ReplaySnapshot | None:
        return next(
            (
                item
                for item in self.snapshots
                if item.result.setup_state is SetupState.READY_TO_CONSIDER
            ),
            None,
        )


@dataclass(frozen=True, slots=True)
class HorizonOutcome:
    horizon_minutes: int
    observed_at: datetime
    directed_return_pct: float
    max_favorable_observed_pct: float
    max_adverse_observed_pct: float


@dataclass(frozen=True, slots=True)
class AnalysisResult:
    audit: AuditReadResult
    episodes: tuple[Episode, ...]
    episode_outcomes: Mapping[int, Mapping[int, HorizonOutcome]]
    metrics: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class OutputFiles:
    report: Path
    episodes: Path
    metrics: Path
    best: Path
    worst: Path
    no_trade: Path
    recommendations: Path


EnumT = TypeVar("EnumT")


def _required(raw: Mapping[str, Any], name: str) -> Any:
    if name not in raw:
        raise ValueError(f"отсутствует поле {name}")
    return raw[name]


def _str(raw: Mapping[str, Any], name: str) -> str:
    value = _required(raw, name)
    if not isinstance(value, str):
        raise ValueError(f"поле {name} должно быть строкой")
    return value


def _optional_str(raw: Mapping[str, Any], name: str) -> str | None:
    value = _required(raw, name)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"поле {name} должно быть строкой или null")
    return value


def _int(raw: Mapping[str, Any], name: str) -> int:
    value = _required(raw, name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"поле {name} должно быть целым")
    return value


def _optional_int(raw: Mapping[str, Any], name: str) -> int | None:
    value = _required(raw, name)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"поле {name} должно быть целым или null")
    return value


def _float(raw: Mapping[str, Any], name: str) -> float | None:
    value = _required(raw, name)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"поле {name} должно быть числом или null")
    return float(value)


def _bool(raw: Mapping[str, Any], name: str) -> bool:
    value = _required(raw, name)
    if not isinstance(value, bool):
        raise ValueError(f"поле {name} должно быть логическим")
    return value


def _optional_bool(raw: Mapping[str, Any], name: str) -> bool | None:
    value = _required(raw, name)
    if value is None:
        return None
    if not isinstance(value, bool):
        raise ValueError(f"поле {name} должно быть логическим или null")
    return value


def _datetime(raw: Mapping[str, Any], name: str) -> datetime:
    value = _str(raw, name)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"поле {name} должно содержать часовой пояс")
    return parsed


def _optional_datetime(raw: Mapping[str, Any], name: str) -> datetime | None:
    value = _optional_str(raw, name)
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"поле {name} должно содержать часовой пояс")
    return parsed


def _enum(enum_type: type[EnumT], value: object, name: str) -> EnumT:
    try:
        return enum_type(value)  # type: ignore[call-arg]
    except (TypeError, ValueError) as exc:
        raise ValueError(f"недопустимое значение поля {name}") from exc


def _optional_enum(enum_type: type[EnumT], value: object, name: str) -> EnumT | None:
    return None if value is None else _enum(enum_type, value, name)


def _strings(raw: Mapping[str, Any], name: str) -> tuple[str, ...]:
    value = _required(raw, name)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"поле {name} должно быть списком строк")
    return tuple(cast(list[str], value))


def _components(raw: Mapping[str, Any]) -> tuple[ScoreComponent, ...]:
    value = _required(raw, "component_scores")
    if not isinstance(value, list):
        raise ValueError("поле component_scores должно быть списком")
    result: list[ScoreComponent] = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("элемент component_scores должен быть объектом")
        component = cast(dict[str, Any], item)
        raw_value = _required(component, "raw_value")
        if raw_value is not None and not isinstance(raw_value, (str, int, float, bool)):
            raise ValueError("raw_value компонента имеет недопустимый тип")
        points = _float(component, "points")
        maximum = _float(component, "maximum_points")
        if points is None or maximum is None:
            raise ValueError("points и maximum_points не могут быть null")
        result.append(
            ScoreComponent(
                score_kind=_str(component, "score_kind"),
                score_name_ru=_str(component, "score_name_ru"),
                component_id=_str(component, "component_id"),
                raw_value=raw_value,
                points=points,
                maximum_points=maximum,
                reason=_str(component, "reason"),
                explanation_ru=_str(component, "explanation_ru"),
            )
        )
    return tuple(result)


def v2_result_from_json(raw: Mapping[str, Any]) -> EarlyDiscoveryV2Result:
    return EarlyDiscoveryV2Result(
        schema_version=_int(raw, "schema_version"),
        scan_id=_str(raw, "scan_id"),
        scanned_at=_datetime(raw, "scanned_at"),
        symbol=_str(raw, "symbol"),
        market_direction=_enum(
            MarketDirection, _required(raw, "market_direction"), "market_direction"
        ),
        direction_v1=_optional_enum(
            MarketDirection, _required(raw, "direction_v1"), "direction_v1"
        ),
        direction_v2=_optional_enum(
            MarketDirection, _required(raw, "direction_v2"), "direction_v2"
        ),
        stage_v1=_optional_enum(DiscoveryStage, _required(raw, "stage_v1"), "stage_v1"),
        stage_v2=_enum(DiscoveryStage, _required(raw, "stage_v2"), "stage_v2"),
        display_stage_v2_ru=_str(raw, "display_stage_v2_ru"),
        discovery_score_v1=_float(raw, "discovery_score_v1"),
        discovery_score_v2=_float(raw, "discovery_score_v2"),
        readiness_score_v1=_float(raw, "readiness_score_v1"),
        readiness_score_v2=_float(raw, "readiness_score_v2"),
        consecutive_active_scans=_int(raw, "consecutive_active_scans"),
        consecutive_ready_scans=_int(raw, "consecutive_ready_scans"),
        first_detected_at=_optional_datetime(raw, "first_detected_at"),
        first_ready_at=_optional_datetime(raw, "first_ready_at"),
        second_confirmation_at=_optional_datetime(raw, "second_confirmation_at"),
        third_confirmation_at=_optional_datetime(raw, "third_confirmation_at"),
        reset_reason=_optional_str(raw, "reset_reason"),
        breakout_level=_float(raw, "breakout_level"),
        current_price=_float(raw, "current_price"),
        absolute_distance=_float(raw, "absolute_distance"),
        signed_distance_atr=_float(raw, "signed_distance_atr"),
        absolute_distance_atr=_float(raw, "absolute_distance_atr"),
        distance_sign=_optional_int(raw, "distance_sign"),
        is_correct_side_of_level=_optional_bool(raw, "is_correct_side_of_level"),
        breakout_hold_candles=_optional_int(raw, "breakout_hold_candles"),
        returned_to_level=_optional_bool(raw, "returned_to_level"),
        retest_state=_optional_enum(
            RetestState, _required(raw, "retest_state"), "retest_state"
        ),
        breakout_failure=_optional_bool(raw, "breakout_failure"),
        returned_inside_range=_optional_bool(raw, "returned_inside_range"),
        breakout_age_bars=_optional_int(raw, "breakout_age_bars"),
        spread_pct=_float(raw, "spread_pct"),
        price_change_24h_pct=_float(raw, "price_change_24h_pct"),
        production_rank=_optional_int(raw, "production_rank"),
        is_in_production_top20=_bool(raw, "is_in_production_top20"),
        component_scores=_components(raw),
        confirmations=_strings(raw, "confirmations"),
        warnings=_strings(raw, "warnings"),
        technical_error=_optional_str(raw, "technical_error"),
        reason_v2_ru=_str(raw, "reason_v2_ru"),
    )


def read_v2_audit(path: Path) -> AuditReadResult:
    before_bytes = path.read_bytes()
    before = hashlib.sha256(before_bytes).hexdigest()
    snapshots: list[ReplaySnapshot] = []
    rejected: list[RejectedLine] = []
    observed: set[str] = set()
    empty = 0
    lines = before_bytes.decode("utf-8-sig").splitlines()
    for number, line in enumerate(lines, 1):
        if not line.strip():
            empty += 1
            continue
        try:
            loaded = json.loads(line)
            if not isinstance(loaded, dict):
                raise ValueError("корень JSON должен быть объектом")
            raw = cast(dict[str, Any], loaded)
            observed.update(raw)
            source = v2_result_from_json(raw)
            setup_input = input_from_early_discovery_v2(source)
            result = analyze_setup(setup_input)
            snapshots.append(ReplaySnapshot(number, source, setup_input, result))
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            rejected.append(RejectedLine(number, str(exc)))
    after = hashlib.sha256(path.read_bytes()).hexdigest()
    return AuditReadResult(
        snapshots=tuple(
            sorted(snapshots, key=lambda item: (item.at, item.line_number))
        ),
        total_lines=len(lines),
        empty_lines=empty,
        rejected=tuple(rejected),
        sha256_before=before,
        sha256_after=after,
        observed_fields=tuple(sorted(observed)),
    )


def build_episodes(
    snapshots: Sequence[ReplaySnapshot], config: AnalyzerConfig
) -> tuple[Episode, ...]:
    grouped: dict[str, list[ReplaySnapshot]] = defaultdict(list)
    for item in snapshots:
        grouped[item.result.symbol].append(item)
    episodes: list[Episode] = []
    for symbol in sorted(grouped):
        current: Episode | None = None
        no_trade_run = 0
        for item in sorted(grouped[symbol], key=lambda value: value.at):
            if item.source.technical_error is not None:
                # A transport/data gap is audit evidence, not a market transition.
                # The next valid row is compared with the last valid row, so a
                # short gap preserves the episode and a real configured gap closes it.
                continue
            start_new = current is None
            if current is not None:
                previous = current.last
                start_new = bool(
                    item.at - previous.at >= timedelta(minutes=config.gap_minutes)
                    or item.result.direction is not previous.result.direction
                    or item.result.setup_type is not previous.result.setup_type
                    or no_trade_run >= config.no_trade_scans_to_close
                    or (
                        previous.result.setup_state is SetupState.CANCELLED
                        and item.result.setup_state is not SetupState.CANCELLED
                    )
                )
            if start_new:
                current = Episode(len(episodes) + 1)
                episodes.append(current)
                no_trade_run = 0
            assert current is not None
            current.snapshots.append(item)
            if item.result.setup_type is SetupType.NO_TRADE:
                no_trade_run += 1
            else:
                no_trade_run = 0
    return tuple(episodes)


def _directed_return(direction: SetupDirection, start: float, end: float) -> float:
    sign = 1.0 if direction is SetupDirection.UP else -1.0
    return (end / start - 1.0) * 100.0 * sign


def outcome_for_snapshot(
    anchor: ReplaySnapshot,
    all_snapshots: Sequence[ReplaySnapshot],
    horizon_minutes: int,
    tolerance_minutes: int,
) -> HorizonOutcome | None:
    if (
        anchor.result.direction is SetupDirection.NEUTRAL
        or anchor.result.current_price is None
    ):
        return None
    target = anchor.at + timedelta(minutes=horizon_minutes)
    latest = target + timedelta(minutes=tolerance_minutes)
    future = [
        item
        for item in all_snapshots
        if item.result.symbol == anchor.result.symbol
        and anchor.at < item.at <= latest
        and item.result.current_price is not None
    ]
    observation = next((item for item in future if item.at >= target), None)
    if observation is None or observation.result.current_price is None:
        return None
    prices = [
        item.result.current_price
        for item in future
        if item.at <= observation.at and item.result.current_price is not None
    ]
    returns = [
        _directed_return(anchor.result.direction, anchor.result.current_price, price)
        for price in prices
    ]
    return HorizonOutcome(
        horizon_minutes,
        observation.at,
        _directed_return(
            anchor.result.direction,
            anchor.result.current_price,
            observation.result.current_price,
        ),
        max(returns),
        min(returns),
    )


def _percentile(values: Sequence[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _outcome_metrics(outcomes: Iterable[HorizonOutcome]) -> dict[str, object]:
    collected = list(outcomes)
    values = [item.directed_return_pct for item in collected]
    return {
        "available": len(values),
        "correct_direction_share": _share(
            sum(value > 0 for value in values), len(values)
        ),
        "mean_pct": statistics.fmean(values) if values else None,
        "median_pct": statistics.median(values) if values else None,
        "q25_pct": _percentile(values, 0.25),
        "q75_pct": _percentile(values, 0.75),
        "max_favorable_observed_pct": max(
            (item.max_favorable_observed_pct for item in collected), default=None
        ),
        "max_adverse_observed_pct": min(
            (item.max_adverse_observed_pct for item in collected), default=None
        ),
        "share_ge_0_25_pct": _share(
            sum(value >= 0.25 for value in values), len(values)
        ),
        "share_ge_0_5_pct": _share(sum(value >= 0.5 for value in values), len(values)),
        "share_ge_1_pct": _share(sum(value >= 1.0 for value in values), len(values)),
    }


def _share(part: int, whole: int) -> float | None:
    return part / whole if whole else None


def _episode_state(episode: Episode) -> SetupState:
    return max(
        (item.result.setup_state for item in episode.snapshots),
        key=lambda state: STATE_RANK[state],
    )


def _episode_row(
    episode: Episode, outcomes: Mapping[int, HorizonOutcome]
) -> dict[str, object]:
    first = episode.first.result
    ready = episode.ready
    reasons = tuple(
        dict.fromkeys(
            reason for item in episode.snapshots for reason in item.result.reasons
        )
    )
    warnings = tuple(
        dict.fromkeys(
            warning for item in episode.snapshots for warning in item.result.warnings
        )
    )
    exact_first: dict[SetupState, str] = {}
    for item in episode.snapshots:
        exact_first.setdefault(item.result.setup_state, item.at.isoformat())
    anchor_snapshot = ready if ready is not None else episode.first
    anchor = anchor_snapshot.result
    return {
        "Номер эпизода": episode.number,
        "Символ": first.symbol,
        "Направление": first.direction.value,
        "Тип сетапа": first.setup_type.value,
        "Начало": episode.first.at.isoformat(),
        "Окончание": episode.last.at.isoformat(),
        "Длительность, мин": (episode.last.at - episode.first.at).total_seconds() / 60,
        "Первое состояние": first.setup_state.value,
        "Максимальное состояние": _episode_state(episode).value,
        "Первый FORMING": exact_first.get(SetupState.FORMING, ""),
        "Первый CONFIRMING": exact_first.get(SetupState.CONFIRMING, ""),
        "Первый READY": exact_first.get(SetupState.READY_TO_CONSIDER, ""),
        "Повторения READY": sum(
            item.result.setup_state is SetupState.READY_TO_CONSIDER
            for item in episode.snapshots
        ),
        "Максимальная уверенность": max(
            item.result.confidence for item in episode.snapshots
        ),
        "Триггер": anchor.trigger_level,
        "Инвалидация": anchor.invalidation_level,
        "Цена первого READY": ready.result.current_price if ready else None,
        "Расстояние ATR": anchor.distance_to_trigger_atr,
        "Спред, %": anchor_snapshot.source.spread_pct,
        "Удержание, свечей": anchor.hold_candles,
        "Ретест обнаружен": anchor.retest_detected,
        "Ретест удержан": anchor.retest_held,
        "Причины": " | ".join(reasons),
        "Предупреждения": " | ".join(warnings),
        **{
            f"Результат {minutes}м, %": (
                outcomes[minutes].directed_return_pct if minutes in outcomes else None
            )
            for minutes in HORIZONS
        },
    }


def _reason_categories(snapshot: ReplaySnapshot) -> tuple[str, ...]:
    result = snapshot.result
    text = " ".join((*result.reasons, *result.warnings, *result.missing_data)).lower()
    categories: list[str] = []
    checks = (
        ("нейтральное направление", result.direction is SetupDirection.NEUTRAL),
        ("слабая структура", "структур" in text),
        ("неполные данные", bool(result.missing_data)),
        ("конфликт подтверждений", "конфликт" in text),
        ("провал пробоя", result.breakout_failed),
        ("слишком далеко", (result.distance_to_trigger_atr or 0) > 2),
        ("движение уже позднее", result.is_late),
        ("широкий спред", not result.spread_ok),
        ("низкая ликвидность", not result.liquidity_ok),
    )
    categories.extend(name for name, matched in checks if matched)
    return tuple(categories or ("прочее",))


def _safety_violations(snapshot: ReplaySnapshot) -> tuple[str, ...]:
    result = snapshot.result
    source = snapshot.source
    violations: list[str] = []
    if abs(source.price_change_24h_pct or 0) >= 15:
        violations.append("движение 24ч >= 15%")
    if (source.spread_pct or 0) > 0.2:
        violations.append("спред > 0.2%")
    if (result.distance_to_trigger_atr or 0) > 2:
        violations.append("расстояние > 2 ATR")
    if result.breakout_failed:
        violations.append("провал пробоя")
    if result.direction is SetupDirection.NEUTRAL:
        violations.append("нейтральное направление")
    if source.technical_error is not None:
        violations.append("техническая ошибка")
    if result.missing_data:
        violations.append("неполные данные")
    return tuple(violations)


def _anchor_for_level(episode: Episode, level: str) -> ReplaySnapshot | None:
    if level == "A":
        return next(
            (
                item
                for item in episode.snapshots
                if item.result.direction is not SetupDirection.NEUTRAL
            ),
            None,
        )
    states = {
        "B": {SetupState.FORMING, SetupState.CONFIRMING, SetupState.READY_TO_CONSIDER},
        "C": {SetupState.CONFIRMING, SetupState.READY_TO_CONSIDER},
        "D": {SetupState.READY_TO_CONSIDER},
    }[level]
    return next(
        (item for item in episode.snapshots if item.result.setup_state in states), None
    )


def _calculate_metrics(
    audit: AuditReadResult,
    episodes: Sequence[Episode],
    config: AnalyzerConfig,
) -> dict[str, object]:
    snapshots = audit.snapshots

    def episode_outcome(episode: Episode, horizon: int) -> HorizonOutcome | None:
        return outcome_for_snapshot(
            episode.ready or episode.first,
            snapshots,
            horizon,
            config.outcome_tolerance_minutes,
        )

    ready_snapshots = [
        item
        for item in snapshots
        if item.result.setup_state is SetupState.READY_TO_CONSIDER
    ]
    type_metrics: dict[str, object] = {}
    for setup_type in SetupType:
        type_episodes = [
            ep for ep in episodes if ep.first.result.setup_type is setup_type
        ]
        ready_episodes = [ep for ep in type_episodes if ep.ready is not None]
        type_metrics[setup_type.value] = {
            "snapshots": sum(
                item.result.setup_type is setup_type for item in snapshots
            ),
            "episodes": len(type_episodes),
            "ready_episodes": len(ready_episodes),
            "ready_share": _share(len(ready_episodes), len(type_episodes)),
            "average_confidence": (
                statistics.fmean(
                    max(item.result.confidence for item in ep.snapshots)
                    for ep in type_episodes
                )
                if type_episodes
                else None
            ),
            "outcomes": {
                str(horizon): _outcome_metrics(
                    observed
                    for ep in type_episodes
                    if (observed := episode_outcome(ep, horizon)) is not None
                )
                for horizon in HORIZONS
            },
        }
    funnel: dict[str, object] = {}
    for level in ("A", "B", "C", "D"):
        anchors = [
            anchor
            for episode in episodes
            if (anchor := _anchor_for_level(episode, level)) is not None
        ]
        funnel[level] = {
            "episodes": len(anchors),
            "share_of_A": None,
            "outcomes": {
                str(horizon): _outcome_metrics(
                    outcome
                    for anchor in anchors
                    if (
                        outcome := outcome_for_snapshot(
                            anchor,
                            snapshots,
                            horizon,
                            config.outcome_tolerance_minutes,
                        )
                    )
                    is not None
                )
                for horizon in HORIZONS
            },
        }
    a_count = cast(dict[str, object], funnel["A"])["episodes"]
    assert isinstance(a_count, int)
    for value in funnel.values():
        funnel_item = cast(dict[str, object], value)
        count = funnel_item["episodes"]
        assert isinstance(count, int)
        funnel_item["share_of_A"] = _share(count, a_count)

    no_trade = [
        item for item in snapshots if item.result.setup_type is SetupType.NO_TRADE
    ]
    reason_counts: Counter[str] = Counter(
        category for item in no_trade for category in _reason_categories(item)
    )
    missed = 0
    for snapshot in no_trade:
        observed = outcome_for_snapshot(
            snapshot, snapshots, 60, config.outcome_tolerance_minutes
        )
        if observed is not None and abs(observed.directed_return_pct) >= 1:
            missed += 1

    false_breakouts = [
        ep for ep in episodes if ep.first.result.setup_type is SetupType.FALSE_BREAKOUT
    ]
    false_values = [
        observed.directed_return_pct
        for ep in false_breakouts
        if (observed := episode_outcome(ep, 60)) is not None
    ]
    retest: dict[str, object] = {}
    retest_groups = {
        "held": [
            ep
            for ep in episodes
            if any(snapshot.result.retest_held for snapshot in ep.snapshots)
        ],
        "not_held": [
            ep
            for ep in episodes
            if any(snapshot.result.retest_detected for snapshot in ep.snapshots)
            and not any(snapshot.result.retest_held for snapshot in ep.snapshots)
        ],
    }
    for label, selected in retest_groups.items():
        retest[label] = {
            "episodes": len(selected),
            "outcomes": {
                str(horizon): _outcome_metrics(
                    observed
                    for ep in selected
                    if (observed := episode_outcome(ep, horizon)) is not None
                )
                for horizon in HORIZONS
            },
        }
    bands = ((0, 49), (50, 59), (60, 69), (70, 79), (80, 89), (90, 100))
    confidence: dict[str, object] = {}
    for low, high in bands:
        selected = [
            ep
            for ep in episodes
            if low
            <= (ep.ready.result.confidence if ep.ready else ep.first.result.confidence)
            <= high
        ]
        confidence[f"{low}-{high}"] = {
            "episodes": len(selected),
            "outcomes": {
                str(horizon): _outcome_metrics(
                    observed
                    for ep in selected
                    if (observed := episode_outcome(ep, horizon)) is not None
                )
                for horizon in HORIZONS
            },
        }
    violations = [
        {
            "line": item.line_number,
            "symbol": item.result.symbol,
            "at": item.at.isoformat(),
            "violations": _safety_violations(item),
        }
        for item in ready_snapshots
        if _safety_violations(item)
    ]
    return {
        "source": {
            "total_lines": audit.total_lines,
            "valid_rows": len(snapshots),
            "rejected_rows": len(audit.rejected),
            "empty_rows": audit.empty_lines,
            "period_start": snapshots[0].at.isoformat() if snapshots else None,
            "period_end": snapshots[-1].at.isoformat() if snapshots else None,
            "sha256": audit.sha256_before,
            "unchanged": audit.sha256_before == audit.sha256_after,
            "observed_fields": list(audit.observed_fields),
        },
        "episodes": len(episodes),
        "ready_snapshots": len(ready_snapshots),
        "ready_episodes": sum(ep.ready is not None for ep in episodes),
        "type_metrics": type_metrics,
        "filter_funnel": funnel,
        "no_trade": {
            "snapshots": len(no_trade),
            "reasons": dict(reason_counts.most_common()),
            "missed_strong_move_60m_ge_1pct": missed,
        },
        "false_breakout": {
            "episodes": len(false_breakouts),
            "available_60m": len(false_values),
            "continued_original_direction_share": _share(
                sum(value > 0 for value in false_values), len(false_values)
            ),
            "returned_after_false_breakout_share": _share(
                sum(value < 0 for value in false_values), len(false_values)
            ),
        },
        "retest": retest,
        "confidence_bands": confidence,
        "safety": {
            "ready_violations": len(violations),
            "details": violations,
        },
        "limitations": [
            "В аудите нет intrabar high/low: MFE/MAE рассчитаны только по "
            "наблюдаемым снимкам.",
            "Нет отдельного invalidation_level: production-адаптер выводит его "
            "из уровня и ATR, когда это возможно.",
            "Нет сырых свечей и стакана: нельзя независимо перепроверить "
            "признаки, уже рассчитанные V2.",
            "Доступность горизонта зависит от наличия последующего снимка "
            "в окне допуска.",
        ],
    }


def analyze_audit(path: Path, config: AnalyzerConfig | None = None) -> AnalysisResult:
    if config is None:
        config = AnalyzerConfig()
    audit = read_v2_audit(path)
    episodes = build_episodes(audit.snapshots, config)
    outcomes: dict[int, dict[int, HorizonOutcome]] = {}
    for episode in episodes:
        anchor = episode.ready
        episode_outcomes: dict[int, HorizonOutcome] = {}
        if anchor is not None:
            for horizon in HORIZONS:
                outcome = outcome_for_snapshot(
                    anchor,
                    audit.snapshots,
                    horizon,
                    config.outcome_tolerance_minutes,
                )
                if outcome is not None:
                    episode_outcomes[horizon] = outcome
        outcomes[episode.number] = episode_outcomes
    metrics = _calculate_metrics(audit, episodes, config)
    return AnalysisResult(audit, episodes, outcomes, metrics)


def _write_csv(path: Path, rows: Sequence[Mapping[str, object]]) -> None:
    if not rows:
        raise ValueError(f"нет строк для CSV {path.name}")
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(rows[0]),
            delimiter=";",
            lineterminator="\n",
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


def _fmt(value: object) -> str:
    if value is None:
        return "н/д"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def _report_text(result: AnalysisResult) -> str:
    source = cast(dict[str, object], result.metrics["source"])
    funnel = cast(dict[str, object], result.metrics["filter_funnel"])
    type_metrics = cast(dict[str, object], result.metrics["type_metrics"])
    no_trade = cast(dict[str, object], result.metrics["no_trade"])
    safety = cast(dict[str, object], result.metrics["safety"])
    false_breakout = cast(dict[str, object], result.metrics["false_breakout"])
    retest = cast(dict[str, object], result.metrics["retest"])
    confidence = cast(dict[str, object], result.metrics["confidence_bands"])
    lines = [
        "# Итоговый отчёт Setup Engine V1",
        "",
        "Анализ выполнен полностью офлайн: каждая валидная строка V2 преобразована "
        "production-адаптером и передана в существующий `analyze_setup()`.",
        "",
        "## Покрытие",
        "",
        f"- Обработано строк: {source['valid_rows']} из {source['total_lines']}.",
        f"- Отклонено повреждённых/неполных строк: {source['rejected_rows']}.",
        f"- Период: {source['period_start']} — {source['period_end']}.",
        f"- Эпизодов: {result.metrics['episodes']}; READY-эпизодов: "
        f"{result.metrics['ready_episodes']}; READY-снимков: "
        f"{result.metrics['ready_snapshots']}.",
        f"- Исходный audit не изменён: {source['unchanged']}.",
        "",
        "## Фактическая схема и границы проверяемости",
        "",
        "Напрямую использованы поля идентификации и времени, направления и стадий, "
        "цен, breakout/hold/retest/failure, расстояния ATR, спреда, движения 24ч, "
        "component_scores, confirmations, warnings и technical_error.",
        "",
        "Надёжно не перепроверяются исходные свечные/стаканные вычисления V2: в JSONL "
        "нет сырых свечей, intrabar high/low, полного order book и отдельного "
        "invalidation_level. Инвалидация строится существующим production-адаптером.",
        "",
        "## Распределение по типам",
        "",
        "| Тип | Эпизоды | READY | Доля READY | 60м n | 60м медиана |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for setup_type in SetupType:
        item = cast(dict[str, object], type_metrics[setup_type.value])
        type_outcomes = cast(dict[str, object], item["outcomes"])
        outcome_60 = cast(dict[str, object], type_outcomes["60"])
        lines.append(
            f"| {setup_type.value} | {item['episodes']} | {item['ready_episodes']} | "
            f"{_fmt(item['ready_share'])} | {outcome_60['available']} | "
            f"{_fmt(outcome_60['median_pct'])}% |"
        )
    lines.extend(["", "## Воронка фильтрации", ""])
    for level in ("A", "B", "C", "D"):
        item = cast(dict[str, object], funnel[level])
        lines.append(
            f"- {level}: {item['episodes']} эпизодов; доля от A: "
            f"{_fmt(item['share_of_A'])}."
        )
    lines.extend(
        [
            "",
            "A — все направленные V2; B — минимум FORMING; C — минимум "
            "CONFIRMING; D — READY.",
            "",
            "## Результаты READY",
            "",
        ]
    )
    for horizon in HORIZONS:
        d_outcomes = cast(
            dict[str, object], cast(dict[str, object], funnel["D"])["outcomes"]
        )
        item = cast(dict[str, object], d_outcomes[str(horizon)])
        lines.append(
            f"- {horizon} мин: доступно {item['available']}, доля верного "
            f"направления {_fmt(item['correct_direction_share'])}, медиана "
            f"{_fmt(item['median_pct'])}%."
        )
    lines.extend(
        [
            "",
            "MFE/MAE и доходности основаны только на наблюдаемых последующих "
            "снимках, не на внутрисвечных high/low.",
            "",
            "## NO_TRADE и безопасность",
            "",
            f"- NO_TRADE-снимков: {no_trade['snapshots']}; сильных наблюдаемых "
            f"движений >=1% за 60 мин: {no_trade['missed_strong_move_60m_ge_1pct']}.",
            f"- READY-нарушений safety-инвариантов: {safety['ready_violations']}.",
            "",
            "### Причины NO_TRADE",
            "",
        ]
    )
    reasons = cast(dict[str, int], no_trade["reasons"])
    if reasons:
        lines.extend(f"- {reason}: {count}." for reason, count in reasons.items())
    else:
        lines.append("- В выборке нет снимков, классифицированных как NO_TRADE.")
    lines.extend(
        [
            "",
            "## False breakout",
            "",
            f"- Эпизодов: {false_breakout['episodes']}; outcome 60м доступен: "
            f"{false_breakout['available_60m']}.",
            "- Доля продолжения исходного направления: "
            f"{_fmt(false_breakout['continued_original_direction_share'])}.",
            "- Доля возврата против исходного направления: "
            f"{_fmt(false_breakout['returned_after_false_breakout_share'])}.",
            "",
            "## Retest held / not-held",
            "",
        ]
    )
    for label in ("held", "not_held"):
        retest_item = cast(dict[str, object], retest[label])
        retest_outcomes = cast(dict[str, object], retest_item["outcomes"])
        retest_60 = cast(dict[str, object], retest_outcomes["60"])
        lines.append(
            f"- {label}: эпизодов {retest_item['episodes']}; 60м n="
            f"{retest_60['available']}, медиана "
            f"{_fmt(retest_60['median_pct'])}%."
        )
    lines.extend(["", "## Диапазоны confidence", ""])
    for band, raw_item in confidence.items():
        band_item = cast(dict[str, object], raw_item)
        band_outcomes = cast(dict[str, object], band_item["outcomes"])
        band_15 = cast(dict[str, object], band_outcomes["15"])
        band_30 = cast(dict[str, object], band_outcomes["30"])
        lines.append(
            f"- {band}: эпизодов {band_item['episodes']}; 15м n="
            f"{band_15['available']}, median={_fmt(band_15['median_pct'])}%; "
            f"30м n={band_30['available']}, "
            f"median={_fmt(band_30['median_pct'])}%."
        )
    lines.extend(
        [
            "",
            "Полные квартили, MFE/MAE и все горизонты находятся в `метрики.json`.",
            "",
            "## Ограничения",
            "",
        ]
    )
    limitations = cast(list[str], result.metrics["limitations"])
    lines.extend(f"- {item}" for item in limitations)
    return "\n".join(lines) + "\n"


def _recommendations_text(result: AnalysisResult) -> str:
    safety = cast(dict[str, object], result.metrics["safety"])
    no_trade = cast(dict[str, object], result.metrics["no_trade"])
    return (
        "# Рекомендации по результатам офлайн-анализа\n\n"
        "1. Не менять формулы и пороги Setup Engine по одному короткому "
        "audit-периоду.\n"
        "2. Накопить более плотные последующие снимки: это увеличит доступность "
        "горизонтов и точность наблюдаемого MFE/MAE.\n"
        "3. Любые будущие изменения сначала проверять в shadow/offline режиме и "
        "сравнивать уровни A–D одной и той же воронки.\n"
        f"4. Отдельно разобрать {safety['ready_violations']} READY safety-нарушений "
        "и категории NO_TRADE: "
        f"{json.dumps(no_trade['reasons'], ensure_ascii=False)}.\n"
        "5. Не трактовать отсутствие результата горизонта как нулевую доходность.\n"
    )


def write_outputs(result: AnalysisResult, output: Path) -> OutputFiles:
    output.mkdir(parents=True, exist_ok=True)
    files = OutputFiles(
        output / "итоговый_отчёт.md",
        output / "эпизоды.csv",
        output / "метрики.json",
        output / "лучшие_сетапы.csv",
        output / "худшие_сетапы.csv",
        output / "причины_нет_сделки.csv",
        output / "рекомендации.md",
    )
    rows = [
        _episode_row(ep, result.episode_outcomes[ep.number]) for ep in result.episodes
    ]
    _write_csv(files.episodes, rows)
    ready_rows = [
        row for ep, row in zip(result.episodes, rows, strict=True) if ep.ready
    ]
    scored = sorted(
        ready_rows,
        key=lambda row: (
            cast(float, row["Результат 60м, %"])
            if row["Результат 60м, %"] is not None
            else float("-inf")
        ),
        reverse=True,
    )
    placeholder = [{"Сообщение": "Нет READY-эпизодов"}]
    _write_csv(files.best, scored[:20] or placeholder)
    _write_csv(files.worst, list(reversed(scored[-20:])) or placeholder)
    no_trade_metrics = cast(dict[str, object], result.metrics["no_trade"])
    reason_counts = cast(dict[str, int], no_trade_metrics["reasons"])
    no_trade_rows = [
        {
            "Причина": reason,
            "Количество": count,
            "Доля от NO_TRADE": _share(count, cast(int, no_trade_metrics["snapshots"])),
        }
        for reason, count in reason_counts.items()
    ] or [
        {"Причина": "NO_TRADE отсутствует", "Количество": 0, "Доля от NO_TRADE": None}
    ]
    _write_csv(files.no_trade, no_trade_rows)
    files.metrics.write_text(
        json.dumps(result.metrics, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    files.report.write_text(_report_text(result), encoding="utf-8")
    files.recommendations.write_text(_recommendations_text(result), encoding="utf-8")
    return files


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Офлайн-анализ качества Setup Engine V1 по V2 audit JSONL."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--gap-minutes", type=int, default=30)
    parser.add_argument("--no-trade-scans", type=int, default=2)
    parser.add_argument("--outcome-tolerance-minutes", type=int, default=10)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    config = AnalyzerConfig(
        gap_minutes=args.gap_minutes,
        no_trade_scans_to_close=args.no_trade_scans,
        outcome_tolerance_minutes=args.outcome_tolerance_minutes,
    )
    result = analyze_audit(args.input, config)
    files = write_outputs(result, args.output)
    print(
        f"Обработано {len(result.audit.snapshots)} строк; "
        f"эпизодов {len(result.episodes)}; READY "
        f"{result.metrics['ready_episodes']}."
    )
    print(f"Отчёт: {files.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
