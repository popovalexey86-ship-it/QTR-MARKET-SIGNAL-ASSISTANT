from datetime import UTC, datetime

import pytest

from market_signal_assistant.application.models import MarketSummary, ScreeningReport
from market_signal_assistant.inplay.models import (
    InPlayDirection,
    InPlayReport,
    InPlayResult,
)
from market_signal_assistant.telegram.bot import execute_command
from market_signal_assistant.telegram.formatting import (
    format_auto_inplay_results,
    format_inplay_report,
)
from market_signal_assistant.telegram.parsing import parse_command

NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)
EMPTY_SCREENING_REPORT = ScreeningReport(
    NOW,
    (),
    (),
    (),
    MarketSummary(0, 0, 0, 0, 0, 0),
)


def result(
    symbol: str,
    direction: InPlayDirection,
    score: float,
    *,
    new: bool = False,
    reasons: tuple[str, ...] = ("Рост относительного объёма.",),
    warnings: tuple[str, ...] = ("Повышенная волатильность.",),
) -> InPlayResult:
    return InPlayResult(
        symbol=symbol,
        direction=direction,
        inplay_score=score,
        directional_score=None if direction is InPlayDirection.WATCH else score,
        reasons=reasons,
        warnings=warnings,
        first_seen=NOW,
        is_new_listing=new,
        listing_bonus=10 if new else 0,
    )


class Screening:
    def screen(self, request: object) -> ScreeningReport:
        del request
        return EMPTY_SCREENING_REPORT


class InPlay:
    def __init__(self, report: InPlayReport) -> None:
        self.report = report
        self.calls = 0

    def scan(
        self,
        maximum_results: int = 10,
        *,
        scan_source: str = "manual",
    ) -> InPlayReport:
        assert maximum_results == 10
        assert scan_source == "manual"
        self.calls += 1
        return self.report


def test_inplay_command_parses_without_parameters() -> None:
    command = parse_command("/inplay")

    assert command.name == "inplay"
    assert command.request is None


def test_inplay_format_supports_long_short_watch_and_new_listing() -> None:
    report = InPlayReport(
        NOW,
        (
            result("BTCUSDT", InPlayDirection.LONG, 91),
            result("ETHUSDT", InPlayDirection.SHORT, 82),
            result("SOLUSDT", InPlayDirection.WATCH, 74),
            result("NEWUSDT", InPlayDirection.WATCH, 68, new=True),
        ),
    )

    message = format_inplay_report(report)[0]

    assert message.startswith("🔥 IN PLAY")
    assert "🟢 BTCUSDT — ЛОНГ" in message
    assert "🔴 ETHUSDT — ШОРТ" in message
    assert "🟡 SOLUSDT — СЛЕДИМ" in message
    assert "🆕 🟡 NEWUSDT — СЛЕДИМ" in message


def test_inplay_format_limits_indicators_and_warnings() -> None:
    report = InPlayReport(
        NOW,
        (
            result(
                "BTCUSDT",
                InPlayDirection.WATCH,
                80,
                reasons=(
                    "Изменение цены +3,3%",
                    "Волатильность ATR 4,2%",
                    "Относительный объём 2,2×",
                    "Цена вышла из локального диапазона",
                ),
                warnings=("Главный риск", "Второй риск"),
            ),
        ),
    )

    message = format_inplay_report(report)[0]

    indicator_lines = tuple(
        line
        for line in message.splitlines()
        if line.startswith(("📊 Активность", "📈 Цена", "🌊 Волатильность", "🔥 Объём"))
    )
    assert len(indicator_lines) == 4
    assert "Главный риск" in message
    assert "Второй риск" not in message


def test_inplay_metrics_use_plain_language_and_russian_decimal_separator() -> None:
    report = InPlayReport(
        NOW,
        (
            result(
                "BTCUSDT",
                InPlayDirection.WATCH,
                80,
                reasons=(
                    "Относительный объём 2,3×.",
                    "Волатильность ATR 4,2%.",
                    "Изменение цены +3,3%.",
                ),
                warnings=(),
            ),
        ),
    )

    message = format_inplay_report(report)[0]

    assert "📈 Цена: +3,3%" in message
    assert "🌊 Волатильность: высокая — 4,2%" in message
    assert "🔥 Объём: высокий — 2,3×" in message
    assert "×.;" not in message


def test_price_move_warning_has_priority_over_no_risk_fallback() -> None:
    warning = "Движение уже значительно реализовано; повышен риск отката."
    message = format_inplay_report(
        InPlayReport(
            NOW,
            (
                result(
                    "BTCUSDT",
                    InPlayDirection.WATCH,
                    80,
                    reasons=("Изменение цены −21,2%",),
                    warnings=(warning,),
                ),
            ),
        )
    )[0]

    assert "📉 Цена уже упала: −21,2%" in message
    assert "Падение уже состоялось" in message
    assert "❌ Сейчас не входить." in message
    assert "Существенные риски не выявлены" not in message


def test_inplay_format_returns_exact_empty_message() -> None:
    assert format_inplay_report(InPlayReport(NOW, ())) == (
        "Активных IN PLAY монет сейчас нет.",
    )


def test_execute_inplay_is_manual_and_uses_dedicated_service() -> None:
    inplay = InPlay(
        InPlayReport(NOW, (result("BTCUSDT", InPlayDirection.WATCH, 75),))
    )

    execution = execute_command(
        "/inplay",
        chat_id=1,
        allowed_chat_ids=frozenset({1}),
        service=Screening(),
        inplay_service=inplay,
    )

    assert inplay.calls == 1
    assert execution.report is None
    assert execution.view is None
    assert "🔥 IN PLAY" in execution.messages[0]


@pytest.mark.parametrize(
    ("score", "label"),
    [
        (50.0, "умеренная"),
        (59.9, "умеренная"),
        (60.0, "заметная"),
        (69.9, "заметная"),
        (70.0, "сильная"),
        (79.9, "сильная"),
        (80.0, "экстремальная"),
    ],
)
def test_activity_gets_text_classification(score: float, label: str) -> None:
    message = format_inplay_report(
        InPlayReport(NOW, (result("BTCUSDT", InPlayDirection.WATCH, score),))
    )[0]

    assert f"— {label}" in message
    assert f"📊 Активность: {int(score + 0.5)}/100" in message


@pytest.mark.parametrize(
    ("volume", "label"),
    [
        (0.9, "ниже обычного"),
        (1.0, "почти обычный"),
        (1.29, "почти обычный"),
        (1.3, "повышенный"),
        (1.79, "повышенный"),
        (1.8, "высокий"),
        (2.49, "высокий"),
        (2.5, "очень высокий"),
    ],
)
def test_relative_volume_gets_text_classification(
    volume: float,
    label: str,
) -> None:
    message = format_inplay_report(
        InPlayReport(
            NOW,
            (
                result(
                    "BTCUSDT",
                    InPlayDirection.WATCH,
                    70,
                    reasons=(f"Относительный объём {str(volume).replace('.', ',')}×",),
                    warnings=(),
                ),
            ),
        )
    )[0]

    assert f"Объём: {label}" in message


@pytest.mark.parametrize(
    ("volatility", "label"),
    [
        (0.9, "низкая"),
        (1.0, "умеренная"),
        (2.99, "умеренная"),
        (3.0, "высокая"),
        (5.99, "высокая"),
        (6.0, "очень высокая"),
    ],
)
def test_atr_gets_text_classification(
    volatility: float,
    label: str,
) -> None:
    message = format_inplay_report(
        InPlayReport(
            NOW,
            (
                result(
                    "BTCUSDT",
                    InPlayDirection.WATCH,
                    70,
                    reasons=(
                        f"Волатильность ATR {str(volatility).replace('.', ',')}%",
                    ),
                    warnings=(),
                ),
            ),
        )
    )[0]

    assert f"Волатильность: {label}" in message


@pytest.mark.parametrize(
    ("change", "line"),
    [
        (3.3, "📈 Цена: +3,3%"),
        (-3.3, "📉 Цена: −3,3%"),
        (21.2, "📈 Цена уже выросла: +21,2%"),
        (-21.2, "📉 Цена уже упала: −21,2%"),
        (36.5, "🚀 Цена уже выросла: +36,5%"),
        (-36.5, "💥 Цена уже упала: −36,5%"),
    ],
)
def test_price_change_gets_directional_format(change: float, line: str) -> None:
    signed = f"{change:+.1f}".replace("-", "−").replace(".", ",")
    message = format_inplay_report(
        InPlayReport(
            NOW,
            (
                result(
                    "BTCUSDT",
                    InPlayDirection.WATCH,
                    70,
                    reasons=(f"Изменение цены {signed}%",),
                    warnings=(),
                ),
            ),
        )
    )[0]

    assert line in message


def test_watch_display_changes_without_mutating_internal_direction() -> None:
    late = result(
        "DROPUSDT",
        InPlayDirection.WATCH,
        70,
        reasons=("Изменение цены −21,2%",),
        warnings=(),
    )
    extreme = result(
        "PUMPUSDT",
        InPlayDirection.WATCH,
        70,
        reasons=("Изменение цены +36,5%",),
        warnings=(),
    )
    message = format_inplay_report(InPlayReport(NOW, (late, extreme)))[0]

    assert "🟡 DROPUSDT — ПОЗДНИЙ ВХОД" in message
    assert "🔴 PUMPUSDT — НЕ ДОГОНЯТЬ" in message
    assert late.direction is InPlayDirection.WATCH
    assert extreme.direction is InPlayDirection.WATCH


def test_watch_never_looks_like_entry_recommendation() -> None:
    message = format_inplay_report(
        InPlayReport(
            NOW,
            (
                result(
                    "BTCUSDT",
                    InPlayDirection.WATCH,
                    70,
                    warnings=(),
                ),
            ),
        )
    )[0]

    assert "Направление пока не подтверждено." in message
    assert "Риск позднего входа: не выявлен." in message
    assert "Существенные риски не выявлены" not in message


def test_directional_results_keep_long_and_short() -> None:
    message = format_inplay_report(
        InPlayReport(
            NOW,
            (
                result("BTCUSDT", InPlayDirection.LONG, 80),
                result("ETHUSDT", InPlayDirection.SHORT, 80),
            ),
        )
    )[0]

    assert "🟢 BTCUSDT — ЛОНГ" in message
    assert "🔴 ETHUSDT — ШОРТ" in message
    assert message.count(
        "⚠️ Перед входом проверь уровень, стоп и соотношение риска к прибыли."
    ) == 2


def test_manual_inplay_preserves_every_result() -> None:
    results = tuple(
        result(f"COIN{index}USDT", InPlayDirection.WATCH, 70)
        for index in range(10)
    )

    message = "".join(format_inplay_report(InPlayReport(NOW, results)))

    assert all(item.symbol in message for item in results)


def test_automatic_format_keeps_headers_and_limits_to_three() -> None:
    results = (
        result("BTCUSDT", InPlayDirection.LONG, 80),
        result("ETHUSDT", InPlayDirection.WATCH, 75),
        result("SOLUSDT", InPlayDirection.SHORT, 72),
        result("XRPUSDT", InPlayDirection.LONG, 71),
    )

    message = format_auto_inplay_results(results)

    assert "🚨 СИЛЬНЫЙ IN PLAY" in message
    assert "🔥 IN PLAY — ТРЕБУЕТ ВНИМАНИЯ" in message
    assert "BTCUSDT" in message
    assert "ETHUSDT" in message
    assert "SOLUSDT" in message
    assert "XRPUSDT" not in message


def test_status_keeps_existing_lines_and_adds_auto_scanner_state() -> None:
    execution = execute_command(
        "/status",
        chat_id=1,
        allowed_chat_ids=frozenset({1}),
        service=Screening(),
        inplay_service=InPlay(InPlayReport(NOW, ())),
    )

    assert execution.messages == (
        "Статус: готов\n"
        "Онлайн-поток ликвидаций: отключён\n"
            "Режим деривативов: REST\n"
            "Автосканирование IN PLAY: отключено\n"
            "Интервал IN PLAY: 15 минут\n"
            "Автоновости: отключены\n"
            "Интервал новостей: 60 минут",
        )
