import json
import logging
from dataclasses import asdict
from datetime import UTC, datetime

import pytest

from market_signal_assistant.application.models import (
    InstrumentFailure,
    MarketSummary,
    ScreeningReport,
)
from market_signal_assistant.application.presentation import (
    FailureView,
    ReportView,
    SignalView,
)
from market_signal_assistant.models import AssetClass, Instrument
from market_signal_assistant.settings import TelegramSettings
from market_signal_assistant.telegram.bot import execute_command, is_allowed
from market_signal_assistant.telegram.formatting import (
    format_alarm_report,
    format_report,
    format_screen_report,
    split_messages,
)
from market_signal_assistant.telegram.parsing import parse_command

NOW = datetime(2026, 7, 31, tzinfo=UTC)
REPORT = ScreeningReport(NOW, (), (), (), MarketSummary(1, 1, 0, 0, 0, 1))


class Service:
    def __init__(self, report: ScreeningReport = REPORT) -> None:
        self.requests: list[object] = []
        self.report = report

    def screen(self, request: object) -> ScreeningReport:
        self.requests.append(request)
        return self.report


def test_screen_command_parses_documented_example() -> None:
    command = parse_command(
        "/screen BTCUSDT ETHUSDT SOLUSDT interval=1h min_score=60"
    )
    assert command.request is not None
    assert tuple(item.symbol for item in command.request.instruments) == (
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
    )
    assert command.request.interval == "1h"
    assert command.request.minimum_score == 60
    assert command.request.include_derivatives is True


@pytest.mark.parametrize(
    ("command", "message"),
    (
        ("/screen", "укажите хотя бы один инструмент"),
        ("/screen BTCUSDT interval=2h", "неподдерживаемый интервал"),
        ("/screen BTCUSDT min_score=abc", "минимальный балл должен быть числом"),
        (
            "/screen BTCUSDT min_confidence=101",
            "минимальная уверенность должна быть от 0 до 100",
        ),
        (
            "/screen BTCUSDT derivatives=invalid",
            "параметр derivatives должен быть true или false",
        ),
    ),
)
def test_command_validation_errors_are_russian(command: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        parse_command(command)


@pytest.mark.parametrize(
    "command", ["/start", "/help", "/status", "/crypto", "/markets"]
)
def test_supported_commands_parse(command: str) -> None:
    assert parse_command(command).name == command[1:]


def test_allowlist_rejects_unknown_chat_before_screening() -> None:
    service = Service()
    assert is_allowed(10, frozenset()) is False
    assert is_allowed(10, frozenset(), allow_all=True) is True
    assert is_allowed(10, frozenset({10})) is True
    assert is_allowed(11, frozenset({10})) is False
    with pytest.raises(PermissionError):
        execute_command(
            "/crypto",
            chat_id=11,
            allowed_chat_ids=frozenset({10}),
            service=service,
        )
    assert service.requests == []


def test_long_messages_are_split_below_telegram_limit() -> None:
    chunks = split_messages("A" * 9000)
    assert len(chunks) == 3
    assert all(len(chunk) <= 3900 for chunk in chunks)
    assert "".join(chunks) == "A" * 9000


@pytest.mark.parametrize(
    "text",
    (
        "",
        "A" * 3890 + "\n\n" + "B" * 20,
        "first paragraph\n\nsecond paragraph\n\nthird",
        "Сигнал 🚦\n\n" * 700,
    ),
    ids=("empty", "boundary", "paragraphs", "unicode"),
)
def test_message_splitting_preserves_every_character(text: str) -> None:
    chunks = split_messages(text)
    assert all(len(chunk) <= 3900 for chunk in chunks)
    assert "".join(chunks) == text


def test_token_is_not_exposed_by_repr_or_logging(
    caplog: pytest.LogCaptureFixture,
) -> None:
    token = "123456:secret-token"
    settings = TelegramSettings(token, frozenset({1}))
    with caplog.at_level(logging.DEBUG):
        logging.getLogger("market_signal_assistant").debug("settings=%r", settings)
    assert token not in repr(settings)
    assert token not in caplog.text


def test_token_container_cannot_be_dataclass_serialized_or_debug_leaked() -> None:
    token = "123456:secret-token"
    settings = TelegramSettings(token, frozenset({1}))
    with pytest.raises(TypeError):
        asdict(settings)  # type: ignore[call-overload]
    assert token not in json.dumps(settings, default=repr)
    assert token not in str(RuntimeError(f"settings={settings}"))


def test_allow_all_requires_explicit_valid_environment_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.delenv("TELEGRAM_ALLOWED_CHAT_IDS", raising=False)
    monkeypatch.delenv("TELEGRAM_ALLOW_ALL", raising=False)
    assert TelegramSettings.from_environment().allow_all is False
    monkeypatch.setenv("TELEGRAM_ALLOW_ALL", "true")
    assert TelegramSettings.from_environment().allow_all is True
    monkeypatch.setenv("TELEGRAM_ALLOW_ALL", "invalid")
    with pytest.raises(ValueError, match="TELEGRAM_ALLOW_ALL"):
        TelegramSettings.from_environment()


def test_invalid_allowlist_is_controlled_configuration_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("TELEGRAM_ALLOWED_CHAT_IDS", "1,not-an-id")
    with pytest.raises(ValueError, match="TELEGRAM_ALLOWED_CHAT_IDS"):
        TelegramSettings.from_environment()


def test_invalid_derivatives_boolean_is_rejected() -> None:
    with pytest.raises(ValueError, match="derivatives"):
        parse_command("/screen BTCUSDT derivatives=invalid-value")


def test_screen_failure_is_returned_as_compact_message() -> None:
    result = execute_command(
        "/screen BTCUSDT",
        chat_id=1,
        allowed_chat_ids=frozenset({1}),
        service=Service(),
    )
    assert result.report is REPORT
    assert result.messages == ("Сильных торговых сигналов сейчас нет.",)


def test_start_and_status_each_create_one_russian_reply() -> None:
    service = Service()
    start = execute_command(
        "/start", chat_id=1, allowed_chat_ids=frozenset({1}), service=service
    )
    status = execute_command(
        "/status", chat_id=1, allowed_chat_ids=frozenset({1}), service=service
    )
    assert len(start.messages) == 1
    assert "Информационный скринер" in start.messages[0]
    assert len(status.messages) == 1
    assert status.messages[0] == (
        "Статус: готов\n"
        "Онлайн-поток ликвидаций: отключён\n"
        "Режим деривативов: REST\n"
        "Автосканирование IN PLAY: отключено\n"
        "Интервал IN PLAY: 15 минут\n"
        "Автоновости: отключены\n"
        "Интервал новостей: 60 минут"
    )
    assert service.requests == []


def test_compact_signal_contains_scores_reasons_and_risks() -> None:
    signal = SignalView(
        symbol="BTCUSDT",
        asset_class="crypto",
        direction="ШОРТ",
        technical_score=78,
        derivatives_score=-65,
        combined_score=-73,
        confidence=73,
        fusion_effect="ослабили сигнал",
        regime="накопление шортов",
        confirmations=3,
        conflicts=1,
        explanations=("Тренд указывает на снижение.",),
        warnings=("Ставка финансирования повышена.",),
        derivatives_context=(
            "Деривативы не подтвердили техническое движение "
            "и ослабили итоговый сигнал."
        ),
    )
    view = ReportView(
        generated_at=NOW.isoformat(),
        successful_results=(signal,),
        failed_instruments=(),
        ranked_signals=(signal,),
        market_summary=MarketSummary(1, 1, 0, 0, 1, 0),
    )
    message = format_report(view)[0]
    assert "BTCUSDT — ШОРТ" in message
    assert "Итоговый балл: -73,0" in message
    assert "Техническая сила сигнала: 78,0" in message
    assert "Уверенность: 73,0%" in message
    assert "Деривативы: -65,0" in message
    assert "Подтверждения: 3" in message
    assert "Противоречия: 1" in message
    assert "Причины: Тренд указывает на снижение." in message
    assert "Риски и предупреждения: Ставка финансирования повышена." in message
    assert "ослабили итоговый сигнал" in message


def alarm_signal(
    *,
    direction: str = "ЛОНГ",
    technical_score: float = 70,
    combined_score: float = 60,
    confidence: float = 70,
    confirmations: int = 3,
    explanations: tuple[str, ...] = ("Причина 1",),
    warnings: tuple[str, ...] = ("Основной риск",),
) -> SignalView:
    return SignalView(
        symbol="BTCUSDT",
        asset_class="crypto",
        direction=direction,
        technical_score=technical_score,
        derivatives_score=None,
        combined_score=combined_score,
        confidence=confidence,
        fusion_effect="нейтрально",
        regime="нейтральный",
        confirmations=confirmations,
        conflicts=0,
        explanations=explanations,
        warnings=warnings,
        derivatives_context="Деривативы не включены.",
    )


def alarm_view(*signals: SignalView) -> ReportView:
    return ReportView(
        generated_at=NOW.isoformat(),
        successful_results=signals,
        failed_instruments=(),
        ranked_signals=signals,
        market_summary=MarketSummary(len(signals), len(signals), 0, 0, 0, 0),
    )


def screen_view(
    *,
    successful: tuple[SignalView, ...] = (),
    ranked: tuple[SignalView, ...] = (),
    failures: tuple[FailureView, ...] = (),
) -> ReportView:
    return ReportView(
        generated_at=NOW.isoformat(),
        successful_results=successful,
        failed_instruments=failures,
        ranked_signals=ranked,
        market_summary=MarketSummary(
            len(successful) + len(failures),
            len(successful),
            len(failures),
            0,
            0,
            0,
        ),
    )


def technical_failure(symbol: str) -> FailureView:
    return FailureView(
        symbol=symbol,
        stage="технический анализ",
        error_type="MarketDataError",
        message="Рыночные данные недоступны.",
    )


@pytest.mark.parametrize(
    ("direction", "combined_score"),
    (("ЛОНГ", 60), ("ШОРТ", -60)),
)
def test_alarm_signal_passes_all_threshold_boundaries(
    direction: str,
    combined_score: float,
) -> None:
    messages = format_alarm_report(
        alarm_view(
            alarm_signal(direction=direction, combined_score=combined_score)
        )
    )

    assert len(messages) == 1
    assert f"BTCUSDT — {direction}" in messages[0]
    assert "Итоговый балл: 60,0" in messages[0]
    assert "Техническая сила сигнала" not in messages[0]


@pytest.mark.parametrize(
    "signal",
    (
        alarm_signal(combined_score=59.9),
        alarm_signal(technical_score=69.9),
        alarm_signal(confidence=69.9),
        alarm_signal(confirmations=2),
        alarm_signal(direction="НЕЙТРАЛЬНО"),
    ),
    ids=(
        "final-score",
        "technical-score",
        "confidence",
        "confirmations",
        "neutral-direction",
    ),
)
def test_alarm_signal_is_rejected_by_each_individual_rule(
    signal: SignalView,
) -> None:
    assert format_alarm_report(alarm_view(signal)) == (
        "Сильных торговых сигналов сейчас нет.",
    )


def test_alarm_signal_is_rejected_after_technical_data_failure() -> None:
    signal = alarm_signal()
    view = ReportView(
        generated_at=NOW.isoformat(),
        successful_results=(signal,),
        failed_instruments=(
            FailureView(
                symbol=signal.symbol,
                stage="технический анализ",
                error_type="ProviderError",
                message="Рыночные данные недоступны.",
            ),
        ),
        ranked_signals=(signal,),
        market_summary=MarketSummary(1, 0, 1, 0, 0, 1),
    )

    assert format_alarm_report(view) == (
        "Сильных торговых сигналов сейчас нет.",
    )


def test_alarm_report_limits_unique_reasons_to_three_and_warning_to_one() -> None:
    message = format_alarm_report(
        alarm_view(
            alarm_signal(
                explanations=(
                    "Причина 1",
                    "Причина 1",
                    "Причина 2",
                    "Причина 3",
                    "Причина 4",
                ),
                warnings=("Главный риск", "Второй риск"),
            )
        )
    )[0]

    assert message.count("• Причина 1") == 1
    assert "• Причина 2" in message
    assert "• Причина 3" in message
    assert "Причина 4" not in message
    assert "Главный риск" in message
    assert "Второй риск" not in message
    assert message.endswith("Информационный сигнал, не торговая рекомендация.")


@pytest.mark.parametrize("command", ("/screen BTCUSDT", "/crypto"))
def test_alarm_commands_return_exact_empty_message(command: str) -> None:
    result = execute_command(
        command,
        chat_id=1,
        allowed_chat_ids=frozenset({1}),
        service=Service(),
    )

    assert result.messages == ("Сильных торговых сигналов сейчас нет.",)


def test_screen_returns_short_error_for_one_invalid_symbol() -> None:
    failure = InstrumentFailure(
        instrument=Instrument("PEPPEUSDT", AssetClass.CRYPTO),
        stage="technical",
        error_type="MarketDataError",
        message="Market data is unavailable.",
    )
    report = ScreeningReport(
        NOW,
        (),
        (failure,),
        (),
        MarketSummary(1, 0, 1, 0, 0, 0),
    )

    result = execute_command(
        "/screen PEPPEUSDT",
        chat_id=1,
        allowed_chat_ids=frozenset({1}),
        service=Service(report),
    )

    assert result.messages == (
        "PEPPEUSDT: инструмент не найден или рыночные данные недоступны.",
    )


def test_screen_returns_no_signal_for_one_successful_weak_symbol() -> None:
    weak = alarm_signal(technical_score=69.9)

    assert format_screen_report(screen_view(successful=(weak,))) == (
        "Сильных торговых сигналов сейчас нет.",
    )


def test_screen_combines_valid_signal_and_invalid_symbol_error() -> None:
    strong = alarm_signal()
    messages = format_screen_report(
        screen_view(
            successful=(strong,),
            ranked=(strong,),
            failures=(technical_failure("PEPPEUSDT"),),
        )
    )

    assert len(messages) == 1
    assert "BTCUSDT — ЛОНГ" in messages[0]
    assert (
        "PEPPEUSDT: инструмент не найден или рыночные данные недоступны."
        in messages[0]
    )
    assert "Сильных торговых сигналов сейчас нет." not in messages[0]


def test_screen_with_all_market_data_unavailable_returns_only_errors() -> None:
    message = format_screen_report(
        screen_view(
            failures=(
                technical_failure("BTCUSDT"),
                technical_failure("ETHUSDT"),
            )
        )
    )[0]

    assert message.splitlines() == [
        "BTCUSDT: инструмент не найден или рыночные данные недоступны.",
        "ETHUSDT: инструмент не найден или рыночные данные недоступны.",
    ]
    assert "Сильных торговых сигналов сейчас нет." not in message


def test_screen_deduplicates_identical_instrument_errors() -> None:
    failure = technical_failure("PEPPEUSDT")
    message = format_screen_report(
        screen_view(failures=(failure, failure))
    )[0]

    assert message.count("PEPPEUSDT:") == 1


def test_crypto_failure_presentation_remains_unchanged() -> None:
    assert format_alarm_report(
        screen_view(failures=(technical_failure("PEPPEUSDT"),))
    ) == ("Сильных торговых сигналов сейчас нет.",)
