from __future__ import annotations

import asyncio
import logging
import threading
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from market_signal_assistant.application.models import (
    MarketSummary,
    ScreeningReport,
    ScreeningRequest,
)
from market_signal_assistant.inplay.models import (
    InPlayDirection,
    InPlayReport,
    InPlayResult,
)
from market_signal_assistant.inplay.notifications import (
    InPlayNotificationService,
    JsonInPlayNotificationStore,
)
from market_signal_assistant.settings import (
    InPlayAutoSettings,
    InPlayTimingAuditSettings,
    TelegramSettings,
)
from market_signal_assistant.telegram.bot import (
    _run_sdk_bot_handlers,
    execute_command,
)
from market_signal_assistant.telegram.formatting import format_auto_inplay_results
from market_signal_assistant.telegram.inplay_auto import (
    InPlayAutoLoop,
    InPlayAutoNotifier,
)

NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)


def result(
    symbol: str,
    direction: InPlayDirection,
    score: float,
    *,
    new: bool = False,
    reasons: tuple[str, ...] = ("Относительный объём 2,0×",),
    warnings: tuple[str, ...] = ("Повышенная волатильность.",),
) -> InPlayResult:
    return InPlayResult(
        symbol=symbol,
        direction=direction,
        inplay_score=score,
        directional_score=None,
        reasons=reasons,
        warnings=warnings,
        first_seen=NOW,
        is_new_listing=new,
    )


class Scanner:
    def __init__(self, *results: InPlayResult) -> None:
        self.report = InPlayReport(NOW, results)
        self.calls = 0
        self.sources: list[str] = []

    def scan(
        self,
        maximum_results: int = 10,
        *,
        scan_source: str = "manual",
    ) -> InPlayReport:
        assert maximum_results == 10
        self.calls += 1
        self.sources.append(scan_source)
        return self.report


class Screening:
    def screen(self, request: ScreeningRequest) -> ScreeningReport:
        del request
        return ScreeningReport(NOW, (), (), (), MarketSummary(0, 0, 0, 0, 0, 0))


def notifier(
    tmp_path: Path,
    scanner: Scanner,
    *,
    chat_ids: frozenset[int] = frozenset({100}),
) -> InPlayAutoNotifier:
    decisions = InPlayNotificationService(
        JsonInPlayNotificationStore(tmp_path / "inplay_notifications.json")
    )
    return InPlayAutoNotifier(
        scanner=scanner,
        notification_service=decisions,
        allowed_chat_ids=chat_ids,
        clock=lambda: NOW,
    )


def run_once(
    auto: InPlayAutoNotifier,
    sent: list[tuple[int, str]],
) -> bool:
    async def send(chat_id: int, text: str) -> None:
        sent.append((chat_id, text))

    return asyncio.run(auto.run_once(send))


def test_auto_settings_are_disabled_with_fifteen_minute_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("INPLAY_AUTO_ENABLED", raising=False)
    monkeypatch.delenv("INPLAY_SCAN_INTERVAL_MINUTES", raising=False)

    settings = InPlayAutoSettings.from_environment()

    assert settings.enabled is False
    assert settings.interval_minutes == 15


def test_auto_settings_read_enabled_and_interval_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("INPLAY_AUTO_ENABLED", "true")
    monkeypatch.setenv("INPLAY_SCAN_INTERVAL_MINUTES", "20")

    assert InPlayAutoSettings.from_environment() == InPlayAutoSettings(True, 20)


def test_non_numeric_auto_interval_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("INPLAY_SCAN_INTERVAL_MINUTES", "soon")

    with pytest.raises(ValueError, match="целым числом"):
        InPlayAutoSettings.from_environment()


@pytest.mark.parametrize("value", [0, 4, -10])
def test_auto_interval_below_five_is_rejected(value: int) -> None:
    with pytest.raises(ValueError, match="не меньше 5"):
        InPlayAutoSettings(interval_minutes=value)


@pytest.mark.parametrize(
    ("direction", "score", "expected"),
    [
        (InPlayDirection.LONG, 59.9, False),
        (InPlayDirection.SHORT, 59.9, False),
        (InPlayDirection.LONG, 60.0, True),
        (InPlayDirection.SHORT, 60.0, True),
        (InPlayDirection.WATCH, 69.9, False),
        (InPlayDirection.WATCH, 70.0, True),
    ],
)
def test_automatic_thresholds(
    tmp_path: Path,
    direction: InPlayDirection,
    score: float,
    expected: bool,
) -> None:
    sent: list[tuple[int, str]] = []
    auto = notifier(tmp_path, Scanner(result("BTCUSDT", direction, score)))

    assert run_once(auto, sent) is expected
    assert bool(sent) is expected


def test_new_listing_watch_uses_manual_minimum_score(tmp_path: Path) -> None:
    sent: list[tuple[int, str]] = []
    auto = notifier(
        tmp_path,
        Scanner(result("NEWUSDT", InPlayDirection.WATCH, 50, new=True)),
    )

    assert run_once(auto, sent) is True
    assert "🆕 🟡 NEWUSDT — СЛЕДИМ" in sent[0][1]


def test_cycle_sends_at_most_three_results_sorted_by_score(tmp_path: Path) -> None:
    sent: list[tuple[int, str]] = []
    scanner = Scanner(
        result("AUSDT", InPlayDirection.LONG, 61),
        result("BUSDT", InPlayDirection.SHORT, 99),
        result("CUSDT", InPlayDirection.WATCH, 75),
        result("DUSDT", InPlayDirection.LONG, 80),
    )

    assert run_once(notifier(tmp_path, scanner), sent) is True

    message = sent[0][1]
    assert message.index("BUSDT") < message.index("DUSDT") < message.index("CUSDT")
    assert "AUSDT" not in message
    assert message.count("Активность:") == 3


def test_empty_or_below_threshold_cycle_sends_nothing(tmp_path: Path) -> None:
    sent: list[tuple[int, str]] = []
    auto = notifier(
        tmp_path,
        Scanner(result("BTCUSDT", InPlayDirection.WATCH, 69)),
    )

    assert run_once(auto, sent) is False
    assert sent == []


def test_duplicate_suppression_is_used_across_cycles(tmp_path: Path) -> None:
    sent: list[tuple[int, str]] = []
    auto = notifier(
        tmp_path,
        Scanner(result("BTCUSDT", InPlayDirection.LONG, 70)),
    )

    assert run_once(auto, sent) is True
    assert run_once(auto, sent) is False
    assert len(sent) == 1


@pytest.mark.parametrize(
    ("symbol", "price_change", "score"),
    (
        ("SKYAI1USDT", 39.7, 81.0),
        ("HFTUSDT", 30.9, 91.0),
    ),
)
def test_extreme_move_is_only_sent_once_as_protective_watch(
    tmp_path: Path,
    symbol: str,
    price_change: float,
    score: float,
) -> None:
    warning = (
        "Резкое движение уже состоялось; высокий риск позднего входа "
        "и сильного отката."
    )
    scanner = Scanner(
        result(
            symbol,
            InPlayDirection.LONG,
            score,
            reasons=(f"Изменение цены +{price_change:.1f}%",),
            warnings=(warning,),
        )
    )
    sent: list[tuple[int, str]] = []
    auto = notifier(tmp_path, scanner)

    assert run_once(auto, sent) is True
    assert "НЕ ДОГОНЯТЬ" in sent[0][1]
    assert "— ЛОНГ" not in sent[0][1]
    assert run_once(auto, sent) is False
    assert len(sent) == 1

    state = JsonInPlayNotificationStore(
        tmp_path / "inplay_notifications.json"
    ).load()
    assert state.records[symbol].last_notification.direction is InPlayDirection.LONG


def test_late_short_move_is_not_sent_as_short(tmp_path: Path) -> None:
    warning = "Движение уже значительно реализовано; повышен риск отката."
    scanner = Scanner(
        result(
            "BEATUSDT",
            InPlayDirection.SHORT,
            77,
            reasons=("Изменение цены −25,4%",),
            warnings=(warning,),
        )
    )
    sent: list[tuple[int, str]] = []

    assert run_once(notifier(tmp_path, scanner), sent) is False
    assert sent == []


def test_transition_to_do_not_chase_is_allowed_once_before_one_hour(
    tmp_path: Path,
) -> None:
    scanner = Scanner(result("BTCUSDT", InPlayDirection.LONG, 75))
    sent: list[tuple[int, str]] = []
    auto = notifier(tmp_path, scanner)
    assert run_once(auto, sent) is True

    scanner.report = InPlayReport(
        NOW,
        (
            result(
                "BTCUSDT",
                InPlayDirection.LONG,
                80,
                reasons=("Изменение цены +31,0%",),
                warnings=(
                    "Резкое движение уже состоялось; высокий риск позднего "
                    "входа и сильного отката.",
                ),
            ),
        ),
    )

    assert run_once(auto, sent) is True
    assert "НЕ ДОГОНЯТЬ" in sent[1][1]
    assert run_once(auto, sent) is False
    assert len(sent) == 2


def test_empty_allowlist_prevents_scan_and_logs_warning(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    scanner = Scanner(result("BTCUSDT", InPlayDirection.LONG, 70))
    sent: list[tuple[int, str]] = []

    with caplog.at_level(logging.WARNING):
        outcome = run_once(notifier(tmp_path, scanner, chat_ids=frozenset()), sent)

    assert outcome is False
    assert scanner.calls == 0
    assert sent == []
    assert "TELEGRAM_ALLOWED_CHAT_IDS" in caplog.text


def test_partial_instrument_failure_does_not_hide_successful_result(
    tmp_path: Path,
) -> None:
    # The existing scanner excludes failed instruments and returns surviving results.
    scanner = Scanner(result("ETHUSDT", InPlayDirection.SHORT, 75))
    sent: list[tuple[int, str]] = []

    assert run_once(notifier(tmp_path, scanner), sent) is True
    assert "ETHUSDT — ШОРТ" in sent[0][1]


def test_telegram_network_error_does_not_commit_or_escape(tmp_path: Path) -> None:
    path = tmp_path / "inplay_notifications.json"
    auto = notifier(
        tmp_path,
        Scanner(result("BTCUSDT", InPlayDirection.LONG, 70)),
    )

    class NetworkError(Exception):
        pass

    async def failed_send(chat_id: int, text: str) -> None:
        del chat_id, text
        raise NetworkError("secret-bearing transport details")

    assert asyncio.run(auto.run_once(failed_send)) is False
    assert JsonInPlayNotificationStore(path).load().records == {}


def test_partial_chat_delivery_does_not_commit_global_state(tmp_path: Path) -> None:
    path = tmp_path / "inplay_notifications.json"
    decisions = InPlayNotificationService(JsonInPlayNotificationStore(path))
    auto = InPlayAutoNotifier(
        scanner=Scanner(result("BTCUSDT", InPlayDirection.LONG, 70)),
        notification_service=decisions,
        allowed_chat_ids=frozenset({100, 200}),
        clock=lambda: NOW,
    )
    delivered: list[int] = []

    async def partial_send(chat_id: int, text: str) -> None:
        del text
        if chat_id == 200:
            raise OSError("transport unavailable")
        delivered.append(chat_id)

    assert asyncio.run(auto.run_once(partial_send)) is False
    assert delivered == [100]
    assert JsonInPlayNotificationStore(path).load().records == {}


def test_state_is_committed_only_after_successful_send(tmp_path: Path) -> None:
    path = tmp_path / "inplay_notifications.json"
    auto = notifier(
        tmp_path,
        Scanner(result("BTCUSDT", InPlayDirection.LONG, 70)),
    )
    sent: list[tuple[int, str]] = []

    assert run_once(auto, sent) is True

    state = JsonInPlayNotificationStore(path).load()
    assert state.records["BTCUSDT"].last_notification.inplay_score == 70


def test_restart_uses_persisted_state_without_duplicate(tmp_path: Path) -> None:
    sent: list[tuple[int, str]] = []
    scanner = Scanner(result("BTCUSDT", InPlayDirection.LONG, 70))

    assert run_once(notifier(tmp_path, scanner), sent) is True
    assert run_once(notifier(tmp_path, scanner), sent) is False
    assert len(sent) == 1


def test_two_cycles_do_not_run_concurrently(tmp_path: Path) -> None:
    started = threading.Event()
    release = threading.Event()

    class BlockingScanner(Scanner):
        def scan(
            self,
            maximum_results: int = 10,
            *,
            scan_source: str = "manual",
        ) -> InPlayReport:
            started.set()
            release.wait(timeout=2)
            return super().scan(maximum_results, scan_source=scan_source)

    auto = notifier(
        tmp_path,
        BlockingScanner(result("BTCUSDT", InPlayDirection.LONG, 70)),
    )
    sent: list[tuple[int, str]] = []

    async def scenario() -> tuple[bool, bool]:
        async def send(chat_id: int, text: str) -> None:
            sent.append((chat_id, text))

        first = asyncio.create_task(auto.run_once(send))
        await asyncio.to_thread(started.wait, 1)
        second = await auto.run_once(send)
        release.set()
        return await first, second

    assert asyncio.run(scenario()) == (True, False)
    assert len(sent) == 1


def test_auto_loop_runs_immediately_and_stops_cleanly(tmp_path: Path) -> None:
    auto = notifier(
        tmp_path,
        Scanner(result("BTCUSDT", InPlayDirection.LONG, 70)),
    )
    sent = asyncio.Event()

    async def scenario() -> None:
        async def send(chat_id: int, text: str) -> None:
            del chat_id, text
            sent.set()

        loop = InPlayAutoLoop(auto, send, interval_seconds=300)
        loop.start()
        await asyncio.wait_for(sent.wait(), timeout=1)
        await loop.stop()
        assert loop.running is False

    asyncio.run(scenario())


def test_telegram_lifecycle_starts_and_stops_in_process_auto_loop(
    tmp_path: Path,
) -> None:
    scanner = Scanner(result("BTCUSDT", InPlayDirection.LONG, 70))
    notifications = InPlayNotificationService(
        JsonInPlayNotificationStore(tmp_path / "inplay_notifications.json")
    )

    class FakeBot:
        def __init__(self) -> None:
            self.messages: list[tuple[int, str]] = []

        async def send_message(self, *, chat_id: int, text: str) -> None:
            self.messages.append((chat_id, text))

    class FakeApplication:
        def __init__(self, builder: FakeBuilder) -> None:
            self._builder = builder
            self.bot = FakeBot()

        def add_handler(self, handler: object) -> None:
            del handler

        def run_polling(self) -> None:
            async def lifecycle() -> None:
                assert self._builder.on_start is not None
                assert self._builder.on_stop is not None
                await self._builder.on_start(self)
                for _ in range(100):
                    if self.bot.messages:
                        break
                    await asyncio.sleep(0.001)
                await self._builder.on_stop(self)

            asyncio.run(lifecycle())

    class FakeBuilder:
        def __init__(self) -> None:
            self.on_start: Any = None
            self.on_stop: Any = None
            self.application: FakeApplication | None = None

        def token(self, value: str) -> FakeBuilder:
            assert value == "token"
            return self

        def post_init(self, callback: Any) -> FakeBuilder:
            self.on_start = callback
            return self

        def post_shutdown(self, callback: Any) -> FakeBuilder:
            self.on_stop = callback
            return self

        def build(self) -> FakeApplication:
            self.application = FakeApplication(self)
            return self.application

    builder = FakeBuilder()
    _run_sdk_bot_handlers(
        TelegramSettings("token", frozenset({100})),
        Screening(),
        scanner,
        False,
        InPlayAutoSettings(True, 5),
        notifications,
        lambda: builder,
        lambda *args: args,
        SimpleNamespace(COMMAND=object()),
    )

    assert builder.application is not None
    assert len(builder.application.bot.messages) == 1
    assert builder.application.bot.messages[0][0] == 100


def test_shadow_audit_lifecycle_runs_with_inplay_auto_disabled_without_sending(
    tmp_path: Path,
) -> None:
    scanner = Scanner()
    notification_path = tmp_path / "inplay_notifications.json"
    notifications = InPlayNotificationService(
        JsonInPlayNotificationStore(notification_path)
    )

    class FakeBot:
        def __init__(self) -> None:
            self.messages: list[tuple[int, str]] = []

        async def send_message(self, *, chat_id: int, text: str) -> None:
            self.messages.append((chat_id, text))

    class FakeApplication:
        def __init__(self, builder: FakeBuilder) -> None:
            self._builder = builder
            self.bot = FakeBot()

        def add_handler(self, handler: object) -> None:
            del handler

        def run_polling(self) -> None:
            async def lifecycle() -> None:
                assert self._builder.on_start is not None
                assert self._builder.on_stop is not None
                await self._builder.on_start(self)
                for _ in range(100):
                    if scanner.calls:
                        break
                    await asyncio.sleep(0.001)
                await self._builder.on_stop(self)

            asyncio.run(lifecycle())

    class FakeBuilder:
        def __init__(self) -> None:
            self.on_start: Any = None
            self.on_stop: Any = None
            self.application: FakeApplication | None = None

        def token(self, value: str) -> FakeBuilder:
            assert value == "token"
            return self

        def post_init(self, callback: Any) -> FakeBuilder:
            self.on_start = callback
            return self

        def post_shutdown(self, callback: Any) -> FakeBuilder:
            self.on_stop = callback
            return self

        def build(self) -> FakeApplication:
            self.application = FakeApplication(self)
            return self.application

    builder = FakeBuilder()
    _run_sdk_bot_handlers(
        TelegramSettings("token", frozenset({100})),
        Screening(),
        scanner,
        False,
        InPlayAutoSettings(False, 15),
        notifications,
        lambda: builder,
        lambda *args: args,
        SimpleNamespace(COMMAND=object()),
        timing_audit_settings=InPlayTimingAuditSettings(
            enabled=True,
            auto_enabled=True,
            interval_minutes=5,
        ),
    )

    assert builder.application is not None
    assert scanner.sources == ["timing_audit_auto"]
    assert builder.application.bot.messages == []
    assert notification_path.exists() is False


def test_automatic_format_limits_indicators_and_warning() -> None:
    message = format_auto_inplay_results(
        (
            result(
                "BTCUSDT",
                InPlayDirection.LONG,
                80,
                reasons=(
                    "Изменение цены +3,3%",
                    "Волатильность ATR 4,2%",
                    "Относительный объём 2,2×",
                    "Цена вышла из локального диапазона",
                ),
                warnings=("Главный риск", "Второй риск"),
            ),
        )
    )

    assert message.startswith("🚨 СИЛЬНЫЙ IN PLAY")
    assert message.count("Активность:") == 1
    assert "Цена: +3,3%" in message
    assert "Волатильность: высокая" in message
    assert "Объём: высокий" in message
    assert "Главный риск" not in message
    assert "Второй риск" not in message
    assert message.endswith(
        "⚠️ Перед входом проверь уровень, стоп и соотношение риска к прибыли."
    )


def test_automatic_watch_with_breakout_does_not_say_wait_for_breakout() -> None:
    message = format_auto_inplay_results(
        (
            result(
                "BTCUSDT",
                InPlayDirection.WATCH,
                75,
                reasons=(
                    "Относительный объём 2,0×",
                    "Цена вышла из локального диапазона",
                ),
            ),
        )
    )

    assert "Пробой диапазона зафиксирован" in message
    assert "Ждём пробой" not in message
    assert "Ждём закрепление, ретест" in message


def test_status_shows_auto_state_and_interval() -> None:
    execution = execute_command(
        "/status",
        chat_id=1,
        allowed_chat_ids=frozenset({1}),
        service=Screening(),
        inplay_auto_enabled=True,
        inplay_scan_interval_minutes=20,
    )

    assert "Автосканирование IN PLAY: включено" in execution.messages[0]
    assert "Интервал IN PLAY: 20 минут" in execution.messages[0]


def test_manual_inplay_keeps_manual_header_with_new_plain_language_body() -> None:
    from market_signal_assistant.telegram.formatting import format_inplay_report

    message = format_inplay_report(
        InPlayReport(
            NOW,
            (result("BTCUSDT", InPlayDirection.WATCH, 70),),
        )
    )[0]

    assert message.startswith("🔥 IN PLAY\n\n1. 🟡 BTCUSDT — СЛЕДИМ")
    assert "ТРЕБУЕТ ВНИМАНИЯ" not in message
