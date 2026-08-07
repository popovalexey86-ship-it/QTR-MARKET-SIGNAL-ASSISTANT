import builtins
from typing import Any
from unittest.mock import Mock

import pytest

from market_signal_assistant.composition import build_screening_service
from market_signal_assistant.providers.bybit_liquidations import (
    BybitLiquidationAccumulator,
    BybitLiquidationStream,
)
from market_signal_assistant.telegram import bot
from market_signal_assistant.web import app as web_app


def test_bootstrap_is_offline() -> None:
    getter = Mock(side_effect=AssertionError("HTTP opened"))
    socket_factory = Mock(side_effect=AssertionError("WebSocket opened"))
    service, derivatives = build_screening_service(
        getter=getter, websocket_factory=socket_factory
    )
    assert service is not None
    assert derivatives.stream.running is False
    getter.assert_not_called()
    socket_factory.assert_not_called()


def test_rest_only_stream_construction_does_not_import_pybit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_import = builtins.__import__

    def guarded_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name.startswith("pybit"):
            raise AssertionError("pybit imported in REST-only mode")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    stream = BybitLiquidationStream(BybitLiquidationAccumulator())
    assert stream.running is False


@pytest.mark.parametrize("entrypoint", [bot.main, web_app.main])
def test_interface_help_does_not_build_composition(
    entrypoint: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_import = builtins.__import__

    def guarded_import(name: str, *args: Any, **kwargs: Any) -> Any:
        forbidden = (
            name == "market_signal_assistant.composition"
            or name.startswith("pybit")
            or name == "telegram.ext"
        )
        if forbidden:
            raise AssertionError(f"runtime dependency imported during --help: {name}")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    with pytest.raises(SystemExit) as exit_info:
        entrypoint(["--help"])
    assert exit_info.value.code == 0


def test_telegram_help_description_is_russian(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exit_info:
        bot.main(["--help"])

    assert exit_info.value.code == 0
    help_text = capsys.readouterr().out
    assert "Telegram-бот информационного помощника" in help_text
    assert "Market Signal Telegram bot" not in help_text
    assert "использование:" in help_text
    assert "параметры:" in help_text
    assert "показать справку и выйти" in help_text


def test_missing_telegram_sdk_prevents_composition_and_live_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("DERIVATIVES_LIVE_ENABLED", "true")
    monkeypatch.setenv("DERIVATIVES_LIVE_SYMBOLS", "BTCUSDT")

    def missing_sdk() -> object:
        raise RuntimeError("Telegram support requires the optional dependency.")

    monkeypatch.setattr(bot, "_load_telegram_sdk", missing_sdk)
    original_import = builtins.__import__

    def guarded_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "market_signal_assistant.composition":
            raise AssertionError("composition/WebSocket reached before SDK validation")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    with pytest.raises(RuntimeError, match="optional dependency"):
        bot.main([])
