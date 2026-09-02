from __future__ import annotations

import pytest

from market_signal_assistant.qtr_micro_scalper.live.collector import AsyncWebSocket
from market_signal_assistant.qtr_micro_scalper_v3.service import (
    V3ServiceSettings,
    build_v3_shadow_service,
    parse_args,
)


def test_v3_disabled_by_default_and_shadow_guarded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "QTR_SCALPER_V3_ENABLED",
        "QTR_SCALPER_V3_SHADOW_MODE",
        "QTR_SCALPER_V3_SYMBOLS",
    ):
        monkeypatch.delenv(name, raising=False)
    settings = V3ServiceSettings.from_environment()
    assert settings.enabled is False
    assert settings.shadow_mode is True


def test_build_is_lazy_and_does_not_open_network() -> None:
    calls = 0

    async def socket_factory(_url: str) -> AsyncWebSocket:
        nonlocal calls
        calls += 1
        raise AssertionError("network must be lazy")

    service = build_v3_shadow_service(
        V3ServiceSettings(enabled=True, shadow_mode=True, symbols=("BTCUSDT",)),
        websocket_factory=socket_factory,
    )
    assert service.running is False
    assert calls == 0


def test_cli_help_is_offline() -> None:
    with pytest.raises(SystemExit) as raised:
        parse_args(["--help"])
    assert raised.value.code == 0
