from __future__ import annotations

import asyncio

from market_signal_assistant.qtr_micro_scalper.live.collector import (
    UnifiedMarketDataCollector,
)
from market_signal_assistant.settings import QtrScalperV2LiveSettings


class FakeStream:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.starts = 0
        self.stops = 0

    async def start(self) -> None:
        self.starts += 1
        if self.fail:
            raise RuntimeError("isolated")

    async def stop(self) -> None:
        self.stops += 1


def test_unified_collector_isolates_stream_start_error() -> None:
    async def scenario() -> None:
        healthy, failing = FakeStream(), FakeStream(fail=True)
        collector = UnifiedMarketDataCollector((healthy, failing))
        await collector.start()
        assert collector.status.running
        assert collector.status.stream_errors == ("RuntimeError: isolated",)
        assert healthy.starts == failing.starts == 1
        await collector.stop()
        assert healthy.stops == failing.stops == 1

    asyncio.run(scenario())


def test_live_settings_are_disabled_and_shadow_only_by_default() -> None:
    settings = QtrScalperV2LiveSettings()
    assert not settings.enabled
    assert settings.shadow_mode


def test_live_settings_reject_execution_mode() -> None:
    try:
        QtrScalperV2LiveSettings(enabled=True, shadow_mode=False)
    except ValueError as exc:
        assert "shadow mode only" in str(exc)
    else:
        raise AssertionError("Live V2 must remain shadow-only")
