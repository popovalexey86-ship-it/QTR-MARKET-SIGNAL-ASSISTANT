from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from market_signal_assistant.settings import (
    EarlyDiscoverySettings,
    InPlayAutoSettings,
    InPlayTimingAuditSettings,
    NewsAutoSettings,
    TelegramSettings,
)
from market_signal_assistant.telegram.bot import _run_sdk_bot_handlers


@pytest.mark.parametrize(
    "configured,expected", [("7", True), ("8", False), ("", False)]
)
def test_observer_reuses_existing_sdk_lifecycle_and_explicit_authorized_chats(
    tmp_path, monkeypatch, configured, expected
):
    monkeypatch.setenv("QTR_SCALPER_V2_NOTIFY_CHAT_IDS", configured)
    monkeypatch.setenv(
        "QTR_SCALPER_V2_NOTIFY_CURSOR_PATH", str(tmp_path / "cursor.json")
    )
    monkeypatch.setenv(
        "QTR_SCALPER_V2_CONTROL_SNAPSHOT_PATH", str(tmp_path / "control.json")
    )
    workers = []

    class Observer:
        def __init__(self, snapshots, cursors, config, send):
            self.starts = 0
            self.stops = 0
            self.config = config
            workers.append(self)

        def start(self):
            self.starts += 1

        async def stop(self):
            self.stops += 1

    monkeypatch.setattr(
        "market_signal_assistant.telegram.bot.ScalperNotifier", Observer
    )

    class App:
        def __init__(self, builder):
            self.builder = builder
            self.handlers = []
            self.polls = 0
            self.bot = SimpleNamespace()

        def add_handler(self, handler):
            self.handlers.append(handler)

        def run_polling(self):
            self.polls += 1

            async def scenario():
                if self.builder.on_start:
                    await self.builder.on_start(self)
                if self.builder.on_stop:
                    await self.builder.on_stop(self)

            asyncio.run(scenario())

    class Builder:
        def __init__(self):
            self.on_start = None
            self.on_stop = None

        def token(self, token):
            return self

        def post_init(self, callback):
            self.on_start = callback
            return self

        def post_shutdown(self, callback):
            self.on_stop = callback
            return self

        def build(self):
            self.app = App(self)
            return self.app

    builder = Builder()
    _run_sdk_bot_handlers(
        TelegramSettings("fake-test-token", frozenset({7})),
        None,
        None,
        False,
        InPlayAutoSettings(enabled=False),
        None,
        lambda: builder,
        lambda *a: a[1],
        SimpleNamespace(COMMAND=True),
        news_auto_settings=NewsAutoSettings(enabled=False),
        timing_audit_settings=InPlayTimingAuditSettings(enabled=False),
        early_discovery_settings=EarlyDiscoverySettings(enabled=False),
    )
    assert builder.app.polls == 1 and len(builder.app.handlers) == 1
    assert bool(workers) == expected
    if expected:
        assert workers[0].starts == workers[0].stops == 1
        assert workers[0].config.chat_ids == (7,)
