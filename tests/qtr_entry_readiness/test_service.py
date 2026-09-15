from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any

from market_signal_assistant.providers import BybitPublicProvider, PublicPriceQuote
from market_signal_assistant.qtr_entry_readiness.audit import (
    JsonlEntryReadinessAuditStore,
)
from market_signal_assistant.qtr_entry_readiness.engine import EntryReadinessEngine
from market_signal_assistant.qtr_entry_readiness.models import UserReadiness
from market_signal_assistant.qtr_entry_readiness.service import (
    EntryReadinessShadowService,
)

from .test_engine import NOW, candidate


class SequencePriceProvider:
    def __init__(self, quotes: list[PublicPriceQuote | Exception]) -> None:
        self._quotes = iter(quotes)

    def latest_price(self, symbol: str) -> PublicPriceQuote:
        value = next(self._quotes)
        if isinstance(value, Exception):
            raise value
        assert value.symbol == symbol
        return value


def test_wait_to_now_transition_is_tracked_causally(tmp_path: Path) -> None:
    path = tmp_path / "entry-readiness.jsonl"
    provider = SequencePriceProvider(
        [
            PublicPriceQuote("BTCUSDT", 100.6, NOW),
            PublicPriceQuote("BTCUSDT", 100.2, NOW + timedelta(seconds=20)),
        ]
    )
    service = EntryReadinessShadowService(
        EntryReadinessEngine(),
        provider,
        JsonlEntryReadinessAuditStore(path),
    )

    first = service.evaluate((candidate(),), NOW)[0]
    second_item = candidate(analyzed_at=NOW + timedelta(seconds=20))
    second = service.evaluate((second_item,), NOW + timedelta(seconds=20))[0]

    assert first.user_readiness is UserReadiness.WAIT
    assert second.user_readiness is UserReadiness.NOW
    assert second.previous_user_readiness is UserReadiness.WAIT
    assert second.transition == "WAIT_TO_NOW"
    assert second.wait_to_now_seconds == 20.0
    assert len(path.read_text(encoding="utf-8").splitlines()) == 2


def test_provider_failure_is_isolated_as_suppression(tmp_path: Path) -> None:
    path = tmp_path / "entry-readiness.jsonl"
    service = EntryReadinessShadowService(
        EntryReadinessEngine(),
        SequencePriceProvider([RuntimeError("public feed unavailable")]),
        JsonlEntryReadinessAuditStore(path),
    )

    result = service.evaluate((candidate(),), NOW)

    assert len(result) == 1
    assert result[0].user_readiness is None
    assert json.loads(path.read_text(encoding="utf-8"))["internal_reason"] == (
        "FRESH_PRICE_MISSING"
    )


def test_bybit_public_provider_returns_timestamped_quote_without_private_api() -> None:
    calls: list[str] = []

    def getter(url: str, timeout: float) -> Mapping[str, Any]:
        calls.append(url)
        assert timeout == 3.0
        return {
            "retCode": 0,
            "time": int(NOW.timestamp() * 1000),
            "result": {
                "list": [{"symbol": "BTCUSDT", "lastPrice": "100.25"}]
            },
        }

    quote = BybitPublicProvider(getter=getter, timeout=3.0).latest_price("btcusdt")

    assert quote == PublicPriceQuote("BTCUSDT", 100.25, NOW)
    assert len(calls) == 1
    assert "/v5/market/tickers?" in calls[0]
    assert "symbol=BTCUSDT" in calls[0]
    assert "api_key" not in calls[0].lower()


def test_transition_state_is_bounded(tmp_path: Path) -> None:
    provider = SequencePriceProvider(
        [PublicPriceQuote(f"S{i}USDT", 100.2, NOW) for i in range(3)]
    )
    service = EntryReadinessShadowService(
        EntryReadinessEngine(),
        provider,
        JsonlEntryReadinessAuditStore(tmp_path / "audit.jsonl"),
        state_capacity=2,
    )

    for index in range(3):
        item = candidate()
        source = replace(item.source_input, symbol=f"S{index}USDT")
        result = replace(item.result, symbol=f"S{index}USDT")
        updated = replace(
            item,
            source_input=source,
            result=result,
            episode_id=f"e{index}",
        )
        service.evaluate((updated,), NOW)

    assert service.tracked_episode_count == 2
