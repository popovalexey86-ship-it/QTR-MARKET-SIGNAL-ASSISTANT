from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime, timedelta
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
    def __init__(
        self, quotes: list[Mapping[str, PublicPriceQuote] | Exception]
    ) -> None:
        self._quotes = iter(quotes)
        self.calls: list[tuple[str, ...]] = []

    def latest_prices(
        self, symbols: tuple[str, ...]
    ) -> Mapping[str, PublicPriceQuote]:
        self.calls.append(symbols)
        value = next(self._quotes)
        if isinstance(value, Exception):
            raise value
        return value


def batch(price: float, at: datetime = NOW) -> Mapping[str, PublicPriceQuote]:
    return {"BTCUSDT": PublicPriceQuote("BTCUSDT", price, at)}


def test_wait_to_now_transition_is_tracked_causally(tmp_path: Path) -> None:
    path = tmp_path / "entry-readiness.jsonl"
    provider = SequencePriceProvider(
        [
            batch(100.6),
            batch(100.2, NOW + timedelta(seconds=20)),
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


def test_bybit_public_provider_batches_complete_scan_in_one_request() -> None:
    calls: list[str] = []

    def getter(url: str, timeout: float) -> Mapping[str, Any]:
        calls.append(url)
        assert timeout == 3.0
        return {
            "retCode": 0,
            "time": int(NOW.timestamp() * 1000),
            "result": {
                "list": [
                    {"symbol": "BTCUSDT", "lastPrice": "100.25"},
                    {"symbol": "ETHUSDT", "lastPrice": "25.50"},
                    {"symbol": "IGNORED", "lastPrice": "1"},
                ]
            },
        }

    quotes = BybitPublicProvider(getter=getter, timeout=3.0).latest_prices(
        ("ETHUSDT", "BTCUSDT")
    )

    assert quotes == {
        "BTCUSDT": PublicPriceQuote("BTCUSDT", 100.25, NOW),
        "ETHUSDT": PublicPriceQuote("ETHUSDT", 25.5, NOW),
    }
    assert calls == ["https://api.bybit.com/v5/market/tickers?category=linear"]


def test_service_requests_one_batch_for_multiple_symbols(tmp_path: Path) -> None:
    provider = SequencePriceProvider(
        [
            {
                "BTCUSDT": PublicPriceQuote("BTCUSDT", 100.2, NOW),
                "ETHUSDT": PublicPriceQuote("ETHUSDT", 100.2, NOW),
            }
        ]
    )
    original = candidate()
    other = replace(
        original,
        episode_id="eth-episode",
        source_input=replace(original.source_input, symbol="ETHUSDT"),
        result=replace(original.result, symbol="ETHUSDT"),
    )
    service = EntryReadinessShadowService(
        EntryReadinessEngine(),
        provider,
        JsonlEntryReadinessAuditStore(tmp_path / "audit.jsonl"),
    )

    evaluations = service.evaluate((other, original), NOW)

    assert len(evaluations) == 2
    assert provider.calls == [("BTCUSDT", "ETHUSDT")]


def test_transition_state_is_bounded(tmp_path: Path) -> None:
    provider = SequencePriceProvider(
        [
            {f"S{i}USDT": PublicPriceQuote(f"S{i}USDT", 100.2, NOW)}
            for i in range(3)
        ]
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


def test_same_episode_wait_wait_now_preserves_confirmation_observation(
    tmp_path: Path,
) -> None:
    provider = SequencePriceProvider(
        [batch(100.2), batch(100.2), batch(100.2)]
    )
    service = EntryReadinessShadowService(
        EntryReadinessEngine(),
        provider,
        JsonlEntryReadinessAuditStore(tmp_path / "audit.jsonl"),
    )
    first = candidate(confirmed=False)
    second = replace(
        candidate(confirmed=False, analyzed_at=NOW + timedelta(seconds=10)),
        source_input=replace(
            candidate(confirmed=False).source_input,
            snapshot_ids=("scan-2",),
            analyzed_at=NOW + timedelta(seconds=10),
        ),
    )
    third = candidate(confirmed=True, analyzed_at=NOW + timedelta(seconds=20))

    one = service.evaluate((first,), NOW)[0]
    two = service.evaluate((second,), NOW + timedelta(seconds=10))[0]
    three = service.evaluate((third,), NOW + timedelta(seconds=20))[0]

    assert one.setup_episode_key == two.setup_episode_key == three.setup_episode_key
    assert [one.user_readiness, two.user_readiness, three.user_readiness] == [
        UserReadiness.WAIT,
        UserReadiness.WAIT,
        UserReadiness.NOW,
    ]
    assert three.transition == "WAIT_TO_NOW"
    assert three.first_confirmation_observed_at == NOW + timedelta(seconds=20)


def test_confirmation_age_does_not_reset_on_later_scan(tmp_path: Path) -> None:
    provider = SequencePriceProvider(
        [batch(100.2), batch(100.2, NOW + timedelta(seconds=30))]
    )
    service = EntryReadinessShadowService(
        EntryReadinessEngine(),
        provider,
        JsonlEntryReadinessAuditStore(tmp_path / "audit.jsonl"),
    )

    first = service.evaluate((candidate(),), NOW)[0]
    second = service.evaluate(
        (candidate(analyzed_at=NOW + timedelta(seconds=30)),),
        NOW + timedelta(seconds=30),
    )[0]

    assert first.first_confirmation_observed_at == NOW
    assert second.first_confirmation_observed_at == NOW
    assert second.confirmation_age_seconds == 30.0


def test_restart_recovers_wait_to_now_transition_from_jsonl(tmp_path: Path) -> None:
    path = tmp_path / "audit.jsonl"
    first_service = EntryReadinessShadowService(
        EntryReadinessEngine(),
        SequencePriceProvider([batch(100.6)]),
        JsonlEntryReadinessAuditStore(path),
    )
    waiting = first_service.evaluate((candidate(),), NOW)[0]
    assert waiting.user_readiness is UserReadiness.WAIT

    restarted = EntryReadinessShadowService(
        EntryReadinessEngine(),
        SequencePriceProvider([batch(100.2, NOW + timedelta(seconds=20))]),
        JsonlEntryReadinessAuditStore(path),
    )
    ready = restarted.evaluate(
        (candidate(analyzed_at=NOW + timedelta(seconds=20)),),
        NOW + timedelta(seconds=20),
    )[0]

    assert ready.previous_user_readiness is UserReadiness.WAIT
    assert ready.transition == "WAIT_TO_NOW"
    assert ready.first_wait_at == NOW
    assert ready.wait_to_now_seconds == 20.0
    assert ready.first_confirmation_observed_at == NOW
