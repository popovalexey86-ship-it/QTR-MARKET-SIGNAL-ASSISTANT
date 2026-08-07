from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from market_signal_assistant.news.bybit_provider import BybitAnnouncementProvider
from market_signal_assistant.news.classifier import NewsClassifier
from market_signal_assistant.news.models import (
    NewsAssetType,
    NewsCategory,
    NewsItem,
    NewsSourceRecord,
)
from market_signal_assistant.news.notifications import (
    JsonNewsNotificationStore,
    NewsNotificationDecision,
    NewsNotificationDecisionKind,
    NewsNotificationService,
)
from market_signal_assistant.telegram.formatting import format_auto_news_event

NOW = datetime(2026, 8, 3, 8, tzinfo=UTC)


def announcement(
    title: str,
    *,
    description: str = "",
    type_key: str = "maintenance_updates",
    tags: tuple[str, ...] = (),
    slug: str = "notice",
    published_at: datetime = NOW,
) -> Mapping[str, Any]:
    milliseconds = int(published_at.timestamp() * 1000)
    return {
        "title": title,
        "description": description,
        "type": {"key": type_key, "title": "Maintenance Updates"},
        "tags": list(tags),
        "url": f"https://announcements.bybit.com/en-US/article/{slug}/",
        "dateTimestamp": milliseconds,
        "publishTime": milliseconds,
        "startDateTimestamp": milliseconds,
        "endDateTimestamp": milliseconds,
    }


def instrument(symbol: str, symbol_type: str) -> Mapping[str, Any]:
    return {
        "symbol": symbol,
        "contractType": "LinearPerpetual",
        "status": "Trading",
        "baseCoin": symbol.removesuffix("USDT"),
        "quoteCoin": "USDT",
        "settleCoin": "USDT",
        "symbolType": symbol_type,
    }


def fetch_item(
    row: Mapping[str, Any],
    *instruments: Mapping[str, Any],
) -> NewsItem:
    def getter(url: str, timeout: float) -> Mapping[str, Any]:
        del timeout
        if "instruments-info" in url:
            return {
                "retCode": 0,
                "result": {"list": list(instruments), "nextPageCursor": ""},
            }
        return {"retCode": 0, "result": {"list": [row]}}

    record = BybitAnnouncementProvider(getter=getter).fetch()[0]
    item = NewsClassifier().classify(record)
    assert item is not None
    return item


@pytest.mark.parametrize("symbol", ["JNJUSDT", "XOMUSDT", "SPOTUSDT"])
def test_stock_perpetual_is_derived_from_official_instrument_metadata(
    symbol: str,
) -> None:
    item = fetch_item(
        announcement(
            f"New listing: {symbol} Perpetual Contract, with up to 25x leverage",
            description=(
                f"Bybit has listed the {symbol} Perpetual Contract. "
                "Trading is now open with up to 25x leverage."
            ),
            type_key="new_crypto",
            tags=("Derivatives", "Institutions"),
            slug=symbol.lower(),
        ),
        instrument(symbol, "stock"),
    )

    assert item.asset_type is NewsAssetType.STOCK
    assert item.symbols == (symbol,)


def test_stock_perpetual_uses_stock_specific_user_format() -> None:
    item = fetch_item(
        announcement(
            "New listing: JNJUSDT Perpetual Contract, with up to 25x leverage",
            description="Bybit has listed the JNJUSDT Perpetual Contract.",
            type_key="new_crypto",
            tags=("Derivatives", "Institutions"),
        ),
        instrument("JNJUSDT", "stock"),
    )
    message = format_auto_news_event(
        NewsNotificationDecision(
            item,
            True,
            NewsNotificationDecisionKind.INITIAL,
        ),
        None,
        NOW,
    )

    assert "🟠 НОВОЕ — АКЦИОННЫЙ ПЕРПЕТУАЛ" in message
    assert "Bybit запускает производный контракт на акции JNJ." in message
    assert (
        "Это контракт с плечом, а не покупка акции. Возможны funding, "
        "ликвидация, гэпы и широкий спред."
    ) in message
    assert (
        "Не входить сразу после запуска. Проверить ликвидность, спред, "
        "funding и условия контракта."
    ) in message


def test_uta_loans_is_trading_change_and_uta_is_not_a_symbol() -> None:
    item = fetch_item(
        announcement(
            "[Important] UTA Loans collateral ratios increased",
            description="[Important] UTA Loans collateral ratios increased",
            tags=("Institutions", "UTA"),
        )
    )

    assert item.category is NewsCategory.TRADING_CHANGE
    assert item.symbols == ()
    assert item.description == (
        "Bybit изменяет коэффициенты залога или условия кредитования."
    )
    assert item.recommended_action == (
        "Проверить новые коэффициенты залога, доступный лимит и риск ликвидации."
    )


def test_fee_discount_promotion_is_fully_discarded() -> None:
    record = NewsSourceRecord(
        source="Bybit",
        title="Save up to 14%: Short-dated Options fee discount extended",
        description="Options fee discount in Aug",
        url="https://announcements.bybit.com/en-US/article/discount/",
        type_key="maintenance_updates",
        tags=("Options",),
        published_at=NOW,
        event_starts_at=None,
    )

    assert NewsClassifier().classify(record) is None


def test_extended_without_maintenance_context_is_not_maintenance() -> None:
    record = NewsSourceRecord(
        source="Bybit",
        title="Availability extended for ABCUSDT",
        description="The availability period has been extended.",
        url="https://announcements.bybit.com/en-US/article/extended/",
        type_key="maintenance_updates",
        tags=(),
        published_at=NOW,
        event_starts_at=None,
    )
    item = NewsClassifier().classify(record)

    assert item is not None
    assert item.category is not NewsCategory.MAINTENANCE


def test_zrc_suspension_extracts_date_without_copying_published_time() -> None:
    item = fetch_item(
        announcement(
            "Notice of Suspension for Zircuit (ZRC) Network Deposit and "
            "Withdrawal (Aug 04, 2026)",
            type_key="maintenance_updates",
            tags=("Crypto Deposit", "Upgrades"),
            published_at=NOW,
        )
    )

    assert item.category is NewsCategory.NETWORK
    assert item.event_starts_at is None
    assert item.event_start_date is not None
    assert item.event_start_date.isoformat() == "2026-08-04"
    message = format_auto_news_event(
        NewsNotificationDecision(
            item,
            True,
            NewsNotificationDecisionKind.INITIAL,
        ),
        None,
        NOW,
    )
    assert (
        "Ограничение начинается: 2026-08-04, точное время не указано."
        in message
    )
    assert "2026-08-03 08:00 UTC" not in message


def test_existing_crypto_listing_remains_crypto() -> None:
    item = fetch_item(
        announcement(
            "New listing: BTCUSDT Perpetual Contract",
            description="Bybit has listed the BTCUSDT Perpetual Contract.",
            type_key="new_crypto",
            tags=("Derivatives",),
        ),
        instrument("BTCUSDT", ""),
    )

    assert item.asset_type is NewsAssetType.CRYPTO
    assert item.category is NewsCategory.LISTING
    assert item.description == "Bybit объявляет новый листинг BTCUSDT."


def test_corrected_stock_item_is_suppressed_after_one_successful_send(
    tmp_path: Path,
) -> None:
    item = fetch_item(
        announcement(
            "New listing: JNJUSDT Perpetual Contract",
            description="Bybit has listed the JNJUSDT Perpetual Contract.",
            type_key="new_crypto",
            tags=("Derivatives", "Institutions"),
        ),
        instrument("JNJUSDT", "stock"),
    )
    notifications = NewsNotificationService(
        JsonNewsNotificationStore(tmp_path / "news_notifications.json")
    )

    first = notifications.prepare((item,), NOW)
    notifications.commit(first, frozenset({item.stable_id}))
    repeated = notifications.prepare((item,), NOW)

    assert repeated.decisions[0].should_notify is False
