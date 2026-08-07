from __future__ import annotations

import hashlib
import re
from datetime import timedelta
from urllib.parse import urlsplit, urlunsplit

from market_signal_assistant.news.models import (
    NewsAssetType,
    NewsCategory,
    NewsImportance,
    NewsItem,
    NewsSourceRecord,
)

_PROMOTION = (
    "prize pool",
    "competition",
    "campaign",
    "bonus",
    "launchpool",
    "launch pool",
    "earn",
    "vip",
    "referral",
    "ama",
    "copy trading",
    "holiday",
    "trade and share",
    "trade to share",
    "fee discount",
    "save up to",
    "discount extended",
    "promotional fee",
    "reduced fees promotion",
)
_UNCONDITIONAL_PROMOTION = (
    "fee discount",
    "save up to",
    "discount extended",
    "promotional fee",
    "reduced fees promotion",
)
_STOP_SYMBOLS = frozenset(
    {
        "USDT",
        "USDC",
        "USD",
        "NEW",
        "LISTING",
        "DELISTING",
        "TRADING",
        "TRADE",
        "PAIRS",
        "PAIR",
        "CONTRACT",
        "PERPETUAL",
        "SPOT",
        "BYBIT",
        "UPDATE",
        "NOTICE",
        "SYSTEM",
        "MAINTENANCE",
        "NETWORK",
        "DEPOSIT",
        "WITHDRAWAL",
        "VIP",
        "AMA",
        "API",
        "P2P",
        "NFT",
        "AND",
        "THE",
        "FOR",
        "WITH",
        "FROM",
        "OFFICIAL",
        "PRODUCT",
        "MARKET",
        "WEEKLY",
        "RECAP",
        "EMERGENCY",
        "SECURITY",
        "INCIDENT",
        "SUSPENSION",
        "SERVICES",
        "SHARE",
        "POOL",
        "UTC",
        "GMT",
        "AM",
        "PM",
        "JAN",
        "JANUARY",
        "FEB",
        "FEBRUARY",
        "MAR",
        "MARCH",
        "APR",
        "APRIL",
        "MAY",
        "JUN",
        "JUNE",
        "JUL",
        "JULY",
        "AUG",
        "AUGUST",
        "SEP",
        "SEPT",
        "SEPTEMBER",
        "OCT",
        "OCTOBER",
        "NOV",
        "NOVEMBER",
        "DEC",
        "DECEMBER",
    }
)
_TAG_ONLY_STOP = _STOP_SYMBOLS | {"BTC", "ETH"}
_TOKEN = re.compile(r"\b[A-Z][A-Z0-9]{1,14}\b")
_PARENTHETICAL = re.compile(r"\(([A-Z][A-Z0-9]{1,14})\)")
_DELISTING = re.compile(r"(?i)delisting\s+of\s+([A-Z][A-Z0-9]{1,14})")
_PAIR = re.compile(r"\b[A-Z0-9]{2,15}(?:USDT|USDC|USD)\b")
_ONE_DAY = timedelta(days=1)
_PRE_MARKET = re.compile(
    r"\b(?:perpetual\s+)?pre[-\s]market(?:\s+trading)?\b",
    re.IGNORECASE,
)

_ACTIONS = {
    NewsCategory.LISTING: (
        "Не входить сразу после запуска торгов. Дождаться появления "
        "ликвидности и стабилизации спреда."
    ),
    NewsCategory.DELISTING: (
        "Проверить открытые позиции и ордера. Уточнить официальный срок "
        "прекращения торгов."
    ),
    NewsCategory.MAINTENANCE: (
        "Не планировать вход рядом с периодом обслуживания. Проверить "
        "доступность торгов и переводов после завершения."
    ),
    NewsCategory.SECURITY: (
        "Не выполнять переводы и не открывать новые позиции до официального "
        "подтверждения устранения проблемы."
    ),
    NewsCategory.NETWORK: (
        "Проверить статус депозитов, выводов и сети перед переводом средств."
    ),
    NewsCategory.TRADING_CHANGE: (
        "Проверить новые условия контракта, размер шага, плечо и сроки "
        "вступления изменений в силу."
    ),
    NewsCategory.REGULATION: "Проверить официальные ограничения и сроки изменений.",
    NewsCategory.OTHER: "Изучить официальное объявление до принятия решений.",
}


class NewsClassifier:
    def classify(self, record: NewsSourceRecord) -> NewsItem | None:
        content_text = " ".join(
            (record.title, record.description, *record.tags)
        ).casefold()
        text = f"{content_text} {record.type_key.casefold()}"
        if _contains_any(content_text, _UNCONDITIONAL_PROMOTION):
            return None
        category = _category(content_text, record.type_key)
        if category is NewsCategory.OTHER and _contains_any(content_text, _PROMOTION):
            return None
        importance = _importance(category, text, record)
        pre_market = category is NewsCategory.LISTING and _is_pre_market(content_text)
        collateral_change = _is_collateral_change(content_text)
        asset_type = _asset_type(record, category)
        symbols = extract_symbols(
            record.title,
            record.description,
            record.tags,
        )
        return NewsItem(
            stable_id=_stable_id(record),
            source=record.source,
            title=record.title,
            description=_description(
                category,
                symbols,
                pre_market=pre_market,
                collateral_change=collateral_change,
                asset_type=asset_type,
            ),
            url=record.url,
            category=category,
            importance=importance,
            symbols=symbols,
            published_at=record.published_at,
            event_starts_at=record.event_starts_at,
            tags=record.tags,
            reason=_reason(category, asset_type=asset_type),
            recommended_action=_action(
                category,
                collateral_change=collateral_change,
                asset_type=asset_type,
            ),
            asset_type=asset_type,
            event_start_date=record.event_start_date,
        )


def extract_symbols(
    title: str,
    description: str,
    tags: tuple[str, ...],
) -> tuple[str, ...]:
    selected: dict[str, None] = {}
    body = f"{title} {description}"
    token_stop = (
        _TAG_ONLY_STOP | {"UTA"}
        if _is_uta_product(body, tags)
        else _TAG_ONLY_STOP
    )
    for pattern in (_PARENTHETICAL, _DELISTING, _PAIR):
        for match in pattern.finditer(body):
            symbol = match.group(1) if match.lastindex else match.group()
            _add_symbol(selected, symbol, _STOP_SYMBOLS)
    for symbol in _TOKEN.findall(body):
        _add_symbol(selected, symbol, token_stop)
    for tag in tags:
        for symbol in _TOKEN.findall(tag):
            _add_symbol(selected, symbol, token_stop)
    return tuple(selected)


def _add_symbol(
    selected: dict[str, None],
    value: str,
    stop: frozenset[str],
) -> None:
    symbol = value.upper()
    if symbol not in stop and not symbol.isdigit():
        selected.setdefault(symbol, None)


def _category(text: str, type_key: str = "") -> NewsCategory:
    if _contains_any(text, ("delist", "cease trading", "removal of trading")):
        return NewsCategory.DELISTING
    if _contains_any(
        text,
        (
            "new listing",
            "listing of",
            "will list",
            "spot listings",
        ),
    ) or type_key.casefold() == "new_crypto":
        return NewsCategory.LISTING
    if "perpetual contract" in text and _contains_any(
        text,
        ("launch", "listing", "starts trading"),
    ):
        return NewsCategory.LISTING
    if _contains_any(text, ("hack", "exploit", "security incident", "compromised")):
        return NewsCategory.SECURITY
    if _contains_any(
        text,
        ("deposit", "withdrawal", "network upgrade", "hard fork"),
    ) and _contains_any(text, ("suspend", "resume", "upgrade", "support")):
        return NewsCategory.NETWORK
    if _contains_any(text, ("maintenance", "system upgrade", "service interruption")):
        return NewsCategory.MAINTENANCE
    if _contains_any(
        text,
        (
            "tick size",
            "leverage",
            "funding rate",
            "settlement",
            "contract adjustment",
            "trading rules",
            "trading conditions",
            "collateral ratio",
            "collateral ratios",
            "loan collateral",
            "lending terms",
            "loan terms",
        ),
    ):
        return NewsCategory.TRADING_CHANGE
    if _contains_any(text, ("regulation", "regulatory", "jurisdiction")):
        return NewsCategory.REGULATION
    return NewsCategory.OTHER


def _importance(
    category: NewsCategory,
    text: str,
    record: NewsSourceRecord,
) -> NewsImportance:
    urgent = _contains_any(
        text,
        (
            "emergency",
            "urgent",
            "effective immediately",
            "unexpected",
            "hack",
            "exploit",
            "security incident",
            "services suspended",
            "suspension of withdrawals",
            "suspension of deposits",
        ),
    )
    near_event = (
        record.event_starts_at is not None
        and record.event_starts_at >= record.published_at
        and record.event_starts_at - record.published_at <= _ONE_DAY
    )
    if category is NewsCategory.SECURITY or urgent:
        return NewsImportance.CRITICAL
    if category is NewsCategory.DELISTING and near_event:
        return NewsImportance.CRITICAL
    if category in {
        NewsCategory.LISTING,
        NewsCategory.DELISTING,
        NewsCategory.TRADING_CHANGE,
        NewsCategory.NETWORK,
    }:
        return NewsImportance.HIGH
    if category is NewsCategory.MAINTENANCE:
        return NewsImportance.HIGH if "trading" in text else NewsImportance.MEDIUM
    if category is NewsCategory.REGULATION or "update" in text:
        return NewsImportance.MEDIUM
    return NewsImportance.LOW


def _description(
    category: NewsCategory,
    symbols: tuple[str, ...],
    *,
    pre_market: bool = False,
    collateral_change: bool = False,
    asset_type: NewsAssetType = NewsAssetType.UNKNOWN,
) -> str:
    subject = ", ".join(symbols) if symbols else "указанных инструментов"
    if category is NewsCategory.LISTING and asset_type is NewsAssetType.STOCK:
        base_asset = _base_asset(symbols[0]) if symbols else "указанный актив"
        return f"Bybit запускает производный контракт на акции {base_asset}."
    if category is NewsCategory.LISTING and pre_market:
        return f"Bybit запускает предрыночную торговлю {subject}."
    if category is NewsCategory.TRADING_CHANGE and collateral_change:
        return "Bybit изменяет коэффициенты залога или условия кредитования."
    descriptions = {
        NewsCategory.LISTING: f"Bybit объявляет новый листинг {subject}.",
        NewsCategory.DELISTING: f"Bybit прекращает поддержку торговли {subject}.",
        NewsCategory.MAINTENANCE: "Bybit сообщает о техническом обслуживании сервисов.",
        NewsCategory.SECURITY: "Bybit сообщает об инциденте безопасности.",
        NewsCategory.NETWORK: "Bybit изменяет доступность депозитов, выводов или сети.",
        NewsCategory.TRADING_CHANGE: "Bybit изменяет условия торговли или контракта.",
        NewsCategory.REGULATION: "Bybit сообщает об изменении регуляторных условий.",
        NewsCategory.OTHER: "Bybit опубликовал продуктовое обновление.",
    }
    return descriptions[category]


def _is_pre_market(text: str) -> bool:
    return _PRE_MARKET.search(text) is not None


def _reason(
    category: NewsCategory,
    *,
    asset_type: NewsAssetType = NewsAssetType.UNKNOWN,
) -> str:
    if category is NewsCategory.LISTING and asset_type is NewsAssetType.STOCK:
        return (
            "Это контракт с плечом, а не покупка акции. Возможны funding, "
            "ликвидация, гэпы и широкий спред."
        )
    reasons = {
        NewsCategory.LISTING: (
            "После запуска возможны низкая ликвидность и широкий спред."
        ),
        NewsCategory.DELISTING: (
            "Открытые позиции и активные ордера могут потребовать действий до срока."
        ),
        NewsCategory.MAINTENANCE: "Обслуживание может временно ограничить операции.",
        NewsCategory.SECURITY: (
            "Инцидент может затронуть безопасность средств и операций."
        ),
        NewsCategory.NETWORK: "Переводы могут быть временно недоступны или задержаны.",
        NewsCategory.TRADING_CHANGE: (
            "Новые параметры могут изменить исполнение ордеров."
        ),
        NewsCategory.REGULATION: "Ограничения могут изменить доступность сервисов.",
        NewsCategory.OTHER: "Обновление может повлиять на использование продукта.",
    }
    return reasons[category]


def _action(
    category: NewsCategory,
    *,
    collateral_change: bool,
    asset_type: NewsAssetType,
) -> str:
    if category is NewsCategory.LISTING and asset_type is NewsAssetType.STOCK:
        return (
            "Не входить сразу после запуска. Проверить ликвидность, спред, "
            "funding и условия контракта."
        )
    if category is NewsCategory.TRADING_CHANGE and collateral_change:
        return (
            "Проверить новые коэффициенты залога, доступный лимит и риск "
            "ликвидации."
        )
    return _ACTIONS[category]


def _is_collateral_change(text: str) -> bool:
    return _contains_any(
        text,
        (
            "collateral ratio",
            "collateral ratios",
            "loan collateral",
            "lending terms",
            "loan terms",
        ),
    )


def _is_uta_product(body: str, tags: tuple[str, ...]) -> bool:
    text = " ".join((body, *tags)).casefold()
    return "uta" in text and _contains_any(
        text,
        ("uta loan", "unified trading account", "collateral ratio"),
    )


def _asset_type(
    record: NewsSourceRecord,
    category: NewsCategory,
) -> NewsAssetType:
    if record.asset_type is not NewsAssetType.UNKNOWN:
        return record.asset_type
    if category is NewsCategory.LISTING and any(
        tag.casefold() in {"spot", "spot listings"} for tag in record.tags
    ):
        return NewsAssetType.CRYPTO
    return NewsAssetType.UNKNOWN


def _base_asset(symbol: str) -> str:
    for quote in ("USDT", "USDC", "USD"):
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return symbol[: -len(quote)]
    return symbol


def _stable_id(record: NewsSourceRecord) -> str:
    parsed = urlsplit(record.url)
    normalized_url = urlunsplit(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path.rstrip("/"),
            "",
            "",
        )
    )
    identity = (
        f"{record.source.casefold()}|{normalized_url}"
        if normalized_url
        else (
            f"{record.source.casefold()}|{record.title.strip().casefold()}|"
            f"{record.published_at.isoformat()}"
        )
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _contains_any(text: str, values: tuple[str, ...]) -> bool:
    return any(value in text for value in values)
