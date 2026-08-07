from __future__ import annotations

from typing import Protocol

from market_signal_assistant.news.models import NewsSourceRecord


class NewsDataError(RuntimeError):
    """Safe provider-level failure for public news data."""


class AnnouncementProvider(Protocol):
    def fetch(
        self,
        page: int = 1,
        limit: int = 20,
    ) -> tuple[NewsSourceRecord, ...]: ...
