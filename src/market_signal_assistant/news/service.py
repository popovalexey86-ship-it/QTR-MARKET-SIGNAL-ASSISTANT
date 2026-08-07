from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from market_signal_assistant.news.classifier import NewsClassifier
from market_signal_assistant.news.models import NewsImportance, NewsItem, NewsReport
from market_signal_assistant.news.provider import AnnouncementProvider

_LOGGER = logging.getLogger(__name__)
_IMPORTANCE_ORDER = {
    NewsImportance.CRITICAL: 0,
    NewsImportance.HIGH: 1,
    NewsImportance.MEDIUM: 2,
    NewsImportance.LOW: 3,
}


class NewsService:
    PAGE_LIMIT = 20
    MAXIMUM_PAGES = 5
    MAXIMUM_RESULTS = 10

    def __init__(
        self,
        provider: AnnouncementProvider,
        classifier: NewsClassifier,
        *,
        lookback_hours: int = 24,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not 1 <= lookback_hours <= 168:
            raise ValueError("News lookback must be between 1 and 168 hours.")
        self._provider = provider
        self._classifier = classifier
        self._lookback_hours = lookback_hours
        self._clock = clock or (lambda: datetime.now(UTC))

    def get_important(self) -> NewsReport:
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("News service clock must return timezone-aware time.")
        now = now.astimezone(UTC)
        cutoff = now - timedelta(hours=self._lookback_hours)
        items: list[NewsItem] = []
        for page in range(1, self.MAXIMUM_PAGES + 1):
            records = self._provider.fetch(page=page, limit=self.PAGE_LIMIT)
            for index, record in enumerate(records):
                try:
                    item = self._classifier.classify(record)
                except (TypeError, ValueError) as error:
                    _LOGGER.warning(
                        "Пропущена некорректная классификация новости "
                        "(page=%s, index=%s, error=%s).",
                        page,
                        index,
                        type(error).__name__,
                    )
                    continue
                if (
                    item is not None
                    and item.importance is not NewsImportance.LOW
                    and cutoff <= item.published_at <= now
                ):
                    items.append(item)
            if len(records) < self.PAGE_LIMIT:
                break
        ranked = tuple(
            sorted(
                {item.stable_id: item for item in items}.values(),
                key=lambda item: (
                    _IMPORTANCE_ORDER[item.importance],
                    -item.published_at.timestamp(),
                ),
            )[: self.MAXIMUM_RESULTS]
        )
        return NewsReport(now, self._lookback_hours, ranked)
