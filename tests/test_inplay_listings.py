from datetime import UTC, datetime, timedelta
from pathlib import Path

from market_signal_assistant.inplay.listings import JsonListingStore, ListingTracker

NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)


def test_first_snapshot_initializes_baseline_without_new_listings(
    tmp_path: Path,
) -> None:
    store = JsonListingStore(tmp_path / "listings.json")
    tracker = ListingTracker(store)

    statuses = tracker.observe(("BTCUSDT", "ETHUSDT"), NOW)

    assert tuple(item.symbol for item in statuses) == ("BTCUSDT", "ETHUSDT")
    assert all(item.is_new_listing is False for item in statuses)
    assert all(item.listing_bonus == 0 for item in statuses)


def test_new_symbol_is_detected_and_first_seen_is_persisted(
    tmp_path: Path,
) -> None:
    path = tmp_path / "listings.json"
    tracker = ListingTracker(JsonListingStore(path))
    tracker.observe(("BTCUSDT",), NOW)

    detected_at = NOW + timedelta(hours=2)
    statuses = tracker.observe(("BTCUSDT", "NEWUSDT"), detected_at)
    reloaded = ListingTracker(JsonListingStore(path)).observe(
        ("BTCUSDT", "NEWUSDT"), detected_at + timedelta(hours=1)
    )

    new_status = next(item for item in statuses if item.symbol == "NEWUSDT")
    persisted = next(item for item in reloaded if item.symbol == "NEWUSDT")
    assert new_status.is_new_listing is True
    assert new_status.first_seen == detected_at
    assert persisted.first_seen == detected_at


def test_listing_bonus_decreases_with_age_and_expires_after_seven_days(
    tmp_path: Path,
) -> None:
    tracker = ListingTracker(JsonListingStore(tmp_path / "listings.json"))
    tracker.observe(("BTCUSDT",), NOW)
    first_seen = NOW + timedelta(minutes=1)
    tracker.observe(("BTCUSDT", "NEWUSDT"), first_seen)

    bonuses = tuple(
        next(
            item
            for item in tracker.observe(("BTCUSDT", "NEWUSDT"), observed_at)
            if item.symbol == "NEWUSDT"
        ).listing_bonus
        for observed_at in (
            first_seen + timedelta(hours=23),
            first_seen + timedelta(hours=24),
            first_seen + timedelta(hours=72),
            first_seen + timedelta(days=7),
        )
    )

    assert bonuses == (10.0, 6.0, 3.0, 0.0)


def test_disappeared_symbol_is_kept_in_state_but_not_returned(
    tmp_path: Path,
) -> None:
    path = tmp_path / "listings.json"
    tracker = ListingTracker(JsonListingStore(path))
    tracker.observe(("BTCUSDT",), NOW)
    tracker.observe(("BTCUSDT", "NEWUSDT"), NOW + timedelta(hours=1))

    statuses = tracker.observe(("BTCUSDT",), NOW + timedelta(hours=2))
    snapshot = JsonListingStore(path).load()

    assert tuple(item.symbol for item in statuses) == ("BTCUSDT",)
    assert "NEWUSDT" in snapshot.records
    assert snapshot.active_symbols == ("BTCUSDT",)
