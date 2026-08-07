from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_signal_assistant.inplay.models import InPlayDirection, InPlayResult
from market_signal_assistant.inplay.notifications import (
    DEFAULT_INPLAY_NOTIFICATION_STATE_PATH,
    InPlayNotificationService,
    JsonInPlayNotificationStore,
    NotificationDecisionReason,
)

NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)


def result(
    symbol: str = "BTCUSDT",
    *,
    direction: InPlayDirection = InPlayDirection.LONG,
    score: float = 70.0,
    reasons: tuple[str, ...] = ("Относительный объём 2,0×",),
    warnings: tuple[str, ...] = (),
    is_new_listing: bool = False,
) -> InPlayResult:
    return InPlayResult(
        symbol=symbol,
        direction=direction,
        inplay_score=score,
        directional_score=None,
        reasons=reasons,
        warnings=warnings,
        first_seen=NOW - timedelta(days=30),
        is_new_listing=is_new_listing,
    )


def service(tmp_path: Path) -> InPlayNotificationService:
    return InPlayNotificationService(
        JsonInPlayNotificationStore(tmp_path / "inplay_notifications.json")
    )


def test_first_event_is_allowed_and_identical_event_is_suppressed(
    tmp_path: Path,
) -> None:
    notifications = service(tmp_path)

    first = notifications.evaluate((result(),), NOW)[0]
    repeated = notifications.evaluate((result(),), NOW + timedelta(minutes=5))[0]

    assert first.should_notify is True
    assert first.reason is NotificationDecisionReason.FIRST_APPEARANCE
    assert repeated.should_notify is False
    assert repeated.reason is NotificationDecisionReason.UNCHANGED


@pytest.mark.parametrize("increase", [0.1, 9.9])
def test_score_growth_below_ten_is_suppressed(
    tmp_path: Path,
    increase: float,
) -> None:
    notifications = service(tmp_path)
    notifications.evaluate((result(score=60),), NOW)

    decision = notifications.evaluate(
        (result(score=60 + increase),), NOW + timedelta(minutes=30)
    )[0]

    assert decision.should_notify is False


@pytest.mark.parametrize("increase", [10.0, 20.0])
def test_score_growth_of_ten_or_more_is_allowed(
    tmp_path: Path,
    increase: float,
) -> None:
    notifications = service(tmp_path)
    notifications.evaluate((result(score=60),), NOW)

    decision = notifications.evaluate(
        (result(score=60 + increase),), NOW + timedelta(minutes=61)
    )[0]

    assert decision.should_notify is True
    assert decision.reason is NotificationDecisionReason.SCORE_INCREASED


def test_long_short_direction_change_is_allowed(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    notifications.evaluate((result(direction=InPlayDirection.LONG),), NOW)

    decision = notifications.evaluate(
        (result(direction=InPlayDirection.SHORT),), NOW + timedelta(minutes=5)
    )[0]

    assert decision.should_notify is True
    assert decision.reason is NotificationDecisionReason.DIRECTION_CHANGED


@pytest.mark.parametrize("direction", [InPlayDirection.LONG, InPlayDirection.SHORT])
def test_watch_to_directional_signal_is_allowed(
    tmp_path: Path,
    direction: InPlayDirection,
) -> None:
    notifications = service(tmp_path)
    notifications.evaluate((result(direction=InPlayDirection.WATCH),), NOW)

    decision = notifications.evaluate(
        (result(direction=direction),), NOW + timedelta(minutes=5)
    )[0]

    assert decision.should_notify is True
    assert decision.reason is NotificationDecisionReason.DIRECTION_CONFIRMED


@pytest.mark.parametrize("direction", [InPlayDirection.LONG, InPlayDirection.SHORT])
def test_directional_to_watch_is_not_sent(
    tmp_path: Path,
    direction: InPlayDirection,
) -> None:
    notifications = service(tmp_path)
    notifications.evaluate((result(direction=direction),), NOW)

    decision = notifications.evaluate(
        (result(direction=InPlayDirection.WATCH, score=95),),
        NOW + timedelta(hours=7),
    )[0]

    assert decision.should_notify is False
    assert decision.reason is NotificationDecisionReason.DIRECTION_WEAKENED


def test_new_important_confirmation_is_allowed(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    notifications.evaluate((result(),), NOW)

    decision = notifications.evaluate(
        (
            result(
                reasons=(
                    "Относительный объём 2,0×",
                    "Цена вышла из локального диапазона",
                )
            ),
        ),
        NOW + timedelta(minutes=61),
    )[0]

    assert decision.should_notify is True
    assert decision.reason is NotificationDecisionReason.IMPORTANT_CONFIRMATION


@pytest.mark.parametrize(
    ("symbol", "initial", "updated", "minutes"),
    (
        ("BLESSUSDT", 81.0, 83.0, 16),
        ("MMTUSDT", 79.0, 81.0, 32),
    ),
)
def test_small_score_growth_does_not_repeat_same_semantic_state(
    tmp_path: Path,
    symbol: str,
    initial: float,
    updated: float,
    minutes: int,
) -> None:
    notifications = service(tmp_path)
    notifications.evaluate((result(symbol=symbol, score=initial),), NOW)

    repeated = notifications.evaluate(
        (result(symbol=symbol, score=updated),),
        NOW + timedelta(minutes=minutes),
    )[0]

    assert repeated.should_notify is False


def test_numeric_market_metrics_do_not_create_semantic_event(tmp_path: Path) -> None:
    notifications = service(tmp_path)
    notifications.evaluate(
        (
            result(
                reasons=(
                    "Изменение цены +4,1%",
                    "Волатильность ATR 2,3%",
                    "Относительный объём 1,7×",
                )
            ),
        ),
        NOW,
    )

    repeated = notifications.evaluate(
        (
            result(
                score=72,
                reasons=(
                    "Изменение цены +6,8%",
                    "Волатильность ATR 4,9%",
                    "Относительный объём 2,6×",
                ),
            ),
        ),
        NOW + timedelta(minutes=61),
    )[0]

    assert repeated.should_notify is False


def test_generic_derivatives_confirmation_does_not_allow_repeat(
    tmp_path: Path,
) -> None:
    notifications = service(tmp_path)
    notifications.evaluate((result(),), NOW)

    repeated = notifications.evaluate(
        (
            result(
                reasons=(
                    "Относительный объём 2,0×",
                    "Деривативы подтверждают активное позиционирование.",
                )
            ),
        ),
        NOW + timedelta(minutes=61),
    )[0]

    assert repeated.should_notify is False


def test_score_increase_is_blocked_by_absolute_sixty_minute_cooldown(
    tmp_path: Path,
) -> None:
    notifications = service(tmp_path)
    notifications.evaluate((result(score=60),), NOW)

    repeated = notifications.evaluate(
        (result(score=80),), NOW + timedelta(minutes=59)
    )[0]

    assert repeated.should_notify is False


@pytest.mark.parametrize(
    ("absence", "expected", "reason"),
    [
        (timedelta(minutes=59), False, NotificationDecisionReason.REAPPEARED_TOO_SOON),
        (timedelta(minutes=60), True, NotificationDecisionReason.REAPPEARED),
    ],
)
def test_reappearance_uses_sixty_minute_boundary(
    tmp_path: Path,
    absence: timedelta,
    expected: bool,
    reason: NotificationDecisionReason,
) -> None:
    notifications = service(tmp_path)
    notifications.evaluate((result(),), NOW)
    notifications.evaluate((), NOW + timedelta(minutes=10))

    decision = notifications.evaluate(
        (result(),), NOW + timedelta(minutes=10) + absence
    )[0]

    assert decision.should_notify is expected
    assert decision.reason is reason


def test_cooldown_allows_repeated_notification_after_six_hours(
    tmp_path: Path,
) -> None:
    notifications = service(tmp_path)
    notifications.evaluate((result(),), NOW)

    decision = notifications.evaluate((result(),), NOW + timedelta(hours=6))[0]

    assert decision.should_notify is True
    assert decision.reason is NotificationDecisionReason.COOLDOWN_EXPIRED


def test_new_listing_is_sent_only_once(tmp_path: Path) -> None:
    notifications = service(tmp_path)

    first = notifications.evaluate((result(is_new_listing=True),), NOW)[0]
    repeated = notifications.evaluate(
        (result(is_new_listing=True),), NOW + timedelta(minutes=5)
    )[0]

    assert first.should_notify is True
    assert repeated.should_notify is False


def test_missing_state_file_is_created_safely(tmp_path: Path) -> None:
    path = tmp_path / "inplay_notifications.json"

    snapshot = JsonInPlayNotificationStore(path).load()

    assert snapshot.records == {}
    assert json.loads(path.read_text(encoding="utf-8"))["records"] == {}


def test_corrupt_state_warns_and_recovers_with_empty_state(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    path = tmp_path / "inplay_notifications.json"
    path.write_text("{broken", encoding="utf-8")

    with caplog.at_level(logging.WARNING):
        snapshot = JsonInPlayNotificationStore(path).load()

    assert snapshot.records == {}
    assert "повреждён" in caplog.text
    assert json.loads(path.read_text(encoding="utf-8"))["records"] == {}


def test_notification_state_uses_a_separate_file() -> None:
    assert DEFAULT_INPLAY_NOTIFICATION_STATE_PATH.is_absolute()
    assert DEFAULT_INPLAY_NOTIFICATION_STATE_PATH.parent.name == "data"
    assert DEFAULT_INPLAY_NOTIFICATION_STATE_PATH.name != "inplay_listings.json"


def test_atomic_save_writes_valid_json_without_temporary_file(
    tmp_path: Path,
) -> None:
    path = tmp_path / "inplay_notifications.json"
    notifications = service(tmp_path)

    notifications.evaluate((result(),), NOW)

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["records"]["BTCUSDT"]["last_notification"]["symbol"] == "BTCUSDT"
    assert not path.with_suffix(".json.tmp").exists()


@pytest.mark.parametrize(
    ("symbol", "direction", "change", "expected_direction"),
    (
        ("ICNTUSDT", InPlayDirection.SHORT, "−35,0", "ШОРТ"),
        ("HOMEUSDT", InPlayDirection.LONG, "+38,2", "ЛОНГ"),
    ),
)
def test_extreme_safety_state_preserves_internal_direction_in_version_two(
    tmp_path: Path,
    symbol: str,
    direction: InPlayDirection,
    change: str,
    expected_direction: str,
) -> None:
    path = tmp_path / "inplay_notifications.json"
    notifications = service(tmp_path)

    notifications.evaluate(
        (
            result(
                symbol=symbol,
                direction=direction,
                score=80,
                reasons=(f"Изменение цены {change}%",),
            ),
        ),
        NOW,
    )

    payload = json.loads(path.read_text(encoding="utf-8"))
    record = payload["records"][symbol]
    sent = record["last_notification"]
    assert payload["version"] == 2
    assert record["current_internal_direction"] == expected_direction
    assert record["current_display_status"] == "НЕ ДОГОНЯТЬ"
    assert sent["internal_direction"] == expected_direction
    assert sent["display_status"] == "НЕ ДОГОНЯТЬ"
    assert sent["risk_class"] == "extreme"
    assert sent["semantic_fingerprint"]


def test_semantic_fingerprint_changes_with_display_status(tmp_path: Path) -> None:
    normal_path = tmp_path / "normal.json"
    extreme_path = tmp_path / "extreme.json"
    normal = InPlayNotificationService(JsonInPlayNotificationStore(normal_path))
    extreme = InPlayNotificationService(JsonInPlayNotificationStore(extreme_path))

    normal.evaluate((result(reasons=("Изменение цены +5,0%",)),), NOW)
    extreme.evaluate((result(reasons=("Изменение цены +35,0%",)),), NOW)

    normal_sent = json.loads(normal_path.read_text(encoding="utf-8"))[
        "records"
    ]["BTCUSDT"]["last_notification"]
    extreme_sent = json.loads(extreme_path.read_text(encoding="utf-8"))[
        "records"
    ]["BTCUSDT"]["last_notification"]
    assert normal_sent["reasons_fingerprint"] == extreme_sent[
        "reasons_fingerprint"
    ]
    assert normal_sent["semantic_fingerprint"] != extreme_sent[
        "semantic_fingerprint"
    ]


def test_numeric_metrics_do_not_change_persisted_semantic_fingerprint(
    tmp_path: Path,
) -> None:
    first_path = tmp_path / "first.json"
    second_path = tmp_path / "second.json"
    first = InPlayNotificationService(JsonInPlayNotificationStore(first_path))
    second = InPlayNotificationService(JsonInPlayNotificationStore(second_path))
    first.evaluate(
        (
            result(
                score=70,
                reasons=(
                    "Изменение цены +4,1%",
                    "Волатильность ATR 2,3%",
                    "Относительный объём 1,7×",
                ),
            ),
        ),
        NOW,
    )
    second.evaluate(
        (
            result(
                score=79,
                reasons=(
                    "Изменение цены +6,8%",
                    "Волатильность ATR 4,9%",
                    "Относительный объём 2,6×",
                ),
            ),
        ),
        NOW,
    )

    def fingerprint(path: Path) -> str:
        payload = json.loads(path.read_text(encoding="utf-8"))
        value = payload["records"]["BTCUSDT"]["last_notification"]
        return str(value["semantic_fingerprint"])

    assert fingerprint(first_path) == fingerprint(second_path)


def test_semantic_duplicate_is_suppressed_after_serialize_and_reload(
    tmp_path: Path,
) -> None:
    path = tmp_path / "inplay_notifications.json"
    first = InPlayNotificationService(JsonInPlayNotificationStore(path))
    first.evaluate(
        (
            result(
                score=70,
                reasons=(
                    "Изменение цены +4,1%",
                    "Волатильность ATR 2,3%",
                    "Относительный объём 1,7×",
                ),
            ),
        ),
        NOW,
    )

    restarted = InPlayNotificationService(JsonInPlayNotificationStore(path))
    repeated = restarted.evaluate(
        (
            result(
                score=79,
                reasons=(
                    "Изменение цены +6,8%",
                    "Волатильность ATR 4,9%",
                    "Относительный объём 2,6×",
                ),
            ),
        ),
        NOW + timedelta(minutes=61),
    )[0]

    assert repeated.should_notify is False


def test_legacy_version_one_state_loads_with_semantic_defaults(
    tmp_path: Path,
) -> None:
    path = tmp_path / "inplay_notifications.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "updated_at": NOW.isoformat(),
                "records": {
                    "BTCUSDT": {
                        "symbol": "BTCUSDT",
                        "current_direction": "ЛОНГ",
                        "new_listing_notified": False,
                        "absent_since": None,
                        "last_notification": {
                            "symbol": "BTCUSDT",
                            "direction": "ЛОНГ",
                            "inplay_score": 70.0,
                            "reasons_fingerprint": "legacy",
                            "important_confirmations": [],
                            "sent_at": NOW.isoformat(),
                            "is_new_listing": False,
                        },
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    state = JsonInPlayNotificationStore(path).load()

    sent = state.records["BTCUSDT"].last_notification
    assert sent.display_status.value == "ЛОНГ"
    assert sent.risk_class.value == "normal"
    assert sent.visible_confirmations == ()


def test_legacy_safety_watch_direction_remains_unknown_until_live_scan(
    tmp_path: Path,
) -> None:
    path = tmp_path / "inplay_notifications.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "updated_at": NOW.isoformat(),
                "records": {
                    "HOMEUSDT": {
                        "symbol": "HOMEUSDT",
                        "current_direction": "НАБЛЮДЕНИЕ",
                        "new_listing_notified": False,
                        "absent_since": None,
                        "last_notification": {
                            "symbol": "HOMEUSDT",
                            "direction": "НАБЛЮДЕНИЕ",
                            "inplay_score": 80.0,
                            "reasons_fingerprint": "legacy",
                            "important_confirmations": [],
                            "sent_at": NOW.isoformat(),
                            "is_new_listing": False,
                            "display_status": "НЕ ДОГОНЯТЬ",
                            "risk_class": "extreme",
                            "user_action": "Не догонять цену.",
                            "visible_confirmations": [],
                        },
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    store = JsonInPlayNotificationStore(path)

    migrated = store.load()

    assert migrated.records["HOMEUSDT"].current_internal_direction is None
    assert migrated.records["HOMEUSDT"].last_notification.internal_direction is None
    decision = InPlayNotificationService(store).evaluate(
        (
            result(
                symbol="HOMEUSDT",
                direction=InPlayDirection.LONG,
                score=80,
                reasons=("Изменение цены +38,2%",),
            ),
        ),
        NOW + timedelta(minutes=5),
    )[0]
    assert decision.should_notify is False
    refreshed = store.load().records["HOMEUSDT"]
    assert refreshed.current_internal_direction is InPlayDirection.LONG
    assert refreshed.last_notification.internal_direction is InPlayDirection.LONG


def test_naive_observation_timestamp_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        service(tmp_path).evaluate((result(),), datetime(2026, 8, 2, 12))
