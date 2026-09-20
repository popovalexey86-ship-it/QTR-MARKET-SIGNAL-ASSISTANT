from datetime import UTC, datetime
from pathlib import Path

from market_signal_assistant.watchdog.models import WatchdogState
from market_signal_assistant.watchdog.runtime.models import (
    PollingPolicy,
    RuntimeInterestTier,
    interest_tier,
)
from market_signal_assistant.watchdog.runtime.retry import (
    ApiRequestBudget,
    RetryPolicy,
    retry_call,
)
from market_signal_assistant.watchdog.runtime.schedule import (
    CompletedBucketScheduler,
    JsonBucketCursorStore,
)
from market_signal_assistant.watchdog.universe import UniverseTier

NOW = datetime(2026, 9, 18, 8, 7, 31, tzinfo=UTC)


def test_completed_bucket_boundaries_catch_up_once_without_open_candle() -> None:
    scheduler = CompletedBucketScheduler(maximum_catchup_buckets=20)

    one_minute = scheduler.due(
        now=NOW,
        interval="1m",
        last_completed=datetime(2026, 9, 18, 8, 4, tzinfo=UTC),
    )
    five_minute = scheduler.due(
        now=NOW,
        interval="5m",
        last_completed=datetime(2026, 9, 18, 7, 55, tzinfo=UTC),
    )
    fifteen_minute = scheduler.due(
        now=NOW,
        interval="15m",
        last_completed=None,
    )

    assert one_minute == (
        datetime(2026, 9, 18, 8, 5, tzinfo=UTC),
        datetime(2026, 9, 18, 8, 6, tzinfo=UTC),
        datetime(2026, 9, 18, 8, 7, tzinfo=UTC),
    )
    assert five_minute == (
        datetime(2026, 9, 18, 8, 0, tzinfo=UTC),
        datetime(2026, 9, 18, 8, 5, tzinfo=UTC),
    )
    assert fifteen_minute == (datetime(2026, 9, 18, 8, 0, tzinfo=UTC),)
    assert scheduler.due(now=NOW, interval="1m", last_completed=one_minute[-1]) == ()


def test_bucket_cursor_survives_restart(tmp_path: Path) -> None:
    path = tmp_path / "buckets.json"
    boundary = datetime(2026, 9, 18, 8, 5, tzinfo=UTC)
    JsonBucketCursorStore(path).save("abcusdt", "5m", boundary)

    restarted = JsonBucketCursorStore(path)

    assert restarted.get("ABCUSDT", "5m") == boundary
    assert not tuple(tmp_path.glob("*.tmp"))


def test_polling_policy_increases_interest_without_heavy_whole_universe() -> None:
    policy = PollingPolicy()

    assert interest_tier(UniverseTier.COLD_START, WatchdogState.NORMAL) is (
        RuntimeInterestTier.COLD_START
    )
    assert interest_tier(UniverseTier.CORE, WatchdogState.NORMAL) is (
        RuntimeInterestTier.CORE
    )
    assert interest_tier(UniverseTier.STANDARD, WatchdogState.WATCH) is (
        RuntimeInterestTier.WATCH
    )
    assert policy.rule(RuntimeInterestTier.COLD_START).interval == "15m"
    assert policy.rule(RuntimeInterestTier.ACTIVE).interval == "5m"
    assert policy.rule(RuntimeInterestTier.HIGH_ATTENTION).interval == "1m"
    assert (
        policy.rule(RuntimeInterestTier.HIGH_ATTENTION).check_every
        < policy.rule(RuntimeInterestTier.WATCH).check_every
        < policy.rule(RuntimeInterestTier.CORE).check_every
    )


def test_api_budget_waits_instead_of_bursting() -> None:
    clock = [0.0]
    waits: list[float] = []

    def sleep(delay: float) -> None:
        waits.append(delay)
        clock[0] += delay

    budget = ApiRequestBudget(
        2,
        monotonic=lambda: clock[0],
        sleep=sleep,
    )
    budget.acquire()
    budget.acquire()
    budget.acquire()

    assert budget.total_calls == 3
    assert waits == [30.0, 30.0]


def test_retry_uses_exponential_backoff_with_injected_jitter() -> None:
    attempts = [0]
    delays: list[float] = []

    def operation() -> str:
        attempts[0] += 1
        if attempts[0] < 3:
            raise TimeoutError
        return "ok"

    result = retry_call(
        operation,
        policy=RetryPolicy(3, 0.5, 8.0, 0.25),
        sleep=delays.append,
        random_value=lambda: 0.5,
    )

    assert result == "ok"
    assert delays == [0.5, 1.0]
