import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from market_signal_assistant.watchdog.runtime import soak
from market_signal_assistant.watchdog.runtime.liveness import (
    ConfirmedHealthyClock,
    RuntimeLiveness,
    SoakLivenessError,
)

NOW = datetime(2026, 9, 19, 18, tzinfo=UTC)


def _probe(
    *,
    marker: int,
    running: bool = True,
    fatal_error: str | None = None,
    last_loop_at: datetime = NOW,
    last_market_progress_at: datetime = NOW,
    degraded: bool = False,
    market_progress_due_at: datetime | None = None,
) -> RuntimeLiveness:
    return RuntimeLiveness(
        observed_at=NOW,
        running=running,
        fatal_error=fatal_error,
        last_loop_at=last_loop_at,
        last_market_progress_at=last_market_progress_at,
        progress_marker=(marker,),
        degraded=degraded,
        market_progress_due_at=market_progress_due_at,
    )


def test_clock_advances_only_when_healthy_market_progress_is_confirmed() -> None:
    clock = ConfirmedHealthyClock(0.0, stale_after_seconds=60.0)

    assert clock.observe(_probe(marker=1), monotonic_now=0.0) is False
    assert clock.observe(_probe(marker=1), monotonic_now=5.0) is False
    assert clock.accumulated_seconds == 0.0
    assert clock.observe(_probe(marker=2), monotonic_now=10.0) is True
    assert clock.accumulated_seconds == 10.0


def test_degraded_provider_interval_is_never_backfilled_after_recovery() -> None:
    clock = ConfirmedHealthyClock(3.0, stale_after_seconds=60.0)
    clock.observe(_probe(marker=1), monotonic_now=0.0)
    clock.observe(_probe(marker=2), monotonic_now=10.0)

    assert clock.observe(_probe(marker=2, degraded=True), monotonic_now=20.0) is False
    assert clock.observe(_probe(marker=3), monotonic_now=30.0) is False
    assert clock.observe(_probe(marker=4), monotonic_now=40.0) is True
    assert clock.accumulated_seconds == 23.0


@pytest.mark.parametrize(
    ("probe", "message"),
    (
        (_probe(marker=1, fatal_error="JournalConflictError"), "runtime failure"),
        (_probe(marker=1, running=False), "thread is not alive"),
        (
            _probe(marker=1, last_loop_at=NOW - timedelta(seconds=61)),
            "heartbeat is stale",
        ),
        (
            _probe(
                marker=1,
                last_market_progress_at=NOW - timedelta(seconds=61),
            ),
            "market progress is stale",
        ),
    ),
)
def test_fatal_thread_death_and_stale_health_fail_fast(
    probe: RuntimeLiveness,
    message: str,
) -> None:
    clock = ConfirmedHealthyClock(7.0, stale_after_seconds=60.0)

    with pytest.raises(SoakLivenessError, match=message):
        clock.observe(probe, monotonic_now=0.0)

    assert clock.accumulated_seconds == 7.0


def test_frozen_market_loop_fails_without_advancing_acceptance_clock() -> None:
    clock = ConfirmedHealthyClock(0.0, stale_after_seconds=60.0)
    clock.observe(_probe(marker=1), monotonic_now=0.0)

    with pytest.raises(SoakLivenessError, match="did not advance"):
        clock.observe(_probe(marker=1), monotonic_now=61.0)

    assert clock.accumulated_seconds == 0.0


def test_planned_slow_poll_does_not_trigger_false_stale_failure() -> None:
    clock = ConfirmedHealthyClock(0.0, stale_after_seconds=60.0)
    future_due = NOW + timedelta(minutes=15)
    clock.observe(
        _probe(marker=1, market_progress_due_at=future_due), monotonic_now=0.0
    )

    assert (
        clock.observe(
            _probe(
                marker=1,
                last_market_progress_at=NOW - timedelta(minutes=10),
                market_progress_due_at=future_due,
            ),
            monotonic_now=600.0,
        )
        is False
    )
    assert clock.accumulated_seconds == 0.0


def test_old_persisted_health_is_treated_as_startup_pending(tmp_path: Path) -> None:
    health_path = tmp_path / "health.json"
    health_path.write_text(
        json.dumps(
            {
                "started_at": (NOW - timedelta(hours=1)).isoformat(),
                "last_loop_at": (NOW - timedelta(hours=1)).isoformat(),
                "last_successful_market_update": (NOW - timedelta(hours=1)).isoformat(),
                "symbols_processed": 10,
                "api_calls": 10,
                "degraded": False,
            }
        ),
        encoding="utf-8",
    )
    runtime = SimpleNamespace(running=True, fatal_error=None)

    probe = soak._runtime_liveness(runtime, NOW, health_path, expected_started_at=NOW)

    assert probe.last_loop_at is None
    assert probe.progress_marker == ("startup-pending",)


def test_provider_outage_freezes_then_fails_without_false_progress() -> None:
    clock = ConfirmedHealthyClock(12.0, stale_after_seconds=30.0)
    clock.observe(_probe(marker=1), monotonic_now=0.0)
    assert clock.observe(_probe(marker=1, degraded=True), monotonic_now=5.0) is False

    with pytest.raises(SoakLivenessError, match="remained degraded"):
        clock.observe(_probe(marker=1, degraded=True), monotonic_now=36.0)

    assert clock.accumulated_seconds == 12.0


def test_forced_kill_and_systemd_restart_resume_only_confirmed_time() -> None:
    first_process = ConfirmedHealthyClock(0.0, stale_after_seconds=60.0)
    first_process.observe(_probe(marker=1), monotonic_now=0.0)
    first_process.observe(_probe(marker=2), monotonic_now=10.0)
    persisted = first_process.accumulated_seconds

    restarted = ConfirmedHealthyClock(persisted, stale_after_seconds=60.0)
    restarted.observe(_probe(marker=100), monotonic_now=1_000.0)
    restarted.observe(_probe(marker=101), monotonic_now=1_010.0)

    assert persisted == 10.0
    assert restarted.accumulated_seconds == 20.0


def test_soak_supervisor_returns_nonzero_and_freezes_checkpoint_on_worker_death(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class FailedRuntime:
        running = False
        fatal_error = RuntimeError("injected worker death")

        def start(self) -> None:
            self.running = False

        def stop(self, *, timeout: float) -> None:
            del timeout

        def health_snapshot(self) -> SimpleNamespace:
            return SimpleNamespace(
                last_loop_at=NOW,
                last_successful_market_update=NOW,
                symbols_processed=1,
                api_calls=1,
                degraded=True,
            )

    runtime = FailedRuntime()
    monkeypatch.setattr(
        soak,
        "build_bybit_shadow_runtime",
        lambda *_args, **_kwargs: SimpleNamespace(runtime=runtime),
    )
    monkeypatch.setattr(soak, "inspect", lambda *_args, **_kwargs: {})
    state = tmp_path / "state"
    state.mkdir()
    (state / "health.json").write_text(
        json.dumps(
            {
                "last_loop_at": NOW.isoformat(),
                "last_successful_market_update": NOW.isoformat(),
                "symbols_processed": 1,
                "api_calls": 1,
                "degraded": True,
            }
        ),
        encoding="utf-8",
    )

    exit_code = soak.main(
        [
            "--data-root",
            str(tmp_path),
            "--duration-hours",
            "0.001",
            "--probe-seconds",
            "0.001",
            "--liveness-timeout-seconds",
            "1",
        ]
    )
    checkpoint = json.loads(
        (tmp_path / "state" / "soak.json").read_text(encoding="utf-8")
    )

    assert exit_code == 1
    assert checkpoint["accumulated_seconds"] == 0.0
    assert checkpoint["status"] == "failed:runtime failure: RuntimeError"


def test_stale_health_json_cannot_report_false_healthy(
    tmp_path: Path,
) -> None:
    health_path = tmp_path / "health.json"
    health_path.write_text(
        json.dumps(
            {
                "last_loop_at": (NOW - timedelta(seconds=61)).isoformat(),
                "last_successful_market_update": NOW.isoformat(),
                "symbols_processed": 10,
                "api_calls": 10,
                "degraded": False,
            }
        ),
        encoding="utf-8",
    )
    runtime = SimpleNamespace(running=True, fatal_error=None)
    probe = soak._runtime_liveness(runtime, NOW, health_path)
    clock = ConfirmedHealthyClock(5.0, stale_after_seconds=60.0)

    with pytest.raises(SoakLivenessError, match="heartbeat is stale"):
        clock.observe(probe, monotonic_now=0.0)

    assert clock.accumulated_seconds == 5.0
