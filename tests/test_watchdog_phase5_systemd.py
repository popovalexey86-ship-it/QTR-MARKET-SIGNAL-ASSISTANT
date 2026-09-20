from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
UNIT = ROOT / "deployment" / "systemd" / (
    "qtr-market-watchdog-phase5-soak.service"
)
STABILITY_UNIT = ROOT / "deployment" / "systemd" / (
    "qtr-market-watchdog-phase5b-stability.service"
)


def test_phase5_unit_tracks_fail_fast_supervisor_as_main_process() -> None:
    text = UNIT.read_text(encoding="utf-8")

    assert "Type=simple" in text
    assert "User=qtr" in text
    assert "WorkingDirectory=/opt/qtr/watchdog-shadow" in text
    assert "market_signal_assistant.watchdog.runtime.soak" in text
    assert "--data-root /opt/qtr/watchdog-shadow-data/phase5-soak" in text
    assert "Restart=on-failure" in text
    assert "KillSignal=SIGTERM" in text
    assert "EnvironmentFile=" not in text
    assert "SuccessExitStatus=" not in text
    assert "/opt/qtr/scanner" not in text


def test_phase5_unit_can_write_only_its_isolated_data_root() -> None:
    text = UNIT.read_text(encoding="utf-8")

    assert "ProtectSystem=strict" in text
    assert "ProtectHome=true" in text
    assert (
        "ReadWritePaths=/opt/qtr/watchdog-shadow-data/phase5-soak" in text
    )


def test_phase5b_stability_unit_is_isolated_and_bounded_to_two_hours() -> None:
    text = STABILITY_UNIT.read_text(encoding="utf-8")

    assert "Type=simple" in text
    assert "User=qtr" in text
    assert "--duration-hours 2" in text
    assert "--symbols-per-loop 8" in text
    assert "--api-calls-per-minute 60" in text
    assert "Restart=on-failure" in text
    assert "EnvironmentFile=" not in text
    assert "/opt/qtr/scanner" not in text
    assert (
        "ReadWritePaths=/opt/qtr/watchdog-shadow-data/"
        "phase5b-stability-20260920T1145Z" in text
    )
