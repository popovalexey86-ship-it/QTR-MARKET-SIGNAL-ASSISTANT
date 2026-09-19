from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
UNIT = ROOT / "deployment" / "systemd" / (
    "qtr-market-watchdog-phase5-soak.service"
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
