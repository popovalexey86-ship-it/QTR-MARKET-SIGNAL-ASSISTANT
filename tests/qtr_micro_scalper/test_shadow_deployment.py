from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
UNIT = ROOT / "deployment" / "systemd" / "qtr-scanner-scalper-shadow.service"
GUIDE = ROOT / "deployment" / "QTR_SCALPER_SHADOW_INSTALL.md"


def unit_text() -> str:
    return UNIT.read_text(encoding="utf-8")


def guide_text() -> str:
    return GUIDE.read_text(encoding="utf-8")


def test_shadow_systemd_unit_has_fail_closed_environment() -> None:
    text = unit_text()
    assert "Environment=QTR_SCALPER_V2_ENABLED=false" in text
    assert "Environment=QTR_SCALPER_V2_SHADOW_MODE=true" in text
    assert "Environment=QTR_SCALPER_V2_LIVE_ENABLED=false" in text
    assert (
        "Environment=QTR_SCALPER_V2_SETUP_AUDIT_PATH="
        "/opt/qtr/scanner/data/qtr_setup_telegram_pilot_audit.jsonl"
    ) in text
    assert "\n[Install]\n" not in text


def test_unit_uses_existing_dedicated_identity_and_working_directory() -> None:
    text = unit_text()
    assert "User=qtr" in text
    assert "Group=qtr" in text
    assert "WorkingDirectory=/opt/qtr/scalper-shadow" in text
    assert "UMask=0027" in text


def test_unit_runs_only_shadow_cli_without_execution() -> None:
    text = unit_text().casefold()
    assert (
        "execstart=/opt/qtr/scalper-shadow/.venv/bin/python -m "
        "market_signal_assistant.qtr_micro_scalper.cli --refresh-seconds 30"
    ) in text
    for forbidden in (
        "qtr_micro.cli",
        "execution",
        "create-order",
        "place-order",
        "api_key",
        "api_secret",
        "market_signal_assistant.telegram",
    ):
        assert forbidden not in text


def test_restart_logging_and_graceful_shutdown_are_explicit() -> None:
    text = unit_text()
    assert "Restart=on-failure" in text
    assert "RestartSec=10s" in text
    assert "KillSignal=SIGTERM" in text
    assert "TimeoutStopSec=30s" in text
    assert "StandardOutput=journal" in text
    assert "StandardError=journal" in text
    assert "SyslogIdentifier=qtr-scanner-scalper-shadow" in text


def test_unit_limits_writes_to_project_data() -> None:
    text = unit_text()
    assert "NoNewPrivileges=true" in text
    assert "ProtectSystem=strict" in text
    assert "ProtectHome=true" in text
    assert (
        "ReadOnlyPaths=-"
        "/opt/qtr/scanner/data/qtr_setup_telegram_pilot_audit.jsonl"
    ) in text
    assert "ReadWritePaths=/opt/qtr/scalper-shadow/data" in text
    assert "ReadWritePaths=/opt/qtr/scanner" not in text


def test_installation_guide_does_not_start_or_enable_service() -> None:
    text = guide_text()
    assert "systemctl start" not in text
    assert "systemctl enable" not in text
    assert "systemctl enable --now" not in text
    assert "sudo systemctl daemon-reload" in text
    assert "systemctl is-enabled" in text
    assert "systemctl is-active" in text


def test_installation_guide_preserves_qtr_ownership() -> None:
    text = guide_text()
    assert "sudo useradd --system --user-group" in text
    assert "sudo -u qtr python3 -m venv" in text
    assert "sudo -u qtr /opt/qtr/scalper-shadow/.venv/bin/python" in text
    assert "chown" not in text


def test_installation_guide_requires_no_trading_credentials() -> None:
    text = guide_text().casefold()
    assert "api keys" in text
    assert "не требуются" in text
    assert "bybit_demo_api_key" not in text
    assert "bybit_demo_api_secret" not in text
