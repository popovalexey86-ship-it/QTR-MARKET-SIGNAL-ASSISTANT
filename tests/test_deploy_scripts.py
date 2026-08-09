import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
COMMON = ROOT / "scripts" / "deploy-common.sh"
DEPLOY = ROOT / "scripts" / "deploy.sh"
ROLLBACK = ROOT / "scripts" / "rollback.sh"


def find_bash() -> str | None:
    discovered = shutil.which("bash")
    if discovered is not None:
        return discovered
    for candidate in (
        Path("C:/Program Files/Git/bin/bash.exe"),
        Path("C:/Program Files/Git/usr/bin/bash.exe"),
    ):
        if candidate.is_file():
            return str(candidate)
    return None


def run_common_test(source: str) -> subprocess.CompletedProcess[str]:
    bash = find_bash()
    if bash is None:
        pytest.skip("bash is not available on this platform")
    return subprocess.run(
        [bash, "-c", f"source scripts/deploy-common.sh\n{source}"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize("script", [COMMON, DEPLOY, ROLLBACK])
def test_deployment_scripts_have_valid_bash_syntax(script: Path) -> None:
    bash = find_bash()
    if bash is None:
        pytest.skip("bash is not available on this platform")
    subprocess.run([bash, "-n", str(script)], check=True)


@pytest.mark.parametrize("script", [COMMON, DEPLOY, ROLLBACK])
def test_deployment_scripts_keep_production_safety_boundaries(script: Path) -> None:
    source = script.read_text(encoding="utf-8")
    assert "git clean" not in source
    assert "/opt/qtr/data" not in source
    assert "/opt/qtr/backups" not in source
    assert "/etc/qtr" not in source


def test_common_pins_qtr_identity_and_local_health() -> None:
    source = COMMON.read_text(encoding="utf-8")
    assert 'PROJECT_DIR="/opt/qtr/scanner"' in source
    assert 'VENV_DIR="${PROJECT_DIR}/.venv"' in source
    assert 'PRODUCTION_USER="qtr"' in source
    assert 'PRODUCTION_GROUP="qtr"' in source
    assert 'HEALTH_URL="http://127.0.0.1:8000/health"' in source
    assert 'http.client.HTTPConnection("127.0.0.1", 8000' in source
    assert 'connection.request("GET", "/health")' in source
    assert "urllib.request" not in source
    assert "getent passwd" in source
    assert "getent group" in source


def test_venv_creation_install_and_checks_run_as_qtr() -> None:
    common = COMMON.read_text(encoding="utf-8")
    deploy = DEPLOY.read_text(encoding="utf-8")
    assert 'run_as_qtr python3 -m venv "$VENV_DIR"' in common
    assert 'run_as_qtr "$VENV_DIR/bin/python" -m pip install' in common
    assert 'run_as_qtr "$VENV_DIR/bin/python" -m pytest' in deploy
    assert 'run_as_qtr "$VENV_DIR/bin/python" -m mypy src' in deploy
    assert 'run_as_qtr "$VENV_DIR/bin/ruff" check .' in deploy
    assert "python3 -m venv" not in common.replace(
        'run_as_qtr python3 -m venv', ""
    )


def test_git_helper_runs_checkout_commands_as_qtr() -> None:
    source = COMMON.read_text(encoding="utf-8")
    assert 'git_as_qtr()' in source
    assert 'run_as_qtr git -C "$PROJECT_DIR" "$@"' in source

    result = run_common_test(
        """
runuser() { printf '%s\\n' "$*"; }
git_as_qtr status --porcelain
"""
    )
    assert result.returncode == 0, result.stderr
    assert "--user qtr --" in result.stdout
    assert "git -C /opt/qtr/scanner status --porcelain" in result.stdout


@pytest.mark.parametrize("script", [DEPLOY, ROLLBACK])
def test_production_scripts_have_no_root_git_invocations(script: Path) -> None:
    source = script.read_text(encoding="utf-8")
    assert re.search(r"(?m)^\s*git(?:\s|$)", source) is None
    assert "chown" not in source


def test_deploy_routes_all_git_operations_through_qtr() -> None:
    source = DEPLOY.read_text(encoding="utf-8")
    expected_commands = (
        "git_as_qtr rev-parse --show-toplevel",
        "git_as_qtr symbolic-ref --quiet --short HEAD",
        "git_as_qtr status --porcelain --untracked-files=normal",
        'git_as_qtr reset --hard "$PREVIOUS_COMMIT"',
        "git_as_qtr fetch origin",
        "git_as_qtr rev-parse --verify HEAD^{commit}",
        "git_as_qtr rev-parse --verify refs/remotes/origin/main^{commit}",
        'git_as_qtr reset --hard "$TARGET_COMMIT"',
    )
    for command in expected_commands:
        assert command in source


def test_rollback_routes_all_git_operations_through_qtr() -> None:
    source = ROLLBACK.read_text(encoding="utf-8")
    assert "git_as_qtr rev-parse --show-toplevel" in source
    assert "git_as_qtr symbolic-ref --quiet --short HEAD" in source
    assert 'git_as_qtr rev-parse --verify "${PREVIOUS_COMMIT}^{commit}"' in source
    assert 'git_as_qtr cat-file -e "${PREVIOUS_COMMIT}^{commit}"' in source
    assert 'git_as_qtr reset --hard "$PREVIOUS_COMMIT"' in source


def test_root_owned_venv_is_rejected() -> None:
    result = run_common_test(
        """
stat() {
    if [[ "$2" == "%U" ]]; then printf 'root\\n'; else printf 'root\\n'; fi
}
run_as_qtr() { return 0; }
if verify_venv_owner_and_traversal; then exit 90; fi
"""
    )
    assert result.returncode == 0, result.stderr
    assert "expected qtr:qtr, got root:root" in result.stderr


def test_runtime_executables_are_checked_as_qtr() -> None:
    source = COMMON.read_text(encoding="utf-8")
    assert 'run_as_qtr /usr/bin/test -x "$VENV_DIR"' in source
    assert "market-signal-web market-signal-telegram" in source
    assert 'run_as_qtr /usr/bin/test -x "$VENV_DIR/bin/$executable"' in source


def test_service_activating_then_active_succeeds() -> None:
    result = run_common_test(
        """
log() { :; }
READY=0
systemctl() {
    if (( READY == 0 )); then printf 'activating\\n'; else printf 'active\\n'; fi
}
sleep() { READY=1; }
wait_for_service qtr-scanner-web 3 0
"""
    )
    assert result.returncode == 0, result.stderr


def test_failed_service_is_an_immediate_error() -> None:
    result = run_common_test(
        """
log() { :; }
SLEEP_COUNT=0
systemctl() { printf 'failed\\n'; }
sleep() { SLEEP_COUNT=$((SLEEP_COUNT + 1)); }
if wait_for_service qtr-scanner-web 3 0; then exit 90; fi
[[ "$SLEEP_COUNT" == 0 ]]
"""
    )
    assert result.returncode == 0, result.stderr
    assert "entered failed state" in result.stderr


def test_service_wait_times_out() -> None:
    result = run_common_test(
        """
log() { :; }
SLEEP_COUNT=0
systemctl() { printf 'activating\\n'; }
sleep() { SLEEP_COUNT=$((SLEEP_COUNT + 1)); }
if wait_for_service qtr-scanner-web 3 0; then exit 90; fi
[[ "$SLEEP_COUNT" == 2 ]]
"""
    )
    assert result.returncode == 0, result.stderr
    assert "Timed out" in result.stderr


def test_health_initial_failure_then_success() -> None:
    result = run_common_test(
        """
log() { :; }
ATTEMPT=0
health_check_once() {
    ATTEMPT=$((ATTEMPT + 1))
    (( ATTEMPT >= 2 ))
}
sleep() { :; }
wait_for_health 3 0
[[ "$ATTEMPT" == 2 ]]
"""
    )
    assert result.returncode == 0, result.stderr


def test_rollback_uses_shared_safe_rebuild_and_waits() -> None:
    source = ROLLBACK.read_text(encoding="utf-8")
    assert 'source "$SCRIPT_DIR/deploy-common.sh"' in source
    assert "rebuild_venv" in source
    assert "install_runtime" in source
    assert "start_services_and_wait" in source
    assert "wait_for_health" in source
    assert "python3 -m venv" not in source


def test_failure_diagnostics_are_safe_and_bounded() -> None:
    common = COMMON.read_text(encoding="utf-8")
    deploy = DEPLOY.read_text(encoding="utf-8")
    rollback = ROLLBACK.read_text(encoding="utf-8")
    assert 'systemctl status "$service" --no-pager -l' in common
    assert 'journalctl -u "$service" -n 50 --no-pager' in common
    assert "safe_service_diagnostics" in deploy
    assert "safe_service_diagnostics" in rollback
    assert "Environment" not in common

    result = run_common_test(
        """
systemctl() { printf 'systemctl %s\\n' "$*"; return 1; }
journalctl() { printf 'journalctl %s\\n' "$*"; return 1; }
safe_service_diagnostics
"""
    )
    assert result.returncode == 0, result.stderr
    for service in ("qtr-scanner-web", "qtr-scanner-telegram"):
        assert f"systemctl status {service} --no-pager -l" in result.stdout
        assert f"journalctl -u {service} -n 50 --no-pager" in result.stdout
