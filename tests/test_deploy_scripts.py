import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DEPLOY = ROOT / "scripts" / "deploy.sh"
ROLLBACK = ROOT / "scripts" / "rollback.sh"


@pytest.mark.parametrize("script", [DEPLOY, ROLLBACK])
def test_deployment_scripts_have_valid_bash_syntax(script: Path) -> None:
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash is not available on this platform")
    subprocess.run([bash, "-n", str(script)], check=True)


@pytest.mark.parametrize("script", [DEPLOY, ROLLBACK])
def test_deployment_scripts_pin_production_paths_and_local_health(script: Path) -> None:
    source = script.read_text(encoding="utf-8")
    assert 'PROJECT_DIR="/opt/qtr/scanner"' in source
    assert 'VENV_DIR="${PROJECT_DIR}/.venv"' in source
    assert 'HEALTH_URL="http://127.0.0.1:8000/health"' in source
    assert "set -Eeuo pipefail" in source
    assert 'git symbolic-ref --quiet --short HEAD)" == "main"' in source
    assert "git clean" not in source
    assert "/opt/qtr/data" not in source
    assert "/opt/qtr/backups" not in source
    assert "/etc/qtr" not in source


def test_deploy_has_required_verification_and_automatic_rollback() -> None:
    source = DEPLOY.read_text(encoding="utf-8")
    assert "git fetch origin" in source
    assert "refs/remotes/origin/main^{commit}" in source
    assert "git status --porcelain --untracked-files=normal" in source
    assert 'python" -m pytest' in source
    assert 'python" -m mypy src' in source
    assert 'ruff" check .' in source
    assert "rollback_after_failure" in source
    assert 'rm -rf -- "$VENV_DIR"' in source


def test_rollback_uses_only_saved_verified_commit() -> None:
    source = ROLLBACK.read_text(encoding="utf-8")
    assert 'STATE_FILE="/opt/qtr/.deploy/scanner-previous-commit"' in source
    assert 'git rev-parse --verify "${PREVIOUS_COMMIT}^{commit}"' in source
    assert 'git reset --hard "$PREVIOUS_COMMIT"' in source
    assert 'rm -rf -- "$VENV_DIR"' in source
