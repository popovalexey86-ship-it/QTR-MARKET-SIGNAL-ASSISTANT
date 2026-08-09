#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="/opt/qtr/scanner"
VENV_DIR="${PROJECT_DIR}/.venv"
STATE_DIR="/opt/qtr/.deploy"
STATE_FILE="${STATE_DIR}/scanner-previous-commit"
WEB_SERVICE="qtr-scanner-web"
TELEGRAM_SERVICE="qtr-scanner-telegram"
HEALTH_URL="http://127.0.0.1:8000/health"
PREVIOUS_COMMIT=""
DEPLOY_MUTATED=false
state_tmp=""

log() { printf '[deploy] %s\n' "$*"; }

require_command() {
    command -v "$1" >/dev/null 2>&1 || {
        printf '[deploy] Required command is missing: %s\n' "$1" >&2
        return 1
    }
}

ensure_expected_repository() {
    [[ "$(pwd -P)" == "$PROJECT_DIR" ]] || {
        printf '[deploy] Run this script from %s\n' "$PROJECT_DIR" >&2
        return 1
    }
    [[ "$(git rev-parse --show-toplevel)" == "$PROJECT_DIR" ]] || {
        printf '[deploy] Unexpected Git repository.\n' >&2
        return 1
    }
    [[ "$(git symbolic-ref --quiet --short HEAD)" == "main" ]] || {
        printf '[deploy] Production checkout must be on branch main.\n' >&2
        return 1
    }
}

ensure_clean_worktree() {
    [[ -z "$(git status --porcelain --untracked-files=normal)" ]] || {
        printf '[deploy] Git working tree is not clean; deploy aborted.\n' >&2
        return 1
    }
}

rebuild_venv() {
    log "Rebuilding virtual environment at ${VENV_DIR}"
    rm -rf -- "$VENV_DIR" || return 1
    python3 -m venv "$VENV_DIR" || return 1
    "$VENV_DIR/bin/python" -m pip install --upgrade pip || return 1
}

install_runtime() {
    log "Installing runtime dependencies"
    "$VENV_DIR/bin/python" -m pip install -e ".[web,telegram]" || return 1
}

install_check_tools() {
    log "Installing verification tools"
    "$VENV_DIR/bin/python" -m pip install pytest mypy ruff || return 1
}

health_check_once() {
    "$VENV_DIR/bin/python" - "$HEALTH_URL" <<'PY'
import json
import sys
import urllib.request

request = urllib.request.Request(sys.argv[1], method="GET")
try:
    with urllib.request.urlopen(request, timeout=3) as response:
        payload = json.load(response)
        healthy = response.status == 200 and payload == {"status": "ok"}
except Exception:
    healthy = False
raise SystemExit(0 if healthy else 1)
PY
}

wait_for_health() {
    local attempt
    log "Checking Web health at ${HEALTH_URL}"
    for attempt in {1..30}; do
        if health_check_once; then return 0; fi
        sleep 1
    done
    printf '[deploy] Web health check failed.\n' >&2
    return 1
}

verify_services() {
    systemctl is-active --quiet "$WEB_SERVICE"
    systemctl is-active --quiet "$TELEGRAM_SERVICE"
}

rollback_after_failure() {
    local recovery_failed=0
    log "Failure detected; restoring ${PREVIOUS_COMMIT}"
    systemctl stop "$WEB_SERVICE" "$TELEGRAM_SERVICE" || recovery_failed=1
    if git reset --hard "$PREVIOUS_COMMIT"; then
        if rebuild_venv && install_runtime; then :; else recovery_failed=1; fi
    else
        recovery_failed=1
    fi
    systemctl start "$WEB_SERVICE" || recovery_failed=1
    systemctl start "$TELEGRAM_SERVICE" || recovery_failed=1
    systemctl is-active --quiet "$WEB_SERVICE" || recovery_failed=1
    systemctl is-active --quiet "$TELEGRAM_SERVICE" || recovery_failed=1
    wait_for_health || recovery_failed=1
    if (( recovery_failed != 0 )); then
        printf '[deploy] Automatic rollback was incomplete; manual recovery is required.\n' >&2
        return 1
    fi
    log "Automatic rollback completed"
}

on_error() {
    local exit_code=$?
    trap - ERR
    if [[ "$DEPLOY_MUTATED" == true && -n "$PREVIOUS_COMMIT" ]]; then
        rollback_after_failure || true
    fi
    printf '[deploy] Deploy failed.\n' >&2
    exit "$exit_code"
}

cleanup() {
    if [[ -n "$state_tmp" ]]; then rm -f -- "$state_tmp"; fi
}

trap on_error ERR
trap cleanup EXIT

log "Validating production repository"
require_command git
require_command python3
require_command systemctl
ensure_expected_repository
ensure_clean_worktree
log "Fetching origin"
git fetch origin
PREVIOUS_COMMIT="$(git rev-parse --verify HEAD^{commit})"
TARGET_COMMIT="$(git rev-parse --verify refs/remotes/origin/main^{commit})"

if [[ "$PREVIOUS_COMMIT" == "$TARGET_COMMIT" ]]; then
    log "already up to date"
    exit 0
fi

log "Saving rollback state"
umask 077
mkdir -p -- "$STATE_DIR"
state_tmp="$(mktemp "${STATE_FILE}.tmp.XXXXXX")"
printf '%s\n' "$PREVIOUS_COMMIT" >"$state_tmp"
mv -f -- "$state_tmp" "$STATE_FILE"
state_tmp=""

DEPLOY_MUTATED=true
log "Stopping services"
systemctl stop "$WEB_SERVICE" "$TELEGRAM_SERVICE"
log "Updating repository to verified origin/main commit ${TARGET_COMMIT}"
git reset --hard "$TARGET_COMMIT"
rebuild_venv
install_runtime
install_check_tools
log "Running pytest"
"$VENV_DIR/bin/python" -m pytest
log "Running mypy"
"$VENV_DIR/bin/python" -m mypy src
log "Running ruff"
"$VENV_DIR/bin/ruff" check .
log "Starting services"
systemctl start "$WEB_SERVICE" "$TELEGRAM_SERVICE"
log "Checking systemd services"
verify_services
wait_for_health

DEPLOY_MUTATED=false
log "Deploy completed successfully at ${TARGET_COMMIT}"
