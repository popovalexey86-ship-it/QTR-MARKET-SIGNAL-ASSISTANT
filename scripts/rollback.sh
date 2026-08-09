#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="/opt/qtr/scanner"
VENV_DIR="${PROJECT_DIR}/.venv"
STATE_FILE="/opt/qtr/.deploy/scanner-previous-commit"
WEB_SERVICE="qtr-scanner-web"
TELEGRAM_SERVICE="qtr-scanner-telegram"
HEALTH_URL="http://127.0.0.1:8000/health"

log() { printf '[rollback] %s\n' "$*"; }

require_command() {
    command -v "$1" >/dev/null 2>&1 || {
        printf '[rollback] Required command is missing: %s\n' "$1" >&2
        return 1
    }
}

ensure_expected_repository() {
    [[ "$(pwd -P)" == "$PROJECT_DIR" ]] || {
        printf '[rollback] Run this script from %s\n' "$PROJECT_DIR" >&2
        return 1
    }
    [[ "$(git rev-parse --show-toplevel)" == "$PROJECT_DIR" ]] || {
        printf '[rollback] Unexpected Git repository.\n' >&2
        return 1
    }
    [[ "$(git symbolic-ref --quiet --short HEAD)" == "main" ]] || {
        printf '[rollback] Production checkout must be on branch main.\n' >&2
        return 1
    }
}

rebuild_venv() {
    log "Rebuilding virtual environment at ${VENV_DIR}"
    rm -rf -- "$VENV_DIR" || return 1
    python3 -m venv "$VENV_DIR" || return 1
    "$VENV_DIR/bin/python" -m pip install --upgrade pip || return 1
    "$VENV_DIR/bin/python" -m pip install -e ".[web,telegram]" || return 1
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
    printf '[rollback] Web health check failed.\n' >&2
    return 1
}

on_error() {
    local exit_code=$?
    trap - ERR
    printf '[rollback] Rollback failed; manual recovery may be required.\n' >&2
    exit "$exit_code"
}

trap on_error ERR

log "Validating production repository and rollback state"
require_command git
require_command python3
require_command systemctl
ensure_expected_repository
[[ -f "$STATE_FILE" ]] || {
    printf '[rollback] Rollback state file is missing: %s\n' "$STATE_FILE" >&2
    exit 1
}
IFS= read -r PREVIOUS_COMMIT <"$STATE_FILE"
[[ "$PREVIOUS_COMMIT" =~ ^[0-9a-fA-F]{40,64}$ ]] || {
    printf '[rollback] Rollback state contains an invalid commit id.\n' >&2
    exit 1
}
PREVIOUS_COMMIT="$(git rev-parse --verify "${PREVIOUS_COMMIT}^{commit}")"
git cat-file -e "${PREVIOUS_COMMIT}^{commit}"
log "Stopping services"
systemctl stop "$WEB_SERVICE" "$TELEGRAM_SERVICE"
log "Restoring commit ${PREVIOUS_COMMIT}"
git reset --hard "$PREVIOUS_COMMIT"
rebuild_venv
log "Starting services"
systemctl start "$WEB_SERVICE" "$TELEGRAM_SERVICE"
log "Checking systemd services"
systemctl is-active --quiet "$WEB_SERVICE"
systemctl is-active --quiet "$TELEGRAM_SERVICE"
wait_for_health

log "Rollback completed successfully at ${PREVIOUS_COMMIT}"
