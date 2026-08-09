#!/usr/bin/env bash

PROJECT_DIR="/opt/qtr/scanner"
VENV_DIR="${PROJECT_DIR}/.venv"
PRODUCTION_USER="qtr"
PRODUCTION_GROUP="qtr"
WEB_SERVICE="qtr-scanner-web"
TELEGRAM_SERVICE="qtr-scanner-telegram"
HEALTH_URL="http://127.0.0.1:8000/health"
WAIT_ATTEMPTS=30
WAIT_INTERVAL_SECONDS=1
HEALTH_WAIT_INTERVAL_SECONDS=0.8

require_root() {
    if (( EUID != 0 )); then
        printf '[safety] Run this script with root privileges.\n' >&2
        return 1
    fi
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || {
        printf '[safety] Required command is missing: %s\n' "$1" >&2
        return 1
    }
}

ensure_production_identity() {
    getent passwd "$PRODUCTION_USER" >/dev/null || {
        printf '[safety] Production user is missing: %s\n' "$PRODUCTION_USER" >&2
        return 1
    }
    getent group "$PRODUCTION_GROUP" >/dev/null || {
        printf '[safety] Production group is missing: %s\n' "$PRODUCTION_GROUP" >&2
        return 1
    }
    [[ "$(id -gn "$PRODUCTION_USER")" == "$PRODUCTION_GROUP" ]] || {
        printf '[safety] Production user primary group must be %s.\n' \
            "$PRODUCTION_GROUP" >&2
        return 1
    }
}

run_as_qtr() {
    runuser --user "$PRODUCTION_USER" -- /bin/sh -c \
        'umask 022; exec "$@"' qtr-production-command "$@"
}

git_as_qtr() {
    run_as_qtr git -C "$PROJECT_DIR" "$@"
}

rebuild_venv() {
    log "Rebuilding virtual environment as ${PRODUCTION_USER} at ${VENV_DIR}"
    rm -rf -- "$VENV_DIR" || return 1
    run_as_qtr python3 -m venv "$VENV_DIR" || return 1
    verify_venv_owner_and_traversal || return 1
    run_as_qtr "$VENV_DIR/bin/python" -m pip install --upgrade pip || return 1
}

install_runtime() {
    log "Installing runtime dependencies as ${PRODUCTION_USER}"
    run_as_qtr "$VENV_DIR/bin/python" -m pip install -e \
        "${PROJECT_DIR}[web,telegram]" || return 1
    verify_runtime_executables || return 1
}

install_check_tools() {
    log "Installing verification tools as ${PRODUCTION_USER}"
    run_as_qtr "$VENV_DIR/bin/python" -m pip install pytest mypy ruff || return 1
}

verify_venv_owner_and_traversal() {
    local actual_owner actual_group
    actual_owner="$(stat -c '%U' "$VENV_DIR")" || return 1
    actual_group="$(stat -c '%G' "$VENV_DIR")" || return 1
    if [[ "$actual_owner" != "$PRODUCTION_USER" || \
          "$actual_group" != "$PRODUCTION_GROUP" ]]; then
        printf '[safety] Invalid venv ownership: expected %s:%s, got %s:%s.\n' \
            "$PRODUCTION_USER" "$PRODUCTION_GROUP" \
            "$actual_owner" "$actual_group" >&2
        return 1
    fi
    run_as_qtr /usr/bin/test -x "$VENV_DIR" || {
        printf '[safety] Production user cannot traverse %s.\n' "$VENV_DIR" >&2
        return 1
    }
}

verify_runtime_executables() {
    verify_venv_owner_and_traversal || return 1
    local executable
    for executable in market-signal-web market-signal-telegram; do
        run_as_qtr /usr/bin/test -x "$VENV_DIR/bin/$executable" || {
            printf '[safety] Executable is unavailable to %s: %s\n' \
                "$PRODUCTION_USER" "$VENV_DIR/bin/$executable" >&2
            return 1
        }
    done
}

wait_for_service() {
    local service="$1"
    local attempts="${2:-$WAIT_ATTEMPTS}"
    local interval="${3:-$WAIT_INTERVAL_SECONDS}"
    local attempt state
    log "Waiting up to ${attempts}s for ${service} to become active"
    for (( attempt = 1; attempt <= attempts; attempt++ )); do
        state="$(systemctl is-active "$service" 2>/dev/null || true)"
        case "$state" in
            active)
                return 0
                ;;
            failed)
                printf '[safety] Service entered failed state: %s\n' \
                    "$service" >&2
                return 1
                ;;
        esac
        if (( attempt < attempts )); then
            sleep "$interval"
        fi
    done
    printf '[safety] Timed out waiting for active service: %s\n' \
        "$service" >&2
    return 1
}

start_services_and_wait() {
    log "Starting systemd services"
    systemctl start --no-block "$WEB_SERVICE" "$TELEGRAM_SERVICE" || return 1
    wait_for_service "$WEB_SERVICE" || return 1
    wait_for_service "$TELEGRAM_SERVICE" || return 1
}

health_check_once() {
    run_as_qtr "$VENV_DIR/bin/python" - <<'PY'
import http.client
import json

connection = http.client.HTTPConnection("127.0.0.1", 8000, timeout=0.2)
try:
    connection.request("GET", "/health")
    response = connection.getresponse()
    payload = json.loads(response.read())
    healthy = response.status == 200 and payload == {"status": "ok"}
except Exception:
    healthy = False
finally:
    connection.close()
raise SystemExit(0 if healthy else 1)
PY
}

wait_for_health() {
    local attempts="${1:-$WAIT_ATTEMPTS}"
    local interval="${2:-$HEALTH_WAIT_INTERVAL_SECONDS}"
    local attempt
    log "Waiting up to ${attempts}s for Web health at ${HEALTH_URL}"
    for (( attempt = 1; attempt <= attempts; attempt++ )); do
        if health_check_once; then
            return 0
        fi
        if (( attempt < attempts )); then
            sleep "$interval"
        fi
    done
    printf '[safety] Web health check timed out.\n' >&2
    return 1
}

safe_service_diagnostics() {
    local service
    printf '[diagnostics] Safe systemd diagnostics follow.\n' >&2
    for service in "$WEB_SERVICE" "$TELEGRAM_SERVICE"; do
        systemctl status "$service" --no-pager -l || true
        journalctl -u "$service" -n 50 --no-pager || true
    done
}
