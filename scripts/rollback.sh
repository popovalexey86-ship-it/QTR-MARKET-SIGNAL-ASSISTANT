#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=deploy-common.sh
source "$SCRIPT_DIR/deploy-common.sh"

STATE_FILE="/opt/qtr/.deploy/scanner-previous-commit"

log() { printf '[rollback] %s\n' "$*"; }

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

on_error() {
    local exit_code=$?
    trap - ERR
    safe_service_diagnostics
    printf '[rollback] Rollback failed; manual recovery may be required.\n' >&2
    exit "$exit_code"
}

main() {
    trap on_error ERR

    log "Validating production repository and user ${PRODUCTION_USER}"
    require_root
    require_command git
    require_command python3
    require_command systemctl
    require_command journalctl
    require_command runuser
    require_command getent
    require_command stat
    ensure_production_identity
    ensure_expected_repository

    [[ -f "$STATE_FILE" ]] || {
        printf '[rollback] Rollback state file is missing: %s\n' "$STATE_FILE" >&2
        return 1
    }
    IFS= read -r PREVIOUS_COMMIT <"$STATE_FILE"
    [[ "$PREVIOUS_COMMIT" =~ ^[0-9a-fA-F]{40,64}$ ]] || {
        printf '[rollback] Rollback state contains an invalid commit id.\n' >&2
        return 1
    }
    PREVIOUS_COMMIT="$(git rev-parse --verify "${PREVIOUS_COMMIT}^{commit}")"
    git cat-file -e "${PREVIOUS_COMMIT}^{commit}"

    log "Stopping services"
    systemctl stop "$WEB_SERVICE" "$TELEGRAM_SERVICE"
    log "Restoring commit ${PREVIOUS_COMMIT}"
    git reset --hard "$PREVIOUS_COMMIT"
    rebuild_venv
    install_runtime
    start_services_and_wait
    wait_for_health
    log "Rollback completed successfully at ${PREVIOUS_COMMIT}"
}

main "$@"
