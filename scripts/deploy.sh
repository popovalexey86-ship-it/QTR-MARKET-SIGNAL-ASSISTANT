#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
# shellcheck source=deploy-common.sh
source "$SCRIPT_DIR/deploy-common.sh"

STATE_DIR="/opt/qtr/.deploy"
STATE_FILE="${STATE_DIR}/scanner-previous-commit"
PREVIOUS_COMMIT=""
DEPLOY_MUTATED=false
state_tmp=""

log() { printf '[deploy] %s\n' "$*"; }

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

rollback_after_failure() {
    local recovery_failed=0
    log "Failure detected; restoring ${PREVIOUS_COMMIT}"
    systemctl stop "$WEB_SERVICE" "$TELEGRAM_SERVICE" || recovery_failed=1
    if git reset --hard "$PREVIOUS_COMMIT"; then
        if rebuild_venv && install_runtime; then :; else recovery_failed=1; fi
    else
        recovery_failed=1
    fi
    if start_services_and_wait; then
        wait_for_health || recovery_failed=1
    else
        recovery_failed=1
    fi
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
    safe_service_diagnostics
    printf '[deploy] Deploy failed.\n' >&2
    exit "$exit_code"
}

cleanup() {
    if [[ -n "$state_tmp" ]]; then rm -f -- "$state_tmp"; fi
}

main() {
    trap on_error ERR
    trap cleanup EXIT

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
    ensure_clean_worktree

    log "Fetching origin"
    git fetch origin
    PREVIOUS_COMMIT="$(git rev-parse --verify HEAD^{commit})"
    TARGET_COMMIT="$(git rev-parse --verify refs/remotes/origin/main^{commit})"
    if [[ "$PREVIOUS_COMMIT" == "$TARGET_COMMIT" ]]; then
        log "already up to date"
        return 0
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
    log "Running pytest as ${PRODUCTION_USER}"
    run_as_qtr "$VENV_DIR/bin/python" -m pytest
    log "Running mypy as ${PRODUCTION_USER}"
    run_as_qtr "$VENV_DIR/bin/python" -m mypy src
    log "Running ruff as ${PRODUCTION_USER}"
    run_as_qtr "$VENV_DIR/bin/ruff" check .

    start_services_and_wait
    wait_for_health
    DEPLOY_MUTATED=false
    log "Deploy completed successfully at ${TARGET_COMMIT}"
}

main "$@"
