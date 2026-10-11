# QTR Micro P0 observability review

Date: 2026-10-10 (America/New_York).

Dev repository: `C:/Users/Legion/Desktop/QTR_AI2/micro-p0-observability`.
Branch: `deploy/micro-p0-observability-514b9607`.
Exact production/comparison base: `514b9607f689dbc3235c8adf1e7b49776ddda9a6`.
Production identity was supplied as confirmed by the user. The exact Git object
was fetched from GitHub over HTTPS and audited locally; no SSH/VPS access occurred.
Original base: `eea5ffbba8c107ef9bb41f728ef72e35cc3d2489`.
Original completed patch is preserved on `fix/micro-p0-observability` at
`ef8569e2d6a9e5968e03962cb054790b3afee6fc`.
During local validation no deploy, service restart, production edit, or live
trading/API run was performed. Operational deployment evidence is recorded separately.

## Production commit audit and transfer

GitHub origin/main was fetched and matched the exact required 514b9607 SHA.
514b9607 is the PR #6 merge with parents eea5ffb and 948c901, and its tree is
identical to 948c901. The final deployment patch is based directly on 514b9607;
all differential tests now load 514b9607, not the earlier equivalent commit.
Source patch 44872628d668fff8e55c375f1a21d0cd27b65493 is preserved on
fix/micro-p0-observability-prod-base. It was cherry-picked onto the new deployment
branch without conflicts. No additional application patch was included.

The verified sole parent of 948c901 is eea5ffb. The production delta contains
three files, 279 insertions and 5 deletions:

- `client.py:BybitDemoTradingClient.position_mode`: catch DemoApiError 10002 and
  retry the GET `/v5/position/list` exactly once with identical symbol/category.
  A second failure propagates. Other errors are never retried. Each authenticated
  request rebuilds timestamp/signature; order creation never inherits this retry.
- `runtime.py:handle_candidates`: annotate position_mode, instrument_rules,
  prepare_entry, fresh_price, revalidation and submit_entry stages. The failure log
  includes symbol, stage, Demo ret_code (or n/a), exception type and message.
- `tests/test_qtr_micro.py`: six regressions cover success on the one retry,
  retry bound, other errors, no order retry, fresh authenticated timestamp and
  precise runtime failure logging.

A clean branch was created at the exact production SHA and the saved P0 commit
was cherry-picked. Runtime and test files auto-merged; there were no textual
conflicts and no manual conflict resolutions. Client and reconciliation files
match production source exactly (normalized line endings); the entire production
regression block is also unchanged and asserted in tests.

## Audit and exact paths

- `engine.py:QtrMicroEntryEngine.prepare_entry/revalidate_entry`: all entry gates,
  sizing, risk, leverage and stop/target calculations remain unchanged.
- `runtime.py:handle_candidates`: prepare -> fresh equity/price -> revalidate ->
  `execution.py:submit_and_confirm_entry/submit_entry/confirm_entry` -> protection.
  Context is copied from the existing candidate immediately before submission;
  no new analysis or fetch is introduced.
- `runtime.py:post_entry_management_flags/_manage_open`: current breakout failure
  supplies degradation; independent cancellation/opposite confirmation supplies
  structural exit flags. These mappings are unchanged.
- `execution.py:manage_position` -> `engine.py:manage` -> `_reduce` -> pending ACK
  state -> `_confirm_exit` -> `_write_journal` -> `journal.py:append_once`.
- `execution.py:reconcile` first calls the unchanged
  `reconciliation.py:reconcile_demo_state`, then recovers a confirmed fill from
  pending exit, protective stop, external manual close, persisted closed external
  close, or a durable `runner_exit_price` snapshot. All use `_write_journal`.
- `state.py:_position_from_json` loads optional evidence with unknown defaults;
  the existing state schema version and trading-state readers are retained.
- Existing lifecycle, reconciliation, protective-stop, manual-close, restart,
  journal idempotency and Trader routing coverage was audited in
  `test_qtr_micro.py`, `test_qtr_trader.py`, and `test_trader_routing.py`.

### TIME_EXIT ambiguity

After hard structure exit, `manage()` checks runner age, normal age, then early
progress degradation, before TP1/TP2/runner target checks. Defaults remain
15/45/90 minutes. The runner-age branch returns `RUNNER_TIME_EXIT` even when the
position has not reached RUNNER; that existing behavior is preserved.

- `EARLY_PROGRESS_STRUCTURE_DEGRADED`: progress age reached, current R < 0.5,
  and structure_degraded. It can fire on a runner too under existing logic.
- `NORMAL_MAX_HOLD`: normal age reached and stage is neither TP2_FILLED nor RUNNER.
- `RUNNER_MAX_HOLD`: runner-age branch reached; enum remains RUNNER_TIME_EXIT.

Only a detail string is added to the existing branch return. No predicate,
priority, threshold, close quantity, or new stop is changed. If normal and early
conditions both hold at 45m, normal detail wins, matching existing branch order.

### Excursion inconsistency

Normal `_confirm_exit()` already included exit fill in both price extrema.
Reconciliation reconstructed PnL and fees without updating extrema; `_write_journal`
trusted those stale extrema. Entry fill did not initialize extrema either, so an
all-adverse lifecycle could lack the zero-excursion entry anchor. The pending-ACK
branch also returned before recording the already supplied current-price input.

The patch includes confirmed entry fill, confirmed risk-resize fill, already
supplied causal management observations (including during pending confirmation),
partial/full confirmed exit fills and all known recovery fills. Entry price is
an anchor in both extrema helpers, including for old state. Recovery persists
updated extrema; the shared writer defensively includes final fill before
serialization. No sampler, simulation, post-exit price lookup or API call is added.

The unit is signed price difference from actual average entry fill, not R and
not a continuous-market tick maximum. It only covers available observations.

## Serialization and metric semantics

- `result_r` retains legacy net realised PnL / planned risk (legacy zero fallback).
- `gross_r` and `net_r` retain their existing actual-risk formulas and legacy
  truthy-denominator behavior. Every runtime journal path shares one writer.
- `realised_net_r_actual` is an explicit serialized alias of `net_r` only for a
  positive actual_risk_at_fill; otherwise null. It is a property, not independently
  stored/calculated state. It never substitutes planned risk. Tests prove the
  identity for management, partial-to-final and all recovery paths.
- Net realised PnL remains accumulated realised gross PnL minus recorded fees.
  Funding remains the existing separate field; unknown funding is not estimated.
- `-117.79 / 293.30 = -0.4016024548R` is covered with a different planned-risk
  denominator to detect fallback errors.
- Existing setup_type (localized name) and setup_confidence are retained.
  setup_state_at_entry and classification_candidates are copied from the causal
  candidate. Classification candidates serialize as enum values.
- Exit detail/trigger, decision-time position age/R, degradation/cancellation/
  opposite flags, current_breakout_failure and setup_state_at_exit persist through
  ACK/restart/fill. exit_setup_state, exit_structure_degraded and exit_current_r
  expose the requested serialization names for the same values.
- With no current candidate, setup context remains null; management still gets
  its original default flags. Pending confirmation never replaces decision-time
  evidence with later observations. A later STOP/manual close cannot inherit a
  prior partial-exit decision. Historical missing TIME_EXIT subtype is not guessed.

## Semantics proof

TRADING SEMANTICS VS 514b9607: UNCHANGED.

Tests dynamically load original client/engine/reconciliation/execution/runtime
source from the pinned production
Git base. They compare complete prepare/revalidate decisions and plans across
all setup-state/type combinations and six revalidation prices. They compare
management action, close qty and stop across 6,144 cases: both directions, four
stages, boundary seconds around 15/45/90m, eight R values and all flag combinations.

Client call traces and lifecycle results match BEFORE/AFTER for runner TP1/TP2,
normal timeout, early degraded timeout, exchange stop, Human Close and actual-risk
resize. The runtime entry flow also has identical client calls/arguments. These
include client reads, order qty, leverage changes, reduce-only flags and stops.
The comparison tests require Git history containing the pinned base SHA.

Additional production-base proofs compare six ownership/safety scenarios and
six recovery paths, including unknown-fill fail-closed blocking, and compare
all client I/O on initial recovery and replay. They compare runtime failure logs
and call traces at four client-error boundaries and position-mode retry success,
bounded repeated failure and non-10002 failure against original production code.
Only observational extrema/context are excluded from the recovery state equality;
ownership, lifecycle, quantity, PnL, fees, journal markers and safety blocks match.

## Files changed

- `src/market_signal_assistant/qtr_micro/engine.py`: branch detail only.
- `src/market_signal_assistant/qtr_micro/execution.py`: evidence capture and causal
  extrema updates; no order/risk/lifecycle rule changes.
- `src/market_signal_assistant/qtr_micro/runtime.py`: pass existing candidate context.
- `src/market_signal_assistant/qtr_micro/models.py`: optional plan/position evidence.
- `src/market_signal_assistant/qtr_micro/state.py`: backward-compatible evidence load.
- `src/market_signal_assistant/qtr_micro/journal.py`: optional serialized context and
  canonical actual-risk alias; append_once/contains behavior unchanged.
- `tests/test_qtr_micro.py`: existing execution spy accepts observational keywords.
- `tests/test_qtr_micro_observability.py`: differential and lifecycle regressions.
- `docs/qtr_micro_p0_observability.md`: this review artifact.

## Validation

Original old-base focused suite: 168 passed, 0 failed (historical).
Production-base observability regressions: 119 passed, 0 failed.
Production-base focused suite: 293 passed, 0 failed.
Production-base full suite: 1,541 passed, 0 failed (57.53 seconds).
Ruff entire repository: passed, 0 errors.
Strict mypy `src`: passed, 134 source files, 0 errors.
Git diff whitespace check: passed.

Runtime used: source repository's existing `.venv/Scripts/python.exe`; no dependency
or environment configuration changed. Commands: `python -m pytest -q`, focused
pytest on the four Micro/Trader files above, `python -m ruff check .`, and
`python -m mypy src`. Pytest used separate workspace-local basetemp directories.
Validation artifacts are retained under `reports/p0_deploy_514b9607/` and excluded from
the code diff.

## Parked and limitations

- episode_age_seconds: candidate contains episode ID but no causal episode-start
  timestamp. Signal analyzed_at and breakout_age_bars are not substitutes.
- HTF, room-to-target, new indicators/scoring, RETEST/confidence behavior, freshness,
  priority, distance, risk, sizing, leverage, SL/TP, exit decisions and Scanner
  qualification: outside this observability patch.
- Unknown exit fills/emergency-close ACKs remain unknown under the existing
  recovery contract; no extra execution lookup or recovery semantics added.
- Historical journal rows are immutable. Missing historical prices/context cannot
  be reconstructed; the patch does not backfill a claimed full tick history.
- Production SHA was supplied by the user and verified as a Git object locally;
  current VPS/service/configuration and live execution were not inspected.
  This dev patch is for review and does not authorize deployment.

READY FOR DEPLOY REVIEW: YES.
