# Distance Shadow context coverage — final pre-commit fixes

Status: the four second-review and two third-review findings are fixed locally
and ready for final targeted review. All changes remain uncommitted. No commit, push, deployment, VPS
access, environment change, service restart or prospective sample occurred.

Worktree: `distance-shadow-context-coverage`.
Branch: `fix/distance-shadow-context-coverage`.
Exact base/HEAD: `6cb79555a0fc2acb33aa487905c5f2c6cddb37ee`.

## Final pre-commit fixes — third-review findings

Only the two requested findings were changed in this round:

1. **P2 multi-item drain recovery.** `wait()` monitors queue completion and the
   shared worker. Worker termination wakes the drainer, which ensures one
   replacement worker if accepted items remain queued. Admission stays closed.
   Check/replacement occurs without an await, so concurrent drainers cannot start
   duplicate workers. The cancelled item retains its invalid terminal; remaining
   items follow the original processing path. Finalization awaits queue discharge
   and accounting before sealing. Cancelling a drainer does not cancel its worker.
2. **P3 seal version in ownership.** Owner creation, all accounting checkpoints
   and reader identity checks bind `seal_schema_version=1`. Missing, malformed,
   boolean, unsupported and mismatched owner versions reject certification. The
   correct newly created owner/ledger/seal chain remains readable. No old owner
   manifest or evidence file is migrated or rewritten.

The new `test_qtr_micro_distance_shadow_final_fixes.py` has 12 regression cases:
2/5 accepted items cancelled during acquisition; termination before pickup and
immediately after task_done; concurrent drainers with exactly one replacement and
no duplicate records; seven malformed/missing/unsupported owner-version variants.
No external second drain is needed on the success path. Queue, accepted, completed,
cancelled-invalid and pending counters are checked against physical evidence files.

This round changed only dispatcher drain logic, owner seal-version binding,
the new regression file and the two experiment documents. Production/runtime/bot
and the read-only provider were not edited in this round.

## Changes made in Round 3

1. **Immutable ownership.** Accounting captures a required non-empty InvocationID
   and exclusively creates/fsyncs an owner manifest binding run ID, writer ID,
   invocation, creation time and accounting/experiment versions. It refuses all
   existing run files rather than adopting counters, even in the same invocation.
   Interrupted ownership creation cannot be silently resumed. Readers require
   exact manifest identity and monotonic cumulative counters/revisions.
2. **Unique observations.** Identity is `(run_id, production_cycle_id,
   candidate_id)`. Finalization rejects persisted duplicates. Summary counts unique
   valid observations for the snapshot gate and exposes raw/duplicate counts.
   Same candidate in different cycles remains distinct. Episode aggregation stays
   `(run_id, symbol, raw_episode_id)`.
3. **Integrity-bound final state.** Accounting schema 1, seal schema 1, experiment
   schema 3. The exact final checkpoint includes all final counters, count of
   evidence/unique observations, evidence hash, required admission cutoff and
   finalization timestamp. It is fsynced before its hash and seal are published.
   The seal copies the exact persisted checkpoint. Timestamp syntax, timezone,
   lifecycle ordering, schema and all checkpoint bindings are validated. Summary
   parses and hashes the same evidence byte snapshot.
4. **Transactional admission.** Qualified batch metadata is prepared first.
   Accepted accounting precedes queue exposure, without awaits. Synchronous
   failures roll back affected queue entries while preserving other cycles and
   their task/completion ownership. Accounted reservations become explicit invalid
   terminals. Losses are counted once, including overflow scheduling failures.
   Uncertain post-reservation accounting prevents sealing. Async persistence
   failures keep the returned cycle reachable and invalidate certification.

Ordinary drain/shutdown still does not seal. Existing cancellation/thread drain,
startup isolation, stale-checkpoint rejection and qualified-only counting remain
covered. No runtime control endpoint or sample-start operation was added.

## Adversarial replays and new regressions

`tests/test_qtr_micro_distance_shadow_round3.py`: **40 passed**.

- Persisted invalid=1 cannot be replaced by a new owner with zero counters.
- Missing/empty InvocationID is rejected; another invocation cannot reuse run files.
  Another invocation with a fresh run ID and fresh files is allowed.
- Every cumulative counter is tested for regression; persisted owner identity is
  required by readers.
- 100 accounted records / 30 observation identities / 30 episodes produce only
  30 unique snapshots, fail the 100 gate, cannot seal and report EXPERIMENT_INVALID.
- Same candidate in later scanner cycles produces distinct valid observations.
- Missing/malformed/naive timestamps, reverse chronology, unknown/mismatched
  versions, modified cutoff/finalization metadata, count and invocation mutation
  fail closed.
- Final metadata is checked inside the hashed final checkpoint.
- Failure before/after accounting change, scheduling failure, partial insertion,
  rollback beside an existing cycle, overflow failure and asynchronous persistence
  failure prove no orphan, false completion, pending residue or double drop.

## Validation of final code

Local Python 3.14 isolated venv. Physical test files/fsync were exercised by
pytest; no live runtime or real power-loss durability test is claimed.

| Check | Result |
|---|---|
| Final targeted regressions | 12 passed |
| Preserved Round 3 regressions | 40 passed |
| All Shadow tests and adversarial replay | 105 passed |
| Micro | 156 passed |
| P0 observability/reliability | 119 passed |
| Telegram plus Scanner Telegram integration | 88 passed |
| Explicit Bybit 10002 selector | 2 passed; 154 deselected |
| Full suite | 1650 passed in 106.76s |
| mypy src | PASS; 137 source files |
| Ruff src/tests | PASS |
| git diff --check | PASS |

The previous validation report is superseded by these current results.

## Production semantic comparison

**NONE.** Ten frozen modules match the exact base AST: Micro engine, settings,
execution, reconciliation, state, preflight, client; Scanner service/notifications;
Scanner Telegram integration. Existing runtime AST matches after removing the
previously authorized immutable preflight accessor. Bot AST matches after removing
its previously authorized guarded optional fallback wiring/helper.

Production threshold 0.25 ATR, eligibility, episode guards, risk, leverage, SL/TP,
lifecycle, order submission, reconciliation and Bybit retry semantics are unchanged.
The cross-symbol production episode guard remains out of scope and unchanged.
Fallback retains post-production provenance, pure state reads/decoding, one copied
context and one fresh quote. No repair/save/rename/reconciliation or execution
binding was added to the provider. Only Shadow metadata can be renamed.

Round 3 touched Shadow accounting, dispatcher/evidence/summary, Shadow tests and
test-only identity/clock fixtures, and these two experiment documents. The prior
provider, runtime accessor and bot startup-isolation implementation were preserved.
The old invalid sample was not accessed or modified. No sample was started.

Ready for final targeted code review; tests are not review approval or
permission to commit, deploy, restart or collect a sample.
