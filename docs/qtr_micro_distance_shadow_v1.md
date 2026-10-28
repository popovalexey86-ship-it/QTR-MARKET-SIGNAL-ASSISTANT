# QTR Micro distance-threshold shadow experiment V1

## Scope lock

This component compares the existing Micro entry-distance gate at `0.25 ATR`
with read-only shadow gates at `0.40 ATR` and `0.60 ATR`. It is deliberately
integrated through opt-in hooks in the existing Scanner, Micro runtime, and
Telegram process. `QTR_MICRO_DISTANCE_SHADOW_ENABLED` defaults to `False`;
when disabled, no prospective collection occurs. There is no separate service
or systemd component. Enabling prospective collection requires separate
authorization.

Production `QtrMicroSettings.max_entry_distance_atr` remains `0.25`. The
observer rejects construction if its production settings snapshot is not
exactly `0.25`, and the integration does not change production Micro
semantics.

## Observation contract

### Context coverage correction (schema 3)

The existing published context path is preserved. When production completes
without publishing a matching context, the existing bounded Shadow worker
calls `ReadOnlyShadowContextProvider` once in a worker thread. Production
guards and their ordering remain unchanged, including the known cross-symbol
episode guard. No additional call is made on the production handler stack.

The provider reads the existing state bytes and uses its pure decoder; it
never calls `JsonQtrMicroStateStore.load()`, whose corruption recovery can
rename files. Missing, unreadable or corrupt state fails closed. The other
bindings expose only the immutable factual preflight result, equity and
symbol instrument rules. The provider cannot run preflight, reconciliation,
repair, leverage updates, execution or order APIs. Required Demo inputs are
validated and copied; preflight readiness is never fabricated.

`context_source` is exactly `PUBLISHED_RUNTIME_CONTEXT` or
`SHADOW_READ_ONLY_FALLBACK`. The latter is a new live post-production read,
not reconstructed historical context. All branches use one copied context.
Aliases explicitly record `qualified_at`, `production_completed_at`,
`context_acquired_at` and `fresh_quote_at` alongside the prior detailed timing
fields. Published context must precede production completion; fallback
acquisition must follow worker start. Quote timestamps must follow context
acquisition. The original timing rule is not silently relaxed for published
contexts.

Fallback dependency failure writes one `EVIDENCE_MISSING` observation with
`CONTEXT_FALLBACK_FAILED` and a sanitized `stage:ExceptionClass` detail, no
quote attempt, and no invented context. An acquired context gets exactly one
fresh-price request, without retry, even when initial branches reject. Quote
failure retains its explicit evidence-missing behavior. Storage failure
cannot guarantee evidence delivery: it is separately accounted and invalidates
the sample rather than silently succeeding.

The caller must supply one `QualifiedScannerSnapshot` and one
`FrozenRuntimeContext`. Scanner qualification is based on the A+ filter result,
not on whether Telegram sends a message; duplicate notification decisions are
valid observations. A snapshot that did not pass the Scanner A+ boundary is
rejected before evidence is written.

For every qualified snapshot the observer:

1. deep-copies the candidate and runtime context;
2. derives all three settings snapshots from the same production snapshot,
   changing only `max_entry_distance_atr`;
3. runs the existing `QtrMicroEntryEngine.prepare_entry()` for each branch;
4. calls the narrow `FreshPriceReader.current_market_price()` boundary exactly
   once;
5. applies that same causal price to each accepted branch through the existing
   `revalidate_entry()`;
6. appends one durable JSONL record.

No retry or retrospective price substitution is allowed. A missing, invalid,
or failed price read produces `EVIDENCE_MISSING` for all branches.

The maximum positive status is `PRE_SUBMIT_ELIGIBLE`. The observer has no
execution service, production state store, Telegram sender, order API, or trade journal
dependency. Evidence validation rejects execution identity fields.

## Evidence and reporting

New default files are unique per process/run:
`data/qtr_micro_distance_shadow_v2/<experiment_run_id>.jsonl`, with schema 3.
Each evidence record contains the run ID. The old
`data/qtr_micro_distance_shadow_v1.jsonl` is explicitly rejected as a writable
sink. A fresh sink cannot implicitly resume any existing nonempty file;
An accounting writer never adopts an existing run, including under the same
InvocationID. A process restart requires a new run ID and new files. The
low-level evidence reader/sink cannot establish prospective run ownership.
Operational collection still requires separate authorization. No new sample
is started by this implementation task.

The permanently invalid original sample is never repaired or reclassified.
Schema 2 evidence requires a separate historical report; it cannot be mixed
into the new schema 3 sample.

Each JSONL record contains Scanner identity and timestamps, episode and symbol,
Scanner state/eligibility/quality, candidate/trigger/ATR distances, the causal
quote and request/receipt timestamps, settings/context digests, and the three
branch decisions. Credentials are excluded before settings hashing and are
never serialized.

`summarize_distance_shadow()` reports both:

- primary episode-level eligibility keyed by `(experiment_run_id, symbol,
  raw_episode_id)`, where each symbol/episode contributes at most one
  success per threshold; and
- secondary all-snapshot counts and rates.

The report includes absolute and percentage-point deltas from the `0.25`
control and emits only these interpretation categories:

- `NO_MATERIAL_FUNNEL_CHANGE`
- `DISTANCE_INCREASES_ELIGIBILITY`
- `INCONCLUSIVE_SAMPLE`
- `EXPERIMENT_INVALID`

A final verdict requires at least 100 valid A+ snapshots and at least 30 unique
episodes, collected within a maximum operational window of 72 hours. The
72-hour limit is operational rather than code-enforced because the existing
component has no causal experiment-start timestamp. Before both count
requirements are met, the category is `INCONCLUSIVE_SAMPLE`.

The final operational sample is valid only when `invalid_observations == 0`,
unless a future separately approved criterion states otherwise. Invalid
records, evidence-missing records, or nonzero operational invalid observations
produce `EXPERIMENT_INVALID`. An unknown operational counter is not zero and
also fails the validity gate. Mixed run IDs are explicitly invalid. Production
episode identifiers and guards are not changed by symbol-scoped reporting.

### Finalization, terminal accounting and the exact cutoff

The live `dispatcher.accounting.snapshot()` exposes counters and pending work.
`completed_count` means evidence returned successfully after write/flush/fsync;
`failed_invalid_count` and `cancelled_invalid_count` are separate terminal
states. Pending = accepted - completed - failed - cancelled. Cancellation is
invalid even if an underlying thread finished a write while cancellation was
being drained. It must never be reclassified as a successful observation.

`invalid_observations` is queue drops + qualified enqueue drops + worker
exceptions + cancelled observations. Journal failures are a subset of worker
exceptions (or a diagnostic on cancelled work), not double-counted. A written
fallback-missing record separately invalidates via EVIDENCE_MISSING. Admission
qualifies the complete batch first, prepares every queued item before queue
mutation, reserves capacity without awaits, and counts only qualified losses.

At construction, a non-empty InvocationID is mandatory. An exclusively created,
fsynced `<run>.accounting.owner.json` binds the run, invocation, unique writer,
creation timestamp, accounting schema 1, seal schema 1 and experiment schema 3. Existing owner,
accounting, evidence or seal files prevent acquiring a new writer. Missing or
unprovable ownership fails closed. Missing, malformed, unsupported or mismatched
owner seal-schema versions fail certification, including mutation after creation.
The reader rejects decreases in every
cumulative counter and rejects non-increasing revisions.

Admission accounts for the prepared batch before exposing queue entries, with
no intervening await. A synchronous failure rolls back only the affected cycle,
preserving other cycles and their completion handles. Accounted reservations
become failed-invalid terminals, qualified losses are counted once, and uncertain
post-reservation accounting prevents sealing. Async write failures retain the
returned completion handle while preventing certification.

One coalescing asynchronous writer fsyncs provisional revisioned checkpoints
in `<run>.accounting.jsonl`. `await dispatcher.wait()` drains accepted work
and checkpoint writing; **it does not authorize a final verdict**. Checkpoints
are never sufficient to certify a run, even if they report zero pending/invalid
observations and even if a caller supplies `invalid_observations=0`.

A separately authorized run controller must explicitly call
`await dispatcher.finalize()`. That closes admission first (later candidates
are explicitly outside the run cutoff), drains accepted operations and their
outstanding threads, freezes evidence writes, checks journal/count agreement,
and reserves/fsyncs a monotonically increasing final accounting revision.
The drainer monitors both queue completion and worker termination. If a worker
exits with accepted items still queued, it ensures one replacement worker without
reopening admission or requiring another external drain call. Concurrent drainers
share that worker: check/replacement has no intervening await. A cancelled current
item remains cancelled-invalid; remaining items follow the existing observer path.
Cancellation of a drainer only cancels its queue-join monitor, not the shared worker.

Cancellation of the finalization waiter drains the finalizer before returning;
worker cancellation drains its current thread before its terminal accounting.
No successful finalization can leave an evidence-writing thread outstanding.

Observation identity is `(experiment_run_id, production_cycle_id, candidate_id)`.
Finalization rejects duplicate identities. Summary reports raw records and
duplicates separately, uses unique valid observations for the snapshot gate,
and cannot certify duplicates. The same candidate in a later cycle remains
countable; episode grouping remains `(run_id, symbol, raw_episode_id)`.

The version 1 seal binds run ID, InvocationID, final revision, all counters,
accepted/completed/failed/cancelled/pending counts, evidence record count and
SHA-256, unique observation count, the accounting journal SHA-256, admission
cutoff and finalization time. Every final field except the accounting hash itself
is persisted inside the final checkpoint before its hash is computed; the seal
copies that exact checkpoint. Accounting, seal and experiment schema versions
are explicit. Required timestamps are offset-aware ISO values with
`run_created_at <= admission_closed_at <= finalized_at <= checkpoint_at`.
Summary also checks observation timestamps against admission/finalization.
Malformed, missing, mutated or incompatible final metadata fails closed.
It is written to a new Shadow-only `.seal.tmp`, flushed and fsynced, then
atomically published as `.seal.json`. Only Shadow metadata is renamed. A failed
seal write/fsync leaves no published seal; a temporary file is never accepted.
No seal may be overwritten. Losing its directory entry in a crash also fails
closed as an absent seal. Production state files are never renamed or repaired.

`read_shadow_accounting(path, run_id=...)` requires the final seal by default,
validates its exact revision/counters and checkpoint hash, and rejects missing,
partial, mixed-run/invocation or inconsistent metadata. Its explicit
`require_sealed=False` mode is provisional inspection only. Summary additionally
checks the sealed evidence hash/count and never certifies an unsealed, pending,
mismatched or persistence-failed run. An older healthy checkpoint cannot certify
later activity. An explicit external counter cannot bypass the seal; a
contradictory nonzero counter can only invalidate the report. After finalization,
admission and evidence writes stay closed; reopening a sealed sink is rejected.

This implementation does not call finalization on an active VPS, create an
operational control endpoint, or start a sample. A future authorized run must
arrange an explicit live-dispatcher finalization call at its intended cutoff;
ordinary drain/shutdown alone leaves an unsealed, uncertifiable run.


After all validity and count gates pass, a Shadow threshold produces
`DISTANCE_INCREASES_ELIGIBILITY` only when it improves the episode eligibility
rate by at least 10 percentage points and improves eligible unique episodes by
at least 5 versus the `0.25` control. Both conditions are required. Otherwise,
the category is `NO_MATERIAL_FUNNEL_CHANGE`. This category makes a threshold
only a candidate for QTR AI review; it never recommends or applies a production
threshold change.

The prospective 72-hour sample is valid only for one uninterrupted Shadow
collection run. If `qtr-scanner-telegram` restarts after sample start, the
sample must be stopped and restarted or treated as invalid because
`invalid_observations` is process-local and must not be assumed to survive a
restart. Operational deployment verification must record `NRestarts` at
sample start and during the sample; the required delta is zero.

Starting any prospective sample, selecting an evidence path, or enabling the
opt-in integration requires a separate QTR AI authorization and review.
