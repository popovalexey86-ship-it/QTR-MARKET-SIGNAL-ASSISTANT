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
execution service, state store, Telegram sender, order API, or journal
dependency. Evidence validation rejects execution identity fields.

## Evidence and reporting

Each JSONL record contains Scanner identity and timestamps, episode and symbol,
Scanner state/eligibility/quality, candidate/trigger/ATR distances, the causal
quote and request/receipt timestamps, settings/context digests, and the three
branch decisions. Credentials are excluded before settings hashing and are
never serialized.

`summarize_distance_shadow()` reports both:

- primary episode-level eligibility, where each episode contributes at most one
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
produce `EXPERIMENT_INVALID`. The dispatcher's operational counter must be
supplied to `summarize_distance_shadow(invalid_observations=...)` for the final
prospective verdict.

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
