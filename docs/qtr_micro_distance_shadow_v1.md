# QTR Micro distance-threshold shadow experiment V1

## Scope lock

This component compares the existing Micro entry-distance gate at `0.25 ATR`
with read-only shadow gates at `0.40 ATR` and `0.60 ATR`. It is deliberately
not connected to the Scanner loop, Micro runtime, Telegram routing, systemd, or
deployment configuration. Prospective collection therefore cannot start as a
side effect of this change.

Production `QtrMicroSettings.max_entry_distance_atr` remains `0.25`. The
observer rejects construction if its production settings snapshot is not
exactly `0.25`.

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

The default minimum for a conclusive label is 30 unique episodes. Starting any
prospective sample, selecting an evidence path, or wiring the observer into the
running process requires a separate QTR AI authorization and review.
