# Watchdog Evidence Baseline V1 (offline)

This is a descriptive, read-only analysis of existing immutable evidence, not a
Watchdog runtime, a trading signal, or a detector/threshold evaluation. Run one
dataset at a time; no datasets are pooled by default. The manifest's commit is
external provenance and must be checked against launch records independently:
schema version 1 does not prove semantic compatibility.

Run after installing the project in an isolated Python environment:

```text
python scripts/watchdog_evidence_baseline.py \
  docs/watchdog-evidence-baseline-v1-24h.json \
  --output /path/outside/source/dataset/evidence-baseline-24h.md \
  --verify-sqlite
```

The command also writes a deterministic `.rows.jsonl` file beside the Markdown
report. Every event/horizon row identifies its dataset, target, recorded status,
actual observation time, availability time, lag, raw observation ID and source.
The output path and row path must be outside the source dataset. On validation
failure, the Markdown report lists issues and no descriptive aggregates or rows
are produced; the command exits nonzero.

The loader streams physical JSONL lines and never constructs runtime journal
objects, which can synchronize writable SQLite sidecars. Optional SQLite checks
open databases only with `mode=ro&immutable=1`. The analyzer hashes every source
file before and after inspection; a changed source invalidates the report.

`PENDING_DUE_UNRESOLVED` is an analytical label at the frozen manifest cutoff,
not a new `MISSING` outcome. It does not modify journal evidence or call
`mark_missing`. `ON_TIME` and `LATE` retain the runtime source-interval tolerance;
the nominal horizon must never be presented as an exact-minute observation.

All cohorts show event N, unique symbols and outcome coverage. Anomaly labels
are overlapping multi-label memberships. Small-N warnings and actual source/lag
are mandatory. The top-review section is sorted by cohort size, not by movement
or any putative trading value. No control group, inference of causality,
profitability, parameter sweep, or threshold optimization is performed.
