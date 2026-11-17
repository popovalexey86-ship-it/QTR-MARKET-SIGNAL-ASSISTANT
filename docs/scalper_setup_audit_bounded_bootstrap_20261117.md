# Scalper Shadow: bounded Setup Pilot audit provider

Branch: `fix/scalper-setup-audit-bounded-bootstrap`.
Parent: `9ee6b4d71f9bc18aaa085d9d2c8740edbf4a0bef` (bounded journal plus
explicit legacy quarantine).

## Confirmed allocation mechanism

The previous provider read the entire file into one `bytes` value, then called
`(pending + data).split(b"\n")`. Splitting retains approximately another file's
worth of byte strings plus list/object overhead while the original file-sized
allocation is still alive. A nonempty pending prefix can require another large
concatenation allocation. Bootstrap, append refresh and the open-time
replacement/truncation branch all used unbounded `read()`.

This mechanism is consistent with the incident's approximately 7.3 GiB RSS for
a roughly 3.6 GB source. It was independently reproduced safely on a smaller
synthetic file: 33,530,994 input bytes caused 67,186,251 traced peak allocation
bytes with the committed old implementation, versus 308,540 with the fix.
The old implementation was never run against the full VPS file.

## Design and offset contract

All bootstrap, rebuild and append paths share a reader using reads of at most
64 KiB. Each operation captures the opened regular file's size with `fstat`
and reads through that boundary; later appends are picked up on the next
refresh. This prevents a busy producer from extending one refresh indefinitely.

The compact cache still holds only the newest `VerifiedSetupRecord` per symbol,
using `observed_at >= current.observed_at`, including the existing file-order
tie behavior. Parsing, record validation, freshness, source filtering, target
generation, scoring and trading behavior are unchanged.

`_offset` counts bytes already consumed from the file, **including** a deferred
incomplete final line. It is not the last successfully parsed-record boundary.
`_pending` holds only that line's bytes. Reads resume at `_offset`, not before
the pending bytes; subsequent fragments are joined once, parsed only when LF
arrives, and do not duplicate input accounting. UTF-8 decoding happens only
after line completion, so a multibyte codepoint can straddle arbitrary chunks
or refreshes. CRLF and empty lines preserve prior behavior.

Lines over 1 MiB (excluding LF, including optional CR) are rejected. Overflow
increments `malformed_lines` once for that physical line, clears pending bytes,
and discards through its next LF, even across refreshes. A single warning per
provider instance reports the limit; further oversized lines remain counted.
No oversized line or arbitrary unfinished fragment can grow pending memory
without bound. This explicit limit is the only intentional input-acceptance
restriction; legitimate historical rows are checked against it in full-file
validation. Ordinary incomplete lines remain unparsed and are not counted as
malformed merely because they are incomplete.

Retained memory is O(cached symbols + maximum line size + read chunk), independent
of historical bytes and append batch size. This is not a fixed memory guarantee
for an unlimited number of distinct symbols or arbitrary parser object contents.

## Identity, rebuild and metrics

Existing device/inode identity and file-size checks are preserved. Detected
replacement or truncation clears cache, pending bytes and oversized-line discard
state, then uses the same bounded bootstrap. A replacement/truncation between
path `stat` and open is checked again with `fstat` and takes the bounded rebuild
path. No repair, rename, rotation, source write or persistent checkpoint was
added. As before, same-inode rewrites that fully regrow before observation are
outside the append-only/size-shrink detection contract.

Metrics retain their existing fields: bootstrap scans, incremental reads,
bytes read, cached symbols, malformed lines, reset/rotation count. A nonempty
append refresh counts as one incremental read regardless of chunk count. Bytes
count only bytes read from the source, never pending-buffer reprocessing. An
unchanged source causes no extra reads. Malformed and reset counters remain
cumulative; rebuilds do not reset statistics.

## Changed files and isolation

- `price_context_adapter.py`: bounded reader/framer, oversized-line defense.
- `test_setup_provider_bounded.py`: focused regression coverage.
- `validate_setup_audit_provider.py`: read-only replay/RSS/reference validator.
- This implementation/acceptance report.

AST comparison against the parent confirms every top-level function/class
except `JsonlVerifiedSetupProvider` is unchanged, including
`VerifiedPriceContextAdapter`, `_parse_record_line`, `_valid_source`, target
validation and conversion helpers. Minor formatting is not a semantic change.
No settings, environment, service unit, scorer, runtime, risk, execution,
orchestrator, trade/decision journal, analytics or quarantine implementation
was changed.

## Local validation

- Provider regression file: 14 passed; original price-context file: 23 passed.
- Complete Scalper V2 tests: 548 passed.
- Full repository suite: 1,693 passed.
- Mypy: passed, 139 source files. Ruff: passed. Diff whitespace check: passed.

Coverage includes 5,000 out-of-order/tied records across seven symbols, exact
latest-record equivalence, byte-by-byte/7-byte/64-byte/64-KiB chunks, split UTF-8
and CRLF, incomplete-line completion, eight append cycles, malformed JSON and
UTF-8, oversized fragments across refreshes, warning/count deduplication,
bootstrap/replacement/truncation and stat/open races. Instrumented streams
reject unsized or oversized reads, `readlines` and line iteration on all reader
paths, including append and rebuild. The memory regression measures bootstrap
and replacement under tracemalloc and fails the original implementation.

| Synthetic file | Old peak traced allocation | Fixed peak traced allocation |
| ---: | ---: | ---: |
| 2,085,504 bytes | 4,257,331 bytes | 310,995 bytes |
| 33,530,994 bytes | 67,186,251 bytes | 308,540 bytes |

A separate 134,189,148-byte file bootstrapped in 0.63 seconds. Provider peak RSS
was 37.89 MiB; 30 unchanged immediate refresh cycles stayed at 37.89 MiB with
one bootstrap, no incremental reads and exactly 134,189,148 input bytes read.
The cached record matched an independent bounded `readline` reference replay.
These are local Windows measurements, not live service measurements.

The full preserved trade journal was rechecked: 166,560 rows, 22,586 unique
trades, CLOSED 12,235, EXPIRED 10,345, WAITING_ENTRY 5, OPEN 1; six quarantined.
Existing tests confirm zero legacy runtime restoration and unchanged metrics/
analytics aggregation. Full decision-history analytics replay from the preceding
quarantine task was not repeated here; its implementation is byte-for-byte
unchanged and its regression/equivalence tests pass in the full suite.

Ignored `reports/` contains raw measurements and test logs; neither synthetic
data nor historical evidence is committed.

## Full VPS file validation

The initial read-only check found the service inactive/dead with NRestarts 0
and approximately 6.9 GiB MemAvailable. Audit source size was 3,810,428,340 bytes.
The candidate was executed only in an isolated stdin-fed Python process using
the VPS's existing imports, low CPU priority and a 512 MiB address-space limit.
It was not installed into the checkout, and no service or collector was started.
The replay includes six 30-second refresh intervals and an independent bounded
reference scan of the exact captured prefix. Results:

| Measurement | Result |
| --- | --- |
| Initial source size | 3,810,565,761 bytes |
| Captured/reference prefix | 3,810,705,231 bytes |
| Bootstrap duration | 51.61 seconds |
| Observed provider RSS | **35.10 MiB** |
| Six 30-second cycle RSS samples | All 35.10 MiB |
| Cached symbols | 425 |
| Complete physical lines | 2,216,096 |
| Maximum physical line | 2,323 bytes |
| Malformed lines / resets | 0 / 0 |
| Bootstrap / incremental reads | 1 / 1 |
| Pending final bytes | 0 |
| Reference cache equality | Exact match |

The audit producer appended 139,470 bytes during bootstrap/initial snapshot.
The provider consumed those bytes exactly once in an incremental refresh, then
all six timed cycles had unchanged byte/read counters. Consequently source
metadata was **not unchanged** across the whole run; this was producer growth,
not a claim that the source was frozen. All provider/reference opens were
read-only, and no source write/rename/repair was performed. The reference hashes
the exact captured prefix, SHA-256:
`647073206ac78d9bff5c1d567ff5b08f61942d0637c8e85bd691a95d16ed5d47`.

Linux's raw `ru_maxrss` value was 34.87 MiB while VmRSS samples were 35.10 MiB;
the larger observed value is reported here. The validator now reports at least
the current observed VmRSS as its peak to avoid understating this discrepancy.
The measured provider's source SHA-256 (UTF-8 text) is
`824bc5c7da1d0faf1349b4e603acf8a2f1553abfca8548bb3b8a7ffa2ab61532`,
matching the committed candidate. The stdin validator's reporting adjustment
does not change provider code or the recorded raw measurement.

The closing read-only VPS check found deployed HEAD
`5707bb9d758f6ce634d308aaa99f8d895e891707`, service still inactive/dead,
NRestarts 0, ENABLED true, SHADOW_MODE true and LIVE_ENABLED false. The actual
VPS trade journal still contains the same 166,560 rows, 22,586 trades and six
historical unfinished IDs/stages. Its SHA-256 remains exactly
`0b68f2859bb1ba6523c05ef2980aeeb2c99a8ed4ebc53f8fc1840b2b54f0f589`.
No VPS source, configuration or evidence mutation was performed.

## Controlled deployment/start gate (not performed)

Port only the reviewed delta onto the approved VPS-specific line; preserve the
existing bounded journal, quarantine and operational telemetry. Do not replace
the entire VPS checkout with this branch without a separate deployment review.
Verify the service is stopped, safety flags remain LIVE_ENABLED=false and
SHADOW_MODE=true, evidence files are preserved, sufficient headroom is available,
and the reviewed candidate commit is pinned.

After a separately approved port, the following command is an offline provider
check only; it does not start a service or write source history:

```bash
PYTHONPATH=src /path/to/approved/venv/bin/python -B \
  scripts/validate_setup_audit_provider.py \
  --journal /opt/qtr/scanner/data/qtr_setup_telegram_pilot_audit.jsonl \
  --cycles 6 --interval-seconds 30 --reference --limit-mib 512
```

Require matching reference cache, exact byte accounting, no resets/re-reads on
an unchanged file, bounded RSS and six legacy trades still quarantined. Only
then consider a separately authorized controlled service start. Monitor total
service RSS, NRestarts, context availability, worker/queue/write failures and
history counters. Provider replay success does not certify every other live
allocation path or constitute a live-service acceptance result.

Verdict: **SAFE_FOR_CONTROLLED_VPS_START**, after the reviewed VPS port and
preflight. No deployment/start authorization is inferred from this verdict.
