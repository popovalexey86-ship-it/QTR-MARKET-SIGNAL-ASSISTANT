"""Read-only provider replay; never starts collectors/services or writes history."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from dataclasses import asdict
from pathlib import Path


def memory_mib() -> tuple[float, float]:
    if sys.platform == "win32":
        from validate_scalper_bounded_memory import memory_mib as windows_memory

        return windows_memory()
    import resource

    rss = next(
        float(line.split()[1]) / 1024
        for line in Path("/proc/self/status").read_text().splitlines()
        if line.startswith("VmRSS:")
    )
    # Linux's sampled RSS/accounting counters can briefly disagree. Never
    # report a peak lower than the current VmRSS observation.
    return rss, max(rss, resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--cycles", type=int, default=20)
    parser.add_argument("--interval-seconds", type=float, default=0)
    parser.add_argument("--reference", action="store_true")
    parser.add_argument("--limit-mib", type=int, default=512)
    args = parser.parse_args()
    if args.cycles < 0 or args.interval_seconds < 0 or args.limit_mib < 1:
        parser.error("cycles/interval must be nonnegative and limit positive")
    if sys.platform == "linux":
        import resource

        limit = args.limit_mib * 1024 * 1024
        _, hard = resource.getrlimit(resource.RLIMIT_AS)
        if hard != resource.RLIM_INFINITY:
            limit = min(limit, hard)
        resource.setrlimit(resource.RLIMIT_AS, (limit, hard))
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
    from market_signal_assistant.qtr_micro_scalper.price_context_adapter import (
        _SETUP_MAX_LINE_BYTES,
        JsonlVerifiedSetupProvider,
        _parse_record_line,
    )

    before = args.journal.stat()
    start = time.monotonic()
    provider = JsonlVerifiedSetupProvider(args.journal)
    boot_seconds = time.monotonic() - start
    records = provider.latest_records()
    captured_offset = provider._offset
    bootstrap_metrics = provider.metrics
    bootstrap_memory = memory_mib()
    samples = []
    for _ in range(args.cycles):
        if args.interval_seconds:
            time.sleep(args.interval_seconds)
        provider.latest_records()
        samples.append(memory_mib()[0])
    cycles_metrics = provider.metrics
    candidate_memory = memory_mib()
    reference_matches = None
    prefix_sha256 = None
    max_line_bytes = 0
    physical_lines = 0
    if args.reference:
        # Independent bounded readline replay of the exact captured prefix.
        # Hash source bytes in the same pass; never repair a partial final line.
        expected = {}
        digest = hashlib.sha256()
        offset = 0
        discarding = False
        line_bytes = 0
        with args.journal.open("rb") as stream:
            while offset < captured_offset:
                raw = stream.readline(
                    min(_SETUP_MAX_LINE_BYTES + 2, captured_offset - offset)
                )
                if not raw:
                    raise RuntimeError("Audit truncated during reference replay")
                offset += len(raw)
                digest.update(raw)
                complete = raw.endswith(b"\n")
                line_bytes += len(raw) - int(complete)
                max_line_bytes = max(max_line_bytes, line_bytes)
                if discarding or line_bytes > _SETUP_MAX_LINE_BYTES:
                    discarding = not complete
                elif complete:
                    try:
                        row, _ = _parse_record_line(raw.decode("utf8"))
                    except UnicodeDecodeError:
                        row = None
                    if row is not None:
                        old = expected.get(row.symbol)
                        if old is None or row.observed_at >= old.observed_at:
                            expected[row.symbol] = row
                if complete:
                    physical_lines += 1
                    line_bytes = 0
        reference_matches = records == tuple(expected[s] for s in sorted(expected))
        if not reference_matches:
            raise RuntimeError("Provider cache differs from bounded reference replay")
        prefix_sha256 = digest.hexdigest()
    after = args.journal.stat()
    canonical = json.dumps([asdict(r) for r in records], sort_keys=True, default=str)
    print(
        json.dumps(
            {
                "status": "COMPLETE",
                "journal": str(args.journal.resolve()),
                "initial_bytes": before.st_size,
                "captured_offset": captured_offset,
                "final_bytes": after.st_size,
                "source_metadata_unchanged": (
                    before.st_dev,
                    before.st_ino,
                    before.st_size,
                    before.st_mtime_ns,
                )
                == (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns),
                "bootstrap_seconds": boot_seconds,
                "bootstrap_rss_mib": bootstrap_memory[0],
                "bootstrap_peak_rss_mib": bootstrap_memory[1],
                "provider_peak_rss_mib": candidate_memory[1],
                "cycle_rss_mib": samples,
                "bootstrap_metrics": asdict(bootstrap_metrics),
                "cycles_metrics": asdict(cycles_metrics),
                "pending_bytes": len(provider._pending),
                "reference_matches": reference_matches,
                "source_prefix_sha256": prefix_sha256,
                "max_physical_line_bytes": max_line_bytes,
                "complete_physical_lines": physical_lines,
                "cache_sha256": hashlib.sha256(canonical.encode()).hexdigest(),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
