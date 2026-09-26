"""Read-only Watchdog Evidence Baseline V1 command-line entry point."""

from __future__ import annotations

import argparse
from pathlib import Path

from market_signal_assistant.watchdog.evidence_baseline import (
    DatasetProvenance,
    analyze_dataset,
    render_report,
    render_rows_jsonl,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows-output", type=Path)
    parser.add_argument("--verify-sqlite", action="store_true")
    args = parser.parse_args()
    provenance = DatasetProvenance.from_manifest(args.manifest)
    output = args.output.resolve()
    if output.is_relative_to(provenance.path):
        parser.error("Report must be written outside the immutable source dataset.")
    if output == args.manifest.resolve():
        parser.error("Report must not overwrite the provenance manifest.")
    rows_output = (
        args.rows_output.resolve()
        if args.rows_output is not None
        else output.with_suffix(".rows.jsonl")
    )
    if rows_output.is_relative_to(provenance.path):
        parser.error("Analytical rows must be written outside the source dataset.")
    if rows_output in {output, args.manifest.resolve()}:
        parser.error("Analytical rows must have a separate output path.")
    analysis = analyze_dataset(provenance, verify_sqlite=args.verify_sqlite)
    report = render_report(analysis)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(report, encoding="utf-8")
    if analysis.valid:
        rows_output.parent.mkdir(parents=True, exist_ok=True)
        rows_output.write_text(render_rows_jsonl(analysis), encoding="utf-8")
    print(f"{output}: {'PASS' if analysis.valid else 'FAIL'}")
    return 0 if analysis.valid else 2


if __name__ == "__main__":
    raise SystemExit(main())
