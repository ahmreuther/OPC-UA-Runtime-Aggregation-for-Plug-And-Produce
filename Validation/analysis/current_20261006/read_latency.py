# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0
# Copyright (c) 2026 Adrian Reuther
#!/usr/bin/env python3
"""Analyze an immutable E03 Read-latency run and derive Table 4 values."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
from pathlib import Path
from typing import Any, Iterable


SCRIPT_PATH = Path(__file__).resolve()
PERFORMANCE_ROOT = SCRIPT_PATH.parents[2] / "current_20261006" / "E03"
DEFAULT_DERIVED_ROOT = SCRIPT_PATH.parents[3] / "results" / "current_20261006" / "read_latency"
PATH_LABELS = {
    "direct": "Client $\\rightarrow$ Source Server",
    "aggregated": "Client $\\rightarrow$ AS $\\rightarrow$ Source Server",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def percentile_linear(values: Iterable[float], percentile: float) -> float:
    """NumPy-compatible linear percentile without a NumPy dependency."""
    ordered = sorted(float(value) for value in values)
    if not ordered:
        raise ValueError("Cannot calculate a percentile for an empty sample")
    if not 0.0 <= percentile <= 100.0:
        raise ValueError("Percentile must be between 0 and 100")
    position = (len(ordered) - 1) * percentile / 100.0
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def descriptive_statistics(latencies_ns: list[int]) -> dict[str, float | int]:
    if len(latencies_ns) < 2:
        raise ValueError("At least two successful measurements are required")
    milliseconds = [value / 1_000_000.0 for value in latencies_ns]
    return {
        "n": len(milliseconds),
        "mean_ms": statistics.fmean(milliseconds),
        "median_ms": statistics.median(milliseconds),
        "sample_std_dev_ms": statistics.stdev(milliseconds),
        "p90_ms": percentile_linear(milliseconds, 90.0),
        "p99_ms": percentile_linear(milliseconds, 99.0),
        "min_ms": min(milliseconds),
        "max_ms": max(milliseconds),
    }


def read_inputs(run_dir: Path) -> tuple[dict[str, Any], list[dict[str, str]], Path]:
    manifest_path = run_dir / "manifest.json"
    measurements_path = run_dir / "measurements.csv"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Missing E03 manifest: {manifest_path}")
    if not measurements_path.is_file():
        raise FileNotFoundError(f"Missing E03 measurements: {measurements_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema") != "ojies.e03-read-latency-run/v1":
        raise ValueError(f"Unsupported or missing E03 schema in {manifest_path}")
    with measurements_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    return manifest, rows, measurements_path


def analyze_rows(
    manifest: dict[str, Any],
    rows: list[dict[str, str]],
    allow_incomplete: bool = False,
) -> dict[str, Any]:
    expected = int(manifest["protocol"]["samples_per_path"])
    rows_by_path: dict[str, list[dict[str, str]]] = {"direct": [], "aggregated": []}
    invalid_rows: list[dict[str, str]] = []
    by_pair: dict[int, dict[str, dict[str, str]]] = {}

    for row in rows:
        path = row.get("path", "")
        if path not in rows_by_path:
            raise ValueError(f"Unexpected path label in measurements: {path!r}")
        rows_by_path[path].append(row)
        by_pair.setdefault(int(row["pair_index"]), {})[path] = row
        if row.get("status_good") != "true" or row.get("error_type"):
            invalid_rows.append(row)

    count_mismatches = {
        path: len(path_rows)
        for path, path_rows in rows_by_path.items()
        if len(path_rows) != expected
    }
    if not allow_incomplete and (invalid_rows or count_mismatches):
        raise ValueError(
            "E03 run is incomplete: "
            f"invalid_reads={len(invalid_rows)}, count_mismatches={count_mismatches}"
        )

    valid_by_path = {
        path: [
            row
            for row in path_rows
            if row.get("status_good") == "true" and not row.get("error_type")
        ]
        for path, path_rows in rows_by_path.items()
    }
    per_path = {
        path: descriptive_statistics([int(row["latency_ns"]) for row in path_rows])
        for path, path_rows in valid_by_path.items()
    }

    paired_delta_ns: list[int] = []
    value_mismatches = 0
    for pair in by_pair.values():
        if set(pair) != {"direct", "aggregated"}:
            continue
        if not all(
            row.get("status_good") == "true" and not row.get("error_type")
            for row in pair.values()
        ):
            continue
        paired_delta_ns.append(
            int(pair["aggregated"]["latency_ns"]) - int(pair["direct"]["latency_ns"])
        )
        if pair["aggregated"].get("value_sha256") != pair["direct"].get("value_sha256"):
            value_mismatches += 1

    delta = descriptive_statistics(paired_delta_ns)
    return {
        "schema": "ojies.e03-read-latency-analysis/v1",
        "experiment_id": "E03",
        "claim_id": "C04",
        "run_id": manifest["run_id"],
        "input_scientific_validation_status": manifest.get(
            "scientific_validation_status", "not_assessed"
        ),
        "analysis_status": (
            "complete"
            if not invalid_rows and not count_mismatches and value_mismatches == 0
            else "diagnostic_incomplete"
        ),
        "methods": {
            "latency_unit": "milliseconds",
            "standard_deviation": "sample standard deviation (n-1 denominator)",
            "percentile": "linear interpolation at rank (n-1)*p, compatible with numpy method='linear'",
            "delta": "aggregated latency minus direct latency within each randomized pair",
        },
        "expected_samples_per_path": expected,
        "observed_rows_per_path": {
            path: len(path_rows) for path, path_rows in rows_by_path.items()
        },
        "valid_rows_per_path": {
            path: len(path_rows) for path, path_rows in valid_by_path.items()
        },
        "invalid_read_count": len(invalid_rows),
        "paired_value_mismatches": value_mismatches,
        "per_path": per_path,
        "paired_overhead": delta,
    }


def write_outputs(
    summary: dict[str, Any],
    output_dir: Path,
    measurements_path: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=False)
    summary["input_measurements_sha256"] = sha256_file(measurements_path)
    (output_dir / "table4_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )

    columns = ["path", "n", "mean_ms", "median_ms", "sample_std_dev_ms", "p90_ms", "p99_ms"]
    with (output_dir / "table4_summary.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for path in ("direct", "aggregated"):
            writer.writerow({"path": path, **{key: summary["per_path"][path][key] for key in columns[1:]}})

    tex_rows = []
    for path in ("direct", "aggregated"):
        stats = summary["per_path"][path]
        tex_rows.append(
            f"{PATH_LABELS[path]} & {stats['mean_ms']:.2f} & {stats['median_ms']:.2f} & "
            f"{stats['sample_std_dev_ms']:.2f} & {stats['p90_ms']:.2f} & "
            f"{stats['p99_ms']:.2f} \\\\"
        )
    (output_dir / "table4_rows.tex").write_text("\n".join(tex_rows) + "\n", encoding="utf-8")


def print_summary(summary: dict[str, Any]) -> None:
    print("E03 Table 4 statistics (ms)")
    print("path          n       mean    median   std.dev      P90      P99")
    for path in ("direct", "aggregated"):
        stats = summary["per_path"][path]
        print(
            f"{path:<11} {stats['n']:>6} "
            f"{stats['mean_ms']:>9.3f} {stats['median_ms']:>9.3f} "
            f"{stats['sample_std_dev_ms']:>9.3f} "
            f"{stats['p90_ms']:>8.3f} {stats['p99_ms']:>8.3f}"
        )
    overhead = summary["paired_overhead"]
    print(
        "paired overhead: "
        f"mean={overhead['mean_ms']:.3f} ms, "
        f"median={overhead['median_ms']:.3f} ms, n={overhead['n']}"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Derive Table 4 statistics from one E03 raw run.")
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_DERIVED_ROOT)
    parser.add_argument(
        "--allow-incomplete",
        action="store_true",
        help="Produce diagnostic statistics from successful rows of an incomplete run.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    run_dir = args.run_dir.resolve()
    manifest, rows, measurements_path = read_inputs(run_dir)
    summary = analyze_rows(manifest, rows, allow_incomplete=args.allow_incomplete)
    output_dir = args.output_root.resolve() / manifest["run_id"]
    write_outputs(summary, output_dir, measurements_path)
    print_summary(summary)
    print(f"Derived E03 artifacts: {output_dir}")
    return 0 if summary["analysis_status"] == "complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
