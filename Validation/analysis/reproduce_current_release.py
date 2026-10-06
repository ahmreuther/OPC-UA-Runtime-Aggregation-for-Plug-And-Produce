#!/usr/bin/env python3
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0
# Copyright (c) 2026 Adrian Reuther
"""Recalculate the public 2026-10-06 subset using Python's standard library.

This performs numerical checks of the included observations, not a rerun of
the experiments or of the excluded full semantic/lifecycle validation.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import statistics
import sys

ROOT = Path(__file__).resolve().parents[2]
COHORT = "Validation/current_20261006/cohort.json"
RESULTS = "results/current_20261006"
sys.dont_write_bytecode = True


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def read_csv(path):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def require(condition, message):
    if not condition:
        raise ValueError(message)


def close(actual, expected, label, abs_tol=1e-9):
    require(math.isclose(actual, expected, rel_tol=1e-10, abs_tol=abs_tol),
            f"Numerical mismatch: {label}: {actual} != {expected}")


def stats(values):
    return {"n": len(values), "mean": statistics.fmean(values),
            "sample_std_dev": statistics.stdev(values) if len(values) > 1 else None,
            "minimum": min(values), "maximum": max(values)}


def epoch(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def check_hashes(root):
    manifest = read_json(root / RESULTS / "export_manifest.json")
    for item in manifest["files"]:
        path = root / item["path"]
        require(path.is_file(), f"Missing exported file: {item['path']}")
        require(path.stat().st_size == item["size_bytes"], f"Size differs: {item['path']}")
        require(hashlib.sha256(path.read_bytes()).hexdigest() == item["sha256"],
                f"Checksum differs: {item['path']}")
    return len(manifest["files"])


def e01(root, cohort):
    reports = []
    require(len(cohort["E01"]) == 3, "E01 requires three declared runs")
    for entry in cohort["E01"]:
        report = read_json(root / entry["summary"])
        rows = read_csv(root / entry["domain_table"])
        totals = {key: sum(int(row[key]) for row in rows if row["Domain"] != "Total")
                  for key in ("Inst.", "Rules", "Targets")}
        total_row = next(row for row in rows if row["Domain"] == "Total")
        require(all(totals[key] == int(total_row[key]) for key in totals), "E01 domain totals differ")
        require(totals["Inst."] == report["counts"]["sources"], "E01 source count differs")
        require(totals["Rules"] == report["counts"]["rules"], "E01 rule count differs")
        require(report["passed"] and all(report["semantic_criteria"].values()), "E01 recorded audit rejected")
        reports.append({"run_id": report["run_id"], "domain_totals": totals,
                        "recorded_node_count": report["counts"]["nodes"],
                        "recorded_semantic_checks_passed": len(report["semantic_criteria"])})
    return {"runs": reports, "scope": "Domain-table sums recalculated. Semantic conclusions are retained audit outputs; full source models, rules and address spaces are not included."}


def e03(root, cohort):
    path = root / "Validation/analysis/current_20261006/read_latency.py"
    spec = importlib.util.spec_from_file_location("current_read_latency", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    results = []
    require(len(cohort["E03"]) == 3, "E03 requires three declared runs")
    for entry in cohort["E03"]:
        manifest, rows, _ = module.read_inputs((root / entry["manifest"]).parent)
        # Check timing identities and pairing before calling the preserved current formulas.
        pairs = Counter((int(row["pair_index"]), row["path"]) for row in rows)
        require(all(count == 1 for count in pairs.values()), "Duplicate E03 path within a pair")
        require(all(int(row["end_monotonic_ns"]) - int(row["start_monotonic_ns"]) == int(row["latency_ns"]) for row in rows), "E03 duration identity differs")
        result = module.analyze_rows(manifest, rows)
        require(result["analysis_status"] == "complete", "E03 incomplete result")
        require(result["paired_overhead"]["n"] == int(manifest["protocol"]["samples_per_path"]), "E03 incomplete pairing")
        results.append(result)
    return {"runs": results, "excluded_attempts": cohort["E03_excluded_attempts"],
            "scope": "Three independent client sessions with fresh aggregation and discovery processes on the same physical setup. The interrupted earlier acquisition is excluded as a whole, not a successful subset."}


def e08(root, cohort):
    reports = []
    require(len(cohort["E08"]) == 3, "E08 requires three declared runs")
    for entry in cohort["E08"]:
        report = read_json(root / entry["summary"])
        reads = read_json(root / entry["healthy_reads"])
        failures = sum(not row["ok"] for row in reads)
        healthy = sum(row["kind"] == "healthy" and row["outcome"] == "completed"
                      for row in report["terminal_results"])
        require(len(reads) == report["read_count"] and failures == report["read_failures"], "E08 Read counts differ")
        require(healthy == 5 and failures == 0, "E08 outcome differs")
        process = report["process_test"]
        require(process["returncode"] == 70 and process["supervisor"]["following_source_rejected"], "E08 process test differs")
        reports.append({"run_id": entry["run_id"], "healthy_integrations_completed": healthy,
                        "successful_reference_reads": len(reads) - failures, "failed_reference_reads": failures,
                        "separate_process_test_exit_code": process["returncode"],
                        "separate_process_test_rejected_further_integration": process["supervisor"]["following_source_rejected"]})
    require(sum(r["successful_reference_reads"] for r in reports) == 181, "E08 total differs")
    return {"runs": reports, "successful_reference_reads_total": sum(r["successful_reference_reads"] for r in reports),
            "scope": "Read observations recalculated; integration and process-test outcomes are recorded observations. Local synthetic sources only. Process termination requires external restart and is not continuous availability."}


def e09(root, cohort):
    entries = cohort["E09"]
    require(Counter(r["requested_server_count"] for r in entries) == Counter({n: 3 for n in (1, 5, 10, 25, 50, 75, 100)}), "E09 membership differs")
    attempts = read_json(root / cohort["E09_attempt_register"])
    require(len(attempts["attempts"]) == 37, "E09 attempt count differs")
    selected = [r for r in attempts["attempts"] if r["selected_for_numeric_analysis"]]
    require(len(selected) == 21, "E09 selected-attempt count differs")
    key = lambda r: (r["matrix_run_id"], int(r.get("requested_server_count", r.get("server_count"))), int(r["repetition"]))
    require({key(r) for r in selected} == {key(r) for r in entries}, "E09 attempt membership differs")
    timings = read_csv(root / cohort["E09_run_timing"])
    timing_by_key = {key(row): row for row in timings}
    require(len(timings) == len(timing_by_key) == 21 and set(timing_by_key) == {key(r) for r in entries}, "E09 timing membership differs")
    runs = []
    groups = defaultdict(list)
    for entry in entries:
        rows = read_csv(root / entry["resource_csv"])
        require(len(rows) == entry["coverage"]["sample_count"], "E09 resource sample count differs")
        origin = float(entry["coverage"]["network_bin_origin_epoch"])
        start = epoch(entry["acquisition_to_terminal"]["window_start_utc"])
        end = epoch(entry["acquisition_to_terminal"]["window_end_utc"])
        numeric = [{k: float(row[k]) for k in ("sample_interval_seconds_actual", "elapsed_seconds", "cpu_cores_in_use", "total_memory_mb", "net_in_mbps")} for row in rows]
        previous = 0.0
        whole = []
        for row in numeric:
            interval = row["sample_interval_seconds_actual"]
            require(all(math.isfinite(v) and v >= 0 for v in row.values()) and interval > 0, "Invalid E09 number")
            require(row["elapsed_seconds"] > previous, "E09 elapsed times not monotone")
            require(abs(row["elapsed_seconds"] - previous - interval) < 0.000003, "E09 interval identity differs")
            if origin + previous >= start and origin + row["elapsed_seconds"] <= end:
                whole.append(row)
            previous = row["elapsed_seconds"]
        duration = sum(row["sample_interval_seconds_actual"] for row in numeric)
        whole_duration = sum(row["sample_interval_seconds_actual"] for row in whole)
        require(whole_duration > 0 and duration >= 2000, "E09 incomplete resource window")
        peak = max(row["total_memory_mb"] for row in numeric)
        # Historical column net_in_mbps contains MiB/s, not megabits/s.
        network = sum(row["net_in_mbps"] * row["sample_interval_seconds_actual"] for row in numeric)
        cpu = sum(row["cpu_cores_in_use"] * row["sample_interval_seconds_actual"] for row in whole) / whole_duration
        close(peak, entry["expected_full_recording"]["peak_rss_MiB"], "E09 full-recording RSS")
        close(network, entry["expected_full_recording"]["network_MiB"], "E09 full-recording network")
        close(cpu, entry["acquisition_to_terminal"]["cpu_equivalent_cores_weighted_mean"], "E09 whole-interval CPU")
        timing = timing_by_key[key(entry)]
        result = {"run_id": entry["run_id"], "server_count": entry["requested_server_count"],
                  "repetition": entry["repetition"], "samples": len(rows),
                  "full_recording_seconds": duration, "full_recording_peak_rss_MiB": peak,
                  "full_recording_network_MiB": network,
                  "acquisition_to_terminal_seconds": end - start,
                  "acquisition_to_terminal_cpu_equivalent_cores_mean": cpu,
                  "cpu_covered_whole_interval_seconds": whole_duration,
                  "reported_packet_exact_acquisition_to_terminal_network_MiB": entry["acquisition_to_terminal"]["packet_exact_network_MiB"]}
        for field in ("rule_generation_seconds", "aggregation_seconds", "other_processing_seconds", "total_processing_seconds", "rule_commit_seconds", "first_onboarding_to_last_completed_pi_wall_seconds", "acquisition_start_to_last_completed_pi_wall_seconds"):
            result[field] = float(timing[field])
        close(result["rule_generation_seconds"] + result["aggregation_seconds"] + result["other_processing_seconds"], result["total_processing_seconds"], "E09 phase sum")
        require(int(timing["timed_sources"]) == entry["requested_server_count"], "E09 timed-source count differs")
        close(result["acquisition_to_terminal_seconds"], result["acquisition_start_to_last_completed_pi_wall_seconds"], "E09 timing window rounded to microseconds", abs_tol=0.000001)
        runs.append(result)
        groups[entry["requested_server_count"]].append(result)
    fields = [name for name, value in runs[0].items() if isinstance(value, (int, float)) and name not in ("server_count", "repetition", "samples")]
    grouped = [{"server_count": n, "repetitions": len(items),
                "metrics": {field: stats([item[field] for item in items]) for field in fields}}
               for n, items in sorted(groups.items())]
    return {"documented_attempts": 37, "analyzed_complete_runs": 21, "other_documented_attempts": 16,
            "completed_sources_in_selected_runs": sum(r["server_count"] for r in runs),
            "runs": runs, "by_source_count": grouped,
            "methods": {"standard_deviation": "Sample standard deviation across three separate runs, n-1 denominator.",
                        "CPU": "Combined aggregator and discovery-service CPU-time deltas per actual sampling interval. Acquisition-to-terminal mean weights only complete mapped intervals within that window.",
                        "memory": "Peak combined aggregator/discovery-service RSS over each complete unsmoothed recording, in MiB. Source-host memory is excluded.",
                        "network": "Full-recording integral of the published interval rate. Legacy net_in_mbps is in MiB/s. Packet-exact acquisition-to-terminal network values are reported audited outputs and cannot be rederived without the excluded captures.",
                        "timing": "Phase totals are recalculated across the published per-run timing table. Reconstructing individual phases from private original logs is outside this subset; rule commit is included in other processing, not an additional summand.",
                        "display": "Paper resource graphs show a mean of three runs, a 750 s view and a centered 31 s display mean. This CLI uses complete unsmoothed recordings of at least 2000 s; display cropping/smoothing do not enter the numerical summaries."}}


def e10(root, cohort):
    scenarios = {}
    for scenario, expected_count, accepted in (("B100", 100, 2), ("F24", 24, 3)):
        runs = [read_json(root / item["summary"]) for item in cohort["E10"] if item["scenario"] == scenario]
        require(len(runs) == 3 and all(r["completed_unique_identities"] == expected_count for r in runs), "E10 integration counts differ")
        require(sum(r["full_acceptance"] for r in runs) == accepted, "E10 acceptance counts differ")
        scenarios[scenario] = {"trials": len(runs), "all_sources_integrated_trials": len(runs),
                               "sources_per_trial": expected_count,
                               "fully_accepted_trials": sum(r["full_acceptance"] for r in runs), "runs": runs}
    return {**scenarios, "scope": "Counts summarized from projected audit reports. The full original validator requires excluded runtime archives. B100 trial 3 integrated all 100 sources but remained rejected after one discovery failure; offline semantic validation did not finish. F24 injected one controlled Browse failure, not expiry of the outer deadline."}


def reproduce(root):
    checked = check_hashes(root)
    cohort = read_json(root / COHORT)
    return {"schema": "ojies.public-current-reproduction/v1", "as_of": "2026-10-06",
            "input_hashes_verified": checked, "E01": e01(root, cohort),
            "E03": e03(root, cohort), "E08": e08(root, cohort),
            "E09": e09(root, cohort), "E10": e10(root, cohort),
            "limitations": cohort["limitations"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT, help="Public checkout root")
    parser.add_argument("--output-dir", type=Path, help="Default: results/current_20261006/reproduced")
    parser.add_argument("--verify-only", action="store_true", help="Compute and check without writing files")
    args = parser.parse_args()
    root = args.root.resolve()
    summary = reproduce(root)
    if not args.verify_only:
        output = args.output_dir.resolve() if args.output_dir else root / RESULTS / "reproduced"
        output.mkdir(parents=True, exist_ok=True)
        (output / "summary.json").write_bytes((json.dumps(summary, indent=2, ensure_ascii=False) + "\n").encode("utf-8"))
    print(json.dumps({"status": "passed", "input_hashes_verified": summary["input_hashes_verified"],
                      "E03_complete_sessions": len(summary["E03"]["runs"]),
                      "E08_successful_reference_reads": summary["E08"]["successful_reference_reads_total"],
                      "E09_complete_runs_of_documented_attempts": "21/37",
                      "E10_B100_fully_accepted": "2/3", "E10_B100_all_100_integrated": "3/3",
                      "E10_F24_fully_accepted": "3/3"}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
