# SPDX-FileCopyrightText: 2026 Adrian Reuther
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

"""Plot eventual onboarding outcomes and instrumented processing phases.

The inputs are the canonical performance-chain manifests selected through
``active_resource_measurements.json``.  Figure 9 uses the exact counters stored
in those manifests.  Figure 10 additionally requires ``OJIES_PHASE_TIMING=1``
for every selected run and fails closed when the phase records are absent.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

from measurement_selection import load_active_measurement_selection
from plot_paths import PROJECT_ROOT, VALIDATION_ROOT, output_dir


plt.rcParams.update(
    {
        "font.family": "serif",
        "font.serif": ["Times New Roman"] + plt.rcParams["font.serif"],
        "font.size": 9,
        "axes.labelsize": 9,
        "axes.titlesize": 9,
        "legend.fontsize": 8,
        "xtick.labelsize": 8,
        "ytick.labelsize": 8,
        "axes.linewidth": 0.6,
        "xtick.major.width": 0.6,
        "ytick.major.width": 0.6,
        "xtick.minor.width": 0.4,
        "ytick.minor.width": 0.4,
        "xtick.major.size": 3.0,
        "ytick.major.size": 3.0,
        "xtick.minor.size": 1.5,
        "ytick.minor.size": 1.5,
        "xtick.direction": "in",
        "ytick.direction": "in",
        "legend.framealpha": 0.9,
        "legend.edgecolor": "0.7",
        "legend.borderpad": 0.4,
        "legend.labelspacing": 0.3,
        "figure.figsize": (3.5, 2.625),
        "figure.dpi": 150,
        "savefig.dpi": 600,
        "savefig.bbox": "tight",
        "savefig.pad_inches": 0.02,
        "mathtext.fontset": "stix",
    }
)


DERIVED_ROOT = VALIDATION_ROOT / "performance" / "derived" / "onboarding"
FIGURE_9_NAME = "Fig6_8_Registered_vs_Aggregated.pdf"
# Keep the journal asset name stable until ChatGPT Work updates the manuscript
# wording.  The generated content is phase timing, not the legacy
# integration/configuration metric.
FIGURE_10_NAME = "Fig6_6_Integration_Configuration_Time.pdf"

PHASE_RE = re.compile(
    r"OJIES_PHASE_TIMING\s+server=(?P<server>\S+)\s+"
    r"phase=(?P<phase>\S+)\s+elapsed_ms=(?P<elapsed>[0-9]+(?:\.[0-9]+)?)"
)
REQUIRED_PHASES = (
    "main_rule_generation",
    "main_lower_aggregation_wait",
    "main_total",
)

OUTCOME_METRICS = (
    ("source_listening", "Listening", "0.70"),
    ("lds_registered", "LDS registered", "#0072B2"),
    ("aggregation_terminal", "Aggregated", "#E69F00"),
)


def _load_json(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8-sig") as handle:
            document = json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"Cannot read JSON document {path}: {error}") from error
    if not isinstance(document, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return document


def _resolve_project_path(value: str, field: str, manifest_path: Path) -> Path:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{manifest_path}: missing path field {field}")
    candidate = (PROJECT_ROOT / value).resolve()
    if candidate != PROJECT_ROOT and PROJECT_ROOT not in candidate.parents:
        raise ValueError(f"{manifest_path}: {field} escapes the project root")
    if not candidate.is_file():
        raise ValueError(f"{manifest_path}: {field} does not exist: {candidate}")
    return candidate


def load_selected_runs() -> tuple[list[int], list[dict], list[dict]]:
    selection = load_active_measurement_selection()
    server_counts = list(selection["server_counts"])
    platform_documents: list[dict] = []
    run_documents: list[dict] = []

    for platform in selection["resource_platforms"]:
        platform_id = platform["id"]
        repetitions = list(platform["resource_repetitions"])
        matrix_ids: set[str] = set()
        platform_runs: list[dict] = []

        for repetition in repetitions:
            for server_count in server_counts:
                manifest_path = (
                    platform["outcome_manifest_directory"]
                    / f"{repetition}n{server_count}.json"
                )
                document = _load_json(manifest_path)
                accepted_statuses = {
                    "complete",
                    "complete_without_configuration",
                }
                if document.get("status") not in accepted_statuses:
                    raise ValueError(f"Selected run is not complete: {manifest_path}")
                if document.get("promotion_status") != "completed":
                    raise ValueError(f"Selected run is not promoted: {manifest_path}")
                if int(document.get("requested_server_count", -1)) != server_count:
                    raise ValueError(
                        f"Server-count mismatch in selected run: {manifest_path}"
                    )
                if int(document.get("repetition", -1)) != repetition:
                    raise ValueError(
                        f"Repetition mismatch in selected run: {manifest_path}"
                    )
                recorded_platform = document.get(
                    "platform",
                    document.get("resource_platform"),
                )
                if recorded_platform != platform_id:
                    raise ValueError(
                        f"Platform mismatch in selected run: {manifest_path}"
                    )

                observed = document.get("observed_counts")
                if not isinstance(observed, dict):
                    raise ValueError(
                        f"Missing observed_counts in selected run: {manifest_path}"
                    )
                required_counters = (
                    "source_listening",
                    "lds_registered",
                    "aggregation_terminal",
                )
                if any(
                    not isinstance(observed.get(counter), int)
                    for counter in required_counters
                ):
                    raise ValueError(
                        f"Incomplete observed counters in selected run: {manifest_path}"
                    )

                matrix_run_id = document.get(
                    "matrix_run_id",
                    document.get("performance_matrix_run_id"),
                )
                if not isinstance(matrix_run_id, str) or not matrix_run_id:
                    raise ValueError(
                        f"Missing matrix_run_id in selected run: {manifest_path}"
                    )
                matrix_ids.add(matrix_run_id)

                run = {
                    "platform": platform_id,
                    "platform_label": platform["label"],
                    "matrix_run_id": matrix_run_id,
                    "server_count": server_count,
                    "repetition": repetition,
                    "source_listening": observed["source_listening"],
                    "lds_registered": observed["lds_registered"],
                    "aggregation_terminal": observed["aggregation_terminal"],
                    "manifest_path": manifest_path,
                    "manifest": document,
                }
                platform_runs.append(run)
                run_documents.append(run)

        if len(matrix_ids) != 1:
            raise ValueError(
                f"Selected {platform_id} runs span multiple matrices: "
                + ", ".join(sorted(matrix_ids))
            )
        platform_documents.append(
            {
                "id": platform_id,
                "label": platform["label"],
                "matrix_run_id": next(iter(matrix_ids)),
                "repetitions": repetitions,
                "runs": platform_runs,
            }
        )

    return server_counts, platform_documents, run_documents


def write_outcome_rows(runs: list[dict]) -> Path:
    DERIVED_ROOT.mkdir(parents=True, exist_ok=True)
    path = DERIVED_ROOT / "onboarding_outcomes.csv"
    fields = (
        "platform",
        "matrix_run_id",
        "server_count",
        "repetition",
        "source_listening",
        "lds_registered",
        "aggregation_terminal",
    )
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for run in runs:
            writer.writerow({field: run[field] for field in fields})
    return path


def _value_range(values: list[float]) -> tuple[float, float, float]:
    mean = float(np.mean(values))
    return mean, mean - float(np.min(values)), float(np.max(values)) - mean


def plot_outcomes(
    server_counts: list[int],
    platforms: list[dict],
) -> Path:
    row_count = len(platforms)
    fig, axes = plt.subplots(
        row_count,
        1,
        sharex=True,
        squeeze=False,
        figsize=(3.5, 2.625 if row_count == 1 else 2.15 * row_count),
    )
    x = np.arange(len(server_counts))
    bar_width = 0.24
    offsets = (-bar_width, 0.0, bar_width)

    for row_index, platform in enumerate(platforms):
        ax = axes[row_index, 0]
        grouped = defaultdict(list)
        for run in platform["runs"]:
            grouped[run["server_count"]].append(run)

        for metric_index, (field, label, color) in enumerate(OUTCOME_METRICS):
            means: list[float] = []
            low: list[float] = []
            high: list[float] = []
            for server_count in server_counts:
                values = [
                    float(run[field])
                    for run in grouped[server_count]
                ]
                mean, error_low, error_high = _value_range(values)
                means.append(mean)
                low.append(error_low)
                high.append(error_high)

            positions = x + offsets[metric_index]
            ax.bar(
                positions,
                means,
                width=bar_width,
                label=label,
                color=color,
                edgecolor="white",
                linewidth=0.4,
                zorder=3,
            )
            ax.errorbar(
                positions,
                means,
                yerr=[low, high],
                fmt="none",
                ecolor="black",
                elinewidth=0.6,
                capsize=1.8,
                capthick=0.6,
                zorder=4,
            )
            for position, value, upper_error in zip(positions, means, high):
                ax.text(
                    position,
                    value + upper_error + 1.5,
                    f"{value:g}",
                    ha="center",
                    va="bottom",
                    rotation=90,
                    fontsize=6.3,
                    color="0.25",
                    zorder=5,
                )

        ax.set_ylabel("Observed servers")
        ax.set_ylim(0, max(server_counts) * 1.17)
        ax.yaxis.grid(True, linestyle=":", linewidth=0.4, color="0.75")
        ax.set_axisbelow(True)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        # Keep the requested-server axis visible in front of bars that start
        # exactly at y=0.
        ax.spines["bottom"].set_zorder(10)
        ax.xaxis.set_zorder(11)
        ax.tick_params(axis="x", which="both", zorder=11)
        ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())
        ax.tick_params(which="minor", left=True, bottom=False)
        if row_count > 1:
            ax.text(
                0.98,
                0.96,
                platform["label"],
                transform=ax.transAxes,
                ha="right",
                va="top",
                fontsize=8,
            )
        if row_index == 0:
            ax.legend(
                loc="upper left",
                frameon=True,
                fancybox=False,
                ncol=1,
                handlelength=1.2,
                handletextpad=0.5,
            )

    axes[-1, 0].set_xticks(x)
    axes[-1, 0].set_xticklabels([str(value) for value in server_counts])
    axes[-1, 0].set_xlabel("Requested source servers")
    fig.tight_layout(pad=0.35)

    destination = output_dir() / FIGURE_9_NAME
    fig.savefig(destination)
    plt.close(fig)
    return destination


def _parse_phase_timings(log_path: Path) -> dict[str, dict[str, float]]:
    timings: dict[str, dict[str, float]] = defaultdict(dict)
    with log_path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            match = PHASE_RE.search(line)
            if not match:
                continue
            server = match.group("server")
            phase = match.group("phase")
            elapsed_seconds = float(match.group("elapsed")) / 1000.0
            if phase in timings[server]:
                raise ValueError(
                    f"Duplicate timing for server={server} phase={phase} "
                    f"in {log_path}"
                )
            timings[server][phase] = elapsed_seconds
    return timings


def collect_phase_rows(runs: list[dict]) -> list[dict]:
    rows: list[dict] = []
    missing_logs: list[str] = []

    for run in runs:
        manifest = run["manifest"]
        timing_log = manifest.get("aggregation_stdout")
        timing_log_field = "aggregation_stdout"
        if not timing_log:
            artifacts = manifest.get("artifacts")
            if isinstance(artifacts, dict):
                timing_log = artifacts.get("monitor_log")
                timing_log_field = "artifacts.monitor_log"
        log_path = _resolve_project_path(
            timing_log,
            timing_log_field,
            run["manifest_path"],
        )
        timings = _parse_phase_timings(log_path)
        complete_servers = [
            values
            for values in timings.values()
            if all(phase in values for phase in REQUIRED_PHASES)
        ]
        if len(complete_servers) != run["server_count"]:
            missing_logs.append(
                f"{run['platform']} {run['repetition']}n"
                f"{run['server_count']}: {len(complete_servers)}/"
                f"{run['server_count']} complete source timings"
            )
            continue

        rule_generation = sum(
            values["main_rule_generation"] for values in complete_servers
        )
        aggregation = sum(
            values["main_lower_aggregation_wait"] for values in complete_servers
        )
        total = sum(values["main_total"] for values in complete_servers)
        other = total - rule_generation - aggregation
        if other < -1e-6:
            raise ValueError(
                f"Phase sums exceed main_total in {log_path}: {other:.6f} s"
            )
        rows.append(
            {
                "platform": run["platform"],
                "platform_label": run["platform_label"],
                "matrix_run_id": run["matrix_run_id"],
                "server_count": run["server_count"],
                "repetition": run["repetition"],
                "timed_sources": len(complete_servers),
                "rule_generation_seconds": rule_generation,
                "aggregation_seconds": aggregation,
                "other_processing_seconds": max(other, 0.0),
                "total_processing_seconds": total,
                "mean_total_per_source_seconds": total / len(complete_servers),
            }
        )

    if missing_logs:
        details = "\n  ".join(missing_logs)
        raise ValueError(
            "Figure 10 requires OJIES_PHASE_TIMING=1 in every selected run. "
            "The current selection is incomplete:\n  "
            + details
        )
    return rows


def write_phase_rows(rows: list[dict]) -> Path:
    DERIVED_ROOT.mkdir(parents=True, exist_ok=True)
    path = DERIVED_ROOT / "processing_phase_times.csv"
    fields = (
        "platform",
        "matrix_run_id",
        "server_count",
        "repetition",
        "timed_sources",
        "rule_generation_seconds",
        "aggregation_seconds",
        "other_processing_seconds",
        "total_processing_seconds",
        "mean_total_per_source_seconds",
    )
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row[field] for field in fields})
    return path


def plot_phase_times(
    server_counts: list[int],
    platforms: list[dict],
    rows: list[dict],
) -> Path:
    row_count = len(platforms)
    fig, axes = plt.subplots(
        row_count,
        1,
        sharex=True,
        squeeze=False,
        figsize=(3.5, 2.75 if row_count == 1 else 2.3 * row_count),
    )
    x = np.arange(len(server_counts))
    colors = ("#0072B2", "#E69F00", "0.72")
    phase_fields = (
        ("rule_generation_seconds", "Rule generation"),
        ("aggregation_seconds", "Lower-server aggregation"),
        ("other_processing_seconds", "Other processing"),
    )

    for row_index, platform in enumerate(platforms):
        ax = axes[row_index, 0]
        ax_right = ax.twinx()
        platform_rows = [
            row for row in rows if row["platform"] == platform["id"]
        ]
        grouped = defaultdict(list)
        for row in platform_rows:
            grouped[row["server_count"]].append(row)

        bottom = np.zeros(len(server_counts))
        bars = []
        for (field, label), color in zip(phase_fields, colors):
            values = np.array(
                [
                    np.mean([row[field] for row in grouped[count]])
                    for count in server_counts
                ],
                dtype=float,
            )
            container = ax.bar(
                x,
                values,
                bottom=bottom,
                width=0.62,
                color=color,
                edgecolor="white",
                linewidth=0.4,
                label=label,
                zorder=3,
            )
            bars.append(container)
            bottom += values

        total_means = np.array(
            [
                np.mean(
                    [row["total_processing_seconds"] for row in grouped[count]]
                )
                for count in server_counts
            ],
            dtype=float,
        )
        total_low = total_means - np.array(
            [
                np.min(
                    [row["total_processing_seconds"] for row in grouped[count]]
                )
                for count in server_counts
            ],
            dtype=float,
        )
        total_high = np.array(
            [
                np.max(
                    [row["total_processing_seconds"] for row in grouped[count]]
                )
                for count in server_counts
            ],
            dtype=float,
        ) - total_means
        ax.errorbar(
            x,
            total_means,
            yerr=[total_low, total_high],
            fmt="none",
            ecolor="black",
            elinewidth=0.7,
            capsize=2.0,
            capthick=0.7,
            label="Total min/max",
            zorder=5,
        )

        per_source = np.array(
            [
                np.mean(
                    [
                        row["mean_total_per_source_seconds"]
                        for row in grouped[count]
                    ]
                )
                for count in server_counts
            ],
            dtype=float,
        )
        per_source_line = ax_right.plot(
            x,
            per_source,
            color="#009E73",
            marker="o",
            markersize=3.2,
            linewidth=0.9,
            linestyle="--",
            label="Mean total per source",
            zorder=6,
        )[0]

        ax.set_ylabel("Cumulative time per run (s)")
        ax_right.set_ylabel("Mean time per source (s)", color="#009E73")
        ax_right.tick_params(axis="y", colors="#009E73")
        ax_right.spines["right"].set_edgecolor("#009E73")
        ax.yaxis.grid(True, linestyle=":", linewidth=0.4, color="0.75")
        ax.set_axisbelow(True)
        ax.spines["top"].set_visible(False)
        ax_right.spines["top"].set_visible(False)
        ax.set_ylim(bottom=0)
        ax_right.set_ylim(bottom=0)
        ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())
        ax_right.yaxis.set_minor_locator(ticker.AutoMinorLocator())
        ax.tick_params(which="minor", left=True, bottom=False)
        ax_right.tick_params(which="minor", right=True, bottom=False)

        if row_count > 1:
            ax.text(
                0.98,
                0.96,
                platform["label"],
                transform=ax.transAxes,
                ha="right",
                va="top",
                fontsize=8,
            )
        if row_index == 0:
            handles = [container[0] for container in bars]
            handles.extend(
                [
                    ax.lines[0] if ax.lines else per_source_line,
                    per_source_line,
                ]
            )
            labels = [label for _, label in phase_fields]
            labels.extend(["Total min/max", "Mean total per source"])
            error_handle = ax.errorbar(
                [],
                [],
                yerr=[[], []],
                fmt="none",
                ecolor="black",
                elinewidth=0.7,
                capsize=2.0,
                capthick=0.7,
            )
            handles[-2] = error_handle
            ax.legend(
                handles,
                labels,
                loc="upper left",
                frameon=True,
                fancybox=False,
                ncol=1,
                handlelength=1.6,
                handletextpad=0.5,
            )

    axes[-1, 0].set_xticks(x)
    axes[-1, 0].set_xticklabels([str(value) for value in server_counts])
    axes[-1, 0].set_xlabel("Requested source servers")
    fig.tight_layout(pad=0.4)

    destination = output_dir() / FIGURE_10_NAME
    fig.savefig(destination)
    plt.close(fig)
    return destination


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--figure",
        choices=("9", "10", "all"),
        default="all",
        help="Select the paper figure(s) to regenerate.",
    )
    arguments = parser.parse_args()

    server_counts, platforms, runs = load_selected_runs()

    if arguments.figure in {"9", "all"}:
        outcome_csv = write_outcome_rows(runs)
        figure_9 = plot_outcomes(server_counts, platforms)
        print(f"Outcome table written: {outcome_csv}")
        print(f"Figure 9 written: {figure_9}")

    if arguments.figure in {"10", "all"}:
        phase_rows = collect_phase_rows(runs)
        phase_csv = write_phase_rows(phase_rows)
        figure_10 = plot_phase_times(server_counts, platforms, phase_rows)
        print(f"Phase table written: {phase_csv}")
        print(f"Figure 10 written: {figure_10}")


if __name__ == "__main__":
    main()
