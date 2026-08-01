# SPDX-FileCopyrightText: 2026 Adrian Reuther
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

"""Validated resource analysis for legacy or platform-separated measurements."""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from measurement_selection import load_active_measurement_selection
from plot_paths import VALIDATION_ROOT, output_dir


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


FILENAME_PATTERN = re.compile(r"(\d+)n(\d+)\.csv")
METRICS = {
    "cpu": {
        "ylabel": "Combined CPU Share (cores)",
        "column": "cpu_cores_in_use",
        "filename": "Fig6_2_CPU_Load.pdf",
        "legend_loc": "lower right",
    },
    "memory": {
        "ylabel": "Combined RSS (MiB)",
        "column": "total_memory_mb",
        "filename": "Fig6_3_Memory_Usage.pdf",
        "legend_loc": "upper left",
    },
    "network": {
        "ylabel": "Bidirectional TCP Traffic (MiB/s)",
        "column": "net_in_mbps",
        "filename": "Fig6_4_Network_Receive.pdf",
        "legend_loc": "upper right",
        "display_ylim": (0.0, 0.3),
    },
}
COLORS = [
    "#0072B2",
    "#E69F00",
    "#009E73",
    "#D55E00",
    "#CC79A7",
    "#56B4E9",
    "#000000",
]
LINESTYLES = [
    "-",
    "--",
    "-.",
    ":",
    (0, (5, 1.5)),
    (0, (3, 1, 1, 1)),
    (0, (1, 1)),
]
def plot_end_seconds() -> int:
    raw_value = os.environ.get("OJIES_RESOURCE_PLOT_SECONDS", "1800")
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(
            "OJIES_RESOURCE_PLOT_SECONDS must be a positive integer"
        ) from exc
    if value < 1:
        raise ValueError(
            "OJIES_RESOURCE_PLOT_SECONDS must be a positive integer"
        )
    return value


PLOT_END_SECONDS = plot_end_seconds()
MAX_INTERPOLATION_GAP_SECONDS = 5.0
TIME_GRID = np.arange(0, PLOT_END_SECONDS + 1, 1, dtype=float)


def display_smoothing_window_seconds() -> int:
    raw_value = os.environ.get("OJIES_RESOURCE_SMOOTHING_SECONDS", "31")
    try:
        window = int(raw_value)
    except ValueError as exc:
        raise ValueError(
            "OJIES_RESOURCE_SMOOTHING_SECONDS must be an odd integer"
        ) from exc
    if window < 1 or window % 2 == 0:
        raise ValueError(
            "OJIES_RESOURCE_SMOOTHING_SECONDS must be a positive odd integer"
        )
    return window


DISPLAY_SMOOTHING_WINDOW_SECONDS = display_smoothing_window_seconds()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_resource_run(
    file_path: Path,
    manifest_dir: Path,
    expected_server_count: int,
    expected_repetition: int,
    expected_platform: str,
):
    df = pd.read_csv(file_path)
    required_columns = {
        "timestamp",
        "cpu_cores_in_use",
        "total_memory_mb",
        "net_in_mbps",
    }
    missing_columns = sorted(required_columns.difference(df.columns))
    if missing_columns:
        raise ValueError(
            f"{file_path} enthaelt nicht alle Pflichtspalten: "
            + ", ".join(missing_columns)
        )
    if len(df) < 2:
        raise ValueError(f"{file_path} enthaelt weniger als zwei Samples.")

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        errors="raise",
        utc=True,
    )
    df = df.sort_values("timestamp")
    time_s = (
        df["timestamp"] - df["timestamp"].iloc[0]
    ).dt.total_seconds().to_numpy(dtype=float)
    if np.any(np.diff(time_s) <= 0):
        raise ValueError(
            f"{file_path} enthaelt nicht streng ansteigende Zeitstempel."
        )

    for column in required_columns.difference({"timestamp"}):
        df[column] = pd.to_numeric(df[column], errors="raise")
        if not np.all(np.isfinite(df[column].to_numpy(dtype=float))):
            raise ValueError(
                f"{file_path} enthaelt ungueltige Werte in {column}."
            )
    if "sample_interval_seconds_actual" in df.columns:
        df["sample_interval_seconds_actual"] = pd.to_numeric(
            df["sample_interval_seconds_actual"],
            errors="raise",
        )
        intervals = df["sample_interval_seconds_actual"].to_numpy(dtype=float)
        if not np.all(np.isfinite(intervals)) or np.any(intervals <= 0):
            raise ValueError(
                f"{file_path} enthaelt ungueltige Sampleintervalle."
            )

    manifest_path = manifest_dir / f"{file_path.stem}.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(
            f"Kein Manifest fuer ausgewaehlte Datei: {file_path}"
        )
    with manifest_path.open("r", encoding="utf-8-sig") as handle:
        manifest = json.load(handle)

    expected_values = {
        "requested_server_count": expected_server_count,
        "repetition": expected_repetition,
        "series": "standard",
        "status": "complete",
        "promotion_status": "completed",
    }
    for key, expected in expected_values.items():
        if manifest.get(key) != expected:
            raise ValueError(
                f"{manifest_path} hat fuer {key!r} "
                f"{manifest.get(key)!r}; erwartet {expected!r}."
            )
    if (
        expected_platform != "legacy"
        and manifest.get("platform") != expected_platform
    ):
        raise ValueError(
            f"{manifest_path} gehoert zu Plattform "
            f"{manifest.get('platform')!r}; erwartet {expected_platform!r}."
        )

    actual_hash = sha256_file(file_path)
    if manifest.get("csv_sha256") != actual_hash:
        raise ValueError(
            f"SHA-256 stimmt nicht mit Manifest ueberein: {file_path}"
        )
    if os.path.basename(manifest.get("canonical_csv", "")) != file_path.name:
        raise ValueError(
            f"Manifest und CSV-Name widersprechen sich: {manifest_path}"
        )
    return df, time_s, manifest, actual_hash


def collect_platform_groups(selection: dict) -> list[dict]:
    groups = []
    for platform in selection["resource_platforms"]:
        selected_repetitions = set(platform["resource_repetitions"])
        server_files = {count: [] for count in selection["server_counts"]}
        data_dir = platform["data_directory"]
        if not data_dir.is_dir():
            raise FileNotFoundError(
                f"Ressourcenordner fuer {platform['label']} fehlt: {data_dir}"
            )

        for file_path in sorted(data_dir.glob("*.csv")):
            match = FILENAME_PATTERN.fullmatch(file_path.name)
            if match is None:
                continue
            repetition = int(match.group(1))
            server_count = int(match.group(2))
            if (
                server_count in server_files
                and repetition in selected_repetitions
            ):
                server_files[server_count].append(file_path)

        missing = []
        for server_count, files in server_files.items():
            actual_repetitions = {
                int(FILENAME_PATTERN.fullmatch(path.name).group(1))
                for path in files
            }
            absent = sorted(selected_repetitions.difference(actual_repetitions))
            if absent:
                missing.append(f"n={server_count}: repetitions {absent}")
        if missing:
            raise FileNotFoundError(
                f"Unvollstaendige Matrix fuer {platform['label']}: "
                + "; ".join(missing)
            )

        print(f"--- Plattform {platform['label']} ---")
        for server_count, files in server_files.items():
            print(
                f"  {server_count} Server ({len(files)} Messungen): "
                + ", ".join(path.name for path in files)
            )
        groups.append({"platform": platform, "server_files": server_files})
    return groups


def interpolated_runs(csv_files: list[Path], column: str) -> list[np.ndarray]:
    runs = []
    for file_path in csv_files:
        df = pd.read_csv(file_path)
        timestamps = pd.to_datetime(
            df["timestamp"],
            errors="raise",
            utc=True,
        )
        order = np.argsort(timestamps.to_numpy())
        timestamps = timestamps.iloc[order]
        values = pd.to_numeric(
            df[column].iloc[order],
            errors="raise",
        ).to_numpy(dtype=float)
        time_s = (
            timestamps - timestamps.iloc[0]
        ).dt.total_seconds().to_numpy(dtype=float)
        if len(time_s) < 2:
            continue
        runs.append(interpolate_without_long_gaps(time_s, values))
    return runs


def interpolate_without_long_gaps(
    time_s: np.ndarray,
    values: np.ndarray,
) -> np.ndarray:
    interpolated = np.interp(
        TIME_GRID,
        time_s,
        values,
        left=np.nan,
        right=np.nan,
    )
    gaps = np.diff(time_s)
    for index in np.flatnonzero(gaps > MAX_INTERPOLATION_GAP_SECONDS):
        missing = (
            (TIME_GRID > time_s[index])
            & (TIME_GRID < time_s[index + 1])
        )
        interpolated[missing] = np.nan
    return interpolated


def pointwise_curves(runs: list[np.ndarray]):
    matrix = np.vstack(runs)
    valid_counts = np.sum(~np.isnan(matrix), axis=0)
    mean_curve = np.divide(
        np.nansum(matrix, axis=0),
        valid_counts,
        out=np.full(TIME_GRID.shape, np.nan, dtype=float),
        where=valid_counts > 0,
    )
    minimum = np.full(TIME_GRID.shape, np.nan, dtype=float)
    maximum = np.full(TIME_GRID.shape, np.nan, dtype=float)
    valid_grid = valid_counts > 0
    minimum[valid_grid] = np.nanmin(matrix[:, valid_grid], axis=0)
    maximum[valid_grid] = np.nanmax(matrix[:, valid_grid], axis=0)
    return matrix, mean_curve, minimum, maximum


def smoothed_runs_for_display(
    runs: list[np.ndarray],
) -> list[np.ndarray]:
    if DISPLAY_SMOOTHING_WINDOW_SECONDS == 1:
        return [run.copy() for run in runs]
    return [
        pd.Series(run)
        .rolling(
            window=DISPLAY_SMOOTHING_WINDOW_SECONDS,
            center=True,
            min_periods=1,
        )
        .mean()
        .to_numpy(dtype=float)
        for run in runs
    ]


def compute_y_limits(groups: list[dict]) -> dict:
    limits = {}
    for metric, config in METRICS.items():
        if "display_ylim" in config:
            limits[metric] = config["display_ylim"]
            continue
        maximum = 0.0
        for group in groups:
            for files in group["server_files"].values():
                display_runs = smoothed_runs_for_display(
                    interpolated_runs(files, config["column"])
                )
                for run in display_runs:
                    if np.any(np.isfinite(run)):
                        maximum = max(
                            maximum,
                            float(np.nanmax(run)),
                        )
        limits[metric] = (0.0, maximum * 1.05) if maximum > 0 else None
    return limits


def create_plots(groups: list[dict]) -> list[Path]:
    generated = []
    y_limits = compute_y_limits(groups)
    platform_count = len(groups)

    for metric, config in METRICS.items():
        height = 2.625 if platform_count == 1 else 2.0 * platform_count
        fig, axes = plt.subplots(
            platform_count,
            1,
            sharex=True,
            figsize=(3.5, height),
            squeeze=False,
        )
        plotted = False
        for panel_index, group in enumerate(groups):
            ax = axes[panel_index, 0]
            for style_index, (server_count, csv_files) in enumerate(
                group["server_files"].items()
            ):
                runs = interpolated_runs(csv_files, config["column"])
                if not runs:
                    continue
                display_runs = smoothed_runs_for_display(runs)
                _, mean_curve, minimum, maximum = pointwise_curves(
                    display_runs
                )
                if (
                    "display_ylim" in config
                    and np.any(np.isfinite(maximum))
                    and float(np.nanmax(maximum))
                    > float(config["display_ylim"][1])
                ):
                    raise ValueError(
                        f"{metric} display data exceed the configured "
                        f"y-axis maximum {config['display_ylim'][1]}"
                    )
                color = COLORS[style_index]
                ax.plot(
                    TIME_GRID,
                    mean_curve,
                    label=rf"$n_{{\mathrm{{req}}}}={server_count}$",
                    linestyle=LINESTYLES[style_index],
                    color=color,
                    linewidth=0.8,
                )
                ax.fill_between(
                    TIME_GRID,
                    minimum,
                    maximum,
                    color=color,
                    alpha=0.12,
                    linewidth=0,
                )
                plotted = True

            ax.set_ylabel(config["ylabel"])
            ax.set_xlim(0, PLOT_END_SECONDS)
            if y_limits[metric] is not None:
                ax.set_ylim(*y_limits[metric])
            ax.yaxis.grid(
                True,
                linestyle=":",
                linewidth=0.4,
                color="0.75",
                zorder=0,
            )
            ax.xaxis.grid(False)
            ax.set_axisbelow(True)
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            ax.minorticks_on()
            ax.tick_params(which="minor", left=True, bottom=True)

            label = group["platform"]["label"]
            if platform_count > 1:
                letter = chr(ord("a") + panel_index)
                ax.set_title(
                    f"({letter}) {label}",
                    loc="left",
                    pad=2,
                    fontsize=8,
                )

        if not plotted:
            plt.close(fig)
            raise RuntimeError(f"Keine gueltigen Daten fuer {metric!r}.")

        axes[-1, 0].set_xlabel(r"$\Delta t$ (s)")
        axes[0, 0].legend(
            loc=config["legend_loc"],
            frameon=True,
            fancybox=False,
            ncol=1,
            handlelength=2.0,
            handletextpad=0.5,
        )
        fig.tight_layout(pad=0.3, h_pad=0.5)
        save_path = output_dir() / config["filename"]
        try:
            fig.savefig(save_path)
        except PermissionError as exc:
            plt.close(fig)
            raise PermissionError(
                f"Zugriff verweigert auf '{save_path}'. Datei im "
                "PDF-Viewer schliessen und erneut ausfuehren."
            ) from exc
        plt.close(fig)
        generated.append(save_path)
        print(f"IEEE-Diagramm gespeichert unter: {save_path}")
    return generated


def write_numeric_summary(groups: list[dict]) -> tuple[Path, Path]:
    derived_dir = (
        VALIDATION_ROOT / "performance" / "derived" / "resource_usage"
    )
    derived_dir.mkdir(parents=True, exist_ok=True)
    summary_rows = []
    selected_runs = []
    project_root = VALIDATION_ROOT.parent

    for group in groups:
        platform = group["platform"]
        for server_count, csv_files in group["server_files"].items():
            matrices = {metric: [] for metric in METRICS}
            network_volumes = []
            network_observed_seconds = []
            sample_counts = []
            durations = []

            for file_path in csv_files:
                match = FILENAME_PATTERN.fullmatch(file_path.name)
                repetition = int(match.group(1))
                df, time_s, manifest, actual_hash = load_resource_run(
                    file_path,
                    manifest_dir=platform["manifest_directory"],
                    expected_server_count=server_count,
                    expected_repetition=repetition,
                    expected_platform=platform["id"],
                )
                sample_counts.append(len(df))
                durations.append(float(time_s[-1]))
                in_plot_window = time_s <= PLOT_END_SECONDS
                if "sample_interval_seconds_actual" in df.columns:
                    intervals = df[
                        "sample_interval_seconds_actual"
                    ].to_numpy(dtype=float)
                    network_volumes.append(
                        float(
                            (
                                df["net_in_mbps"].to_numpy(dtype=float)[
                                    in_plot_window
                                ]
                                * intervals[in_plot_window]
                            ).sum()
                        )
                    )
                    network_observed_seconds.append(
                        float(intervals[in_plot_window].sum())
                    )
                else:
                    network_volumes.append(
                        float(
                            df["net_in_mbps"].to_numpy(dtype=float)[
                                in_plot_window
                            ].sum()
                        )
                    )
                    network_observed_seconds.append(
                        float(np.sum(in_plot_window))
                    )

                for metric, config in METRICS.items():
                    matrices[metric].append(
                        interpolate_without_long_gaps(
                            time_s,
                            df[config["column"]].to_numpy(dtype=float),
                        )
                    )

                manifest_path = (
                    platform["manifest_directory"] / f"{file_path.stem}.json"
                )
                artifact_dir = (
                    platform["data_directory"]
                    / "artifacts"
                    / file_path.stem
                )
                pi_resource_manifest = {}
                pi_resource_manifest_path = (
                    artifact_dir / "pi" / "resource_manifest.json"
                )
                if pi_resource_manifest_path.is_file():
                    with pi_resource_manifest_path.open(
                        "r", encoding="utf-8"
                    ) as handle:
                        pi_resource_manifest = json.load(handle)

                source_snapshot_manifest = {}
                source_snapshot_manifest_path = (
                    artifact_dir / "pi" / "source_snapshot_manifest.json"
                )
                if source_snapshot_manifest_path.is_file():
                    with source_snapshot_manifest_path.open(
                        "r", encoding="utf-8"
                    ) as handle:
                        source_snapshot_manifest = json.load(handle)

                fixture = manifest.get("fixture") or {}
                measurement_scope = manifest.get("measurement_scope") or {}
                network_capture = (
                    pi_resource_manifest.get("network_capture") or {}
                )
                repository_commit = manifest.get("repository_commit")
                if repository_commit is None:
                    repository_commit = fixture.get(
                        "source_repository_commit"
                    )
                if repository_commit is None:
                    repository_commit = source_snapshot_manifest.get(
                        "nested_repository_commit"
                    )
                repository_dirty = manifest.get(
                    "repository_dirty_before_run"
                )
                if repository_dirty is None:
                    repository_dirty = source_snapshot_manifest.get(
                        "nested_repository_dirty"
                    )
                measurement_chain_manifest = manifest.get(
                    "measurement_chain_manifest"
                )
                if (
                    measurement_chain_manifest is None
                    and manifest.get("schema")
                    == "ojies.distributed-performance-run/v2"
                ):
                    measurement_chain_manifest = (
                        manifest_path.relative_to(project_root).as_posix()
                    )
                selected_runs.append(
                    {
                        "platform": platform["id"],
                        "file": file_path.relative_to(
                            project_root
                        ).as_posix(),
                        "manifest": manifest_path.relative_to(
                            project_root
                        ).as_posix(),
                        "run_id": manifest["run_id"],
                        "requested_server_count": server_count,
                        "repetition": repetition,
                        "sample_count": len(df),
                        "observed_duration_seconds": float(time_s[-1]),
                        "csv_sha256": actual_hash,
                        "repository_commit": repository_commit,
                        "repository_dirty_before_run": repository_dirty,
                        "network_interface": manifest.get("network_interface")
                        or network_capture.get("interface"),
                        "network_backend": manifest.get("network_backend")
                        or platform["measurement_backend"],
                        "network_capture_filter": measurement_scope.get(
                            "network_capture_filter"
                        ),
                        "scientific_validation_status": manifest.get(
                            "scientific_validation_status"
                        ),
                        "observed_counts": manifest.get("observed_counts"),
                        "measurement_chain_manifest": (
                            measurement_chain_manifest
                        ),
                    }
                )

            row = {
                "platform": platform["id"],
                "platform_label": platform["label"],
                "requested_server_count": server_count,
                "run_count": len(csv_files),
                "sample_count_min": min(sample_counts),
                "sample_count_max": max(sample_counts),
                "observed_duration_seconds_min": min(durations),
                "observed_duration_seconds_max": max(durations),
                "network_volume_mib_mean": float(np.mean(network_volumes)),
                "network_volume_mib_min": float(np.min(network_volumes)),
                "network_volume_mib_max": float(np.max(network_volumes)),
                "network_observed_seconds_in_window_min": float(
                    np.min(network_observed_seconds)
                ),
                "network_observed_seconds_in_window_max": float(
                    np.max(network_observed_seconds)
                ),
            }
            for metric, runs in matrices.items():
                matrix, mean_curve, _, _ = pointwise_curves(runs)
                coverage = np.sum(np.isfinite(matrix), axis=0)
                valid = np.isfinite(mean_curve)
                initial = valid & (TIME_GRID <= 10)
                final_start = max(0, PLOT_END_SECONDS - 30)
                final = valid & (TIME_GRID >= final_start)
                peak_index = int(np.nanargmax(mean_curve))
                row.update(
                    {
                        f"{metric}_initial_0_10s_mean": float(
                            np.nanmean(mean_curve[initial])
                        ),
                        f"{metric}_window_mean": float(
                            np.nanmean(mean_curve[valid])
                        ),
                        f"{metric}_final_30s_mean": float(
                            np.nanmean(mean_curve[final])
                        ),
                        f"{metric}_window_coverage_min": int(
                            np.min(coverage[valid])
                        ),
                        f"{metric}_final_30s_coverage_min": int(
                            np.min(coverage[final])
                        ),
                        f"{metric}_pointwise_mean_peak": float(
                            mean_curve[peak_index]
                        ),
                        f"{metric}_pointwise_mean_peak_time_s": float(
                            TIME_GRID[peak_index]
                        ),
                        f"{metric}_pointwise_min_at_peak": float(
                            np.nanmin(matrix[:, peak_index])
                        ),
                        f"{metric}_pointwise_max_at_peak": float(
                            np.nanmax(matrix[:, peak_index])
                        ),
                        f"{metric}_raw_min": float(np.nanmin(matrix)),
                        f"{metric}_raw_max": float(np.nanmax(matrix)),
                    }
                )
            summary_rows.append(row)

    summary_csv = derived_dir / "resource_usage_summary.csv"
    pd.DataFrame(summary_rows).to_csv(
        summary_csv,
        index=False,
        float_format="%.6f",
    )
    summary_json = derived_dir / "resource_usage_summary.json"
    document = {
        "schema": "ojies.resource-usage-analysis/v2",
        "selection": {
            "series": "standard",
            "platforms": [
                {
                    "id": group["platform"]["id"],
                    "label": group["platform"]["label"],
                    "repetitions_per_count": group["platform"][
                        "resource_repetitions"
                    ],
                    "measurement_backend": group["platform"][
                        "measurement_backend"
                    ],
                }
                for group in groups
            ],
            "requested_server_counts": sorted(groups[0]["server_files"]),
            "time_grid_seconds": [0, PLOT_END_SECONDS],
            "time_grid_step_seconds": 1,
            "display_axis_limits": {
                metric: list(config["display_ylim"])
                for metric, config in METRICS.items()
                if "display_ylim" in config
            },
        },
        "measurement_scope": {
            "server_count": (
                "requested metadata; exact observed source listening, URDF "
                "payload, LDS registration, and terminal aggregation counts "
                "are retained per run"
            ),
            "cpu": (
                "Aggregation Server plus LDS interval CPU-time delta divided "
                "by actual wall time; equivalent fully used cores"
            ),
            "memory": "Aggregation Server plus LDS resident working set; MiB",
            "network": (
                "the active distributed matrix sums all bidirectional TCP/IP "
                "bytes between the physical source and aggregation hosts, "
                "divided by actual interval; MiB/s. The capture is dominated "
                "by OPC UA and also contains a small SSH control contribution. "
                "Legacy runs retain their manifest definition"
            ),
        },
        "summary_method": {
            "curve": (
                "per-run linear interpolation to a one-second grid followed "
                "by the pointwise mean; intervals longer than "
                f"{MAX_INTERPOLATION_GAP_SECONDS:g} s remain missing"
            ),
            "band": (
                "pointwise minimum and maximum across available runs; "
                "coverage fields record the contributor count"
            ),
            "display_smoothing": (
                f"centered {DISPLAY_SMOOTHING_WINDOW_SECONDS}-second "
                "arithmetic moving average applied to each interpolated run "
                "before plotting the pointwise mean and band; numeric "
                "summaries remain unsmoothed"
            ),
            "initial": "mean of pointwise mean curve from 0 through 10 s",
            "final": (
                "mean of the final 30 s of the selected display window; "
                "coverage fields record the minimum contributing run count"
            ),
            "network_volume": (
                "sum of rate times actual sample interval for samples whose "
                "timestamps fall inside the selected display window; "
                "suspension gaps are not imputed"
            ),
        },
        "selected_runs": selected_runs,
        "scenarios": summary_rows,
    }
    with summary_json.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(document, handle, indent=2)
        handle.write("\n")
    print(f"Numerische Zusammenfassung: {summary_csv}")
    print(f"Maschinenlesbare Analyse: {summary_json}")
    return summary_csv, summary_json


def process_and_plot() -> list[Path]:
    selection = load_active_measurement_selection()
    groups = collect_platform_groups(selection)
    generated = create_plots(groups)
    if os.environ.get("OJIES_RESOURCE_SKIP_NUMERIC_SUMMARY") != "1":
        write_numeric_summary(groups)
    expected = {output_dir() / config["filename"] for config in METRICS.values()}
    missing = sorted(str(path) for path in expected.difference(generated))
    if missing:
        raise RuntimeError(
            "Standardabbildungen wurden nicht vollstaendig erzeugt: "
            + ", ".join(missing)
        )
    return generated


if __name__ == "__main__":
    process_and_plot()
