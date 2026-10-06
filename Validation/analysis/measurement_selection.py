# SPDX-FileCopyrightText: 2026 Adrian Reuther
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

"""Validated selection of canonical runs used by the five performance plots."""

from __future__ import annotations

import json
import os
from pathlib import Path

from plot_paths import VALIDATION_ROOT


SELECTION_PATH = (
    VALIDATION_ROOT
    / "performance"
    / "raw_data"
    / "active_plot_measurements.json"
)
RESOURCE_SELECTION_PATH = (
    VALIDATION_ROOT
    / "performance"
    / "raw_data"
    / "active_resource_measurements.json"
)
DEFAULT_SELECTION = {
    "server_counts": [1, 5, 10, 25, 50, 75, 100],
    "resource_repetitions": [1, 2, 3],
    "resource_and_scalability_repetitions": [1, 2, 3],
    "integration_configuration_repetitions": [1, 2, 3],
    "resource_platforms": [],
}


def _resource_selection_path() -> Path:
    configured = os.environ.get("OJIES_RESOURCE_SELECTION_FILE")
    if not configured:
        return RESOURCE_SELECTION_PATH
    candidate = Path(configured).expanduser()
    if not candidate.is_absolute():
        candidate = VALIDATION_ROOT.parent / candidate
    candidate = candidate.resolve()
    project_root = VALIDATION_ROOT.parent.resolve()
    if candidate != project_root and project_root not in candidate.parents:
        raise ValueError(
            "OJIES_RESOURCE_SELECTION_FILE must remain inside the project root"
        )
    return candidate


def _positive_unique_integers(value, field: str) -> list[int]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field} must be a non-empty JSON array")
    if any(not isinstance(item, int) or item <= 0 for item in value):
        raise ValueError(f"{field} must contain positive integers only")
    if len(set(value)) != len(value):
        raise ValueError(f"{field} must not contain duplicates")
    return value


def _resource_platform(
    platform_id: str,
    label: str,
    data_directory: str,
    manifest_directory: str,
    repetitions: list[int],
    measurement_backend: str,
    outcome_manifest_directory: str | None = None,
) -> dict:
    if not isinstance(platform_id, str) or not platform_id:
        raise ValueError("resource platform ID must be a non-empty string")
    if not isinstance(label, str) or not label:
        raise ValueError(f"resource platform {platform_id!r} has no label")
    if not isinstance(data_directory, str) or not data_directory:
        raise ValueError(
            f"resource platform {platform_id!r} has no data_directory"
        )
    if not isinstance(manifest_directory, str) or not manifest_directory:
        raise ValueError(
            f"resource platform {platform_id!r} has no manifest_directory"
        )
    project_root = VALIDATION_ROOT.parent.resolve()
    data_path = (project_root / data_directory).resolve()
    manifest_path = (project_root / manifest_directory).resolve()
    outcome_manifest_path = (
        project_root
        / (
            outcome_manifest_directory
            or manifest_directory
        )
    ).resolve()
    for candidate in (data_path, manifest_path, outcome_manifest_path):
        if project_root not in candidate.parents:
            raise ValueError(
                f"resource platform {platform_id!r} escapes the project root"
            )
    return {
        "id": platform_id,
        "label": label,
        "data_directory": data_path,
        "manifest_directory": manifest_path,
        "outcome_manifest_directory": outcome_manifest_path,
        "resource_repetitions": repetitions,
        "measurement_backend": measurement_backend,
    }


def load_active_measurement_selection() -> dict:
    """Read full or resource-only performance selections."""
    selection = {
        key: list(value)
        for key, value in DEFAULT_SELECTION.items()
    }

    if SELECTION_PATH.is_file():
        with SELECTION_PATH.open("r", encoding="utf-8") as handle:
            document = json.load(handle)

        if document.get("schema") != "ojies.active-plot-measurements/v1":
            raise ValueError(
                f"Unsupported measurement selection: {SELECTION_PATH}"
            )

        for field in (
            "server_counts",
            "resource_and_scalability_repetitions",
            "integration_configuration_repetitions",
        ):
            selection[field] = _positive_unique_integers(
                document.get(field),
                field,
            )
        selection["resource_repetitions"] = _positive_unique_integers(
            document.get(
                "resource_repetitions",
                selection["resource_and_scalability_repetitions"],
            ),
            "resource_repetitions",
        )
    resource_selection_path = _resource_selection_path()
    if resource_selection_path.is_file():
        with resource_selection_path.open("r", encoding="utf-8") as handle:
            document = json.load(handle)

        schema = document.get("schema")
        if schema not in {
                "ojies.active-resource-measurements/v1",
                "ojies.active-resource-measurements/v2",
            }:
            raise ValueError(
                "Unsupported resource measurement selection: "
                f"{resource_selection_path}"
            )
        selection["server_counts"] = _positive_unique_integers(
            document.get("server_counts"),
            "server_counts",
        )
        if schema == "ojies.active-resource-measurements/v1":
            selection["resource_repetitions"] = _positive_unique_integers(
                document.get("resource_repetitions"),
                "resource_repetitions",
            )
        else:
            platforms = document.get("platforms")
            platform_order = document.get("platform_order")
            if not isinstance(platforms, dict) or not platforms:
                raise ValueError(
                    "resource measurement v2 selection requires platforms"
                )
            if (
                not isinstance(platform_order, list)
                or not platform_order
                or len(set(platform_order)) != len(platform_order)
                or set(platform_order) != set(platforms)
            ):
                raise ValueError(
                    "resource measurement platform_order must list every "
                    "platform exactly once"
                )
            resource_platforms = []
            for platform_id in platform_order:
                platform = platforms.get(platform_id)
                if not isinstance(platform, dict):
                    raise ValueError(
                        f"invalid resource platform: {platform_id!r}"
                    )
                repetitions = _positive_unique_integers(
                    platform.get("resource_repetitions"),
                    f"platforms.{platform_id}.resource_repetitions",
                )
                backend = platform.get("measurement_backend")
                if backend not in {
                    "dumpcap_tshark_loopback_ip_bytes",
                    "dumpcap_tshark_ethernet_ip_bytes",
                    "tcpdump_tshark_ethernet_ip_bytes",
                }:
                    raise ValueError(
                        f"resource platform {platform_id!r} uses unsupported "
                        f"measurement backend {backend!r}"
                    )
                resource_platforms.append(
                    _resource_platform(
                        platform_id,
                        platform.get("label"),
                        platform.get("data_directory"),
                        platform.get("manifest_directory"),
                        repetitions,
                        backend,
                        platform.get(
                            "outcome_manifest_directory",
                            (
                                "Validation/performance/raw_data/manifests/"
                                f"{platform_id}"
                            ),
                        ),
                    )
                )
            selection["resource_platforms"] = resource_platforms
            selection["resource_repetitions"] = list(
                resource_platforms[0]["resource_repetitions"]
            )

    if selection["server_counts"] != DEFAULT_SELECTION["server_counts"]:
        raise ValueError(
            "Performance selections must select exactly "
            "1, 5, 10, 25, 50, 75, and 100 servers in that order"
        )
    if not selection["resource_platforms"]:
        selection["resource_platforms"] = [
            _resource_platform(
                "legacy",
                "Legacy",
                "Validation/performance/raw_data/resource_usage",
                "Validation/performance/raw_data/resource_usage/manifests",
                list(selection["resource_repetitions"]),
                "legacy_interface_rx",
            )
        ]
    return selection
