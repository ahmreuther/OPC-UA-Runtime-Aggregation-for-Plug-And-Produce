#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Adrian Reuther
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

"""Validate and activate one platform's complete resource measurement matrix."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path


EXPECTED_SERVER_COUNTS = [1, 5, 10, 25, 50, 75, 100]
EXPECTED_REPETITION_COUNT = 3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--platform",
        choices=(
            "windows",
            "linux",
            "pi5_windows_distributed",
            "pi5_linux_distributed",
        ),
        required=True,
    )
    parser.add_argument(
        "--repetitions",
        nargs="+",
        type=int,
        required=True,
    )
    parser.add_argument(
        "--server-counts",
        nargs="+",
        type=int,
        required=True,
    )
    parser.add_argument(
        "--replace-active-platforms",
        action="store_true",
        help=(
            "Replace the active platform set after validation instead of "
            "adding this matrix beside existing platforms."
        ),
    )
    return parser.parse_args()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_matrix(
    resource_dir: Path,
    platform: str,
    repetitions: list[int],
    server_counts: list[int],
) -> None:
    manifest_dir = resource_dir / "manifests"
    errors: list[str] = []
    for repetition in repetitions:
        for server_count in server_counts:
            stem = f"{repetition}n{server_count}"
            csv_path = resource_dir / f"{stem}.csv"
            manifest_path = manifest_dir / f"{stem}.json"
            if not csv_path.is_file():
                errors.append(f"missing CSV: {csv_path}")
                continue
            if not manifest_path.is_file():
                errors.append(f"missing manifest: {manifest_path}")
                continue
            try:
                with manifest_path.open("r", encoding="utf-8-sig") as handle:
                    manifest = json.load(handle)
            except (OSError, json.JSONDecodeError) as exc:
                errors.append(f"invalid manifest {manifest_path}: {exc}")
                continue

            expected = {
                "platform": platform,
                "requested_server_count": server_count,
                "repetition": repetition,
                "series": "standard",
                "status": "complete",
                "promotion_status": "completed",
            }
            for field, value in expected.items():
                if manifest.get(field) != value:
                    errors.append(
                        f"{manifest_path}: {field}={manifest.get(field)!r}, "
                        f"expected {value!r}"
                    )
            observed = manifest.get("observed_counts")
            if not isinstance(observed, dict):
                errors.append(f"{manifest_path}: observed_counts missing")
            else:
                for field in (
                    "source_listening",
                    "source_listening_with_urdf_payload",
                    "lds_registered",
                    "aggregation_terminal",
                ):
                    if observed.get(field) != server_count:
                        errors.append(
                            f"{manifest_path}: observed_counts.{field}="
                            f"{observed.get(field)!r}, expected {server_count}"
                        )

            actual_hash = sha256_file(csv_path)
            if manifest.get("csv_sha256") != actual_hash:
                errors.append(f"{manifest_path}: CSV SHA-256 mismatch")

    if errors:
        raise ValueError(
            "Resource platform matrix is not activatable:\n- "
            + "\n- ".join(errors)
        )


def load_or_create_selection(path: Path) -> dict:
    if not path.exists():
        return {
            "schema": "ojies.active-resource-measurements/v2",
            "server_counts": EXPECTED_SERVER_COUNTS,
            "platforms": {},
        }
    with path.open("r", encoding="utf-8-sig") as handle:
        document = json.load(handle)
    if document.get("schema") != "ojies.active-resource-measurements/v2":
        raise ValueError(
            f"{path} uses {document.get('schema')!r}. Remove or archive the "
            "obsolete active selection before activating platform-separated "
            "measurements; raw data is not modified."
        )
    if document.get("server_counts") != EXPECTED_SERVER_COUNTS:
        raise ValueError(f"Unexpected server count selection in {path}")
    platforms = document.get("platforms")
    if not isinstance(platforms, dict):
        raise ValueError(f"Invalid platforms object in {path}")
    return document


def main() -> int:
    args = parse_args()
    if args.server_counts != EXPECTED_SERVER_COUNTS:
        raise ValueError(
            f"--server-counts must be exactly {EXPECTED_SERVER_COUNTS}"
        )
    if (
        len(args.repetitions) != EXPECTED_REPETITION_COUNT
        or len(set(args.repetitions)) != EXPECTED_REPETITION_COUNT
        or any(value <= 0 for value in args.repetitions)
    ):
        raise ValueError("--repetitions must contain three distinct positives")

    workspace_root = Path(__file__).resolve().parents[3]
    resource_relative = (
        Path("Validation")
        / "performance"
        / "raw_data"
        / "resource_usage"
        / args.platform
    )
    resource_dir = workspace_root / resource_relative
    validate_matrix(
        resource_dir,
        args.platform,
        args.repetitions,
        args.server_counts,
    )

    selection_path = (
        workspace_root
        / "Validation"
        / "performance"
        / "raw_data"
        / "active_resource_measurements.json"
    )
    document = load_or_create_selection(selection_path)
    platform_metadata = {
        "windows": (
            "Windows",
            "dumpcap_tshark_loopback_ip_bytes",
        ),
        "linux": (
            "Linux",
            "dumpcap_tshark_loopback_ip_bytes",
        ),
        "pi5_windows_distributed": (
            "Raspberry Pi 5 with Windows sources",
            "dumpcap_tshark_ethernet_ip_bytes",
        ),
        "pi5_linux_distributed": (
            "Raspberry Pi 5 with Linux sources",
            "tcpdump_tshark_ethernet_ip_bytes",
        ),
    }
    label, measurement_backend = platform_metadata[args.platform]
    if args.platform in {
        "pi5_windows_distributed",
        "pi5_linux_distributed",
    }:
        outcome_manifest_relative = resource_relative / "manifests"
    else:
        outcome_manifest_relative = (
            Path("Validation")
            / "performance"
            / "raw_data"
            / "manifests"
            / args.platform
        )
    if args.replace_active_platforms:
        document["platforms"] = {}
    document["platforms"][args.platform] = {
        "label": label,
        "data_directory": resource_relative.as_posix(),
        "manifest_directory": (resource_relative / "manifests").as_posix(),
        "outcome_manifest_directory": (
            outcome_manifest_relative.as_posix()
        ),
        "resource_repetitions": args.repetitions,
        "measurement_backend": measurement_backend,
        "activation_condition": (
            "all 21 canonical runs passed exact listening, URDF payload, "
            "LDS registration, terminal aggregation, and CSV hash checks"
        ),
    }
    preferred_order = [
        "windows",
        "linux",
        "pi5_windows_distributed",
        "pi5_linux_distributed",
    ]
    document["platform_order"] = [
        item for item in preferred_order if item in document["platforms"]
    ]
    document["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
    document["source"] = (
        "Validation/performance/acquisition/"
        "activate_resource_platform.py"
    )

    selection_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = selection_path.with_name(
        f".{selection_path.name}.{os.getpid()}.tmp"
    )
    try:
        with temp_path.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(document, handle, indent=2)
            handle.write("\n")
        os.replace(temp_path, selection_path)
    finally:
        if temp_path.exists():
            temp_path.unlink()

    print(
        f"Activated {args.platform} resource matrix in {selection_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
