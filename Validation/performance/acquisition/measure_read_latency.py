#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Adrian Reuther
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

"""Acquire paired OPC UA Read latencies for experiment E03.

The measured interval contains only ``Node.read_data_value``. Connection setup,
warm-up reads, value serialization, CSV output, and validation are outside the
timed interval. Every attempted Read remains in the raw CSV, including failures.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import csv
import hashlib
import json
import os
import platform
import random
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timezone
from pathlib import Path
from typing import Any

from asyncua import Client, __version__ as asyncua_version


SCRIPT_PATH = Path(__file__).resolve()
PERFORMANCE_ROOT = SCRIPT_PATH.parents[1]
DEFAULT_STAGING_ROOT = PERFORMANCE_ROOT / "runs" / "read_latency"
DEFAULT_RAW_ROOT = PERFORMANCE_ROOT / "raw_data" / "read_latency"
CSV_FIELDS = [
    "pair_index",
    "position_in_pair",
    "order",
    "path",
    "observed_at_utc",
    "start_monotonic_ns",
    "end_monotonic_ns",
    "latency_ns",
    "status_code",
    "status_good",
    "value_type",
    "value_sha256",
    "value_json",
    "error_type",
    "error_message",
]


@dataclass(frozen=True)
class ReadTarget:
    name: str
    endpoint: str
    node_id: str
    node: Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_text(value: datetime | None = None) -> str:
    return (value or utc_now()).isoformat(timespec="microseconds").replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def git_state(repository: Path) -> dict[str, Any]:
    def run(*arguments: str) -> str:
        completed = subprocess.run(
            ["git", "-C", str(repository), *arguments],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        return completed.stdout.strip() if completed.returncode == 0 else ""

    commit = run("rev-parse", "HEAD")
    status = run("status", "--porcelain") if commit else ""
    return {
        "path": str(repository),
        "commit": commit or None,
        "dirty": bool(status) if commit else None,
    }


def canonical_value(value: Any) -> Any:
    """Convert common OPC UA values to deterministic JSON-compatible data."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, bytes):
        return {"type": "bytes", "base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, bytearray):
        return {
            "type": "bytearray",
            "base64": base64.b64encode(bytes(value)).decode("ascii"),
        }
    if isinstance(value, (datetime, date, datetime_time)):
        return {"type": type(value).__name__, "iso8601": value.isoformat()}
    if isinstance(value, (list, tuple)):
        return [canonical_value(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): canonical_value(item)
            for key, item in sorted(value.items(), key=lambda entry: str(entry[0]))
        }
    if hasattr(value, "value") and type(value).__module__ == "enum":
        return {
            "type": f"{type(value).__module__}.{type(value).__qualname__}",
            "value": canonical_value(value.value),
        }
    return {
        "type": f"{type(value).__module__}.{type(value).__qualname__}",
        "repr": repr(value),
    }


def serialize_value(value: Any) -> tuple[str, str, str]:
    encoded = json.dumps(
        canonical_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return (
        f"{type(value).__module__}.{type(value).__qualname__}",
        hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
        encoded,
    )


def balanced_orders(count: int, seed: int) -> list[tuple[str, str]]:
    """Return a reproducibly shuffled, near-perfectly balanced AB/BA order."""
    orders = [("direct", "aggregated") for _ in range(count // 2)]
    orders.extend(("aggregated", "direct") for _ in range(count // 2))
    if count % 2:
        orders.append(("direct", "aggregated") if seed % 2 == 0 else ("aggregated", "direct"))
    random.Random(seed).shuffle(orders)
    return orders


async def timed_read(target: ReadTarget, pair_index: int, position: int, order: str) -> dict[str, Any]:
    observed_at = utc_text()
    started_ns = time.perf_counter_ns()
    try:
        data_value = await target.node.read_data_value(raise_on_bad_status=False)
        ended_ns = time.perf_counter_ns()
        status_code = int(data_value.StatusCode.value)
        status_good = bool(data_value.StatusCode.is_good())
        value = data_value.Value.Value if data_value.Value is not None else None
        value_type, value_sha256, value_json = serialize_value(value)
        error_type = ""
        error_message = ""
    except Exception as exc:  # The failed request is evidence and must be retained.
        ended_ns = time.perf_counter_ns()
        status_code = ""
        status_good = False
        value_type = ""
        value_sha256 = ""
        value_json = ""
        error_type = f"{type(exc).__module__}.{type(exc).__qualname__}"
        error_message = str(exc)

    return {
        "pair_index": pair_index,
        "position_in_pair": position,
        "order": order,
        "path": target.name,
        "observed_at_utc": observed_at,
        "start_monotonic_ns": started_ns,
        "end_monotonic_ns": ended_ns,
        "latency_ns": ended_ns - started_ns,
        "status_code": status_code,
        "status_good": str(status_good).lower(),
        "value_type": value_type,
        "value_sha256": value_sha256,
        "value_json": value_json,
        "error_type": error_type,
        "error_message": error_message,
    }


async def warm_up(
    targets: dict[str, ReadTarget],
    count: int,
    seed: int,
    require_equal_values: bool,
) -> None:
    for index, order in enumerate(balanced_orders(count, seed), start=1):
        pair: dict[str, dict[str, Any]] = {}
        for position, target_name in enumerate(order, start=1):
            result = await timed_read(targets[target_name], -index, position, ">".join(order))
            pair[target_name] = result
            if result["status_good"] != "true" or result["error_type"]:
                raise RuntimeError(
                    f"Warm-up Read failed for {target_name}: "
                    f"status={result['status_code']} error={result['error_message']}"
                )
        if (
            require_equal_values
            and pair["direct"]["value_sha256"] != pair["aggregated"]["value_sha256"]
        ):
            raise RuntimeError(
                "Warm-up values differ between direct and aggregated paths; "
                "select a stable delegated scalar node."
            )


def summarize_rows(rows: list[dict[str, Any]], samples: int, require_equal_values: bool) -> dict[str, Any]:
    path_counts = {"direct": 0, "aggregated": 0}
    good_counts = {"direct": 0, "aggregated": 0}
    error_counts = {"direct": 0, "aggregated": 0}
    pairs: dict[int, dict[str, dict[str, Any]]] = {}

    for row in rows:
        path = str(row["path"])
        path_counts[path] += 1
        if row["status_good"] == "true" and not row["error_type"]:
            good_counts[path] += 1
        else:
            error_counts[path] += 1
        pairs.setdefault(int(row["pair_index"]), {})[path] = row

    comparable_pairs = 0
    value_mismatches = 0
    for pair in pairs.values():
        if set(pair) != {"direct", "aggregated"}:
            continue
        if not all(item["status_good"] == "true" and not item["error_type"] for item in pair.values()):
            continue
        comparable_pairs += 1
        if pair["direct"]["value_sha256"] != pair["aggregated"]["value_sha256"]:
            value_mismatches += 1

    passed = (
        path_counts == {"direct": samples, "aggregated": samples}
        and error_counts == {"direct": 0, "aggregated": 0}
        and comparable_pairs == samples
        and (not require_equal_values or value_mismatches == 0)
    )
    return {
        "scientific_validation_status": "passed" if passed else "failed",
        "expected_samples_per_path": samples,
        "observed_reads_per_path": path_counts,
        "good_reads_per_path": good_counts,
        "failed_reads_per_path": error_counts,
        "comparable_pairs": comparable_pairs,
        "value_mismatches": value_mismatches,
        "require_equal_values": require_equal_values,
    }


async def acquire(args: argparse.Namespace, run_dir: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    direct_client = Client(url=args.direct_endpoint, timeout=args.timeout)
    aggregated_client = Client(url=args.aggregated_endpoint, timeout=args.timeout)

    async with direct_client, aggregated_client:
        targets = {
            "direct": ReadTarget(
                "direct",
                args.direct_endpoint,
                args.direct_node_id,
                direct_client.get_node(args.direct_node_id),
            ),
            "aggregated": ReadTarget(
                "aggregated",
                args.aggregated_endpoint,
                args.aggregated_node_id,
                aggregated_client.get_node(args.aggregated_node_id),
            ),
        }
        await warm_up(
            targets,
            args.warmup,
            args.seed ^ 0xE03,
            require_equal_values=not args.allow_value_mismatch,
        )

        csv_path = run_dir / "measurements.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
            writer.writeheader()
            for pair_index, order in enumerate(balanced_orders(args.samples, args.seed), start=1):
                order_text = ">".join(order)
                for position, target_name in enumerate(order, start=1):
                    row = await timed_read(
                        targets[target_name], pair_index, position, order_text
                    )
                    rows.append(row)
                    writer.writerow(row)
                if pair_index % args.progress_every == 0 or pair_index == args.samples:
                    print(f"E03 progress: {pair_index}/{args.samples} paired Reads", flush=True)

    return summarize_rows(rows, args.samples, not args.allow_value_mismatch), rows


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def promote_run(run_dir: Path, raw_root: Path, manifest: dict[str, Any]) -> Path:
    raw_root.mkdir(parents=True, exist_ok=True)
    destination = raw_root / run_dir.name
    temporary = raw_root / f".{run_dir.name}.tmp"
    if destination.exists() or temporary.exists():
        raise FileExistsError(f"Raw-data destination already exists: {destination}")

    manifest["promotion_status"] = "completed"
    manifest["canonical_run_directory"] = str(destination)
    write_json(run_dir / "manifest.json", manifest)
    try:
        shutil.copytree(run_dir, temporary)
        os.replace(temporary, destination)
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary)
        manifest["promotion_status"] = "failed"
        manifest["canonical_run_directory"] = None
        write_json(run_dir / "manifest.json", manifest)
        raise
    return destination


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Acquire reproducible direct and delegated OPC UA Read latencies for E03."
    )
    parser.add_argument("--direct-endpoint", required=True)
    parser.add_argument("--direct-node-id", required=True)
    parser.add_argument("--aggregated-endpoint", required=True)
    parser.add_argument("--aggregated-node-id", required=True)
    parser.add_argument("--samples", type=int, default=10_000)
    parser.add_argument("--warmup", type=int, default=1_000)
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--progress-every", type=int, default=500)
    parser.add_argument("--run-label", default="")
    parser.add_argument("--operator", default="")
    parser.add_argument("--notes", default="")
    parser.add_argument("--allow-value-mismatch", action="store_true")
    parser.add_argument("--no-promote", action="store_true")
    parser.add_argument("--staging-root", type=Path, default=DEFAULT_STAGING_ROOT)
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    return parser


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    for name in ("samples", "warmup", "progress_every"):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be a positive integer")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.seed is None:
        args.seed = int.from_bytes(os.urandom(8), "big")
    if args.run_label and not re.fullmatch(r"[A-Za-z0-9_.-]+", args.run_label):
        parser.error("--run-label may contain only letters, digits, '.', '_' and '-'")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    validate_args(parser, args)

    timestamp = utc_now().strftime("%Y%m%dT%H%M%S.%fZ")
    suffix = f"-{args.run_label}" if args.run_label else ""
    run_id = f"{timestamp}-e03-{os.getpid()}{suffix}"
    run_dir = args.staging_root.resolve() / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    started_at = utc_text()

    server_repository = PERFORMANCE_ROOT.parent.parent / "ojies_agg_server"
    manifest: dict[str, Any] = {
        "schema": "ojies.e03-read-latency-run/v1",
        "experiment_id": "E03",
        "claim_id": "C04",
        "run_id": run_id,
        "runtime_status": "running",
        "scientific_validation_status": "not_assessed",
        "started_at_utc": started_at,
        "ended_at_utc": None,
        "operator": args.operator or None,
        "notes": args.notes or None,
        "protocol": {
            "samples_per_path": args.samples,
            "warmup_reads_per_path": args.warmup,
            "pairing": "one direct and one aggregated Read per pair",
            "order": "balanced AB/BA order, shuffled with recorded seed",
            "seed": args.seed,
            "timer": "time.perf_counter_ns",
            "timed_operation": "asyncua Node.read_data_value only",
            "session_policy": "one persistent session per endpoint",
            "security_policy": "asyncua default (None); anonymous client",
            "value_equality_required": not args.allow_value_mismatch,
        },
        "targets": {
            "direct": {
                "endpoint": args.direct_endpoint,
                "node_id": args.direct_node_id,
            },
            "aggregated": {
                "endpoint": args.aggregated_endpoint,
                "node_id": args.aggregated_node_id,
            },
        },
        "client_environment": {
            "hostname": platform.node(),
            "platform": platform.platform(),
            "python": sys.version,
            "asyncua": asyncua_version,
            "aggregation_server_repository": git_state(server_repository),
        },
        "artifacts": {
            "staging_run_directory": str(run_dir),
            "measurements_csv": "measurements.csv",
            "outcome_json": "outcome.json",
        },
        "promotion_status": "disabled" if args.no_promote else "pending",
        "canonical_run_directory": None,
    }
    write_json(run_dir / "manifest.json", manifest)

    try:
        outcome, _rows = asyncio.run(acquire(args, run_dir))
        write_json(run_dir / "outcome.json", outcome)
        manifest["runtime_status"] = "complete"
        manifest["scientific_validation_status"] = outcome[
            "scientific_validation_status"
        ]
        manifest["ended_at_utc"] = utc_text()
        manifest["artifacts"]["measurements_sha256"] = sha256_file(
            run_dir / "measurements.csv"
        )
        manifest["artifacts"]["outcome_sha256"] = sha256_file(
            run_dir / "outcome.json"
        )
        write_json(run_dir / "manifest.json", manifest)

        destination = None
        if not args.no_promote:
            destination = promote_run(run_dir, args.raw_root.resolve(), manifest)

        print(f"E03 staging run: {run_dir}")
        if destination:
            print(f"E03 preserved raw run: {destination}")
        print(f"Scientific status: {outcome['scientific_validation_status']}")
        return 0 if outcome["scientific_validation_status"] == "passed" else 2
    except Exception as exc:
        manifest["runtime_status"] = "failed"
        manifest["scientific_validation_status"] = "failed"
        manifest["ended_at_utc"] = utc_text()
        manifest["failure"] = {
            "type": f"{type(exc).__module__}.{type(exc).__qualname__}",
            "message": str(exc),
        }
        write_json(run_dir / "manifest.json", manifest)
        print(f"E03 acquisition failed; staging retained at {run_dir}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
