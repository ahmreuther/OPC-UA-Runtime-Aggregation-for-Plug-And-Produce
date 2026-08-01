#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Adrian Reuther
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

"""Populate the legacy network-rate column from a filtered packet capture.

The CSV column name ``net_in_mbps`` is retained for compatibility with the
existing paper analysis. Its value is MiB/s, not Mbit/s. For packet-capture
runs it contains the sum of captured IPv4 total lengths and IPv6
``40 + payload_length`` values in each actual resource-sampling interval,
divided by 1024**2 and that interval's measured duration.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import subprocess
import sys
import time
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pcap", type=Path, required=True)
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--tshark", required=True)
    parser.add_argument("--measurement-start-epoch", type=float, required=True)
    parser.add_argument("--summary-json", type=Path)
    parser.add_argument(
        "--expected-ip",
        action="append",
        default=[],
        help=(
            "Require the capture to contain exactly these IP endpoints. "
            "Repeat once per expected endpoint."
        ),
    )
    return parser.parse_args()


def first_integer(value: str) -> int | None:
    value = value.strip()
    if not value:
        return None
    first = value.split(",", 1)[0]
    try:
        parsed = int(first)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


def read_intervals(csv_path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames
        if fieldnames is None:
            raise ValueError(f"CSV has no header: {csv_path}")
        required = {
            "net_in_mbps",
            "sample_interval_seconds_actual",
            "elapsed_seconds",
        }
        missing = sorted(required.difference(fieldnames))
        if missing:
            raise ValueError(
                f"CSV is missing required columns: {', '.join(missing)}"
            )
        rows = list(reader)

    if not rows:
        raise ValueError(f"CSV has no resource samples: {csv_path}")

    previous_end = 0.0
    for index, row in enumerate(rows, start=1):
        interval = float(row["sample_interval_seconds_actual"])
        end = float(row["elapsed_seconds"])
        start = end - interval
        if (
            not math.isfinite(interval)
            or not math.isfinite(start)
            or not math.isfinite(end)
            or interval <= 0
            or start < -0.25
            or end <= previous_end
        ):
            raise ValueError(
                f"Invalid sample interval in row {index}: "
                f"start={start}, end={end}, interval={interval}"
            )
        row["_interval_start"] = str(max(0.0, start))
        row["_interval_end"] = str(end)
        row["_captured_ip_bytes"] = "0"
        previous_end = end
    return rows, fieldnames


def packet_lengths(
    tshark: str,
    pcap_path: Path,
) -> tuple[subprocess.Popen[str], list[str]]:
    command = [
        tshark,
        "-r",
        str(pcap_path),
        "-Y",
        "ip || ipv6",
        "-T",
        "fields",
        "-E",
        "occurrence=f",
        "-e",
        "frame.time_epoch",
        "-e",
        "ip.len",
        "-e",
        "ipv6.plen",
        "-e",
        "ip.src",
        "-e",
        "ip.dst",
        "-e",
        "ipv6.src",
        "-e",
        "ipv6.dst",
    ]
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return process, command


def add_capture_bytes(
    rows: list[dict[str, str]],
    pcap_path: Path,
    tshark: str,
    measurement_start_epoch: float,
) -> tuple[int, int, list[str], set[str]]:
    process, command = packet_lengths(tshark, pcap_path)
    assert process.stdout is not None

    interval_index = 0
    packet_count = 0
    byte_count = 0
    observed_ip_endpoints: set[str] = set()
    for raw_line in process.stdout:
        fields = raw_line.rstrip("\r\n").split("\t")
        if len(fields) < 7 or not fields[0]:
            continue
        try:
            relative_seconds = float(fields[0]) - measurement_start_epoch
        except ValueError:
            continue

        ipv4_length = first_integer(fields[1])
        ipv6_payload_length = first_integer(fields[2])
        if ipv4_length is not None:
            ip_length = ipv4_length
        elif ipv6_payload_length is not None:
            ip_length = 40 + ipv6_payload_length
        else:
            continue
        for endpoint in fields[3:7]:
            endpoint = endpoint.strip()
            if endpoint:
                observed_ip_endpoints.add(endpoint.split(",", 1)[0])

        while (
            interval_index < len(rows)
            and relative_seconds
            > float(rows[interval_index]["_interval_end"])
        ):
            interval_index += 1
        if interval_index >= len(rows):
            continue

        interval_start = float(rows[interval_index]["_interval_start"])
        interval_end = float(rows[interval_index]["_interval_end"])
        if relative_seconds < interval_start or relative_seconds > interval_end:
            continue

        current = int(rows[interval_index]["_captured_ip_bytes"])
        rows[interval_index]["_captured_ip_bytes"] = str(current + ip_length)
        packet_count += 1
        byte_count += ip_length

    stderr = ""
    if process.stderr is not None:
        stderr = process.stderr.read()
    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(
            f"tshark failed with exit code {return_code}: {stderr.strip()}"
        )
    return packet_count, byte_count, command, observed_ip_endpoints


def write_updated_csv(
    csv_path: Path,
    rows: list[dict[str, str]],
    fieldnames: list[str],
) -> None:
    temp_path = csv_path.with_name(f".{csv_path.name}.{os.getpid()}.tmp")
    try:
        with temp_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=fieldnames,
                lineterminator="\n",
            )
            writer.writeheader()
            for row in rows:
                interval = float(row["sample_interval_seconds_actual"])
                captured_bytes = int(row["_captured_ip_bytes"])
                row["net_in_mbps"] = (
                    f"{captured_bytes / (1024 * 1024 * interval):.9f}"
                )
                writer.writerow(
                    {field: row.get(field, "") for field in fieldnames}
                )
        replace_error: OSError | None = None
        for _ in range(40):
            try:
                os.replace(temp_path, csv_path)
                replace_error = None
                break
            except PermissionError as exc:
                replace_error = exc
                time.sleep(0.25)
        if replace_error is not None:
            # Some synchronized Windows folders hold the destination open
            # long enough that ReplaceFile semantics remain unavailable.
            # The file is still staging-only here; promotion and hashing occur
            # later. Retry a direct, flushed rewrite and leave the temp intact
            # until the complete copy succeeds.
            rewrite_error: OSError | None = None
            payload = temp_path.read_bytes()
            for _ in range(40):
                try:
                    with csv_path.open("wb") as handle:
                        handle.write(payload)
                        handle.flush()
                        os.fsync(handle.fileno())
                    rewrite_error = None
                    break
                except PermissionError as exc:
                    rewrite_error = exc
                    time.sleep(0.25)
            if rewrite_error is not None:
                raise rewrite_error from replace_error
            temp_path.unlink()
    finally:
        if temp_path.exists():
            temp_path.unlink()


def main() -> int:
    args = parse_args()
    if not args.pcap.is_file():
        raise FileNotFoundError(args.pcap)
    if not args.csv.is_file():
        raise FileNotFoundError(args.csv)
    if not math.isfinite(args.measurement_start_epoch):
        raise ValueError("--measurement-start-epoch must be finite")

    rows, fieldnames = read_intervals(args.csv)
    packet_count, byte_count, command, observed_ip_endpoints = add_capture_bytes(
        rows,
        args.pcap,
        args.tshark,
        args.measurement_start_epoch,
    )
    expected_ip_endpoints = set(args.expected_ip)
    if expected_ip_endpoints and observed_ip_endpoints != expected_ip_endpoints:
        raise ValueError(
            "Captured IP endpoints do not match the expected physical path: "
            f"observed={sorted(observed_ip_endpoints)}, "
            f"expected={sorted(expected_ip_endpoints)}"
        )
    write_updated_csv(args.csv, rows, fieldnames)

    summary = {
        "schema": "ojies.pcap-resource-network/v1",
        "pcap": str(args.pcap),
        "csv": str(args.csv),
        "measurement_start_epoch": args.measurement_start_epoch,
        "sample_count": len(rows),
        "captured_ip_packet_count_in_samples": packet_count,
        "captured_ip_bytes_in_samples": byte_count,
        "captured_ip_mib_in_samples": byte_count / (1024 * 1024),
        "observed_ip_endpoints": sorted(observed_ip_endpoints),
        "expected_ip_endpoints": sorted(expected_ip_endpoints),
        "tshark_command": command,
        "column": "net_in_mbps",
        "unit": "MiB/s",
    }
    if args.summary_json is not None:
        args.summary_json.parent.mkdir(parents=True, exist_ok=True)
        with args.summary_json.open(
            "w",
            encoding="utf-8",
            newline="\n",
        ) as handle:
            json.dump(summary, handle, indent=2)
            handle.write("\n")
    json.dump(summary, sys.stdout)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
