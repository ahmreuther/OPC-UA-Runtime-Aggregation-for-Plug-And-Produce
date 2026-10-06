#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Adrian Reuther
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

"""E08: loopback-only lower-server lifecycle and serialized burst diagnostics.

Build the robustness_fixture Rust example first. Each independent repetition
uses new source identities, random loopback ports, a fresh aggregation process,
and a temporary runtime directory. No LDS, mDNS, plant devices, or stored
runtime configurations are accessed. This is not the full discovery/rule
generation pipeline and is not a performance benchmark.
"""
from __future__ import annotations

import argparse
import asyncio
from contextlib import AsyncExitStack
from datetime import datetime, timezone
import hashlib
import json
import logging
from pathlib import Path
import platform
import socket
import subprocess
import sys
import tempfile
import time
import traceback
import uuid
import zipfile

from asyncua import Client, Server, ua

REPO = Path(__file__).resolve().parents[1]
WORKSPACE = REPO
PREFIX = "ROBUSTNESS_EVENT "


def dump_new(path, value):
    with Path(path).open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, ensure_ascii=False)
        stream.write("\n")


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def relative(path):
    try:
        return str(Path(path).resolve().relative_to(WORKSPACE)).replace("\\", "/")
    except ValueError:
        return str(Path(path).resolve())


def provenance(executable, failstop_executable):
    def git(*arguments):
        result = subprocess.run(
            ["git", "-c", f"safe.directory={REPO}", "-C", str(REPO), *arguments],
            capture_output=True, text=True, encoding="utf-8", errors="replace", check=False,
        )
        return {"exit_code": result.returncode, "stdout": result.stdout, "stderr": result.stderr}

    source_paths = [path for path in (REPO / "opcua_agg_server" / "src").rglob("*") if path.is_file()]
    source_paths += [path for path in (REPO / "opcua_agg_server" / "vendor" / "opcua" / "src").rglob("*") if path.is_file()]
    source_paths += [Path(__file__), REPO / "opcua_agg_server/examples/robustness_fixture.rs",
                     REPO / "opcua_agg_server/Cargo.toml", REPO / "opcua_agg_server/Cargo.lock",
                     REPO / "opcua_agg_server/examples/onboarding_failstop_fixture.rs",
                     REPO / "opcua_agg_server/vendor/opcua/Cargo.toml",
                     REPO / "opcua_agg_server/vendor/opcua/Cargo.lock",
                     REPO / "tools/testing/test_onboarding_robustness_harness.py"]
    return {
        "git_head": git("rev-parse", "HEAD"),
        "git_status": git("status", "--porcelain=v1", "--untracked-files=normal"),
        "git_diff": git("diff", "--binary", "--", "opcua_agg_server/src",
                        "opcua_agg_server/vendor/opcua/src", "opcua_agg_server/Cargo.toml",
                        "opcua_agg_server/Cargo.lock"),
        "binary": {"path": relative(executable), "sha256": sha256(executable)},
        "failstop_binary": {"path": relative(failstop_executable), "sha256": sha256(failstop_executable)},
        "sources": {relative(path): sha256(path) for path in sorted(set(source_paths))},
        "python": sys.version, "platform": platform.platform(),
        "asyncua_version": __import__("asyncua").__version__,
    }


def install_namespace_fault_adapter():
    """Delay one NamespaceArray Read in our tagged source only.

    This is a synthetic fault injector; it changes no installed asyncua files.
    The service is released after Rust reports worker cleanup, to test a late
    response. No production server or untagged source is affected.
    """
    from asyncua.server.uaprocessor import UaProcessor
    from asyncua.ua.ua_binary import struct_from_binary

    if getattr(UaProcessor, "_ojies_robustness_adapter", False):
        return
    original = UaProcessor._process_message

    async def process(processor, typeid, requesthdr, seqhdr, body):
        fault = getattr(processor.iserver, "_ojies_robustness_fault", None)
        if fault and typeid == ua.NodeId(ua.ObjectIds.ReadRequest_Encoding_DefaultBinary):
            parameters = struct_from_binary(ua.ReadParameters, body.copy())
            namespace_request = any(
                item.NodeId == ua.NodeId(ua.ObjectIds.Server_NamespaceArray)
                for item in parameters.NodesToRead
            )
            if namespace_request and not fault["entered"].is_set():
                response = ua.ReadResponse()
                response.Results = await processor.session.read(parameters)
                fault["entered"].set()
                fault["log"]({"event": "namespace_read_held", "source": fault["name"]})

                async def delayed_response():
                    # Keep the prepared reply independently of UaProcessor's
                    # connection task, which asyncua cancels when Rust closes
                    # the timed-out transport. Otherwise there is no late-reply
                    # attempt and the late-response gate must correctly fail.
                    await fault["release"].wait()
                    fault["log"]({"event": "late_response_attempted", "source": fault["name"]})
                    try:
                        processor.send_response(requesthdr.RequestHandle, seqhdr, response)
                    except Exception as error:
                        fault["log"]({"event": "late_response_rejected", "source": fault["name"],
                                      "error": repr(error)})
                    else:
                        fault["log"]({"event": "late_response_returned", "source": fault["name"]})

                fault.setdefault("pending", []).append(asyncio.create_task(delayed_response()))
                return True
        return await original(processor, typeid, requesthdr, seqhdr, body)

    UaProcessor._process_message = process
    UaProcessor._ojies_robustness_adapter = True


async def counter_in_aggregate(client, source_name, namespace, letter):
    namespaces = await client.get_namespace_array()
    for ref in await client.nodes.objects.get_children_descriptions():
        if ref.BrowseName.Name == source_name:
            root = client.get_node(ref.NodeId)
            break
    else:
        raise AssertionError(f"source root missing: {source_name}")
    pending = [(root, 0)]
    seen = set()
    while pending:
        node, depth = pending.pop(0)
        if node.nodeid in seen or depth > 4:
            continue
        seen.add(node.nodeid)
        assert len(seen) < 100, "bounded fixture browse exceeded 100 nodes"
        for ref in await node.get_children_descriptions():
            if (ref.BrowseName.Name == f"RobustCounter_{letter}"
                    and ref.NodeId.NamespaceIndex < len(namespaces)
                    and namespaces[ref.NodeId.NamespaceIndex].startswith(namespace)):
                return client.get_node(ref.NodeId)
            if ref.NodeClass == ua.NodeClass.Object and ref.BrowseName.Name != "Server":
                pending.append((client.get_node(ref.NodeId), depth + 1))
    raise AssertionError(f"counter missing for {source_name}")


def clean_counts(event):
    assert event.get("info_exists") is False, f"residual source status: {event}"
    counts = event["counts"]
    assert counts, "empty cleanup diagnostic cannot establish cleanup"
    assert all(value is False or (type(value) is int and value == 0)
               for value in counts.values()), f"residual source state: {counts}"


def validate_events(events, fault_events, reads, checks, plan):
    terminals = [item for item in events if item["event"] == "terminal"]
    expected = {source["name"]: source for source in plan["sources"]}
    assert len(terminals) == len(expected), "missing or duplicate terminal event"
    assert len({item["source"] for item in terminals}) == len(expected)
    starts = [item for item in events if item["event"] == "started"]
    assert [item["source"] for item in starts] == [source["name"] for source in plan["sources"]]
    for terminal in terminals:
        source = expected[terminal["source"]]
        if source["kind"] == "healthy":
            assert terminal["outcome"] == "completed", terminal
        else:
            assert terminal["outcome"] in ("failed", "deadline"), terminal
            assert terminal["elapsed_ms"] <= plan["fault_timeout_ms"] + 1500, terminal
    # Serial completion: no later source starts before the preceding source has
    # terminally completed, and failed sources are cleaned before continuing.
    for previous, following in zip(starts, starts[1:]):
        terminal_index = next(i for i, item in enumerate(events)
                              if item["event"] == "terminal" and item["source"] == previous["source"])
        assert events.index(following) > terminal_index
        if previous["kind"] != "healthy":
            cleanup_index = next(i for i, item in enumerate(events)
                                 if item["event"] == "cleanup" and item["source"] == previous["source"])
            assert events.index(following) > cleanup_index
    for event in events:
        if event["event"] in ("cleanup", "late_observation"):
            clean_counts(event)
    burst = [source["name"] for source in plan["sources"] if source["batch"] == "simultaneous"]
    burst_queued = [events.index(item) for item in events
                    if item["event"] == "queued" and item["source"] in burst]
    burst_started = [events.index(item) for item in starts if item["source"] in burst]
    assert len(burst_queued) == 3 and max(burst_queued) < min(burst_started)
    for kind, required_event in (("namespace_stall", "namespace_read_held"),
                                 ("ack_stall", "tcp_bytes_without_ack")):
        name = next(source["name"] for source in plan["sources"] if source["kind"] == kind)
        assert any(event["event"] == required_event for event in fault_events), required_event
        if kind == "ack_stall":
            assert any(event["event"] == required_event
                       and event.get("prefix_hex", "").startswith("48454c46")
                       for event in fault_events), "ACK stall did not receive an OPC UA HELF message"
        phase_reads = [item for item in reads if item["phase"] == name]
        assert phase_reads and all(item["ok"] for item in phase_reads), (name, phase_reads)
    assert any(event["event"] == "late_response_attempted" for event in fault_events)
    assert len(checks) == 5 and all(check["ok"] for check in checks)
    assert events[-1]["event"] == "finished"
    return {
        "exact_terminal_results": True, "faults_injected": True,
        "bounded_fault_outcomes": True, "failed_state_removed": True,
        "no_late_state_reappearance": True, "following_source_completes": True,
        "burst_queued_then_serialized": True, "healthy_read_during_faults": True,
        "five_healthy_proxies_readable": True,
    }


async def run_failstop(executable, bundle):
    started = time.monotonic()
    process = await asyncio.create_subprocess_exec(
        str(executable), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), 10)
    except BaseException:
        if process.returncode is None:
            process.kill()
            await process.wait()
        raise
    elapsed_ms = (time.monotonic() - started) * 1000
    for name, content in (("failstop.stdout.log", stdout), ("failstop.stderr.log", stderr)):
        with (bundle / name).open("xb") as stream:
            stream.write(content)
    output = stdout.decode("utf-8", "replace")
    result = {"returncode": process.returncode, "process_elapsed_ms": elapsed_ms}
    result_lines = [line for line in output.splitlines() if line.startswith("FAILSTOP_RESULT ")]
    if result_lines:
        result["supervisor"] = json.loads(result_lines[-1][len("FAILSTOP_RESULT "):])
    dump_new(bundle / "failstop.json", result)
    assert process.returncode == 70, result
    assert "WORKER_WAITING_ON_HELD_MUTEX" in output, "worker never reached injected mutex block"
    assert "FOLLOWING_SOURCE_STARTED" not in output, "unsafe second writer was started"
    assert len(result_lines) == 1 and result["supervisor"]["following_source_rejected"]
    assert result["supervisor"]["outcome"] == "UnsafeToContinue"
    assert elapsed_ms < 2000, result
    return result


async def run_once(args, repetition):
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ") + "-" + uuid.uuid4().hex[:8]
    bundle = args.output_root / run_id
    bundle.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    run_events, fault_events, reads, checks = [], [], [], []
    phase = {"source": "startup"}
    exception = None
    process = None
    reader_tasks, source_tasks = [], []
    client = None
    probe = None
    runtime_directory = None
    result_queue = asyncio.Queue()
    ports = set()

    def next_port():
        while (port := free_port()) in ports:
            pass
        ports.add(port)
        return port

    prefix = f"urn:ojies:e08:{run_id}"
    sources = []
    for letter in "ABCDEF":
        sources.append({"name": f"{prefix}:{letter}___{letter}", "port": next_port(),
                        "kind": "namespace_stall" if letter == "B" else "healthy",
                        "batch": "simultaneous" if letter in "DEF" else letter,
                        "letter": letter, "namespace": f"{prefix}:{letter}"})
    sources.append({"name": f"{prefix}:ACK___ACK", "port": next_port(),
                    "kind": "ack_stall", "batch": "ACK", "letter": "ACK"})
    plan = {"aggregation_port": next_port(), "healthy_timeout_ms": args.healthy_timeout_ms,
            "fault_timeout_ms": args.fault_timeout_ms,
            "cleanup_timeout_ms": args.cleanup_timeout_ms,
            "late_observation_ms": 750, "sources": sources}
    dump_new(bundle / "plan.json", plan)
    captured_provenance = provenance(args.aggregator_exe, args.failstop_exe)
    if args.build_log:
        with (bundle / "build.log").open("xb") as stream:
            stream.write(args.build_log.read_bytes())
        captured_provenance["build_log"] = {"path": relative(args.build_log), "sha256": sha256(args.build_log)}
    with zipfile.ZipFile(bundle / "source_snapshot.zip", "x", compression=zipfile.ZIP_DEFLATED) as snapshot:
        for source_path, expected_hash in captured_provenance["sources"].items():
            payload = (WORKSPACE / source_path).read_bytes()
            assert hashlib.sha256(payload).hexdigest() == expected_hash, "source changed while capturing provenance"
            snapshot.writestr(source_path, payload)
    dump_new(bundle / "provenance.json", captured_provenance)
    source_log = (bundle / "source_events.jsonl").open("x", encoding="utf-8")

    def fault_log(event):
        event = {**event, "elapsed_ms": (time.monotonic() - started) * 1000}
        fault_events.append(event)
        source_log.write(json.dumps(event) + "\n")
        source_log.flush()

    fault = {"name": sources[1]["name"], "entered": asyncio.Event(),
             "release": asyncio.Event(), "log": fault_log}
    install_namespace_fault_adapter()
    python_log = logging.FileHandler(bundle / "source_diagnostics.log", mode="x", encoding="utf-8")
    logging.getLogger().addHandler(python_log)
    logging.getLogger().setLevel(logging.WARNING)

    async def stream_reader(stream, path, parse):
        with path.open("x", encoding="utf-8") as output:
            while line := await stream.readline():
                text = line.decode("utf-8", "replace")
                output.write(text)
                output.flush()
                if parse and text.startswith(PREFIX):
                    event = json.loads(text[len(PREFIX):])
                    event["received_elapsed_ms"] = (time.monotonic() - started) * 1000
                    run_events.append(event)
                    await result_queue.put(event)
            if parse:
                await result_queue.put({"event": "eof"})

    async def probe_healthy(node):
        while True:
            phase_at_start = phase["source"]
            begin = time.monotonic()
            try:
                value = await asyncio.wait_for(node.read_value(), 3)
                observation = {"ok": value == 1001, "value": value}
            except Exception as error:
                observation = {"ok": False, "error": repr(error)}
            reads.append({**observation, "phase": phase_at_start,
                          "elapsed_ms": (begin - started) * 1000,
                          "duration_ms": (time.monotonic() - begin) * 1000})
            await asyncio.sleep(0.1)

    async def blackhole(reader, writer):
        source_tasks.append(asyncio.current_task())
        try:
            data = await reader.readexactly(8)
            fault_log({"event": "tcp_bytes_without_ack", "header_bytes": len(data), "prefix_hex": data.hex()})
            while await reader.read(8192):
                pass
        finally:
            writer.close()
            await writer.wait_closed()

    try:
        await run_failstop(args.failstop_exe, bundle)
        async with AsyncExitStack() as stack:
            for spec in sources[:6]:
                source = Server()
                await source.init()
                await source.set_application_uri(spec["namespace"])
                source.set_endpoint(f"opc.tcp://127.0.0.1:{spec['port']}/")
                source.set_security_policy([ua.SecurityPolicyType.NoSecurity])
                source.set_server_name(f"E08 isolated source {spec['letter']}")
                namespace = await source.register_namespace(spec["namespace"])
                device = await source.nodes.objects.add_object(ua.NodeId("Device", namespace), "Device")
                await device.add_variable(ua.NodeId("Counter", namespace),
                                          f"RobustCounter_{spec['letter']}", 1001)
                if spec["kind"] == "namespace_stall":
                    source.iserver._ojies_robustness_fault = fault
                await stack.enter_async_context(source)
            ack = await asyncio.start_server(blackhole, "127.0.0.1", sources[-1]["port"])
            await stack.enter_async_context(ack)
            runtime_directory = tempfile.TemporaryDirectory(prefix="ojies-e08-")
            runtime = runtime_directory.name
            process = await asyncio.create_subprocess_exec(
                str(args.aggregator_exe), str((bundle / "plan.json").resolve()),
                cwd=runtime, stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
            )
            reader_tasks = [
                asyncio.create_task(stream_reader(process.stdout, bundle / "aggregation.stdout.log", True)),
                asyncio.create_task(stream_reader(process.stderr, bundle / "aggregation.stderr.log", False)),
            ]
            while True:
                event = await asyncio.wait_for(result_queue.get(), 60)
                if event["event"] == "eof":
                    raise AssertionError(f"fixture closed before finished (returncode={process.returncode})")
                if event["event"] == "started":
                    phase["source"] = event["source"]
                elif event["event"] == "cleanup" and event["source"] == fault["name"]:
                    assert fault["entered"].is_set(), "namespace fault was not reached"
                    fault["release"].set()
                    process.stdin.write(f"released {fault['name']}\n".encode())
                    await process.stdin.drain()
                elif (event["event"] == "terminal" and event["source"] == sources[0]["name"]
                      and event["outcome"] == "completed"):
                    client = Client(f"opc.tcp://127.0.0.1:{plan['aggregation_port']}/", timeout=3)
                    await client.connect()
                    first = await counter_in_aggregate(client, sources[0]["name"], sources[0]["namespace"], "A")
                    probe = asyncio.create_task(probe_healthy(first))
                elif event["event"] == "all_sources_processed":
                    assert client is not None
                    phase["source"] = "final_checks"
                    if probe:
                        probe.cancel()
                        await asyncio.gather(probe, return_exceptions=True)
                        probe = None
                    for spec in sources:
                        if spec["kind"] == "healthy":
                            node = await counter_in_aggregate(client, spec["name"], spec["namespace"], spec["letter"])
                            value = await node.read_value()
                            checks.append({"source": spec["name"], "ok": value == 1001,
                                           "value": value, "node_id": node.nodeid.to_string()})
                    await client.disconnect()
                    client = None
                    process.stdin.write(b"verified\n")
                    await process.stdin.drain()
                elif event["event"] == "finished":
                    break
            returncode = await asyncio.wait_for(process.wait(), 10)
            assert returncode == 0, f"fixture exit {returncode}"
            await asyncio.gather(*reader_tasks)
        gates = validate_events(run_events, fault_events, reads, checks, plan)
        gates["held_mutex_process_failstop"] = True
        assert sha256(args.aggregator_exe) == captured_provenance["binary"]["sha256"], "fixture rebuilt during run"
        assert sha256(args.failstop_exe) == captured_provenance["failstop_binary"]["sha256"], "fail-stop fixture rebuilt during run"
        gates["binaries_unchanged_during_run"] = True
    except BaseException as error:
        exception = {"type": type(error).__name__, "message": str(error), "traceback": traceback.format_exc()}
        gates = {}
    finally:
        fault["release"].set()
        if probe:
            probe.cancel()
            await asyncio.gather(probe, return_exceptions=True)
        if client:
            try:
                await asyncio.wait_for(client.disconnect(), 3)
            except Exception:
                client.disconnect_socket()
        if process and process.returncode is None:
            process.kill()
            await process.wait()
        for task in source_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*reader_tasks, *source_tasks, *fault.get("pending", []), return_exceptions=True)
        if runtime_directory:
            runtime_directory.cleanup()
        source_log.close()
        logging.getLogger().removeHandler(python_log)
        python_log.close()
    dump_new(bundle / "aggregation_events.json", run_events)
    dump_new(bundle / "healthy_reads.json", reads)
    dump_new(bundle / "proxy_checks.json", checks)
    summary = {"experiment": "E08", "run_id": run_id, "repetition": repetition,
               "status": "passed" if exception is None else "failed", "exception": exception,
               "scope": "isolated lower-server lifecycle, fault injection and serialized burst",
               "gates": gates, "terminal_results": [event for event in run_events if event["event"] == "terminal"],
               "read_count": len(reads), "read_failures": sum(not read["ok"] for read in reads),
               "elapsed_seconds": time.monotonic() - started}
    dump_new(bundle / "summary.json", summary)
    required_artifacts = {
        "plan.json", "provenance.json", "aggregation.stdout.log", "aggregation.stderr.log",
        "source_diagnostics.log", "source_events.jsonl", "aggregation_events.json",
        "healthy_reads.json", "proxy_checks.json", "failstop.stdout.log", "failstop.stderr.log",
        "failstop.json", "summary.json", "source_snapshot.zip",
    }
    if args.build_log:
        required_artifacts.add("build.log")
    missing_artifacts = sorted(required_artifacts - {path.name for path in bundle.iterdir() if path.is_file()})
    dump_new(bundle / "manifest.json", {
        "run_id": run_id, "created_utc": datetime.now(timezone.utc).isoformat(),
        "artifact_set_status": "incomplete" if missing_artifacts else "complete",
        "missing_artifacts": missing_artifacts, "diagnostic_validation_status": summary["status"],
        "files": {path.name: {"sha256": sha256(path), "size": path.stat().st_size}
                  for path in sorted(bundle.iterdir()) if path.is_file()},
    })
    (bundle / "COMPLETED").write_text("artifact capture completed\n", encoding="utf-8")
    if exception is None:
        (bundle / "VALIDATED").write_text("bounded E08 diagnostic gates passed\n", encoding="utf-8")
    print(json.dumps({"run_id": run_id, "status": summary["status"], "folder": relative(bundle),
                      "error": exception["message"] if exception else None}), flush=True)
    return summary


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aggregator-exe", type=Path, required=True)
    parser.add_argument("--failstop-exe", type=Path)
    parser.add_argument("--build-log", type=Path)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--output-root", type=Path, default=WORKSPACE / "Validation/integration/raw_data/onboarding_robustness")
    parser.add_argument("--healthy-timeout-ms", type=int, default=20000)
    parser.add_argument("--fault-timeout-ms", type=int, default=2000)
    parser.add_argument("--cleanup-timeout-ms", type=int, default=10000)
    args = parser.parse_args()
    args.aggregator_exe = args.aggregator_exe.resolve(strict=True)
    if args.failstop_exe is None:
        args.failstop_exe = args.aggregator_exe.with_name("onboarding_failstop_fixture" + args.aggregator_exe.suffix)
    args.failstop_exe = args.failstop_exe.resolve(strict=True)
    if args.build_log:
        args.build_log = args.build_log.resolve(strict=True)
    args.output_root = args.output_root.resolve()
    if args.repetitions < 1 or min(args.healthy_timeout_ms, args.fault_timeout_ms, args.cleanup_timeout_ms) < 100:
        parser.error("repetitions must be positive and all budgets at least 100 ms")
    results = [await asyncio.wait_for(run_once(args, index + 1), 300) for index in range(args.repetitions)]
    return 0 if all(result["status"] == "passed" for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
