#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Adrian Reuther
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

"""
run_and_export.py

Startet den OPC UA Aggregationsserver (opcua_agg_server), kann die 16
deterministischen E01-Quellserver selbst orchestrieren, wartet auf terminale
per-source Ergebnisse, exportiert danach den kompletten Address Space als XML
(via NodeXmlExporter.py) und liest anschliessend

    - config.json
    - namespaces.json
    - entry_points.json
    - rules.json
    - rules_executor.json
    - nodeset_mapping.json
    - aggregation_results.jsonl
    - <exported_address_space>.xml

aus. Die Rohwerte und die daraus abgeleiteten CSV-Dateien werden als
eindeutiger Validierungslauf unter

    Validation/integration/raw_data/semantic_integration/<run_id>/

gespeichert. Nur ein technisch vollstaendiger Lauf, der alle E01-Gates
besteht, erhaelt ``VALIDATED`` und aktualisiert ``latest_complete_run.txt``.
Bestehende Laeufe werden nie ueberschrieben.

Am Server-Code (Rust) und an XmlExporter.py/NodeXmlExporter.py wird nichts
veraendert.

--------------------------------------------------------------------------
Ordnerstruktur (dieses Skript liegt zusammen mit den beiden Exporter-
Dateien unter tools/testing/):

    tools/
      testing/
        run_and_export.py      <- dieses Skript
        NodeXmlExporter.py
        XmlExporter.py

    opcua_agg_server/         <- Rust-Projekt
      Cargo.toml
      config.json
      rules.json
      ...

Die Pfade werden aus der Repository-Struktur abgeleitet. CARGO_TARGET_DIR
kann gesetzt werden, falls das Release-Binary an einem anderen Ort liegt.
OJIES_SEMANTIC_INTEGRATION_OUTPUT_DIR kann das Zielverzeichnis fuer die
Validierungsartefakte ueberschreiben.
--------------------------------------------------------------------------

Annahme (nicht verifiziert, da XmlExporter.py's Basisklasse
asyncua.common.xmlexporter.XmlExporter verwendet wird): Der XML-Export ist
Standard OPC UA NodeSet2 XML (<UAObject NodeId="..." BrowseName="...">...
<References><Reference ReferenceType="..." IsForward="...">...). Falls das
bei euch abweicht, muss nur parse_nodeset_xml() angepasst werden.
"""

from __future__ import annotations

import csv
import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import tomllib
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse
from uuid import uuid4


# ==========================================================================
# KONFIGURATION - bitte an eure Umgebung anpassen
# ==========================================================================

# Repository, OJIES-Workspace und Rust-Projekt (enthaelt Cargo.toml,
# config.json, rules.json, ...)
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_ROOT = REPOSITORY_ROOT.parent
PROJECT_ROOT = REPOSITORY_ROOT / "opcua_agg_server"

# Kommando zum Starten des Aggregationsservers.
# WICHTIG: direkt das fertig gebaute Binary starten (kein "cargo run"),
# damit nicht bei jedem Start neu geprueft/gebaut wird. Einmalig bauen mit:
#   cd opcua_agg_server && cargo build --release --package roboteach_aggserver --bin roboteach_aggserver
_SERVER_BINARY = "roboteach_aggserver.exe" if sys.platform.startswith("win") else "roboteach_aggserver"


def cargo_target_dir() -> Path:
    configured = os.environ.get("CARGO_TARGET_DIR")
    if configured is None:
        cargo_config = PROJECT_ROOT / ".cargo" / "config.toml"
        if cargo_config.exists():
            # Some Windows editors write a UTF-8 BOM. ``tomllib.load`` rejects
            # that marker, whereas ``utf-8-sig`` removes it without changing
            # ordinary UTF-8 files.
            config_text = cargo_config.read_text(encoding="utf-8-sig")
            configured = tomllib.loads(config_text).get("build", {}).get("target-dir")

    if configured is None:
        default_target = PROJECT_ROOT / "target"
        if sys.platform.startswith("win"):
            shared_windows_target = Path("C:/cargo-target") / PROJECT_ROOT.name
            binary_name = "roboteach_aggserver.exe"
            if (
                (shared_windows_target / "release" / binary_name).is_file()
                and not (default_target / "release" / binary_name).is_file()
            ):
                return shared_windows_target
        return default_target

    target_dir = Path(configured).expanduser()
    return target_dir if target_dir.is_absolute() else PROJECT_ROOT / target_dir


CARGO_TARGET_DIR = cargo_target_dir()
SERVER_CMD = [str(CARGO_TARGET_DIR / "release" / _SERVER_BINARY)]

# Ordner mit NodeXmlExporter.py und XmlExporter.py.
# Liegen standardmaessig direkt neben diesem Skript (gleicher Ordner).
EXPORTER_DIR = Path(__file__).resolve().parent
NODE_XML_EXPORTER = EXPORTER_DIR / "NodeXmlExporter.py"

# Python-Interpreter fuer den Exporter (venv des Servers, falls vorhanden)
_VENV_DIR = PROJECT_ROOT / "python_service" / ".venv"
if sys.platform.startswith("win"):
    _VENV_PY = _VENV_DIR / "Scripts" / "python.exe"
else:
    _VENV_PY = _VENV_DIR / "bin" / "python"
PYTHON_EXE = str(_VENV_PY) if _VENV_PY.exists() else sys.executable

# Paper-bezogene Ausgabe. Jeder Lauf bekommt einen unveraenderlichen Ordner
# mit stabilen Dateinamen. Das Ziel kann fuer isolierte Tests umgebogen werden.
_OUTPUT_DIR_OVERRIDE = os.environ.get("OJIES_SEMANTIC_INTEGRATION_OUTPUT_DIR")
SEMANTIC_INTEGRATION_ROOT = (
    Path(_OUTPUT_DIR_OVERRIDE).expanduser().resolve()
    if _OUTPUT_DIR_OVERRIDE
    else (
        WORKSPACE_ROOT
        / "Validation"
        / "integration"
        / "raw_data"
        / "semantic_integration"
    ).resolve()
)
LATEST_RUN_FILE = SEMANTIC_INTEGRATION_ROOT / "latest_complete_run.txt"

RUNTIME_INPUT_FILES = (
    "config.json",
    "namespaces.json",
    "entry_points.json",
    "rules.json",
    "rules_executor.json",
    "nodeset_mapping.json",
    "aggregation_results.jsonl",
)

# OPC UA Endpoint des Aggregationsservers. None = wird aus config.json (port)
# abgeleitet: opc.tcp://localhost:<port>
SERVER_ENDPOINT: Optional[str] = None
E01_AGGREGATION_HOST = os.environ.get("OJIES_OPCUA_HOST", "127.0.0.1").strip()
SERVER_STARTUP_TIMEOUT_SECONDS = float(
    os.environ.get("OJIES_SERVER_STARTUP_TIMEOUT_SECONDS", "120")
)

# Der Auswertungslauf benoetigt keinen URDF-Export. Die Antwort wird dem
# Aggregationsserver automatisch ueber seine eigene stdin-Pipe uebergeben.
ENABLE_URDF_EXPORT = False

# Hierarchische Referenztypen, die zum Aufbau der Browse-Path-Hierarchie aus
# dem XML-Export verwendet werden (fuer die Aufloesung der Regel-Pfade).
HIERARCHICAL_REF_TYPES = {
    "Organizes", "HasComponent", "HasProperty", "Aggregates", "HasChild",
    "HasOrderedComponent", "HasSubStateMachine", "HasNotifier",
    "HasEventSource", "HasAddIn",
}

OPC_UA_NAMESPACE_URI = "http://opcfoundation.org/UA/"
TREE_FORMING_REFERENCE_IDS = {"i=35", "i=46", "i=47", "i=49"}
REFERENCE_TRANSFORMATION_POLICY = (
    "normalize_tree_reference_to_forward_organizes"
)

# NodeClasses, die als "Node" im Address Space gezaehlt werden
COUNTED_NODE_TAGS = {
    "UAObject", "UAVariable", "UAMethod", "UAObjectType", "UAVariableType",
    "UAReferenceType", "UADataType", "UAView",
}
TYPE_NODE_TAGS = {
    "UAObjectType", "UAVariableType", "UAReferenceType", "UADataType",
}
STANDARD_REFERENCE_TYPE_IDS = {
    "References": "i=31",
    "NonHierarchicalReferences": "i=32",
    "HierarchicalReferences": "i=33",
    "HasChild": "i=34",
    "Organizes": "i=35",
    "HasEventSource": "i=36",
    "HasModellingRule": "i=37",
    "HasEncoding": "i=38",
    "HasDescription": "i=39",
    "HasTypeDefinition": "i=40",
    "GeneratesEvent": "i=41",
    "Aggregates": "i=44",
    "HasSubtype": "i=45",
    "HasProperty": "i=46",
    "HasComponent": "i=47",
    "HasNotifier": "i=48",
    "HasOrderedComponent": "i=49",
}


def is_standard_reference_type(reference_type: str, browse_name: str) -> bool:
    expected = STANDARD_REFERENCE_TYPE_IDS[browse_name]
    actual = STANDARD_REFERENCE_TYPE_IDS.get(reference_type, reference_type)
    return actual == expected or actual == f"ns=0;{expected}"

# ---------------------------------------------------------------------
# Domain-Zuordnung fuer die "Domain / Inst. / Rules / Targets"-Tabelle.
# Domain-Zuordnung fuer die Standard-Source-Server. DI kommt in mehreren
# Domains vor und wird daher hier nicht als alleiniger Domain-Indikator genutzt.
# ---------------------------------------------------------------------
NAMESPACE_TO_DOMAIN = {
    "http://opcfoundation.org/UA/Robotics/": "Robotics",
    "http://opcfoundation.org/UA/MachineVision/": "Machine Vision",
    "http://opcfoundation.org/UA/AdditiveManufacturing/": "Additive Manufacturing",
    "http://opcfoundation.org/UA/Woodworking/": "Woodworking",
}

# Zuordnung Domain -> Substring, der im config.json Host-"name" gesucht wird
# (passend zu euren URNs wie "urn:plcm:machine-vision:001").
DOMAIN_TO_HOST_KEYWORD = {
    "Robotics": "robotics",
    "Machine Vision": "machine-vision",
    "Additive Manufacturing": "additive-manufacturing",
    "Woodworking": "woodworking",
}


# ==========================================================================
# Hilfsfunktionen: JSON laden
# ==========================================================================

def load_json(path: Path):
    if not path.exists():
        print(f"[WARNUNG] Datei nicht gefunden: {path}")
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_jsonl(path: Path) -> list:
    """Read complete JSONL events and tolerate one concurrent trailing write."""
    if not path.exists():
        return []
    events = []
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        if not line.strip():
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            print(
                f"[WARNUNG] Unvollstaendige JSONL-Zeile ignoriert: "
                f"{path}:{line_number}"
            )
    return events


def latest_aggregation_results(events: list) -> list:
    """Collapse append-only source events to the last state per source."""
    latest = {}
    for event in events:
        if isinstance(event, dict) and event.get("source_name"):
            latest[event["source_name"]] = event
    return list(latest.values())


def path_element_name(element) -> str:
    """Return the BrowseName local part from legacy or qualified path JSON."""
    if isinstance(element, str):
        return element
    if isinstance(element, dict) and isinstance(element.get("name"), str):
        return element["name"]
    raise ValueError(f"Ungueltiges Browse-Path-Element: {element!r}")


def path_element_namespace_uri(element) -> Optional[str]:
    """Return an optional namespace URI; legacy string elements are unqualified."""
    if isinstance(element, str):
        return None
    if isinstance(element, dict):
        namespace_uri = element.get("namespace_uri")
        if namespace_uri is None or isinstance(namespace_uri, str):
            return namespace_uri
    raise ValueError(f"Ungueltiges Browse-Path-Element: {element!r}")


def canonical_path(path: list) -> tuple:
    """Hashable path representation that preserves namespace identity."""
    return tuple(
        (path_element_namespace_uri(element), path_element_name(element))
        for element in path
    )


def display_path(path: list) -> str:
    """Stable human-readable form for qualified and legacy paths."""
    parts = []
    for element in path:
        namespace_uri = path_element_namespace_uri(element)
        name = path_element_name(element)
        parts.append(f"{{{namespace_uri}}}{name}" if namespace_uri else name)
    return "/".join(parts)


def canonical_source_id(source_id: str) -> str:
    return source_id.strip().rstrip("/")


LEGACY_EXECUTOR_RULE_FIELDS = {
    "target_node", "source_node", "ref_type", "is_forward",
}


def executor_projection_schema(executor_rules: list) -> str:
    """Identify the historical or current executable rule representation."""
    if any(
        set(rule) - LEGACY_EXECUTOR_RULE_FIELDS
        for rule in executor_rules
        if isinstance(rule, dict)
    ):
        return "qualified_source_bound_v1"
    return "legacy_four_field_v1"


def executor_rule_projection(
        rule: dict, schema: str = "legacy_four_field_v1"
) -> dict:
    """Project one rich rule into the selected executable schema."""
    legacy = {
        "target_node": rule.get("target_node", []),
        "source_node": rule.get("source_node", []),
        "ref_type": rule.get("ref_type", []),
        "is_forward": rule.get("is_forward"),
    }
    if schema == "legacy_four_field_v1":
        return legacy
    if schema != "qualified_source_bound_v1":
        raise ValueError(f"Unbekanntes Executor-Regelschema: {schema}")
    return {
        "target_node": (
            rule.get("target_node_qualified") or rule.get("target_node", [])
        ),
        "source_node": (
            rule.get("source_node_qualified") or rule.get("source_node", [])
        ),
        "ref_type": rule.get("ref_type", []),
        "is_forward": rule.get("is_forward"),
        "source_id": rule.get("source_id", ""),
        "source_node_id": rule.get("source_node_id"),
        "reference_type": rule.get("reference_type"),
        "merge_policy": rule.get("merge_policy", ""),
        "merge_key": rule.get("merge_key", []),
    }


def deduplicated_executor_projection(
        rules: list, schema: str = "legacy_four_field_v1"
) -> list:
    """Project rich rules to the selected executor schema in stable order."""
    projected = []
    for rule in rules:
        candidate = executor_rule_projection(rule, schema)
        if candidate not in projected:
            projected.append(candidate)
    return projected


def namespace_uri_equivalent(left: Optional[str], right: Optional[str]) -> bool:
    return (
        left is not None
        and right is not None
        and left.strip().rstrip("/") == right.strip().rstrip("/")
    )


def namespace_uri_matches(expected: Optional[str], actual: Optional[str]) -> bool:
    """Match exact standardized URIs and their per-source postfixed variants."""
    if expected is None:
        return True
    if namespace_uri_equivalent(actual, expected):
        return True
    if actual is None:
        return False
    if expected.startswith("urn:"):
        prefix = expected.rstrip(":") + ":"
    else:
        prefix = expected.rstrip("/") + "/"
    return actual.startswith(prefix) and actual[len(prefix):].startswith("urn:")


def utc_now_iso() -> str:
    """Return a timezone-explicit timestamp suitable for manifests."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def create_run_directory(run_id: Optional[str] = None) -> Path:
    """Create one exclusive run directory; an existing run is never reused."""
    if run_id is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        run_id = f"{timestamp}-{uuid4().hex[:8]}"
    if not run_id or Path(run_id).name != run_id or run_id in {".", ".."}:
        raise ValueError(f"Ungueltige Run-ID: {run_id!r}")

    SEMANTIC_INTEGRATION_ROOT.mkdir(parents=True, exist_ok=True)
    run_dir = SEMANTIC_INTEGRATION_ROOT / run_id
    run_dir.mkdir(exist_ok=False)
    return run_dir


def workspace_path(path: Path) -> str:
    """Prefer portable workspace-relative paths in generated metadata."""
    resolved = path.resolve()
    try:
        return resolved.relative_to(WORKSPACE_ROOT.resolve()).as_posix()
    except ValueError:
        return str(resolved)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def sha256_directory_tree(root: Path) -> dict:
    """Hash file names and contents of a local dependency deterministically."""
    digest = hashlib.sha256()
    files = sorted(
        (path for path in root.rglob("*") if path.is_file()),
        key=lambda path: path.relative_to(root).as_posix(),
    )
    total_size = 0
    for path in files:
        relative_name = path.relative_to(root).as_posix()
        digest.update(relative_name.encode("utf-8"))
        digest.update(b"\0")
        with path.open("rb") as source:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                total_size += len(block)
                digest.update(block)
        digest.update(b"\0")
    return {
        "algorithm": "sha256(relative_path_nul_file_bytes_nul)",
        "sha256": digest.hexdigest(),
        "file_count": len(files),
        "total_size_bytes": total_size,
    }


def opcua_dependency_provenance() -> dict:
    """Describe and hash the exact OPC UA dependency selected by Cargo.toml."""
    manifest_path = PROJECT_ROOT / "Cargo.toml"
    result = {
        "cargo_manifest": workspace_path(manifest_path),
        "dependency_name": "opcua",
    }
    try:
        manifest = tomllib.loads(manifest_path.read_text(encoding="utf-8-sig"))
        declaration = manifest.get("dependencies", {}).get("opcua")
        result["cargo_declaration"] = declaration
        if isinstance(declaration, dict) and declaration.get("path"):
            dependency_root = (
                manifest_path.parent / str(declaration["path"])
            ).resolve()
            result.update({
                "source_type": "local_path",
                "resolved_path": workspace_path(dependency_root),
                "tree": sha256_directory_tree(dependency_root),
            })
        elif isinstance(declaration, dict) and declaration.get("git"):
            result.update({
                "source_type": "git",
                "git_url": declaration.get("git"),
                "git_rev": declaration.get("rev"),
                "git_branch": declaration.get("branch"),
                "git_tag": declaration.get("tag"),
            })
        else:
            result["source_type"] = "registry_or_unspecified"
    except (OSError, tomllib.TOMLDecodeError) as exc:
        result["capture_error"] = str(exc)
    return result


def observed_discovery_provenance(server_log_path: Path) -> dict:
    """Classify the discovery path from the preserved server console log."""
    result = {
        "evidence_file": workspace_path(server_log_path),
        "find_servers_success": False,
        "find_servers_on_network_success": False,
        "find_servers_on_network_fallback": False,
        "active_mode": "not_observed",
    }
    try:
        log_text = server_log_path.read_text(
            encoding="utf-8", errors="replace"
        )
    except OSError as exc:
        result["capture_error"] = str(exc)
        return result

    result["find_servers_success"] = "FindServers erfolgreich" in log_text
    result["find_servers_on_network_success"] = (
        "FindServersOnNetwork erfolgreich" in log_text
    )
    result["find_servers_on_network_fallback"] = (
        "FindServersOnNetwork nicht verfuegbar" in log_text
    )
    if (
        result["find_servers_success"]
        and result["find_servers_on_network_success"]
    ):
        result["active_mode"] = "find_servers_plus_find_servers_on_network"
    elif (
        result["find_servers_success"]
        and result["find_servers_on_network_fallback"]
    ):
        result["active_mode"] = "find_servers_with_registered_lds_fallback"
    elif result["find_servers_success"]:
        result["active_mode"] = "find_servers_only"
    return result


def file_record(path: Path, run_dir: Path) -> dict:
    return {
        "run_relative_path": path.resolve().relative_to(run_dir.resolve()).as_posix(),
        "workspace_path": workspace_path(path),
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def completion_events(path: Optional[Path], config: Optional[dict] = None) -> list:
    """Adapt the active server's immutable completion log to the E01 schema."""
    if path is None or not path.exists():
        return []
    addresses = {
        host.get("name"): host.get("address")
        for host in (config or {}).get("hosts", [])
        if isinstance(host, dict) and host.get("name")
    }
    events = []
    pattern = re.compile(
        r"^\[(?P<timestamp>[^]]+)] (?P<state>START|DONE AGGREGATED)\s+(?P<name>\S+)\s*$"
    )
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = pattern.match(line)
        if not match:
            continue
        completed = match.group("state") == "DONE AGGREGATED"
        name = match.group("name")
        events.append({
            "source_name": name,
            "source_address": addresses.get(name),
            "status": "completed" if completed else "processing",
            "accepted": True if completed else None,
            "timestamp": match.group("timestamp"),
            "evidence_source": "device_completion_log",
        })
    return events


def snapshot_runtime_inputs(
        run_dir: Path, completion_log_path: Optional[Path] = None
) -> dict[str, Path]:
    """Preserve the post-run JSON state before any further evaluation."""
    input_dir = run_dir / "inputs"
    input_dir.mkdir()
    snapshots: dict[str, Path] = {}
    for filename in RUNTIME_INPUT_FILES:
        source = PROJECT_ROOT / filename
        if filename == "aggregation_results.jsonl" and not source.exists():
            events = completion_events(
                completion_log_path, load_json(PROJECT_ROOT / "config.json") or {}
            )
            if events:
                target = input_dir / filename
                target.write_text(
                    "".join(json.dumps(event, ensure_ascii=False) + "\n" for event in events),
                    encoding="utf-8",
                )
                snapshots[filename] = target
                continue
        if not source.exists():
            print(f"[WARNUNG] Laufzeiteingabe fehlt und wurde nicht gesichert: {source}")
            continue
        target = input_dir / filename
        shutil.copy2(source, target)
        snapshots[filename] = target
    return snapshots


def repository_state() -> dict:
    """Capture the server repository revision without changing Git state."""
    state = {"commit": None, "dirty": None}
    try:
        commit = subprocess.run(
            [
                "git", "-c", f"safe.directory={REPOSITORY_ROOT}",
                "-C", str(REPOSITORY_ROOT), "rev-parse", "HEAD",
            ],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
        )
        status = subprocess.run(
            [
                "git", "-c", f"safe.directory={REPOSITORY_ROOT}",
                "-C", str(REPOSITORY_ROOT), "status", "--porcelain",
            ],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
        )
        state["commit"] = commit.stdout.strip()
        state["dirty"] = bool(status.stdout.strip())
    except (OSError, subprocess.SubprocessError) as exc:
        state["capture_error"] = str(exc)
    return state


def capture_repository_provenance(run_dir: Path) -> dict[str, Path]:
    """Preserve the exact tracked diff and hashes of every untracked file."""
    outputs = {}
    git_base = [
        "git", "-c", f"safe.directory={REPOSITORY_ROOT}",
        "-C", str(REPOSITORY_ROOT),
    ]
    try:
        status = subprocess.run(
            [*git_base, "status", "--porcelain=v1"],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
        )
        status_path = run_dir / "repository_status.txt"
        status_path.write_text(status.stdout, encoding="utf-8")
        outputs["repository_status"] = status_path

        diff = subprocess.run(
            [*git_base, "diff", "--binary", "--no-ext-diff", "HEAD", "--"],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=120,
        )
        diff_path = run_dir / "repository_diff.patch"
        diff_path.write_text(diff.stdout, encoding="utf-8")
        outputs["repository_diff"] = diff_path

        untracked = subprocess.run(
            [*git_base, "ls-files", "--others", "--exclude-standard"],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
        )
        records = []
        for relative_name in untracked.stdout.splitlines():
            path = REPOSITORY_ROOT / relative_name
            if path.is_file():
                records.append({
                    "repository_relative_path": Path(relative_name).as_posix(),
                    "size_bytes": path.stat().st_size,
                    "sha256": sha256_file(path),
                })
        untracked_path = run_dir / "repository_untracked_files.json"
        untracked_path.write_text(
            json.dumps(records, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        outputs["repository_untracked_files"] = untracked_path
    except (OSError, subprocess.SubprocessError) as exc:
        error_path = run_dir / "repository_capture_error.txt"
        error_path.write_text(f"{exc}\n", encoding="utf-8")
        outputs["repository_capture_error"] = error_path
    return outputs


def publish_latest_complete_run(run_dir: Path) -> None:
    """Atomically update the convenience pointer after a complete run."""
    temporary = LATEST_RUN_FILE.with_name(
        f".{LATEST_RUN_FILE.name}.{os.getpid()}.tmp"
    )
    temporary.write_text(f"{run_dir.name}\n", encoding="utf-8")
    temporary.replace(LATEST_RUN_FILE)


# ==========================================================================
# Server starten / stoppen
# ==========================================================================

def print_subprocess_output(line: str) -> None:
    """Forward UTF-8 subprocess output without failing on legacy consoles."""
    try:
        print(line, end="")
    except UnicodeEncodeError:
        encoding = sys.stdout.encoding or "ascii"
        printable = line.encode(encoding, errors="backslashreplace").decode(encoding)
        print(printable, end="")


def start_server(run_dir: Path) -> subprocess.Popen:
    """
    Startet den Aggregationsserver als Subprozess. Der Server fragt beim
    Start "URDF-Export aktivieren? [j/n]:" ab. Damit diese Abfrage nicht mit
    dem Kommando "beenden" kollidiert, bekommt der Subprozess eine eigene
    stdin-Pipe und die konfigurierte Antwort wird automatisch uebergeben.
    """
    print(f"Starte Aggregationsserver: {' '.join(SERVER_CMD)}")
    print(f"  (Arbeitsverzeichnis: {PROJECT_ROOT})")
    server_log_path = run_dir / "server_console.log"
    server_log = server_log_path.open("w", encoding="utf-8", buffering=1)
    completion_dir = PROJECT_ROOT / "logs"
    existing_completion_logs = set(completion_dir.glob("device_completions_*.log"))
    server_started_at_ns = time.time_ns()
    environment = os.environ.copy()
    environment["OJIES_OPCUA_HOST"] = E01_AGGREGATION_HOST
    proc = subprocess.Popen(
        SERVER_CMD,
        cwd=str(PROJECT_ROOT),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
        env=environment,
        creationflags=(
            subprocess.CREATE_NEW_PROCESS_GROUP
            if sys.platform.startswith("win") else 0
        ),
    )

    def preserve_console_output() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            server_log.write(line)
            print_subprocess_output(line)
        server_log.flush()

    output_thread = threading.Thread(
        target=preserve_console_output,
        name="aggregation-server-log",
        daemon=True,
    )
    output_thread.start()
    proc.ojies_output_thread = output_thread
    proc.ojies_server_log = server_log
    proc.ojies_server_log_path = server_log_path

    answer = "j" if ENABLE_URDF_EXPORT else "n"
    print(f"URDF-Export fuer Auswertung: {'AKTIVIERT' if ENABLE_URDF_EXPORT else 'DEAKTIVIERT'}")
    try:
        proc.stdin.write(answer + "\n")
        proc.stdin.flush()
    except (BrokenPipeError, OSError) as e:
        print(f"[WARNUNG] Konnte Antwort nicht an Server weiterleiten: {e}")

    deadline = time.monotonic() + 5.0
    completion_log_path = None
    while time.monotonic() < deadline:
        candidates = set(completion_dir.glob("device_completions_*.log")) - existing_completion_logs
        if candidates:
            completion_log_path = max(candidates, key=lambda path: path.stat().st_mtime_ns)
            break
        time.sleep(0.05)
    proc.ojies_completion_log_path = completion_log_path
    proc.ojies_server_started_at_ns = server_started_at_ns
    proc.ojies_existing_completion_logs = existing_completion_logs

    return proc


def read_aggregation_progress(
        expected_source_count: Optional[int] = None,
        completion_log_path: Optional[Path] = None,
) -> dict:
    """Read the live, atomic per-source result and discovery configuration."""
    try:
        config = load_json(PROJECT_ROOT / "config.json") or {}
    except (json.JSONDecodeError, OSError):
        # config.json is replaced by another process; retry on the next poll.
        config = {}
    events = load_jsonl(PROJECT_ROOT / "aggregation_results.jsonl")
    if not events:
        events = completion_events(completion_log_path, config)
    results = latest_aggregation_results(events)
    hosts = {
        host.get("name")
        for host in config.get("hosts", [])
        if isinstance(host, dict) and host.get("name")
    }
    by_source = {
        result.get("source_name"): result
        for result in results
        if isinstance(result, dict) and result.get("source_name")
    }
    completed = {
        name for name, result in by_source.items()
        if result.get("status") == "completed"
        and result.get("accepted") is True
    }
    failed = {
        name for name, result in by_source.items()
        if result.get("status") == "failed"
    }
    pending = {
        name for name, result in by_source.items()
        if result.get("status") in {"processing", "accepted"}
    }
    missing = hosts - set(by_source)
    expected_reached = (
        expected_source_count is None or len(hosts) >= expected_source_count
    )
    verified = (
        bool(hosts)
        and expected_reached
        and not failed
        and not pending
        and not missing
        and hosts <= completed
    )
    return {
        "expected_source_count": expected_source_count,
        "discovered_source_count": len(hosts),
        "result_count": len(by_source),
        "completed_source_count": len(completed),
        "failed_sources": sorted(failed),
        "pending_sources": sorted(pending),
        "missing_result_sources": sorted(missing),
        "verified": verified,
    }


def wait_for_verified_aggregation(
        proc: subprocess.Popen,
        timeout_seconds: float,
        expected_source_count: Optional[int] = None,
        source_proc: Optional[subprocess.Popen] = None,
) -> dict:
    """Wait until all discovered/expected sources have a terminal success result."""
    deadline = time.monotonic() + timeout_seconds
    last_progress_key = None
    while time.monotonic() < deadline:
        completion_log_path = getattr(proc, "ojies_completion_log_path", None)
        if completion_log_path is None:
            existing_logs = getattr(proc, "ojies_existing_completion_logs", set())
            candidates = [
                path for path in (PROJECT_ROOT / "logs").glob("device_completions_*.log")
                if path not in existing_logs
            ]
            if candidates:
                completion_log_path = max(
                    candidates, key=lambda path: path.stat().st_mtime_ns
                )
                proc.ojies_completion_log_path = completion_log_path
        progress = read_aggregation_progress(
            expected_source_count, completion_log_path
        )
        progress_key = (
            progress["discovered_source_count"],
            progress["result_count"],
            progress["completed_source_count"],
            tuple(progress["failed_sources"]),
            tuple(progress["pending_sources"]),
        )
        if progress_key != last_progress_key:
            print(
                "Aggregation: "
                f"discovered={progress['discovered_source_count']}, "
                f"results={progress['result_count']}, "
                f"completed={progress['completed_source_count']}, "
                f"failed={len(progress['failed_sources'])}, "
                f"pending={len(progress['pending_sources'])}"
            )
            last_progress_key = progress_key

        if progress["verified"]:
            progress["wait_status"] = "verified"
            return progress
        if progress["failed_sources"]:
            progress["wait_status"] = "failed"
            return progress
        return_code = proc.poll()
        if return_code is not None:
            progress["wait_status"] = "server_exited"
            progress["server_return_code"] = return_code
            return progress
        if source_proc is not None and source_proc.poll() is not None:
            progress["wait_status"] = "source_fixtures_exited"
            progress["source_fixture_return_code"] = source_proc.returncode
            return progress
        time.sleep(0.5)

    progress = read_aggregation_progress(
        expected_source_count, getattr(proc, "ojies_completion_log_path", None)
    )
    progress["wait_status"] = "timeout"
    return progress


def stop_server(proc: subprocess.Popen, timeout: float = 15.0) -> None:
    if proc.poll() is not None:
        print("Server-Prozess laeuft bereits nicht mehr.")
        output_thread = getattr(proc, "ojies_output_thread", None)
        if output_thread is not None:
            output_thread.join(timeout=2)
        server_log = getattr(proc, "ojies_server_log", None)
        if server_log is not None and not server_log.closed:
            server_log.close()
        return
    print("Beende Aggregationsserver (SIGTERM)...")
    if sys.platform.startswith("win"):
        proc.send_signal(signal.CTRL_BREAK_EVENT)
    else:
        proc.terminate()
    try:
        proc.wait(timeout=timeout)
        print("Server sauber beendet.")
    except subprocess.TimeoutExpired:
        print("Server reagiert nicht - erzwinge Beendigung (kill)...")
        proc.kill()
        proc.wait()
    output_thread = getattr(proc, "ojies_output_thread", None)
    if output_thread is not None:
        output_thread.join(timeout=2)
    server_log = getattr(proc, "ojies_server_log", None)
    if server_log is not None and not server_log.closed:
        server_log.close()


def start_source_fixtures(
        run_dir: Path, expected_source_count: int
) -> subprocess.Popen:
    """Start the four-domain deterministic E01 source fixture set."""
    if expected_source_count <= 0 or expected_source_count % 4 != 0:
        raise ValueError(
            "--expected-source-count must be a positive multiple of four "
            "when --start-source-fixtures is used"
        )
    replicas = expected_source_count // 4
    source_script = REPOSITORY_ROOT / "tools" / "source_servers.py"
    source_log_path = run_dir / "source_servers_console.log"
    source_log = source_log_path.open("w", encoding="utf-8", buffering=1)
    command = [
        PYTHON_EXE,
        str(source_script),
        "--base-port", "4860",
        "--replicas-per-domain", str(replicas),
        "--advertise-ip", "127.0.0.1",
        "--lds-url", "opc.tcp://127.0.0.1:4840",
    ]
    environment = os.environ.copy()
    environment["PYTHONUNBUFFERED"] = "1"
    print(f"Starte E01-Source-Fixtures: {' '.join(command)}")
    proc = subprocess.Popen(
        command,
        cwd=str(REPOSITORY_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
        env=environment,
    )

    def preserve_source_output() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            source_log.write(line)
            print_subprocess_output(line)
        source_log.flush()

    output_thread = threading.Thread(
        target=preserve_source_output,
        name="e01-source-fixture-log",
        daemon=True,
    )
    output_thread.start()
    proc.ojies_output_thread = output_thread
    proc.ojies_server_log = source_log
    proc.ojies_server_log_path = source_log_path
    return proc


def wait_for_local_tcp_port(port: int, timeout_seconds: float) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def server_endpoint_tcp_address(endpoint: str) -> tuple[str, int]:
    parsed = urlparse(endpoint)
    if parsed.scheme != "opc.tcp" or not parsed.hostname or parsed.port is None:
        raise ValueError(f"Ungueltiger lokaler OPC-UA-Endpoint: {endpoint}")
    return parsed.hostname, parsed.port


def wait_for_server_endpoint(
        proc: subprocess.Popen,
        endpoint: str,
        timeout_seconds: float,
) -> bool:
    host, port = server_endpoint_tcp_address(endpoint)
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return False
        try:
            with socket.create_connection((host, port), timeout=0.25):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def stop_source_fixtures(proc: subprocess.Popen, timeout: float = 15.0) -> None:
    if proc.poll() is None:
        print("Beende E01-Source-Fixtures...")
        proc.terminate()
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
    output_thread = getattr(proc, "ojies_output_thread", None)
    if output_thread is not None:
        output_thread.join(timeout=2)
    source_log = getattr(proc, "ojies_server_log", None)
    if source_log is not None and not source_log.closed:
        source_log.close()


# ==========================================================================
# Address Space Export (NodeXmlExporter.py)
# ==========================================================================

def determine_server_endpoint(config: dict) -> str:
    if SERVER_ENDPOINT:
        return SERVER_ENDPOINT
    port = config.get("port", 48400) if config else 48400
    return f"opc.tcp://{E01_AGGREGATION_HOST}:{port}"


def export_address_space(endpoint: str, run_dir: Path) -> Optional[Path]:
    if not NODE_XML_EXPORTER.exists():
        print(f"[WARNUNG] NodeXmlExporter.py nicht gefunden unter {NODE_XML_EXPORTER} "
              f"- Address-Space-Export wird uebersprungen.")
        return None

    out_file = run_dir / "address_space.xml"

    skipped_nodes_report = run_dir / "xml_export_skipped_nodes.json"
    cmd = [
        PYTHON_EXE, str(NODE_XML_EXPORTER), endpoint, str(out_file),
        "--skipped-nodes-report", str(skipped_nodes_report),
    ]
    print(f"Exportiere Address Space: {' '.join(cmd)}")
    export_log_path = run_dir / "xml_export_console.log"
    with export_log_path.open("w", encoding="utf-8", buffering=1) as export_log:
        result = subprocess.run(
            cmd,
            cwd=str(EXPORTER_DIR),
            stdout=export_log,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )

    if result.returncode != 0:
        print(f"[WARNUNG] Address-Space-Export ist mit Code {result.returncode} "
              f"fehlgeschlagen.")
        return None

    if not out_file.exists():
        print("[WARNUNG] Export-Prozess erfolgreich beendet, aber keine XML-Datei gefunden.")
        return None

    print(f"Address Space exportiert nach: {out_file}")
    return out_file


# ==========================================================================
# NodeSet2-XML parsen (Standard OPC UA UANodeSet-Schema)
# ==========================================================================

@dataclass
class XmlNode:
    node_id: str
    ns_index: int
    browse_name: str
    tag: str
    parent_node_id: Optional[str] = None
    references: list = field(default_factory=list)  # (ref_type, is_forward, target_node_id)


def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _parse_ns_index_from_nodeid(node_id: str) -> int:
    # Format z.B. "ns=2;i=1234" oder "i=1234" (ns=0) oder "ns=2;s=..." etc.
    if node_id.startswith("ns="):
        try:
            return int(node_id.split(";", 1)[0][3:])
        except ValueError:
            return 0
    return 0


def _parse_ns_index_from_bname(bname: str) -> int:
    # Format z.B. "2:DeviceSet" oder einfach "DeviceSet" (ns=0)
    if ":" in bname:
        prefix = bname.split(":", 1)[0]
        if prefix.isdigit():
            return int(prefix)
    return 0


def _bname_text(bname: str) -> str:
    if ":" in bname:
        prefix, rest = bname.split(":", 1)
        if prefix.isdigit():
            return rest
    return bname


def parse_namespace_uris_from_xml(xml_path: Path) -> dict:
    """
    Liest die tatsaechliche Namespace-Tabelle DIREKT aus der exportierten
    NodeSet2-XML (<NamespaceUris><Uri>...</Uri>...</NamespaceUris>).

    WICHTIG: Diese Tabelle kann (und wird bei diesem Server) deutlich mehr
    und andere Eintraege enthalten als namespaces.json! Der Aggregations-
    server registriert offenbar pro angehaengtem Geraet eigene, zusammen-
    gesetzte Namespaces (z.B. ".../Robotics/urn:plcm:robotics:004"), um
    Kollisionen zwischen gleichartigen Geraeten zu vermeiden. namespaces.json
    spiegelt das nicht wider - deshalb wird fuer die eigentliche Pfad-/
    Typ-Aufloesung IMMER diese XML-eigene Tabelle verwendet, nicht
    namespaces.json (das bleibt nur informativ in den Rohdaten erhalten).

    Index 0 ist implizit immer "http://opcfoundation.org/UA/" (Standard-NS,
    wird in NodeSet2-XML nicht explizit aufgelistet), Index 1..N ergeben
    sich aus der Reihenfolge der <Uri>-Eintraege.
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()

    ns_map = {0: "http://opcfoundation.org/UA/"}
    for el in root:
        if _strip_ns(el.tag) != "NamespaceUris":
            continue
        idx = 1
        for uri_el in el:
            if _strip_ns(uri_el.tag) == "Uri":
                ns_map[idx] = (uri_el.text or "").strip()
                idx += 1
        break
    return ns_map


def parse_nodeset_xml(xml_path: Path) -> dict:
    """
    Parst den exportierten Address Space (Standard OPC UA NodeSet2 XML) und
    liefert ein Dict node_id -> XmlNode.
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()

    nodes: dict[str, XmlNode] = {}
    aliases = {}
    for section in root:
        if _strip_ns(section.tag) != "Aliases":
            continue
        for alias in section:
            if _strip_ns(alias.tag) == "Alias" and alias.attrib.get("Alias"):
                aliases[alias.attrib["Alias"]] = (alias.text or "").strip()
        break

    for el in root:
        tag = _strip_ns(el.tag)
        if tag not in COUNTED_NODE_TAGS:
            continue
        node_id = el.attrib.get("NodeId")
        bname_attr = el.attrib.get("BrowseName", "")
        if not node_id:
            continue

        refs = []
        for child in el:
            if _strip_ns(child.tag) != "References":
                continue
            for ref_el in child:
                if _strip_ns(ref_el.tag) != "Reference":
                    continue
                ref_type_raw = ref_el.attrib.get("ReferenceType", "")
                ref_type = aliases.get(ref_type_raw, ref_type_raw)
                is_forward = ref_el.attrib.get("IsForward", "true").lower() != "false"
                target = (ref_el.text or "").strip()
                refs.append((ref_type, is_forward, target))

        nodes[node_id] = XmlNode(
            node_id=node_id,
            ns_index=_parse_ns_index_from_nodeid(node_id),
            browse_name=_bname_text(bname_attr),
            tag=tag,
            parent_node_id=el.attrib.get("ParentNodeId"),
            references=refs,
        )

    return nodes


def build_name_index(nodes: dict) -> dict:
    """
    Baut browse_name -> [node_id, ...] ueber ALLE Knoten im Address Space.
    Wird fuer die Bottom-Up-Pfadaufloesung gebraucht, da Browse-Namen wie
    "MethodSet" oder "CtrlConfiguration_003" im Address Space durchaus
    mehrfach vorkommen koennen (einmal pro Geraet/Host) - Top-Down-Suche
    kann dabei versehentlich im falschen Teilbaum landen.
    """
    name_index = defaultdict(list)
    for nid, node in nodes.items():
        name_index[node.browse_name].append(nid)
    return name_index


def get_ancestor_chain(node_id: str, nodes: dict, root_id: str, max_depth: int = 64) -> Optional[list]:
    """
    Liefert die Kette von node_id bis root_id (inklusive beider Enden,
    Reihenfolge: [node_id, ..., root_id]) anhand ParentNodeId. Liefert
    None, wenn root_id auf diesem Weg nicht erreicht wird (Sackgasse,
    fehlendes ParentNodeId, oder Zyklus/zu tief).
    """
    chain = [node_id]
    current = node_id
    steps = 0
    while current != root_id:
        node = nodes.get(current)
        parent = node.parent_node_id if node else None
        if not parent or parent not in nodes:
            return None
        chain.append(parent)
        current = parent
        steps += 1
        if steps > max_depth:
            return None
    return chain


@dataclass
class PathResolution:
    node: Optional[XmlNode]
    matched_depth: int          # bei Erfolg: Anzahl Pfadsegmente; bei Misserfolg: 0
    total_segments: int
    failed_at_segment: Optional[str]   # gesuchter Leaf-Name (None wenn erfolgreich)
    available_children_at_failure: list  # Diagnose-Hinweise (z.B. Anzahl Kandidaten, Beispiel-Pfade)
    resolution_mode: str = "unresolved"
    namespace_projection: list = field(default_factory=list)


def resolve_browse_path(path_segments: list, name_index: dict, nodes: dict,
                        nsid_to_uri: Optional[dict] = None,
                        source_application_uri: Optional[str] = None,
                        root_id: str = "i=85") -> PathResolution:
    """
    Bottom-Up-Aufloesung: nimmt den LETZTEN Pfad-Namen (Leaf, z.B.
    "Robot_004"), sucht ALLE Knoten mit diesem BrowseName im gesamten
    Address Space, und prueft fuer jeden Kandidaten, ob seine Ahnen-Kette
    (via ParentNodeId nach oben bis root_id) exakt dem gegebenen Pfad
    entspricht. Der erste passende Kandidat wird genommen; bei mehreren
    Knoten mit identischem Leaf-Namen in unterschiedlichen Teilbaeumen wird
    so garantiert der tatsaechlich richtige gefunden (bzw. bei keinem
    Treffer sauber als nicht aufloesbar markiert), statt sich beim
    Top-Down-Abstieg im falschen Teilbaum zu verlaufen.
    """
    segments = list(path_segments)
    nsid_to_uri = nsid_to_uri or {}
    root_node = nodes.get(root_id)

    # Fuehrendes "Objects" (Root-BrowseName) fuer den Vergleich entfernen,
    # falls vorhanden - konsistent mit dem Format in rules.json.
    if (
        segments
        and root_node
        and path_element_name(segments[0]) == root_node.browse_name
        and namespace_uri_matches(
            path_element_namespace_uri(segments[0]),
            nsid_to_uri.get(root_node.ns_index),
        )
    ):
        norm_segments = segments[1:]
    else:
        norm_segments = segments

    if not norm_segments:
        return PathResolution(
            root_node, 0, 0, None, [], "namespace_qualified", []
        )

    leaf_name = path_element_name(norm_segments[-1])
    candidates = name_index.get(leaf_name, [])

    if not candidates:
        return PathResolution(None, 0, len(norm_segments), leaf_name,
                              ["0 Kandidaten mit diesem Namen im gesamten Address Space gefunden"])

    example_paths = []
    local_name_matches = []
    exact_namespace_matches = []
    namespace_qualified_matches = []
    source_scoped_matches = []
    for cand_id in candidates:
        chain = get_ancestor_chain(cand_id, nodes, root_id)
        if chain is None:
            continue
        # chain ist [leaf, ..., root_id] -> umdrehen fuer root->leaf Reihenfolge
        ancestor_nodes = [nodes[nid] for nid in reversed(chain)]
        ancestor_nodes_no_root = ancestor_nodes[1:]
        if len(ancestor_nodes_no_root) == len(norm_segments) and all(
            path_element_name(segment) == node.browse_name
            for segment, node in zip(norm_segments, ancestor_nodes_no_root)
        ):
            namespace_projection = [
                {
                    "segment_index": segment_index,
                    "name": path_element_name(segment),
                    "rule_namespace_uri": path_element_namespace_uri(segment),
                    "exported_namespace_uri": nsid_to_uri.get(node.ns_index),
                }
                for segment_index, (segment, node) in enumerate(
                    zip(norm_segments, ancestor_nodes_no_root), start=1
                )
                if not namespace_uri_matches(
                    path_element_namespace_uri(segment),
                    nsid_to_uri.get(node.ns_index),
                )
            ]
            local_name_matches.append((nodes[cand_id], namespace_projection))
            if all(
                path_element_namespace_uri(segment) is None
                or namespace_uri_equivalent(
                    path_element_namespace_uri(segment),
                    nsid_to_uri.get(node.ns_index),
                )
                for segment, node in zip(
                    norm_segments, ancestor_nodes_no_root
                )
            ):
                exact_namespace_matches.append(
                    (nodes[cand_id], namespace_projection)
                )
            if source_application_uri:
                normalized_source_uri = source_application_uri.strip().rstrip("/")
                exported_path_uris = [
                    (nsid_to_uri.get(node.ns_index) or "").strip().rstrip("/")
                    for node in ancestor_nodes_no_root
                ]
                if any(
                    exported_uri == normalized_source_uri
                    or exported_uri.endswith(normalized_source_uri)
                    for exported_uri in exported_path_uris
                ):
                    source_scoped_matches.append(
                        (nodes[cand_id], namespace_projection)
                    )
            if not namespace_projection:
                namespace_qualified_matches.append(
                    (nodes[cand_id], namespace_projection)
                )
        if len(example_paths) < 3:
            example_paths.append("/".join(node.browse_name for node in ancestor_nodes))

    # Multiple lower servers may intentionally expose the same qualified
    # browse path. The aggregation library appends the source ApplicationUri
    # to the copied source namespaces. Bind such a path to its rule source
    # before considering the weaker globally unique local-name projection.
    if len(source_scoped_matches) == 1:
        node, namespace_projection = source_scoped_matches[0]
        return PathResolution(
            node,
            len(norm_segments),
            len(norm_segments),
            None,
            [],
            "source_scoped_namespace_projection",
            namespace_projection,
        )

    # The harmonized target uses the rule namespace URI exactly, whereas
    # lower-server copies use per-source postfixed variants. Prefer the
    # exact full-chain match before the broader postfix-compatible match.
    if len(exact_namespace_matches) == 1:
        node, namespace_projection = exact_namespace_matches[0]
        return PathResolution(
            node,
            len(norm_segments),
            len(norm_segments),
            None,
            [],
            "namespace_qualified",
            namespace_projection,
        )

    # Do not select the first namespace-compatible candidate. Generic source
    # namespace URIs intentionally match every per-source postfixed variant;
    # without a unique source binding, such duplicate paths are ambiguous.
    if len(namespace_qualified_matches) == 1:
        node, namespace_projection = namespace_qualified_matches[0]
        return PathResolution(
            node,
            len(norm_segments),
            len(norm_segments),
            None,
            [],
            "namespace_qualified",
            namespace_projection,
        )

    # The pinned aggregation library can reassign a copied node's BrowseName
    # namespace to the target parent namespace. The rich rule still records
    # the original source URI and NodeId. Accept this documented projection
    # only when the complete local-name chain identifies exactly one exported
    # node; otherwise namespace collisions remain a hard failure.
    if len(local_name_matches) == 1:
        node, namespace_projection = local_name_matches[0]
        return PathResolution(
            node,
            len(norm_segments),
            len(norm_segments),
            None,
            [],
            "unique_local_name_projection",
            namespace_projection,
        )

    return PathResolution(
        None, 0, len(norm_segments), leaf_name,
        [f"{len(candidates)} Kandidaten mit Namen '{leaf_name}' gefunden, "
         f"aber keiner passte zum vollen Pfad. Eindeutige lokale Pfadtreffer: "
         f"{len(local_name_matches)}. Beispiel-Pfade dieser Kandidaten: "]
        + example_paths
    )


# ==========================================================================
# Rohdaten sammeln
# ==========================================================================

def collect_raw_data(input_dir: Path) -> tuple[dict, list]:
    """Evaluate the preserved JSON snapshots, never mutable live files."""
    config = load_json(input_dir / "config.json") or {}
    namespaces = load_json(input_dir / "namespaces.json") or []
    entry_points = load_json(input_dir / "entry_points.json") or []
    rules = load_json(input_dir / "rules.json") or []
    executor_rules = load_json(input_dir / "rules_executor.json") or []
    nodeset_mapping = load_json(input_dir / "nodeset_mapping.json") or {}
    aggregation_events = load_jsonl(input_dir / "aggregation_results.jsonl")
    aggregation_results = latest_aggregation_results(aggregation_events)
    projection_schema = executor_projection_schema(executor_rules)

    nsid_to_uri = {ns["nsid"]: ns["url"] for ns in namespaces if "nsid" in ns and "url" in ns}

    data = {
        "timestamp": utc_now_iso(),
        "config": {
            "raw": config,
            "host_count": len(config.get("hosts", [])),
            "hosts": config.get("hosts", []),
        },
        "namespaces": {
            "raw": namespaces,
            "count": len(namespaces),
        },
        "entry_points": {
            "raw": entry_points,
            "namespace_count": len(entry_points),
            "total_entry_points": sum(len(e.get("entry_points", [])) for e in entry_points),
            "per_namespace": [
                {
                    "namespace_uri": e.get("namespace_uri"),
                    "namespace_index": e.get("namespace_index"),
                    "entry_point_count": len(e.get("entry_points", [])),
                }
                for e in entry_points
            ],
        },
        "nodeset_mapping": {
            "raw": nodeset_mapping,
            "mapping_count": len(nodeset_mapping.get("mappings", [])),
            "unique_nodeset_files": sorted(set(
                m.get("nodeset_file") for m in nodeset_mapping.get("mappings", [])
                if m.get("nodeset_file")
            )),
        },
        "rules": {
            "count": len(rules),
            "unique_source_paths": len(set(canonical_path(r.get("source_node", [])) for r in rules)),
            "unique_target_paths": len(set(canonical_path(r.get("target_node", [])) for r in rules)),
            "is_forward_true": sum(1 for r in rules if r.get("is_forward") is True),
            "is_forward_false": sum(1 for r in rules if r.get("is_forward") is False),
            "ref_type_leaf_distribution": dict(Counter(
                (
                    (r.get("reference_type") or {}).get("identifier")
                    or path_element_name((r.get("ref_type") or ["(missing)"])[-1])
                )
                for r in rules
            )),
            "max_source_path_depth": max((len(r.get("source_node", [])) for r in rules), default=0),
            "max_target_path_depth": max((len(r.get("target_node", [])) for r in rules), default=0),
            "executor_rule_count": len(executor_rules),
            "executor_projection_schema": projection_schema,
            "executor_projection_matches_rich_rules": (
                executor_rules
                == deduplicated_executor_projection(rules, projection_schema)
            ),
        },
        "aggregation_results": {
            "raw": aggregation_results,
            "events": aggregation_events,
            "event_count": len(aggregation_events),
            "count": len(aggregation_results),
            "accepted_count": sum(
                1 for result in aggregation_results if result.get("accepted") is True
            ),
            "completed_count": sum(
                1 for result in aggregation_results
                if result.get("status") == "completed"
            ),
            "failed_count": sum(
                1 for result in aggregation_results
                if result.get("status") == "failed"
            ),
            "pending_count": sum(
                1 for result in aggregation_results
                if result.get("status") in {"processing", "accepted"}
            ),
        },
        "rules_enriched": [],
        "namespace_distribution": {},
        "address_space": {
            "xml_path": None,
            "total_node_count": None,
            "node_count_by_type": {},
            "namespace_count_in_xml": None,
        },
        "nsid_to_uri": nsid_to_uri,
    }
    return data, rules


def get_type_definition_node(node_id: str, nodes: dict) -> Optional[str]:
    """
    Liefert die NodeId der TypeDefinition eines Knotens (ueber die forward
    "HasTypeDefinition"-Referenz), falls vorhanden.
    """
    node = nodes.get(node_id)
    if node is None:
        return None
    for ref_type, is_forward, target in node.references:
        if (
            is_standard_reference_type(ref_type, "HasTypeDefinition")
            and is_forward
            and target in nodes
        ):
            return target
    return None


def nodeid_identifier(node_id: str) -> str:
    """Return the namespace-independent identifier part of a NodeId."""
    if node_id.startswith("ns=") and ";" in node_id:
        return node_id.split(";", 1)[1]
    return node_id


def build_nodeset_model_expectations(
        nodeset_mapping: dict,
        nodeset_dir: Optional[Path] = None,
) -> dict:
    """Read the nodes and type nodes declared by each mapped model itself."""
    nodeset_dir = nodeset_dir or (PROJECT_ROOT / "nodesets")
    expected_nodes = defaultdict(set)
    expected_types = defaultdict(set)
    mapped_uris = set()
    errors = []

    mappings = (
        nodeset_mapping.get("mappings", [])
        if isinstance(nodeset_mapping, dict)
        else []
    )
    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        model_uri = mapping.get("model_uri") or mapping.get("namespace_uri")
        nodeset_file = mapping.get("nodeset_file")
        if not model_uri or not nodeset_file:
            continue
        model_key = model_uri.strip().rstrip("/")
        mapped_uris.add(model_key)
        nodeset_path = Path(nodeset_file)
        if not nodeset_path.is_absolute():
            nodeset_path = nodeset_dir / nodeset_path
        try:
            root = ET.parse(nodeset_path).getroot()
        except (OSError, ET.ParseError) as error:
            errors.append({
                "namespace_uri": model_uri,
                "nodeset_file": str(nodeset_path),
                "error": str(error),
            })
            continue

        namespace_uris = ["http://opcfoundation.org/UA/"]
        namespace_table = next(
            (element for element in root if _strip_ns(element.tag) == "NamespaceUris"),
            None,
        )
        if namespace_table is not None:
            namespace_uris.extend(
                (element.text or "").strip()
                for element in namespace_table
                if _strip_ns(element.tag) == "Uri" and (element.text or "").strip()
            )

        for element in root.iter():
            tag = _strip_ns(element.tag)
            if tag not in COUNTED_NODE_TAGS:
                continue
            node_id = element.attrib.get("NodeId")
            if not node_id:
                continue
            namespace_index = _parse_ns_index_from_nodeid(node_id)
            if namespace_index >= len(namespace_uris):
                continue
            if not namespace_uri_equivalent(
                namespace_uris[namespace_index], model_uri
            ):
                continue
            identifier = nodeid_identifier(node_id)
            expected_nodes[model_key].add(identifier)
            if tag in TYPE_NODE_TAGS:
                expected_types[model_key].add(identifier)

    return {
        "nodes": expected_nodes,
        "types": expected_types,
        "mapped_uris": mapped_uris,
        "errors": errors,
    }


def build_type_integrity_report(
        nodes: dict,
        nsid_to_uri: dict,
        standardized_namespaces: list,
        nodeset_mapping: Optional[dict] = None,
        nodeset_dir: Optional[Path] = None,
) -> dict:
    """Validate declared model nodes, declared types and HasTypeDefinition targets."""
    required_uris = sorted({
        entry.get("url")
        for entry in standardized_namespaces
        if isinstance(entry, dict)
        and entry.get("nsid", 0) > 0
        and entry.get("url")
    })
    expectations = build_nodeset_model_expectations(
        nodeset_mapping or {}, nodeset_dir
    )
    nodes_by_uri = defaultdict(set)
    types_by_uri = defaultdict(set)
    for node in nodes.values():
        uri_key = (nsid_to_uri.get(node.ns_index) or "").strip().rstrip("/")
        identifier = nodeid_identifier(node.node_id)
        nodes_by_uri[uri_key].add(identifier)
        if node.tag in TYPE_NODE_TAGS:
            types_by_uri[uri_key].add(identifier)

    missing_nodeset_mappings = [
        uri for uri in required_uris
        if uri.rstrip("/") not in expectations["mapped_uris"]
    ]
    missing_model_nodes = {
        uri: sorted(
            expectations["nodes"].get(uri.rstrip("/"), set())
            - nodes_by_uri.get(uri.rstrip("/"), set())
        )
        for uri in required_uris
    }
    missing_model_nodes = {
        uri: identifiers
        for uri, identifiers in missing_model_nodes.items()
        if identifiers
    }
    expectation_error_uris = {
        error["namespace_uri"].rstrip("/") for error in expectations["errors"]
    }
    # A required namespace is present once at least one node declared by its
    # mapped model is present. Missing individual declarations remain visible
    # below as coverage diagnostics, but are not equivalent to a missing
    # namespace (some node classes are intentionally represented differently
    # by the current importer).
    missing_required_models = sorted(set(
        missing_nodeset_mappings
        + [
            uri for uri in required_uris
            if uri.rstrip("/") in expectation_error_uris
            or (
                expectations["nodes"].get(uri.rstrip("/"), set())
                and expectations["nodes"].get(uri.rstrip("/"), set()).isdisjoint(
                    nodes_by_uri.get(uri.rstrip("/"), set())
                )
            )
        ]
    ))
    missing_required_types = [
        uri for uri in required_uris
        if expectations["types"].get(uri.rstrip("/"), set())
        - types_by_uri.get(uri.rstrip("/"), set())
    ]

    dangling = []
    remapped = 0
    unremapped = []
    for node in nodes.values():
        source_uri = nsid_to_uri.get(node.ns_index)
        for ref_type, is_forward, target in node.references:
            if (
                not is_standard_reference_type(ref_type, "HasTypeDefinition")
                or not is_forward
            ):
                continue
            target_node = nodes.get(target)
            if target_node is None:
                dangling.append({
                    "source_node_id": node.node_id,
                    "source_browse_name": node.browse_name,
                    "target_node_id": target,
                })
                continue
            target_uri = nsid_to_uri.get(target_node.ns_index)
            source_is_postfixed = any(
                not namespace_uri_equivalent(source_uri, uri)
                and namespace_uri_matches(uri, source_uri)
                for uri in required_uris
            )
            if not source_is_postfixed:
                continue
            if any(
                namespace_uri_equivalent(target_uri, uri)
                for uri in required_uris
            ):
                remapped += 1
            elif any(
                not namespace_uri_equivalent(target_uri, uri)
                and namespace_uri_matches(uri, target_uri)
                for uri in required_uris
            ):
                unremapped.append({
                    "source_node_id": node.node_id,
                    "source_namespace_uri": source_uri,
                    "target_node_id": target,
                    "target_namespace_uri": target_uri,
                })

    return {
        "required_standardized_namespace_count": len(required_uris),
        "expected_node_count_by_required_namespace": {
            uri: len(expectations["nodes"].get(uri.rstrip("/"), set()))
            for uri in required_uris
        },
        "node_count_by_required_namespace": {
            uri: len(nodes_by_uri.get(uri.rstrip("/"), set()))
            for uri in required_uris
        },
        "missing_required_model_namespaces": missing_required_models,
        "missing_required_model_node_examples": {
            uri: identifiers[:20]
            for uri, identifiers in missing_model_nodes.items()
        },
        "missing_required_model_node_count_by_namespace": {
            uri: len(identifiers)
            for uri, identifiers in missing_model_nodes.items()
        },
        "missing_nodeset_mapping_namespaces": missing_nodeset_mappings,
        "nodeset_expectation_errors": expectations["errors"],
        "expected_type_count_by_required_namespace": {
            uri: len(expectations["types"].get(uri.rstrip("/"), set()))
            for uri in required_uris
        },
        "type_count_by_required_namespace": {
            uri: len(types_by_uri.get(uri.rstrip("/"), set()))
            for uri in required_uris
        },
        "missing_required_type_namespaces": missing_required_types,
        "dangling_type_definition_count": len(dangling),
        "dangling_type_definition_examples": dangling[:20],
        "verified_remapped_type_definition_count": remapped,
        "unremapped_standardized_type_definition_count": len(unremapped),
        "unremapped_standardized_type_definition_examples": unremapped[:20],
        "passed": (
            not missing_required_models
            and not missing_required_types
            and not dangling
            and not unremapped
        ),
    }


def nodeid_identity(node_id: str, nsid_to_uri: dict) -> tuple[Optional[str], str]:
    """Convert an exported NodeId to the URI-based identity used by rules."""
    ns_index = _parse_ns_index_from_nodeid(node_id)
    identifier = node_id.split(";", 1)[-1] if ";" in node_id else node_id
    return nsid_to_uri.get(ns_index), identifier


def exported_reference_identity(
        reference_type: str, nsid_to_uri: dict
) -> tuple[Optional[str], str]:
    node_id = STANDARD_REFERENCE_TYPE_IDS.get(reference_type, reference_type)
    return nodeid_identity(node_id, nsid_to_uri)


def rule_reference_identity(rule: dict) -> Optional[tuple[Optional[str], str]]:
    identity = rule.get("reference_type")
    if isinstance(identity, dict) and identity.get("identifier"):
        return identity.get("namespace_uri"), identity["identifier"]
    legacy = rule.get("ref_type") or []
    if legacy:
        leaf = path_element_name(legacy[-1])
        return (
            "http://opcfoundation.org/UA/",
            STANDARD_REFERENCE_TYPE_IDS.get(leaf, leaf),
        )
    return None


def reference_semantics_match(
        rule: dict,
        source_node: Optional[XmlNode],
        target_node: Optional[XmlNode],
        nsid_to_uri: dict,
) -> bool:
    """Check exact reference identity and direction between resolved rule nodes."""
    if source_node is None or target_node is None:
        return False
    expected_identity = rule_reference_identity(rule)
    if expected_identity is None or not isinstance(rule.get("is_forward"), bool):
        return False
    for ref_type, is_forward, target in target_node.references:
        actual_identity = exported_reference_identity(ref_type, nsid_to_uri)
        if (
            target == source_node.node_id
            and is_forward == rule["is_forward"]
            and actual_identity[1] == expected_identity[1]
            and namespace_uri_equivalent(actual_identity[0], expected_identity[0])
        ):
            return True
    return False


def enrich_rules_with_namespace(
        rules: list,
        nodes: dict,
        nsid_to_uri: dict,
        source_application_uris: Optional[dict] = None,
) -> list:
    name_index = build_name_index(nodes)
    source_application_uris = source_application_uris or {}
    enriched = []
    for r in rules:
        source_path = r.get("source_node_qualified") or r.get("source_node", [])
        target_path = r.get("target_node_qualified") or r.get("target_node", [])
        source_id = r.get("source_id")
        source_application_uri = (
            source_application_uris.get(canonical_source_id(source_id))
            if isinstance(source_id, str) and source_id.strip()
            else None
        )

        source_res = resolve_browse_path(
            source_path,
            name_index,
            nodes,
            nsid_to_uri,
            source_application_uri=source_application_uri,
        )
        target_res = resolve_browse_path(
            target_path, name_index, nodes, nsid_to_uri
        )

        source_ns_idx = source_res.node.ns_index if source_res.node else None
        target_ns_idx = target_res.node.ns_index if target_res.node else None

        # TypeDefinition-basierte Namespace-Aufloesung: oft aussagekraeftiger
        # fuer die Domaenen-Zuordnung als der Namespace der Instanz selbst,
        # da Instanzen haeufig in einem generischen Namespace liegen, ihr
        # Typ (z.B. RobotType, CameraType) aber im domaenenspezifischen
        # Companion-Spec-Namespace.
        source_type_id = source_res.node and get_type_definition_node(source_res.node.node_id, nodes)
        target_type_id = target_res.node and get_type_definition_node(target_res.node.node_id, nodes)
        source_type_ns_idx = nodes[source_type_id].ns_index if source_type_id else None
        target_type_ns_idx = nodes[target_type_id].ns_index if target_type_id else None
        reference_verified = reference_semantics_match(
            r, source_res.node, target_res.node, nsid_to_uri
        )

        enriched.append({
            "source_node": source_path,
            "target_node": target_path,
            "ref_type": r.get("ref_type", []),
            "reference_type": r.get("reference_type"),
            "is_forward": r.get("is_forward"),
            "source_id": r.get("source_id"),
            "source_node_id": r.get("source_node_id"),
            "source_reference_type": r.get("source_reference_type"),
            "source_reference_is_forward": r.get(
                "source_reference_is_forward"
            ),
            "reference_policy": r.get("reference_policy"),
            "merge_policy": r.get("merge_policy"),
            "merge_key": r.get("merge_key"),
            "reference_semantics_verified": reference_verified,
            "source_namespace_index": source_ns_idx,
            "source_namespace_uri": nsid_to_uri.get(source_ns_idx) if source_ns_idx is not None else None,
            "target_namespace_index": target_ns_idx,
            "target_namespace_uri": nsid_to_uri.get(target_ns_idx) if target_ns_idx is not None else None,
            "source_type_browse_name": nodes[source_type_id].browse_name if source_type_id else None,
            "source_type_namespace_index": source_type_ns_idx,
            "source_type_namespace_uri": nsid_to_uri.get(source_type_ns_idx) if source_type_ns_idx is not None else None,
            "target_type_browse_name": nodes[target_type_id].browse_name if target_type_id else None,
            "target_type_namespace_index": target_type_ns_idx,
            "target_type_namespace_uri": nsid_to_uri.get(target_type_ns_idx) if target_type_ns_idx is not None else None,
            "source_resolved": source_res.node is not None,
            "target_resolved": target_res.node is not None,
            "source_matched_depth": source_res.matched_depth,
            "source_total_segments": source_res.total_segments,
            "source_failed_at_segment": source_res.failed_at_segment,
            "source_available_children_at_failure": source_res.available_children_at_failure,
            "source_resolution_mode": source_res.resolution_mode,
            "source_namespace_projection": source_res.namespace_projection,
            "target_matched_depth": target_res.matched_depth,
            "target_total_segments": target_res.total_segments,
            "target_failed_at_segment": target_res.failed_at_segment,
            "target_available_children_at_failure": target_res.available_children_at_failure,
            "target_resolution_mode": target_res.resolution_mode,
            "target_namespace_projection": target_res.namespace_projection,
        })
    return enriched


def build_rule_semantics_report(rules_enriched: list) -> dict:
    """Audit source identity, reference normalization, and consolidation."""
    violations = []
    source_identity_paths = defaultdict(set)
    target_groups = defaultdict(set)
    for index, rule in enumerate(rules_enriched):
        source_id = rule.get("source_id")
        source_node_id = rule.get("source_node_id")
        if not isinstance(source_id, str) or not source_id.strip():
            violations.append({"rule_index": index, "kind": "missing_source_id"})
        if (
            not isinstance(source_node_id, dict)
            or not source_node_id.get("namespace_uri")
            or not source_node_id.get("identifier")
        ):
            violations.append({
                "rule_index": index, "kind": "missing_source_node_id"
            })
        else:
            source_identity_paths[(
                canonical_source_id(source_id),
                source_node_id.get("namespace_uri"),
                source_node_id.get("identifier"),
            )].add(canonical_path(rule.get("source_node", [])))

        source_reference_type = rule.get("source_reference_type")
        if (
            not isinstance(source_reference_type, dict)
            or source_reference_type.get("identifier")
            not in TREE_FORMING_REFERENCE_IDS
            or not namespace_uri_equivalent(
                source_reference_type.get("namespace_uri"),
                OPC_UA_NAMESPACE_URI,
            )
        ):
            violations.append({
                "rule_index": index,
                "kind": "invalid_source_tree_reference_type",
            })
        if rule.get("source_reference_is_forward") is not True:
            violations.append({
                "rule_index": index,
                "kind": "source_reference_not_forward",
            })
        if rule.get("reference_policy") != REFERENCE_TRANSFORMATION_POLICY:
            violations.append({
                "rule_index": index,
                "kind": "invalid_reference_transformation_policy",
            })
        normalized_reference_type = rule.get("reference_type")
        if (
            not isinstance(normalized_reference_type, dict)
            or normalized_reference_type.get("identifier") != "i=35"
            or not namespace_uri_equivalent(
                normalized_reference_type.get("namespace_uri"),
                OPC_UA_NAMESPACE_URI,
            )
            or rule.get("is_forward") is not True
        ):
            violations.append({
                "rule_index": index,
                "kind": "invalid_normalized_target_reference",
            })

        if rule.get("merge_policy") != "merge_at_qualified_target":
            violations.append({
                "rule_index": index, "kind": "invalid_merge_policy"
            })
        merge_key = rule.get("merge_key")
        if not isinstance(merge_key, list) or canonical_path(merge_key) != canonical_path(
            rule.get("target_node", [])
        ):
            violations.append({
                "rule_index": index, "kind": "merge_key_target_mismatch"
            })
        else:
            target_groups[canonical_path(merge_key)].add(source_id)

    source_identity_collisions = [
        {
            "source_id": identity[0],
            "namespace_uri": identity[1],
            "identifier": identity[2],
            "source_paths": [
                [
                    {"namespace_uri": namespace_uri, "name": name}
                    for namespace_uri, name in path
                ]
                for path in sorted(paths, key=repr)
            ],
        }
        for identity, paths in source_identity_paths.items()
        if len(paths) > 1
    ]
    reference_failures = [
        index for index, rule in enumerate(rules_enriched)
        if not rule.get("reference_semantics_verified")
    ]
    projected_source_paths = [
        index for index, rule in enumerate(rules_enriched)
        if rule.get("source_resolution_mode") in {
            "unique_local_name_projection",
            "source_scoped_namespace_projection",
        }
    ]
    projected_target_paths = [
        index for index, rule in enumerate(rules_enriched)
        if rule.get("target_resolution_mode") == "unique_local_name_projection"
    ]
    intended_consolidations = [
        {
            "merge_key": [
                {"namespace_uri": namespace_uri, "name": name}
                for namespace_uri, name in key
            ],
            "source_ids": sorted(source_id for source_id in source_ids if source_id),
        }
        for key, source_ids in target_groups.items()
        if len(source_ids) > 1
    ]
    return {
        "rule_count": len(rules_enriched),
        "reference_semantics_unverified_count": len(reference_failures),
        "reference_semantics_unverified_rule_indices": reference_failures[:100],
        "unique_source_namespace_projection_count": len(projected_source_paths),
        "unique_target_namespace_projection_count": len(projected_target_paths),
        "source_identity_collision_count": len(source_identity_collisions),
        "source_identity_collision_examples": source_identity_collisions[:20],
        # Backwards-compatible key retained for existing report consumers.
        "duplicate_source_identity_count": len(source_identity_collisions),
        "duplicate_source_identity_examples": source_identity_collisions[:20],
        "intended_consolidation_group_count": len(intended_consolidations),
        "intended_consolidation_examples": intended_consolidations[:20],
        "policy_violation_count": len(violations),
        "policy_violation_examples": violations[:100],
        "passed": (
            not reference_failures
            and not source_identity_collisions
            and not violations
        ),
    }


def build_machine_vision_assignment_report(rules_enriched: list) -> dict:
    """Verify the deterministic four-source/two-system vision assignment."""
    system_sources = defaultdict(set)
    system_pattern = re.compile(
        r"^DualCameraVisionSystem_(\d{3})(?:_Assets)?$"
    )
    for rule in rules_enriched:
        source_id = rule.get("source_id")
        if not source_id:
            continue
        for element in rule.get("target_node", []):
            match = system_pattern.fullmatch(path_element_name(element))
            if match:
                system_sources[match.group(1)].add(
                    canonical_source_id(source_id)
                )
                break

    systems = [
        {
            "system_id": system_id,
            "source_count": len(source_ids),
            "source_ids": sorted(source_ids),
        }
        for system_id, source_ids in sorted(system_sources.items())
    ]
    all_source_ids = [
        source_id
        for system in systems
        for source_id in system["source_ids"]
    ]
    criteria = {
        "exactly_two_semantic_systems": [
            system["system_id"] for system in systems
        ] == ["001", "002"],
        "exactly_two_sources_per_system": (
            len(systems) == 2
            and all(system["source_count"] == 2 for system in systems)
        ),
        "four_distinct_sources_assigned_once": (
            len(all_source_ids) == 4
            and len(set(all_source_ids)) == 4
        ),
    }
    return {
        "fixture_contract": (
            "four machine-vision source servers form two semantic systems; "
            "replicas 1/2 map to system 001 and replicas 3/4 to system 002"
        ),
        "systems": systems,
        "criteria": criteria,
        "passed": all(criteria.values()),
    }


def build_namespace_distribution(rules_enriched: list) -> dict:
    """
    Zaehlt, wie viele Regeln pro Namespace-URI vorkommen (getrennt nach
    source_node und target_node). Reine Rohdaten-Aggregation, keine
    Domaenen-Interpretation.
    """
    source_counter = Counter(
        r["source_namespace_uri"] if r["source_namespace_uri"] else "(nicht aufgeloest)"
        for r in rules_enriched
    )
    target_counter = Counter(
        r["target_namespace_uri"] if r["target_namespace_uri"] else "(nicht aufgeloest)"
        for r in rules_enriched
    )
    # Kombination (source_uri, target_uri) -> Anzahl Regeln, oft die
    # aussagekraeftigste Gruppierung, da eine Regel zwei Seiten hat.
    pair_counter = Counter(
        (
            r["source_namespace_uri"] if r["source_namespace_uri"] else "(nicht aufgeloest)",
            r["target_namespace_uri"] if r["target_namespace_uri"] else "(nicht aufgeloest)",
        )
        for r in rules_enriched
    )
    return {
        "rules_per_source_namespace_uri": dict(source_counter),
        "rules_per_target_namespace_uri": dict(target_counter),
        "rules_per_source_target_namespace_pair": {
            f"{src} -> {tgt}": count for (src, tgt), count in pair_counter.items()
        },
    }


# Keywords, die im (ggf. zusammengesetzten) Namespace-URI direkt auf eine
# Domain hinweisen - z.B. steckt in
# "http://opcfoundation.org/UA/Robotics/urn:plcm:robotics:004" das Wort
# "robotics" direkt drin. Zuverlässiger als exakter URI-Vergleich, da diese
# zusammengesetzten Namespaces pro Geraet/Host unterschiedlich sind.
DOMAIN_URI_KEYWORDS = [
    ("robotics", "Robotics"),
    ("machine-vision", "Machine Vision"),
    ("additive-manufacturing", "Additive Manufacturing"),
    ("woodworking", "Woodworking"),
]


def classify_domain_from_uri(uri: Optional[str]) -> Optional[str]:
    if not uri:
        return None
    u = uri.lower()
    for keyword, domain in DOMAIN_URI_KEYWORDS:
        if keyword in u:
            return domain
    # Kein Fallback auf eine bare Companion-Spec-URI (z.B. reines "DI/" ohne
    # Host-Suffix): DI wird von mehreren Domains gemeinsam genutzt,
    # ein exakter Match dort waere falsch (fuehrte vorher dazu, dass ALLE
    # Regeln faelschlich einer Domain zugeordnet wurden). Lieber "Unbekannt"
    # als eine falsche Domain.
    return None


def _rule_domain(r: dict) -> Optional[str]:
    """
    Domain einer Regel, primaer ueber die Namespace-URI der TypeDefinition
    von target_node bestimmt (z.B. "MotionDeviceSystemType" -> Robotics-
    Namespace), da das oft aussagekraeftiger ist als der Namespace der
    Instanz selbst. Fallback-Reihenfolge:
      1. target_type_namespace_uri
      2. source_type_namespace_uri
      3. source_namespace_uri (Instanz)
      4. target_namespace_uri (Instanz)
    """
    for uri in (
            r.get("target_type_namespace_uri"),
            r.get("source_type_namespace_uri"),
            r.get("source_namespace_uri"),
            r.get("target_namespace_uri"),
    ):
        domain = classify_domain_from_uri(uri)
        if domain:
            return domain
    return None


def build_domain_table(rules_enriched: list, hosts: list) -> dict:
    """
    Baut die "Domain / Inst. / Rules / Targets"-Tabelle:
      - Inst.:   Anzahl Hosts in config.json, deren Name das jeweilige
                 Domain-Keyword enthaelt (siehe DOMAIN_TO_HOST_KEYWORD)
      - Rules:   Anzahl Regeln, deren source/target-Namespace-URI der
                 Domain zugeordnet ist (siehe NAMESPACE_TO_DOMAIN)
      - Targets: Anzahl EINDEUTIGER target_node-Pfade innerhalb dieser Regeln
    Regeln, die keiner bekannten Domain zugeordnet werden konnten, landen
    unter "Unbekannt / Sonstiges" (nicht stillschweigend verworfen).
    """
    domains = list(dict.fromkeys(NAMESPACE_TO_DOMAIN.values()))  # eindeutig, Reihenfolge erhalten

    rows = []
    for domain in domains:
        keyword = DOMAIN_TO_HOST_KEYWORD.get(domain, "")
        inst_count = sum(1 for h in hosts if keyword and keyword in h.get("name", "").lower())

        domain_rules = [r for r in rules_enriched if _rule_domain(r) == domain]
        target_paths = set(canonical_path(r["target_node"]) for r in domain_rules)

        rows.append({
            "domain": domain,
            "inst": inst_count,
            "rules": len(domain_rules),
            "targets": len(target_paths),
        })

    # Nicht zuordenbare Regeln separat ausweisen, damit nichts verschwindet
    unmatched = [r for r in rules_enriched if _rule_domain(r) is None]
    if unmatched:
        target_paths = set(canonical_path(r["target_node"]) for r in unmatched)
        rows.append({
            "domain": "Unbekannt / Sonstiges",
            "inst": None,
            "rules": len(unmatched),
            "targets": len(target_paths),
        })

    total_inst = sum(r["inst"] for r in rows if r["inst"] is not None)
    total_rules = sum(r["rules"] for r in rows)
    total_targets = len(set(
        canonical_path(r["target_node"]) for r in rules_enriched
    ))

    return {
        "rows": rows,
        "total": {"inst": total_inst, "rules": total_rules, "targets": total_targets},
    }



# ==========================================================================
# Ausgabe
# ==========================================================================

def write_outputs(data: dict, run_dir: Path) -> dict[str, Path]:
    """Write stable filenames inside an already exclusive run directory."""
    outputs: dict[str, Path] = {}

    summary_path = run_dir / "summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    outputs["summary"] = summary_path
    print(f"\nZusammenfassung (JSON) gespeichert: {summary_path}")

    rules_csv_path = run_dir / "rules_enriched.csv"
    with open(rules_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "source_node", "target_node", "reference_type", "is_forward",
            "source_id", "source_node_id", "merge_policy", "merge_key",
            "source_reference_type", "source_reference_is_forward",
            "reference_policy",
            "reference_semantics_verified",
            "source_namespace_index", "source_namespace_uri",
            "target_namespace_index", "target_namespace_uri",
            "source_type_browse_name", "source_type_namespace_uri",
            "target_type_browse_name", "target_type_namespace_uri",
            "source_resolved", "target_resolved",
            "source_resolution_mode", "target_resolution_mode",
            "source_namespace_projection", "target_namespace_projection",
            "source_matched_depth", "source_total_segments",
            "source_failed_at_segment", "source_available_children_at_failure",
            "target_matched_depth", "target_total_segments",
            "target_failed_at_segment", "target_available_children_at_failure",
        ])
        for r in data["rules_enriched"]:
            writer.writerow([
                display_path(r["source_node"]),
                display_path(r["target_node"]),
                json.dumps(
                    r.get("reference_type") or r.get("ref_type"),
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                r["is_forward"],
                r.get("source_id"),
                json.dumps(r.get("source_node_id"), ensure_ascii=False),
                r.get("merge_policy"),
                display_path(r.get("merge_key") or []),
                json.dumps(r.get("source_reference_type"), ensure_ascii=False),
                r.get("source_reference_is_forward"),
                r.get("reference_policy"),
                r.get("reference_semantics_verified"),
                r["source_namespace_index"],
                r["source_namespace_uri"],
                r["target_namespace_index"],
                r["target_namespace_uri"],
                r.get("source_type_browse_name"),
                r.get("source_type_namespace_uri"),
                r.get("target_type_browse_name"),
                r.get("target_type_namespace_uri"),
                r["source_resolved"],
                r["target_resolved"],
                r.get("source_resolution_mode"),
                r.get("target_resolution_mode"),
                json.dumps(
                    r.get("source_namespace_projection", []),
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                json.dumps(
                    r.get("target_namespace_projection", []),
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                r["source_matched_depth"],
                r["source_total_segments"],
                r["source_failed_at_segment"],
                "|".join(r["source_available_children_at_failure"]),
                r["target_matched_depth"],
                r["target_total_segments"],
                r["target_failed_at_segment"],
                "|".join(r["target_available_children_at_failure"]),
            ])
    outputs["rules_enriched"] = rules_csv_path
    print(f"Regeln inkl. Namespace-URI (CSV) gespeichert: {rules_csv_path}")

    # Kompakte Diagnose: an welchem Pfad-Segment (Tiefe) scheitert die
    # Aufloesung am haeufigsten? Hilft, systematische Ursachen zu finden.
    failed_source = [r for r in data["rules_enriched"] if not r["source_resolved"]]
    failed_target = [r for r in data["rules_enriched"] if not r["target_resolved"]]
    if failed_source or failed_target:
        print("\nDiagnose nicht aufgeloester Pfade:")
        if failed_source:
            depth_counter = Counter(r["source_matched_depth"] for r in failed_source)
            segment_counter = Counter(r["source_failed_at_segment"] for r in failed_source)
            print(f"  source: {len(failed_source)} fehlgeschlagen")
            print(f"    Abbruch nach N erfolgreich aufgeloesten Segmenten: {dict(sorted(depth_counter.items()))}")
            print(f"    haeufigste fehlende Segment-Namen: {segment_counter.most_common(5)}")
            example = failed_source[0]
            print(f"    Beispiel: Pfad={display_path(example['source_node'])}")
            print(f"              aufgeloest bis Tiefe {example['source_matched_depth']}, "
                  f"gesucht='{example['source_failed_at_segment']}', "
                  f"tatsaechlich vorhanden={example['source_available_children_at_failure']}")
        if failed_target:
            depth_counter = Counter(r["target_matched_depth"] for r in failed_target)
            segment_counter = Counter(r["target_failed_at_segment"] for r in failed_target)
            print(f"  target: {len(failed_target)} fehlgeschlagen")
            print(f"    Abbruch nach N erfolgreich aufgeloesten Segmenten: {dict(sorted(depth_counter.items()))}")
            print(f"    haeufigste fehlende Segment-Namen: {segment_counter.most_common(5)}")

    if data.get("namespace_distribution"):
        ns_dist_csv_path = run_dir / "rules_per_namespace.csv"
        with open(ns_dist_csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["seite", "namespace_uri", "anzahl_regeln"])
            for uri, count in data["namespace_distribution"]["rules_per_source_namespace_uri"].items():
                writer.writerow(["source", uri, count])
            for uri, count in data["namespace_distribution"]["rules_per_target_namespace_uri"].items():
                writer.writerow(["target", uri, count])
        outputs["rules_per_namespace"] = ns_dist_csv_path
        print(f"Regel-Verteilung nach Namespace (CSV) gespeichert: {ns_dist_csv_path}")

        ns_pair_csv_path = run_dir / "rules_per_namespace_pair.csv"
        with open(ns_pair_csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["source_namespace_uri", "target_namespace_uri", "anzahl_regeln"])
            for pair, count in data["namespace_distribution"]["rules_per_source_target_namespace_pair"].items():
                src, tgt = pair.split(" -> ", 1)
                writer.writerow([src, tgt, count])
        outputs["rules_per_namespace_pair"] = ns_pair_csv_path
        print(f"Namespace-Paare (CSV) gespeichert: {ns_pair_csv_path}")

    print("\n" + "=" * 70)
    print("ROHDATEN")
    print("=" * 70)
    print(f"Hosts (config.json):                {data['config']['host_count']}")
    print(f"Namespaces (namespaces.json):        {data['namespaces']['count']}")
    print(f"NodeSet-Mappings:                    {data['nodeset_mapping']['mapping_count']}")
    print(f"  davon eindeutige NodeSet-Dateien:   {len(data['nodeset_mapping']['unique_nodeset_files'])}")
    print(f"Entry Points (gesamt):                {data['entry_points']['total_entry_points']}")
    print(f"Regeln (rules.json) gesamt:           {data['rules']['count']}")
    print(f"  eindeutige source-Pfade:            {data['rules']['unique_source_paths']}")
    print(f"  eindeutige target-Pfade:            {data['rules']['unique_target_paths']}")
    print(f"  is_forward=true:                    {data['rules']['is_forward_true']}")
    print(f"  is_forward=false:                   {data['rules']['is_forward_false']}")
    print(f"  ref_type (letztes Segment) Verteilung: {data['rules']['ref_type_leaf_distribution']}")
    print(f"  max. Pfadtiefe (source):             {data['rules']['max_source_path_depth']}")
    print(f"  max. Pfadtiefe (target):             {data['rules']['max_target_path_depth']}")
    if data["address_space"]["total_node_count"] is not None:
        print(f"Address Space - Nodes gesamt:         {data['address_space']['total_node_count']}")
        print(f"  nach Typ:                           {data['address_space']['node_count_by_type']}")
        print(f"  Namespaces in XML (live):           {data['address_space']['namespace_count_in_xml']}"
              f"  (namespaces.json: {data['namespaces']['count']})")
    unresolved = sum(
        1 for r in data["rules_enriched"]
        if not r["source_resolved"] or not r["target_resolved"]
    )
    if data["rules_enriched"]:
        print(f"Regeln mit nicht aufgeloestem Pfad im XML: {unresolved} / {len(data['rules_enriched'])}")
    if data.get("namespace_distribution"):
        print("\nRegeln pro source_namespace_uri:")
        for uri, count in sorted(
                data["namespace_distribution"]["rules_per_source_namespace_uri"].items(),
                key=lambda x: -x[1]
        ):
            print(f"  {count:5d}  {uri}")
        print("\nRegeln pro target_namespace_uri:")
        for uri, count in sorted(
                data["namespace_distribution"]["rules_per_target_namespace_uri"].items(),
                key=lambda x: -x[1]
        ):
            print(f"  {count:5d}  {uri}")
    print("=" * 70)

    if data.get("domain_table"):
        dt = data["domain_table"]
        print("\nDomain-Tabelle (heuristische Zuordnung ueber Hostnamen sowie")
        print("TypeDefinition-/Instanz-Namespace-URIs; siehe Skript und Manifest):")
        print(f"{'Domain':<26} {'Inst.':>6} {'Rules':>7} {'Targets':>8}")
        print("-" * 50)
        for row in dt["rows"]:
            inst_str = str(row["inst"]) if row["inst"] is not None else "-"
            print(f"{row['domain']:<26} {inst_str:>6} {row['rules']:>7} {row['targets']:>8}")
        print("-" * 50)
        print(f"{'Total':<26} {dt['total']['inst']:>6} {dt['total']['rules']:>7} {dt['total']['targets']:>8}")

        domain_csv_path = run_dir / "domain_table.csv"
        with open(domain_csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Domain", "Inst.", "Rules", "Targets"])
            for row in dt["rows"]:
                writer.writerow([row["domain"], row["inst"], row["rules"], row["targets"]])
            writer.writerow(["Total", dt["total"]["inst"], dt["total"]["rules"], dt["total"]["targets"]])
        outputs["domain_table"] = domain_csv_path
        print(f"\nDomain-Tabelle (CSV) gespeichert: {domain_csv_path}")

    if data.get("machine_vision_assignment"):
        vision_csv_path = run_dir / "machine_vision_assignment.csv"
        with open(vision_csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["system_id", "source_count", "source_ids"])
            for system in data["machine_vision_assignment"]["systems"]:
                writer.writerow([
                    system["system_id"],
                    system["source_count"],
                    "|".join(system["source_ids"]),
                ])
        outputs["machine_vision_assignment"] = vision_csv_path
        print(
            "Machine-Vision-Zuordnung (CSV) gespeichert: "
            f"{vision_csv_path}"
        )
    return outputs


def artifact_set_is_complete(
        data: dict,
        snapshots: dict[str, Path],
        xml_path: Optional[Path],
) -> bool:
    return (
        len(snapshots) == len(RUNTIME_INPUT_FILES)
        and xml_path is not None
        and xml_path.exists()
        and (xml_path.parent / "xml_export_skipped_nodes.json").exists()
        and data["address_space"]["total_node_count"] is not None
    )


def build_e01_acceptance(data: dict, expected_source_count: Optional[int]) -> dict:
    """Evaluate the non-negotiable E01 acceptance gates from preserved data."""
    unresolved_source = sum(
        1 for rule in data["rules_enriched"] if not rule["source_resolved"]
    )
    unresolved_target = sum(
        1 for rule in data["rules_enriched"] if not rule["target_resolved"]
    )
    results = data["aggregation_results"]["raw"]
    accepted = [result for result in results if result.get("accepted") is True]
    accepted_addresses = {
        canonical_source_id(result.get("source_address")) for result in accepted
        if result.get("source_address")
    }
    rule_source_ids = {
        canonical_source_id(rule.get("source_id")) for rule in data["rules_enriched"]
        if rule.get("source_id")
    }
    accepted_not_completed = [
        result.get("source_name")
        for result in accepted
        if result.get("status") != "completed"
    ]
    failed = [
        result.get("source_name")
        for result in results
        if result.get("status") == "failed"
    ]
    host_count = data["config"]["host_count"]
    expected_count_ok = (
        expected_source_count is None or host_count == expected_source_count
    )
    all_segments_qualified = all(
        isinstance(element, dict)
        and bool(element.get("namespace_uri"))
        and bool(element.get("name"))
        for rule in data["rules_enriched"]
        for field_name in ("source_node", "target_node")
        for element in rule[field_name]
    )
    type_integrity = data.get("type_integrity", {})
    rule_semantics = data.get("rule_semantics", {})
    machine_vision_assignment = data.get("machine_vision_assignment", {})
    criteria = {
        "zero_unresolved_source_paths": unresolved_source == 0,
        "zero_unresolved_target_paths": unresolved_target == 0,
        "zero_dangling_type_references": (
            type_integrity.get("dangling_type_definition_count") == 0
        ),
        "required_standardized_types_present": not type_integrity.get(
            "missing_required_type_namespaces", ["not assessed"]
        ),
        "required_standardized_models_present": not type_integrity.get(
            "missing_required_model_namespaces", ["not assessed"]
        ),
        "standardized_type_definitions_remapped": (
            type_integrity.get(
                "unremapped_standardized_type_definition_count"
            ) == 0
        ),
        "all_rule_path_segments_namespace_qualified": all_segments_qualified,
        "reference_transformation_policy_verified": (
            rule_semantics.get("passed") is True
        ),
        "source_identity_and_merge_policy_valid": (
            rule_semantics.get("policy_violation_count") == 0
            and rule_semantics.get("duplicate_source_identity_count") == 0
        ),
        "rule_sources_match_accepted_sources": (
            rule_source_ids == accepted_addresses
        ),
        "executor_projection_matches_rich_rules": data["rules"].get(
            "executor_projection_matches_rich_rules"
        ) is True,
        "machine_vision_two_system_assignment_verified": (
            expected_source_count != 16
            or machine_vision_assignment.get("passed") is True
        ),
        "expected_source_count_reached": expected_count_ok,
        "every_discovered_source_has_result": len(results) == host_count,
        "every_discovered_source_was_accepted": len(accepted) == host_count,
        "every_accepted_source_verified_complete": not accepted_not_completed,
        "no_source_aggregation_failure": not failed,
    }
    return {
        "expected_source_count": expected_source_count,
        "discovered_source_count": host_count,
        "accepted_source_count": len(accepted),
        "unresolved_source_path_count": unresolved_source,
        "unresolved_target_path_count": unresolved_target,
        "accepted_not_completed_sources": sorted(
            name for name in accepted_not_completed if name
        ),
        "failed_sources": sorted(name for name in failed if name),
        "criteria": criteria,
        "passed": all(criteria.values()),
    }


def write_run_manifest(
        run_dir: Path,
        started_at: str,
        endpoint: str,
        snapshots: dict[str, Path],
        outputs: dict[str, Path],
        data: dict,
) -> Path:
    """Write hashes and machine-readable links for later evidence retrieval."""
    completed_at = utc_now_iso()
    unresolved_source = sum(
        1 for rule in data["rules_enriched"] if not rule["source_resolved"]
    )
    unresolved_target = sum(
        1 for rule in data["rules_enriched"] if not rule["target_resolved"]
    )
    status = data["validation_run"]["artifact_set_status"]

    binary_path = Path(SERVER_CMD[0])
    binary = {"path": str(binary_path.resolve()), "exists": binary_path.exists()}
    if binary_path.exists():
        binary.update({
            "size_bytes": binary_path.stat().st_size,
            "sha256": sha256_file(binary_path),
        })

    acceptance = data.get("e01_acceptance", {"passed": False, "criteria": {}})
    scientific_status = "passed" if acceptance.get("passed") else "failed"
    manifest = {
        "schema": "ojies.semantic-integration-run/v2",
        "run_id": run_dir.name,
        "started_at_utc": started_at,
        "completed_at_utc": completed_at,
        "artifact_set_status": status,
        "scientific_validation_status": scientific_status,
        "related_claims": ["C01", "C02"],
        "related_experiments": ["E01"],
        "generator": workspace_path(Path(__file__)),
        "command": [sys.executable, *sys.argv],
        "server_endpoint": endpoint,
        "execution_environment": {
            "operator": os.environ.get("USERNAME") or os.environ.get("USER"),
            "hostname": socket.gethostname(),
            "operating_system": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "python_version": platform.python_version(),
            "source_fixture_orchestration": data["validation_run"].get(
                "source_fixture_orchestration"
            ),
            "aggregation_host": E01_AGGREGATION_HOST,
        },
        "repository": repository_state(),
        "server_binary": binary,
        "implementation_provenance": {
            "opcua_dependency": opcua_dependency_provenance(),
            "discovery": observed_discovery_provenance(
                run_dir / "server_console.log"
            ),
        },
        "inputs": {
            name: file_record(path, run_dir)
            for name, path in sorted(snapshots.items())
        },
        "outputs": {
            name: file_record(path, run_dir)
            for name, path in sorted(outputs.items())
            if path.exists()
        },
        "data_quality": {
            "rule_count": data["rules"]["count"],
            "executor_rule_count": data["rules"].get("executor_rule_count"),
            "executor_projection_matches_rich_rules": data["rules"].get(
                "executor_projection_matches_rich_rules"
            ),
            "unresolved_source_path_count": unresolved_source,
            "unresolved_target_path_count": unresolved_target,
            "indexed_namespace_count_in_xml": data["address_space"]["namespace_count_in_xml"],
            "dangling_type_definition_count": data.get("type_integrity", {}).get(
                "dangling_type_definition_count"
            ),
            "missing_required_type_namespaces": data.get("type_integrity", {}).get(
                "missing_required_type_namespaces", []
            ),
            "missing_required_model_namespaces": data.get("type_integrity", {}).get(
                "missing_required_model_namespaces", []
            ),
            "accepted_source_count": data["aggregation_results"]["accepted_count"],
            "verified_completed_source_count": data["aggregation_results"]["completed_count"],
            "unverified_reference_semantics_count": data.get("rule_semantics", {}).get(
                "reference_semantics_unverified_count"
            ),
            "merge_policy_violation_count": data.get("rule_semantics", {}).get(
                "policy_violation_count"
            ),
            "unique_source_namespace_projection_count": data.get(
                "rule_semantics", {}
            ).get("unique_source_namespace_projection_count"),
            "unique_target_namespace_projection_count": data.get(
                "rule_semantics", {}
            ).get("unique_target_namespace_projection_count"),
            "machine_vision_two_system_assignment_verified": data.get(
                "machine_vision_assignment", {}
            ).get("passed"),
            "namespace_count_convention": (
                "includes the implicit OPC UA namespace at index 0"
            ),
        },
        "e01_acceptance": acceptance,
        "manuscript_consumers": {
            "tab_artefact_metrics": {
                "primary_file": "summary.json",
                "json_sections": [
                    "config", "namespaces", "nodeset_mapping", "entry_points",
                    "rules", "address_space",
                ],
            },
            "tab_rule_distribution": {
                "primary_file": "domain_table.csv",
                "derivation_file": "summary.json#/domain_table",
            },
        },
        "interpretation_note": (
            "COMPLETED means that all expected files were captured and parsed. "
            "VALIDATED is written only when every E01 acceptance criterion passes."
        ),
    }

    manifest_path = run_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    if status == "complete":
        (run_dir / "COMPLETED").write_text(
            f"{completed_at}\n", encoding="utf-8"
        )
    if status == "complete" and acceptance.get("passed"):
        (run_dir / "VALIDATED").write_text(
            f"{completed_at}\n", encoding="utf-8"
        )
        publish_latest_complete_run(run_dir)
    return manifest_path


# ==========================================================================

def build_unresolved_rules_enriched(rules: list) -> list:
    """Fallback, wenn keine/keine auswertbare Address-Space-XML vorliegt."""
    return [
        {
            "source_node": (
                r.get("source_node_qualified") or r.get("source_node", [])
            ),
            "target_node": (
                r.get("target_node_qualified") or r.get("target_node", [])
            ),
            "ref_type": r.get("ref_type", []),
            "reference_type": r.get("reference_type"),
            "is_forward": r.get("is_forward"),
            "source_id": r.get("source_id"),
            "source_node_id": r.get("source_node_id"),
            "source_reference_type": r.get("source_reference_type"),
            "source_reference_is_forward": r.get(
                "source_reference_is_forward"
            ),
            "reference_policy": r.get("reference_policy"),
            "merge_policy": r.get("merge_policy"),
            "merge_key": r.get("merge_key"),
            "reference_semantics_verified": False,
            "source_namespace_index": None,
            "source_namespace_uri": None,
            "target_namespace_index": None,
            "target_namespace_uri": None,
            "source_resolved": False,
            "target_resolved": False,
            "source_matched_depth": 0,
            "source_total_segments": len(
                r.get("source_node_qualified") or r.get("source_node", [])
            ),
            "source_failed_at_segment": None,
            "source_available_children_at_failure": [],
            "source_resolution_mode": "unresolved",
            "source_namespace_projection": [],
            "target_matched_depth": 0,
            "target_total_segments": len(
                r.get("target_node_qualified") or r.get("target_node", [])
            ),
            "target_failed_at_segment": None,
            "target_available_children_at_failure": [],
            "target_resolution_mode": "unresolved",
            "target_namespace_projection": [],
        }
        for r in rules
    ]


def parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run and preserve the E01 semantic-integration validation."
    )
    parser.add_argument(
        "--expected-source-count",
        type=int,
        default=int(os.environ.get("OJIES_EXPECTED_SOURCE_COUNT", "16")),
        help="Exact number of sources required before E01 can pass (default: 16).",
    )
    parser.add_argument(
        "--aggregation-timeout",
        type=float,
        default=float(os.environ.get("OJIES_AGGREGATION_TIMEOUT_SECONDS", "900")),
        help="Seconds to wait for verified per-source completion (default: 900).",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Wait for the legacy 'beenden' command instead of completion results.",
    )
    parser.add_argument(
        "--start-source-fixtures",
        action="store_true",
        help="Start and preserve the deterministic four-domain source fixtures.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_cli_args()
    binary_path = Path(SERVER_CMD[0])
    if not binary_path.is_file():
        raise FileNotFoundError(
            "Release-Binary nicht gefunden: "
            f"{binary_path}. Baue den Server mit 'cargo build --release' oder "
            "setze CARGO_TARGET_DIR auf das Verzeichnis, das den release-Ordner "
            "enthaelt."
        )
    started_at = utc_now_iso()
    run_dir = create_run_directory()
    print(f"Validierungslauf angelegt: {run_dir}")

    initial_config = load_json(PROJECT_ROOT / "config.json") or {}
    endpoint = determine_server_endpoint(initial_config)
    xml_path: Optional[Path] = None
    aggregation_wait: dict = {"wait_status": "not_started"}

    proc = start_server(run_dir)
    source_proc: Optional[subprocess.Popen] = None
    try:
        if not wait_for_server_endpoint(
                proc, endpoint, SERVER_STARTUP_TIMEOUT_SECONDS
        ):
            return_code = proc.poll()
            detail = (
                f"; Server-Prozess endete mit Code {return_code}"
                if return_code is not None else ""
            )
            raise RuntimeError(
                "Aggregationsserver wurde nicht innerhalb von "
                f"{SERVER_STARTUP_TIMEOUT_SECONDS:.0f} s unter {endpoint} bereit"
                f"{detail}"
            )
        if args.start_source_fixtures:
            if not wait_for_local_tcp_port(4840, 15.0):
                raise RuntimeError("LDS wurde nicht innerhalb von 15 s erreichbar")
            source_proc = start_source_fixtures(
                run_dir, args.expected_source_count
            )
        if args.interactive:
            print("\nServer laeuft. Zum Beenden 'beenden' eingeben und Enter druecken:")
            while True:
                try:
                    cmd = input().strip().lower()
                except EOFError:
                    cmd = "beenden"
                if cmd == "beenden":
                    break
                print("Unbekanntes Kommando. Zum Auswerten und Beenden 'beenden' eingeben:")
            aggregation_wait = read_aggregation_progress(
                args.expected_source_count,
                getattr(proc, "ojies_completion_log_path", None),
            )
            aggregation_wait["wait_status"] = "interactive_stop"
        else:
            aggregation_wait = wait_for_verified_aggregation(
                proc,
                args.aggregation_timeout,
                args.expected_source_count,
                source_proc,
            )
            print(f"Aggregations-Warteergebnis: {aggregation_wait['wait_status']}")

        print("Starte den XML-Export; das kann einige Minuten dauern.")

        # 1) Address Space exportieren, WAEHREND der Server noch laeuft
        xml_path = export_address_space(endpoint, run_dir)
    finally:
        # 2) Server auch bei Ctrl+C oder einem Exportfehler stoppen
        stop_server(proc)
        if source_proc is not None:
            stop_source_fixtures(source_proc)

    # 3) Laufzeit-JSON unveraendert sichern und nur die Snapshots auswerten
    completion_log_path = getattr(proc, "ojies_completion_log_path", None)
    snapshots = snapshot_runtime_inputs(run_dir, completion_log_path)
    data, rules = collect_raw_data(run_dir / "inputs")
    data["validation_run"] = {
        "run_id": run_dir.name,
        "started_at_utc": started_at,
        "run_directory": workspace_path(run_dir),
        "manifest": "manifest.json",
        "artifact_set_status": "incomplete",
        "expected_source_count": args.expected_source_count,
        "aggregation_timeout_seconds": args.aggregation_timeout,
        "source_fixture_orchestration": (
            "runner_started_four_domain_fixture"
            if args.start_source_fixtures else "external"
        ),
    }
    data["aggregation_wait"] = aggregation_wait

    # 4) Address-Space-XML parsen und Regeln damit anreichern
    if xml_path is not None:
        data["address_space"]["xml_path"] = "address_space.xml"
        try:
            nodes = parse_nodeset_xml(xml_path)
            live_nsid_to_uri = parse_namespace_uris_from_xml(xml_path)
            data["address_space"]["total_node_count"] = len(nodes)
            data["address_space"]["node_count_by_type"] = dict(
                Counter(n.tag for n in nodes.values())
            )
            data["address_space"]["namespace_count_in_xml"] = len(live_nsid_to_uri)
            data["type_integrity"] = build_type_integrity_report(
                nodes,
                live_nsid_to_uri,
                data["namespaces"]["raw"],
                data["nodeset_mapping"]["raw"],
            )
            # namespaces.json wird hier bewusst NICHT verwendet: die XML hat
            # ihre eigene, tatsaechliche Namespace-Tabelle (siehe Docstring
            # von parse_namespace_uris_from_xml).
            data["rules_enriched"] = enrich_rules_with_namespace(
                rules,
                nodes,
                live_nsid_to_uri,
                {
                    canonical_source_id(host.get("address")): host.get("name")
                    for host in data["config"]["hosts"]
                    if host.get("address") and host.get("name")
                },
            )
        except Exception as e:
            print(f"[WARNUNG] Konnte Address-Space-XML nicht auswerten: {e}")
            data["rules_enriched"] = build_unresolved_rules_enriched(rules)
            data["type_integrity"] = {
                "passed": False,
                "dangling_type_definition_count": None,
                "missing_required_type_namespaces": ["not assessed"],
                "missing_required_model_namespaces": ["not assessed"],
                "unremapped_standardized_type_definition_count": None,
            }
    else:
        data["rules_enriched"] = build_unresolved_rules_enriched(rules)
        data["type_integrity"] = {
            "passed": False,
            "dangling_type_definition_count": None,
            "missing_required_type_namespaces": ["not assessed"],
            "missing_required_model_namespaces": ["not assessed"],
            "unremapped_standardized_type_definition_count": None,
        }

    data["rule_semantics"] = build_rule_semantics_report(data["rules_enriched"])
    data["machine_vision_assignment"] = (
        build_machine_vision_assignment_report(data["rules_enriched"])
    )
    data["namespace_distribution"] = build_namespace_distribution(data["rules_enriched"])
    data["domain_table"] = build_domain_table(data["rules_enriched"], data["config"]["hosts"])
    data["validation_run"]["artifact_set_status"] = (
        "complete" if artifact_set_is_complete(data, snapshots, xml_path)
        else "incomplete"
    )
    data["e01_acceptance"] = build_e01_acceptance(
        data, args.expected_source_count
    )

    # 5) Ausgeben, hashen und erst danach den Latest-Zeiger aktualisieren
    outputs = write_outputs(data, run_dir)
    server_log_path = run_dir / "server_console.log"
    if server_log_path.exists():
        outputs["server_console_log"] = server_log_path
    source_log_path = run_dir / "source_servers_console.log"
    if source_log_path.exists():
        outputs["source_servers_console_log"] = source_log_path
    if completion_log_path is not None and completion_log_path.exists():
        preserved_completion_log = run_dir / "device_completions.log"
        shutil.copy2(completion_log_path, preserved_completion_log)
        outputs["device_completions_log"] = preserved_completion_log
    xml_export_log_path = run_dir / "xml_export_console.log"
    if xml_export_log_path.exists():
        outputs["xml_export_console_log"] = xml_export_log_path
    xml_export_skipped_path = run_dir / "xml_export_skipped_nodes.json"
    if xml_export_skipped_path.exists():
        outputs["xml_export_skipped_nodes"] = xml_export_skipped_path
    exported_xml = run_dir / "address_space.xml"
    if exported_xml.exists():
        outputs["address_space"] = exported_xml
    outputs.update(capture_repository_provenance(run_dir))
    manifest_path = write_run_manifest(
        run_dir, started_at, endpoint, snapshots, outputs, data
    )
    print(f"Manifest gespeichert: {manifest_path}")
    if data["validation_run"]["artifact_set_status"] != "complete":
        print("[WARNUNG] Artefaktsatz unvollstaendig; latest_complete_run.txt bleibt unveraendert.")
    elif not data["e01_acceptance"]["passed"]:
        failed_criteria = [
            name for name, passed in data["e01_acceptance"]["criteria"].items()
            if not passed
        ]
        print(
            "[WARNUNG] E01 nicht bestanden; kein VALIDATED-Marker. "
            f"Fehlgeschlagen: {failed_criteria}"
        )


if __name__ == "__main__":
    main()
