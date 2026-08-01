# SPDX-FileCopyrightText: 2026 Adrian Reuther
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

import argparse
import asyncio
import hashlib
import importlib.util
import ipaddress
import logging
import math
import os
import random
import signal
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
import re
import socket
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from zeroconf import ServiceBrowser, ServiceInfo, ServiceListener, Zeroconf

from asyncua import Server, ua, uamethod

try:
    import msvcrt
except ImportError:  # pragma: no cover - Windows has msvcrt
    msvcrt = None

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX platforms
    fcntl = None


# =============================================================================
# Namespace URIs
# =============================================================================

NS_DI = "http://opcfoundation.org/UA/DI/"
NS_MACHINERY = "http://opcfoundation.org/UA/Machinery/"
NS_ROBOTICS = "http://opcfoundation.org/UA/Robotics/"
NS_MACHINE_VISION = "http://opcfoundation.org/UA/MachineVision"
NS_MACHINE_VISION_AMCM = "http://opcfoundation.org/UA/MachineVision/AMCM/"
NS_ADDITIVE_MANUFACTURING = "http://opcfoundation.org/UA/AdditiveManufacturing/"
NS_WOODWORKING = "http://opcfoundation.org/UA/Woodworking/"

NS_VENDOR_BASE = "urn:plcm:validation"
OPCUA_MDNS_TYPE = "_opcua-tcp._tcp.local."
LDS_MDNS_CAPABILITY = "LDS"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPO_NODESETS = PROJECT_ROOT / "nodesets"
LOCAL_NODESETS = PROJECT_ROOT / "opcua_agg_server" / "nodesets"
OPEN62541_NODESETS = PROJECT_ROOT / "open62541" / "deps" / "ua-nodeset"
CONFIGURATION_SOURCE_ROOT = (
    PROJECT_ROOT.parent / "Validation" / "configuration" / "source_servers"
)
GENERIC_ROBOT_ROOT = CONFIGURATION_SOURCE_ROOT / "generic-robot"
GENERIC_ROBOT_NODESETS = (
    GENERIC_ROBOT_ROOT / "src" / "generic_robot_server" / "nodesets"
)
GENERIC_ROBOT_FILE_TRANSFER = (
    GENERIC_ROBOT_ROOT / "src" / "generic_robot_server" / "file_transfer.py"
)
GENERIC_ROBOT_CORE_NODESETS = (
    "Devices_1601277154-fixed.xml",
    "Industrial Automation - Basics_3106070943.xml",
    "Robotics - Vertical Integration_2086359919.xml",
    "plcm_generic_nodeset.xml",
    "plcm_generic_skills.xml",
)
ENRICHED_URDF_FIXTURES = (
    (
        "eva-automata",
        CONFIGURATION_SOURCE_ROOT
        / "eva-automata"
        / "nodesets"
        / "eva_generic_urdf_enriched.xml",
    ),
    (
        "franka-research-3",
        CONFIGURATION_SOURCE_ROOT
        / "franka-research-3"
        / "nodesets"
        / "franka_nodes_urdf_enriched.xml",
    ),
    (
        "ur5e",
        CONFIGURATION_SOURCE_ROOT
        / "ur5e"
        / "nodesets"
        / "ur5e_nodeset_urdf_enriched.xml",
    ),
)
NS_URDF_TRANSFORMER = "URDF-Transformer"
MIN_SESSION_TIMEOUT_MS = 30 * 60 * 1000
MAX_SESSION_TIMEOUT_MS = 60 * 60 * 1000
LDS_MAX_CONCURRENT_REGISTRATIONS = 2
LDS_REGISTRATION_STAGGER_STEP_S = 2.5
LDS_REGISTRATION_TIMEOUT_S = 10.0
LDS_REGISTRATION_PERIOD_S = 30
LOCAL_LDS_FALLBACK_HOST = "127.0.0.1"
LOCAL_LDS_FALLBACK_PORT = 4840

_lds_registration_gate: Optional[asyncio.Semaphore] = None


def measurement_event(event: str, details: str) -> None:
    """Emit one timestamped, line-oriented event for validation acquisition."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    run_id = os.getenv("OJIES_RUN_ID", "").strip()
    run_field = f" run_id={run_id}" if run_id else ""
    print(f"[{timestamp}] {event}{run_field} {details}", flush=True)


# =============================================================================
# OPC UA Methods
# =============================================================================

@uamethod
async def reset(parent, *args):
    return "OK: reset executed"


@uamethod
async def home(parent):
    return "OK: robot homed"


@uamethod
async def go_to(parent, target):
    return f"OK: robot moved to {target}"


@uamethod
async def trigger_camera(parent, *args):
    return "OK: image acquisition triggered"


@uamethod
async def start_job(parent, job_name):
    return f"OK: additive manufacturing job started: {job_name}"


@uamethod
async def pause_job(parent):
    return "OK: job paused"


@uamethod
async def request_job_response(parent, *args):
    return "OK: job response requested"


# =============================================================================
# Data Structures
# =============================================================================

@dataclass
class SourceServerConfig:
    domain: str
    instance_id: int
    port: int
    namespace_uris: List[str]
    semantic_instance_id: Optional[int] = None
    semantic_source_slot: Optional[int] = None
    urdf_fixture_name: Optional[str] = None
    urdf_nodeset_path: Optional[Path] = None
    urdf_nodeset_sha256: Optional[str] = None


def machine_vision_semantic_mapping(
    instance_id: int,
    replicas_per_domain: int,
) -> Tuple[int, int]:
    if replicas_per_domain > 1 and replicas_per_domain % 2 == 0:
        group_size = replicas_per_domain // 2
        if instance_id <= group_size:
            return 1, instance_id
        return 2, instance_id - group_size

    return instance_id, instance_id


def source_application_uri(config: SourceServerConfig) -> str:
    if config.urdf_fixture_name is not None:
        return (
            f"urn:plcm:robot-server:{config.urdf_fixture_name}-"
            f"{config.instance_id:03d}"
        )
    return f"urn:plcm:{config.domain}:{config.instance_id:03d}"


# =============================================================================
# Helper Functions
# =============================================================================

async def register_namespaces(
    server: Server,
    namespace_uris: List[str],
    shuffle_namespaces: bool,
) -> Dict[str, int]:
    """
    Registers namespaces on the OPC UA server.

    If shuffle_namespaces=True, the namespace registration order is randomized.
    This is useful to test whether the Aggregation Server correctly resolves
    NamespaceArray URIs instead of relying on fixed namespace indices.
    """
    uris = list(namespace_uris)

    if shuffle_namespaces:
        random.shuffle(uris)

    ns_map: Dict[str, int] = {}

    for uri in uris:
        ns_map[uri] = await server.register_namespace(uri)

    return ns_map


async def add_writable_variable(parent, ns_idx: int, name: str, value):
    var = await parent.add_variable(ns_idx, name, value)
    await var.set_writable()
    return var


def clamp_requested_session_timeout(server: Server) -> None:
    original_create_session = server.iserver.create_session

    def create_session_with_clamped_timeout(*args, **kwargs):
        session = original_create_session(*args, **kwargs)
        original_session_create = session.create_session

        async def create_session(params, sockname=None):
            requested_timeout = getattr(params, "RequestedSessionTimeout", None)
            if requested_timeout is None or requested_timeout <= 0:
                params.RequestedSessionTimeout = MAX_SESSION_TIMEOUT_MS
            else:
                params.RequestedSessionTimeout = max(
                    MIN_SESSION_TIMEOUT_MS,
                    min(requested_timeout, MAX_SESSION_TIMEOUT_MS),
                )
            return await original_session_create(params, sockname=sockname)

        session.create_session = create_session
        return session

    server.iserver.create_session = create_session_with_clamped_timeout


async def import_xml_file(
    server: Server,
    path: Path,
    *,
    strip_required_models: bool = False,
) -> List[ua.NodeId]:
    if not path.exists():
        raise FileNotFoundError(f"Missing NodeSet XML: {path}")

    if strip_required_models:
        xml = path.read_text(encoding="utf-8")
        xml = re.sub(r"\s*<RequiredModel\b[^>]*/>", "", xml)
        try:
            return await server.import_xml(
                xmlstring=xml,
                strict_mode=False,
                auto_load_definitions=False,
            )
        except TypeError:
            return await server.import_xml(xmlstring=xml, strict_mode=False)

    try:
        return await server.import_xml(
            str(path),
            strict_mode=False,
            auto_load_definitions=False,
        )
    except TypeError:
        return await server.import_xml(str(path), strict_mode=False)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_embedded_file_transfer_class():
    """Load Generic Robot's FileType adapter only for the opt-in URDF fixture."""
    if not GENERIC_ROBOT_FILE_TRANSFER.is_file():
        raise FileNotFoundError(
            f"Missing Generic Robot FileType adapter: {GENERIC_ROBOT_FILE_TRANSFER}"
        )

    spec = importlib.util.spec_from_file_location(
        "ojies_generic_robot_file_transfer",
        GENERIC_ROBOT_FILE_TRANSFER,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(
            f"Unable to load FileType adapter: {GENERIC_ROBOT_FILE_TRANSFER}"
        )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.EmbeddedFileTypeTransfer


async def import_enriched_urdf_fixture(
    server: Server,
    config: SourceServerConfig,
):
    """Import one robot NodeSet with the exact Generic Robot dependencies."""
    if config.urdf_nodeset_path is None or config.urdf_fixture_name is None:
        raise ValueError("URDF fixture configuration is incomplete")

    for nodeset_name in GENERIC_ROBOT_CORE_NODESETS:
        await import_xml_file(server, GENERIC_ROBOT_NODESETS / nodeset_name)

    imported_node_ids = await import_xml_file(server, config.urdf_nodeset_path)
    imported_nodes = [server.get_node(node_id) for node_id in imported_node_ids]

    file_transfer_type = load_embedded_file_transfer_class()
    file_transfer = file_transfer_type(server)
    bound_file_count = await file_transfer.bind_imported_nodes(imported_nodes)
    if bound_file_count < 1:
        raise RuntimeError(
            f"URDF fixture {config.urdf_fixture_name!r} has no bound FileType payload"
        )

    namespaces = await server.get_namespace_array()
    if NS_URDF_TRANSFORMER not in namespaces:
        raise RuntimeError(
            f"URDF fixture {config.urdf_fixture_name!r} did not import "
            f"the {NS_URDF_TRANSFORMER!r} namespace"
        )
    urdf_namespace_index = namespaces.index(NS_URDF_TRANSFORMER)
    urdf_name = server.get_node(ua.NodeId(1, urdf_namespace_index))
    if (await urdf_name.read_browse_name()).Name != "URDFName":
        raise RuntimeError(
            f"URDF fixture {config.urdf_fixture_name!r} has no verifiable URDFName node"
        )

    measurement_event(
        "URDF FIXTURE IMPORTED",
        (
            f"fixture={config.urdf_fixture_name} "
            f"nodeset={config.urdf_nodeset_path.name} "
            f"sha256={config.urdf_nodeset_sha256} "
            f"imported_nodes={len(imported_node_ids)} "
            f"bound_filetypes={bound_file_count}"
        ),
    )
    return file_transfer


async def import_additive_nodesets(server: Server) -> None:
    for path in [
        OPEN62541_NODESETS / "DI" / "Opc.Ua.Di.NodeSet2.xml",
        OPEN62541_NODESETS / "IA" / "Opc.Ua.IA.NodeSet2.xml",
        OPEN62541_NODESETS / "PADIM" / "Opc.Ua.IRDI.NodeSet2.xml",
        OPEN62541_NODESETS / "PADIM" / "Opc.Ua.PADIM.NodeSet2.xml",
        OPEN62541_NODESETS / "Machinery" / "Opc.Ua.Machinery.NodeSet2.xml",
        OPEN62541_NODESETS / "Machinery" / "ProcessValues" / "opc.ua.machinery.processvalues.xml",
        OPEN62541_NODESETS / "ISA95-JOBCONTROL" / "opc.ua.isa95-jobcontrol.nodeset2.xml",
        OPEN62541_NODESETS / "MachineTool" / "Opc.Ua.MachineTool.NodeSet2.xml",
    ]:
        await import_xml_file(server, path)

    # This AdditiveManufacturing NodeSet is newer than some dependency XMLs
    # available locally, so only the RequiredModel version gate is relaxed.
    await import_xml_file(
        server,
        LOCAL_NODESETS / "Opc.Ua.AdditiveManufacturing.Nodeset2.xml",
        strip_required_models=True,
    )


async def import_woodworking_nodesets(server: Server) -> None:
    for path in [
        OPEN62541_NODESETS / "DI" / "Opc.Ua.Di.NodeSet2.xml",
        OPEN62541_NODESETS / "Machinery" / "Opc.Ua.Machinery.NodeSet2.xml",
    ]:
        await import_xml_file(server, path)

    # Prefer the repository-local Woodworking NodeSet. If newer dependency
    # models are not available in the current environment, fall back to the
    # vendored open62541 Woodworking NodeSet.
    candidates = [
        LOCAL_NODESETS / "Opc.Ua.Woodworking.NodeSet2.xml",
        OPEN62541_NODESETS / "Woodworking" / "Opc.Ua.Woodworking.NodeSet2.xml",
    ]
    last_error: Optional[Exception] = None

    for candidate in candidates:
        try:
            await import_xml_file(
                server,
                candidate,
                strip_required_models=True,
            )
            return
        except Exception as exc:
            last_error = exc
            print(f"[NodeSet] Failed to import {candidate}: {exc!r}")

    raise RuntimeError("Unable to import a Woodworking NodeSet") from last_error


async def import_robotics_nodesets(server: Server) -> None:
    logging.getLogger("asyncua.common.xmlimporter").setLevel(logging.ERROR)

    di_candidates = [
        REPO_NODESETS / "Opc.Ua.Di.NodeSet2.xml",
        OPEN62541_NODESETS / "DI" / "Opc.Ua.Di.NodeSet2.xml",
    ]
    robotics_candidates = [
        REPO_NODESETS / "Opc.Ua.Robotics.NodeSet2.xml",
        LOCAL_NODESETS / "Opc.Ua.Robotics.NodeSet2.xml",
        OPEN62541_NODESETS / "Robotics" / "Opc.Ua.Robotics.NodeSet2.xml",
    ]

    last_error: Optional[Exception] = None

    for candidate in di_candidates:
        try:
            await import_xml_file(server, candidate, strip_required_models=True)
            break
        except Exception as exc:
            last_error = exc
            print(f"[NodeSet] Failed to import {candidate}: {exc!r}")
    else:
        raise RuntimeError("Unable to import a DI NodeSet for Robotics") from last_error

    for candidate in robotics_candidates:
        try:
            await import_xml_file(server, candidate, strip_required_models=True)
            return
        except Exception as exc:
            last_error = exc
            print(f"[NodeSet] Failed to import {candidate}: {exc!r}")

    raise RuntimeError("Unable to import a Robotics NodeSet") from last_error


async def import_machine_vision_nodesets(server: Server) -> None:
    logging.getLogger("asyncua.common.xmlimporter").setLevel(logging.ERROR)

    dependency_groups = [
        (
            "DI",
            [
                REPO_NODESETS / "Opc.Ua.Di.NodeSet2.xml",
                OPEN62541_NODESETS / "DI" / "Opc.Ua.Di.NodeSet2.xml",
            ],
        ),
        (
            "Machinery",
            [
                REPO_NODESETS / "Opc.Ua.Machinery.NodeSet2.xml",
                OPEN62541_NODESETS / "Machinery" / "Opc.Ua.Machinery.NodeSet2.xml",
            ],
        ),
    ]

    last_error: Optional[Exception] = None

    for group_name, candidates in dependency_groups:
        for candidate in candidates:
            try:
                await import_xml_file(server, candidate, strip_required_models=True)
                break
            except Exception as exc:
                last_error = exc
                print(f"[NodeSet] Failed to import {candidate}: {exc!r}")
        else:
            raise RuntimeError(f"Unable to import a {group_name} NodeSet for MachineVision") from last_error

    machine_vision_candidates = [
        REPO_NODESETS / "Opc.Ua.MachineVision.NodeSet2.xml",
        LOCAL_NODESETS / "Opc.Ua.MachineVision.NodeSet2.xml",
        OPEN62541_NODESETS / "MachineVision" / "Opc.Ua.MachineVision.NodeSet2.xml",
    ]

    for candidate in machine_vision_candidates:
        try:
            await import_xml_file(
                server,
                candidate,
                strip_required_models=True,
            )
            break
        except Exception as exc:
            last_error = exc
            print(f"[NodeSet] Failed to import {candidate}: {exc!r}")
    else:
        raise RuntimeError("Unable to import a MachineVision NodeSet") from last_error

    amcm_candidates = [
        REPO_NODESETS / "Opc.Ua.MachineVision.AMCM.NodeSet2.xml",
    ]

    for candidate in amcm_candidates:
        try:
            await import_xml_file(
                server,
                candidate,
                strip_required_models=True,
            )
            return
        except Exception as exc:
            last_error = exc
            print(f"[NodeSet] Failed to import {candidate}: {exc!r}")

    raise RuntimeError("Unable to import a MachineVision AMCM NodeSet") from last_error


async def import_companion_nodesets(server: Server, domain: str) -> None:
    if domain == "additive-manufacturing":
        await import_additive_nodesets(server)
    elif domain == "machine-vision":
        await import_machine_vision_nodesets(server)
    elif domain == "robotics":
        await import_robotics_nodesets(server)
    elif domain == "woodworking":
        await import_woodworking_nodesets(server)


async def namespace_index(server: Server, namespace_uri: str) -> int:
    namespaces = await server.get_namespace_array()
    if namespace_uri not in namespaces:
        return await server.register_namespace(namespace_uri)
    return namespaces.index(namespace_uri)


async def find_child(parent, browse_name: str):
    for child in await parent.get_children():
        try:
            if (await child.read_browse_name()).Name == browse_name:
                return child
        except Exception:
            continue
    return None


async def ensure_object(parent, ns_idx: int, browse_name: str):
    existing = await find_child(parent, browse_name)
    if existing is not None:
        return existing
    return await parent.add_object(ns_idx, browse_name)


async def ensure_path(parent, ns_idx: int, browse_names: Iterable[str]):
    node = parent
    for browse_name in browse_names:
        node = await ensure_object(node, ns_idx, browse_name)
    return node


async def ensure_variable(
    parent,
    ns_idx: int,
    browse_name: str,
    value,
    *,
    writable: bool = True,
):
    existing = await find_child(parent, browse_name)
    if existing is not None:
        try:
            if await existing.read_node_class() == ua.NodeClass.Variable:
                await write_value_compatible(existing, value)
                if writable:
                    await existing.set_writable()
                return existing
        except Exception:
            return existing

    if writable:
        return await add_writable_variable(parent, ns_idx, browse_name, value)
    return await parent.add_variable(ns_idx, browse_name, value)


def _coerce_for_variant_type(value, variant_type: ua.VariantType):
    if value is None:
        return value

    if variant_type == ua.VariantType.LocalizedText:
        if isinstance(value, ua.LocalizedText):
            return value
        return ua.LocalizedText(str(value))

    if variant_type == ua.VariantType.Boolean:
        if isinstance(value, bool):
            return value
        return bool(value)

    if variant_type in {
        ua.VariantType.SByte,
        ua.VariantType.Byte,
        ua.VariantType.Int16,
        ua.VariantType.UInt16,
        ua.VariantType.Int32,
        ua.VariantType.UInt32,
        ua.VariantType.Int64,
        ua.VariantType.UInt64,
    }:
        return int(value)

    if variant_type in {ua.VariantType.Float, ua.VariantType.Double}:
        return float(value)

    if variant_type == ua.VariantType.String:
        return str(value)

    return value


async def write_value_compatible(node, value) -> None:
    variant_type: Optional[ua.VariantType] = None

    try:
        variant_type = await node.read_data_type_as_variant_type()
    except Exception:
        variant_type = None

    if variant_type is None:
        await node.write_value(value)
        return

    coerced = _coerce_for_variant_type(value, variant_type)
    await node.write_value(ua.Variant(coerced, variant_type))


class SingleInstanceGuard:
    """Best-effort process guard to prevent duplicate source_servers runs."""

    def __init__(self, lock_name: str = "source_servers.lock"):
        self.lock_path = Path(tempfile.gettempdir()) / lock_name
        self.handle = None

    def acquire(self) -> bool:
        self.handle = open(self.lock_path, "a+", encoding="utf-8")

        try:
            if os.name == "nt" and msvcrt is not None:
                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            elif fcntl is not None:
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            else:
                # No locking backend available; keep running without a guard.
                return True

            self.handle.seek(0)
            self.handle.truncate(0)
            self.handle.write(str(os.getpid()))
            self.handle.flush()
            return True
        except OSError:
            self.release()
            return False

    def release(self) -> None:
        if self.handle is None:
            return

        try:
            if os.name == "nt" and msvcrt is not None:
                self.handle.seek(0)
                try:
                    msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
                except OSError:
                    pass
            elif fcntl is not None:
                try:
                    fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
                except OSError:
                    pass
        finally:
            self.handle.close()
            self.handle = None


def _get_lds_registration_gate() -> asyncio.Semaphore:
    global _lds_registration_gate
    if _lds_registration_gate is None:
        _lds_registration_gate = asyncio.Semaphore(LDS_MAX_CONCURRENT_REGISTRATIONS)
    return _lds_registration_gate


def normalize_lds_url(lds_url: str) -> str:
    # mDNS replies may include a trailing slash, while explicit URLs often do
    # not. Keep one canonical form so logs and reconnect behavior stay stable.
    return lds_url.rstrip("/")


async def link_or_add_method(
    server: Server,
    parent,
    ns_idx: int,
    browse_name: str,
    callback,
    input_args: List[ua.VariantType],
    output_args: List[ua.VariantType],
):
    existing = await find_child(parent, browse_name)
    if existing is not None:
        try:
            if await existing.read_node_class() == ua.NodeClass.Method:
                server.link_method(existing, callback)
                return existing
        except Exception:
            pass

    return await parent.add_method(ns_idx, browse_name, callback, input_args, output_args)


async def find_object_type(server: Server, namespace_uri: str, browse_name: str):
    namespaces = await server.get_namespace_array()
    queue = [server.nodes.base_object_type]
    seen = set()

    while queue:
        node = queue.pop(0)
        key = node.nodeid.to_string()
        if key in seen:
            continue
        seen.add(key)

        try:
            node_browse_name = await node.read_browse_name()
            if (
                node_browse_name.Name == browse_name
                and node_browse_name.NamespaceIndex < len(namespaces)
                and namespaces[node_browse_name.NamespaceIndex] == namespace_uri
            ):
                return node

            queue.extend(await node.get_children())
        except Exception:
            continue

    raise ValueError(f"ObjectType {browse_name!r} not found in namespace {namespace_uri!r}")


async def find_reference_type(server: Server, namespace_uri: str, browse_name: str):
    namespaces = await server.get_namespace_array()
    queue = [server.nodes.reference_types]
    seen = set()

    while queue:
        node = queue.pop(0)
        key = node.nodeid.to_string()
        if key in seen:
            continue
        seen.add(key)

        try:
            node_browse_name = await node.read_browse_name()
            if (
                node_browse_name.Name == browse_name
                and node_browse_name.NamespaceIndex < len(namespaces)
                and namespaces[node_browse_name.NamespaceIndex] == namespace_uri
            ):
                return node

            queue.extend(await node.get_children())
        except Exception:
            continue

    raise ValueError(f"ReferenceType {browse_name!r} not found in namespace {namespace_uri!r}")


async def add_reference_if_available(
    server: Server,
    source,
    target,
    namespace_uri: str,
    reference_type_name: str,
    *,
    forward: bool = True,
) -> None:
    try:
        reference_type = await find_reference_type(server, namespace_uri, reference_type_name)
        await source.add_reference(target, reference_type.nodeid, forward=forward, bidirectional=True)
    except Exception:
        return


async def find_reference_type_by_browse_name(server: Server, browse_name: str):
    queue = [server.nodes.reference_types]
    seen = set()

    while queue:
        node = queue.pop(0)
        key = node.nodeid.to_string()
        if key in seen:
            continue
        seen.add(key)

        try:
            node_browse_name = await node.read_browse_name()
            if node_browse_name.Name == browse_name:
                return node

            queue.extend(await node.get_children())
        except Exception:
            continue

    return None


async def add_reference_by_browse_name_if_available(
    server: Server,
    source,
    target,
    reference_type_name: str,
    *,
    forward: bool = True,
) -> None:
    try:
        reference_type = await find_reference_type_by_browse_name(
            server,
            reference_type_name,
        )
        if reference_type is not None:
            await source.add_reference(
                target,
                reference_type.nodeid,
                forward=forward,
                bidirectional=True,
            )
    except Exception:
        return


async def instantiate_object_type(
    server: Server,
    parent,
    namespace_uri: str,
    ns_idx: int,
    instance_name: str,
    type_name: str,
    *,
    allow_plain_fallback: bool = False,
    instantiate_optional: bool = True,
):
    object_type = await find_object_type(server, namespace_uri, type_name)
    try:
        return await parent.add_object(
            ns_idx,
            instance_name,
            objecttype=object_type.nodeid,
            instantiate_optional=instantiate_optional,
        )
    except ua.UaError:
        if allow_plain_fallback:
            return await ensure_object(parent, ns_idx, instance_name)
        raise


async def ensure_typed_object(
    server: Server,
    parent,
    namespace_uri: str,
    ns_idx: int,
    browse_name: str,
    type_name: str,
    *,
    instantiate_optional: bool = True,
):
    existing = await find_child(parent, browse_name)
    if existing is not None:
        return existing

    return await instantiate_object_type(
        server,
        parent,
        namespace_uri,
        ns_idx,
        browse_name,
        type_name,
        allow_plain_fallback=True,
        instantiate_optional=instantiate_optional,
    )


def is_placeholder_browse_name(name: str) -> bool:
    return name.startswith("<") and name.endswith(">")


async def remove_placeholder_children(parent, seen: Optional[set] = None) -> None:
    if seen is None:
        seen = set()

    key = parent.nodeid.to_string()
    if key in seen:
        return
    seen.add(key)

    for child in list(await parent.get_children()):
        try:
            browse_name = (await child.read_browse_name()).Name
        except Exception:
            continue

        if is_placeholder_browse_name(browse_name):
            await child.delete(delete_references=True, recursive=True)
        else:
            await remove_placeholder_children(child, seen)


async def remove_child_if_present(parent, browse_name: str) -> None:
    child = await find_child(parent, browse_name)
    if child is not None:
        await child.delete(delete_references=True, recursive=True)


async def set_child_value_if_present(
    parent,
    browse_name: str,
    value,
    *,
    writable: bool = False,
):
    child = await find_child(parent, browse_name)
    if child is None:
        return None

    try:
        if await child.read_node_class() == ua.NodeClass.Variable:
            await write_value_compatible(child, value)
            if writable:
                await child.set_writable()
    except Exception:
        return child

    return child


async def ensure_am_process_value(
    server: Server,
    parent,
    idx_am: int,
    idx_instance: int,
    browse_name: str,
    value,
    *,
    category: int,
    severity: Optional[int] = 0,
):
    process_value = await ensure_typed_object(
        server,
        parent,
        NS_ADDITIVE_MANUFACTURING,
        idx_instance,
        browse_name,
        "ProcessValueAMType",
        instantiate_optional=True,
    )
    await remove_placeholder_children(process_value)
    await set_child_value_if_present(process_value, "Category", category)

    if severity is not None:
        await set_child_value_if_present(process_value, "Severity", severity)

    analog_signal = await find_child(process_value, "AnalogSignal")
    if analog_signal is None:
        analog_signal = await ensure_variable(process_value, idx_am, "AnalogSignal", value)
    else:
        await write_value_compatible(analog_signal, value)
        try:
            await analog_signal.set_writable()
        except Exception:
            pass

    return analog_signal



def get_lan_ip() -> str:
    """
    Ermittelt die lokale LAN-IP, die im mDNS/LDS-ME Announcement veröffentlicht wird.
    Wichtig: Nicht 0.0.0.0 announcen, sondern die tatsächlich erreichbare Host-IP.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        sock.connect(("8.8.8.8", 80))
        return sock.getsockname()[0]
    except Exception:
        return socket.gethostbyname(socket.gethostname())
    finally:
        sock.close()


def _decode_txt_property(properties: Dict[bytes, bytes], key: str) -> str:
    value = properties.get(key.encode("utf-8"), b"")
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace").strip()
    return str(value).strip()


def _service_has_capability(info: ServiceInfo, capability: str) -> bool:
    raw_capabilities = _decode_txt_property(info.properties, "caps")
    capabilities = {
        item.strip().upper()
        for item in re.split(r"[,\s]+", raw_capabilities)
        if item.strip()
    }
    return capability.upper() in capabilities


def _address_affinity(address: str, preferred_ip: Optional[str]) -> int:
    if not preferred_ip:
        return 0

    try:
        candidate = ipaddress.ip_address(address.split("%", 1)[0])
        preferred = ipaddress.ip_address(preferred_ip.split("%", 1)[0])
    except ValueError:
        return -1

    if candidate.version != preferred.version:
        return -1

    xor = int(candidate) ^ int(preferred)
    return candidate.max_prefixlen if xor == 0 else candidate.max_prefixlen - xor.bit_length()


def _is_loopback_ip(address: str) -> bool:
    try:
        return ipaddress.ip_address(address.split("%", 1)[0]).is_loopback
    except ValueError:
        return False


def _requires_loopback_lds(preferred_ip: Optional[str]) -> bool:
    if not preferred_ip:
        return False
    return _is_loopback_ip(preferred_ip)


def _service_info_to_discovery_url(
    info: ServiceInfo,
    preferred_ip: Optional[str] = None,
) -> Optional[str]:
    addresses = info.parsed_addresses()
    if _requires_loopback_lds(preferred_ip):
        addresses = [address for address in addresses if _is_loopback_ip(address)]

    if preferred_ip:
        addresses.sort(
            key=lambda address: _address_affinity(address, preferred_ip),
            reverse=True,
        )

    host = next((address for address in addresses if ":" not in address), None)
    host = host or (addresses[0] if addresses else None)
    host = host or (info.server.rstrip(".") if info.server else None)

    if _requires_loopback_lds(preferred_ip) and host:
        normalized_host = host.strip("[]")
        if not _is_loopback_ip(normalized_host) and normalized_host.lower() not in {
            "localhost",
            "localhost.local",
        }:
            return None

    if not host or not info.port:
        return None

    if ":" in host and not host.startswith("["):
        host = f"[{host}]"

    path = _decode_txt_property(info.properties, "path")
    if path and not path.startswith("/"):
        path = f"/{path}"

    return f"opc.tcp://{host}:{info.port}{path}"


class MdnsLdsDiscoveryListener(ServiceListener):
    """Selects OPC UA mDNS records advertising the standard LDS capability."""

    def __init__(self, preferred_ip: Optional[str] = None):
        self.discovery_url: Optional[str] = None
        self.found = threading.Event()
        self.preferred_ip = preferred_ip
        self._resolver_pool = ThreadPoolExecutor(
            max_workers=4,
            thread_name_prefix="opcua-lds-mdns",
        )

    def _inspect_service(self, zeroconf: Zeroconf, service_type: str, name: str) -> None:
        if self.found.is_set():
            return

        info = zeroconf.get_service_info(service_type, name, timeout=2000)
        if info is None or not _service_has_capability(info, LDS_MDNS_CAPABILITY):
            return

        discovery_url = _service_info_to_discovery_url(info, self.preferred_ip)
        if discovery_url is None:
            return

        self.discovery_url = discovery_url
        self.found.set()

    def _queue_inspection(self, zeroconf: Zeroconf, service_type: str, name: str) -> None:
        # Zeroconf invokes listeners on its own event-loop thread. Resolving the
        # full record there would block that loop. A small bounded pool also
        # avoids creating one thread per announced source server in stress tests.
        if self.found.is_set():
            return
        try:
            self._resolver_pool.submit(
                self._inspect_service,
                zeroconf,
                service_type,
                name,
            )
        except RuntimeError:
            # The browser may deliver one final callback during shutdown.
            pass

    def close(self) -> None:
        self._resolver_pool.shutdown(wait=True, cancel_futures=True)

    def add_service(self, zeroconf: Zeroconf, service_type: str, name: str) -> None:
        self._queue_inspection(zeroconf, service_type, name)

    def update_service(self, zeroconf: Zeroconf, service_type: str, name: str) -> None:
        self._queue_inspection(zeroconf, service_type, name)

    def remove_service(self, zeroconf: Zeroconf, service_type: str, name: str) -> None:
        return


def discover_lds_via_mdns(
    timeout: float = 5.0,
    preferred_ip: Optional[str] = None,
) -> Optional[str]:
    """Probe the multicast subnet and return the first announced LDS-ME URL."""

    listener = MdnsLdsDiscoveryListener(preferred_ip)
    zeroconf = Zeroconf()
    browser = ServiceBrowser(zeroconf, OPCUA_MDNS_TYPE, listener=listener)

    try:
        listener.found.wait(timeout=max(timeout, 0.1))
        return listener.discovery_url
    finally:
        browser.cancel()
        listener.close()
        zeroconf.close()


async def discover_lds_until_available(timeout: float, preferred_ip: str) -> str:
    attempt = 0
    while True:
        attempt += 1
        print(f"[LDS-ME/mDNS] Multicast probe {attempt} on UDP 5353")
        discovery_url = await asyncio.to_thread(
            discover_lds_via_mdns,
            timeout,
            preferred_ip,
        )
        if discovery_url:
            print(f"[LDS-ME/mDNS] LDS announcement received: {discovery_url}")
            return discovery_url

        # On Windows, two mDNS implementations sharing UDP 5353 on the same
        # host do not reliably receive each other's multicast packets. Keep
        # mDNS as the primary discovery mechanism, but when the complete test
        # setup explicitly uses loopback, verify the local standard LDS port
        # and continue with the normal OPC UA RegisterServer workflow.
        if _requires_loopback_lds(preferred_ip):
            try:
                _, writer = await asyncio.wait_for(
                    asyncio.open_connection(
                        LOCAL_LDS_FALLBACK_HOST,
                        LOCAL_LDS_FALLBACK_PORT,
                    ),
                    timeout=1.0,
                )
            except (OSError, asyncio.TimeoutError):
                pass
            else:
                writer.close()
                await writer.wait_closed()
                local_lds_url = (
                    f"opc.tcp://{LOCAL_LDS_FALLBACK_HOST}:"
                    f"{LOCAL_LDS_FALLBACK_PORT}/"
                )
                print(
                    "[LDS-ME/mDNS] Multicast announcement unavailable on "
                    f"loopback; verified local LDS: {local_lds_url}"
                )
                return local_lds_url

        print("[LDS-ME/mDNS] No LDS announcement received; retrying")
        await asyncio.sleep(2.0)


# =============================================================================
# Domain-Specific Address Spaces
# =============================================================================

async def build_robotics_server(
    server: Server,
    ns: Dict[str, int],
    instance_id: int,
):
    idx_di = ns[NS_DI]
    idx_robotics = ns[NS_ROBOTICS]
    idx_vendor = ns[f"{NS_VENDOR_BASE}:robotics"]

    device_set = await ensure_object(server.nodes.objects, idx_di, "DeviceSet")
    motion_device_system = await instantiate_object_type(
        server,
        device_set,
        NS_ROBOTICS,
        idx_robotics,
        "MotionDeviceSystem",
        "MotionDeviceSystemType",
        instantiate_optional=False,
    )

    motion_devices = await ensure_object(motion_device_system, idx_robotics, "MotionDevices")
    controllers = await ensure_object(motion_device_system, idx_robotics, "Controllers")
    safety_states = await ensure_object(motion_device_system, idx_robotics, "SafetyStates")
    await remove_placeholder_children(motion_devices)
    await remove_placeholder_children(controllers)
    await remove_placeholder_children(safety_states)

    motion_device = await ensure_typed_object(
        server,
        motion_devices,
        NS_ROBOTICS,
        idx_robotics,
        f"SixAxisRobot_{instance_id:03d}",
        "MotionDeviceType",
        instantiate_optional=False,
    )

    await ensure_variable(motion_device, idx_di, "Manufacturer", "PLCM Robotics", writable=False)
    await ensure_variable(motion_device, idx_di, "Model", "ValidationSixAxisRobot", writable=False)
    await ensure_variable(motion_device, idx_di, "ProductCode", "PLCM-6R-10", writable=False)
    await ensure_variable(
        motion_device,
        idx_di,
        "SerialNumber",
        f"ROB-{instance_id:03d}",
        writable=False,
    )
    await ensure_variable(motion_device, idx_robotics, "MotionDeviceCategory", 1, writable=False)

    parameter_set = await ensure_object(motion_device, idx_di, "ParameterSet")
    await ensure_variable(parameter_set, idx_robotics, "OnPath", True)
    await ensure_variable(parameter_set, idx_robotics, "InControl", True)

    speed_override = await ensure_variable(
        parameter_set,
        idx_robotics,
        "SpeedOverride",
        75.0,
    )
    robot_state = await ensure_variable(
        parameter_set,
        idx_vendor,
        "RobotState",
        "Idle",
    )
    await ensure_variable(
        parameter_set,
        idx_vendor,
        "OperationMode",
        "Automatic",
    )

    axes = await ensure_object(motion_device, idx_robotics, "Axes")
    power_trains = await ensure_object(motion_device, idx_robotics, "PowerTrains")
    await remove_placeholder_children(axes)
    await remove_placeholder_children(power_trains)
    axis_vars = []

    for i in range(1, 7):
        axis = await ensure_typed_object(
            server,
            axes,
            NS_ROBOTICS,
            idx_robotics,
            f"Axis_{i}",
            "AxisType",
            instantiate_optional=False,
        )
        await remove_placeholder_children(axis)
        await ensure_variable(axis, idx_di, "ComponentName", f"J{i}")
        await ensure_variable(axis, idx_robotics, "MotionProfile", 1, writable=False)

        axis_parameter_set = await ensure_object(axis, idx_di, "ParameterSet")
        actual_position = await ensure_variable(
            axis_parameter_set,
            idx_robotics,
            "ActualPosition",
            0.0,
        )
        await ensure_variable(axis_parameter_set, idx_robotics, "ActualSpeed", 0.0)
        await ensure_variable(axis_parameter_set, idx_robotics, "ActualAcceleration", 0.0)
        await ensure_variable(axis_parameter_set, idx_vendor, "TargetPosition", 0.0)
        axis_vars.append(actual_position)

        power_train = await ensure_typed_object(
            server,
            power_trains,
            NS_ROBOTICS,
            idx_robotics,
            f"PowerTrain_{i}",
            "PowerTrainType",
            instantiate_optional=False,
        )
        await remove_placeholder_children(power_train)
        await ensure_variable(power_train, idx_di, "ComponentName", f"PT{i}")

        motor = await ensure_typed_object(
            server,
            power_train,
            NS_ROBOTICS,
            idx_robotics,
            f"ServoMotor_{i}",
            "MotorType",
            instantiate_optional=False,
        )
        await remove_placeholder_children(motor)
        await ensure_variable(motor, idx_di, "Manufacturer", "PLCM Drives", writable=False)
        await ensure_variable(motor, idx_di, "Model", "ServoMotor", writable=False)
        await ensure_variable(motor, idx_di, "ProductCode", f"SM-{i:02d}", writable=False)
        await ensure_variable(motor, idx_di, "SerialNumber", f"MOT-{instance_id:03d}-{i}", writable=False)
        motor_parameter_set = await ensure_object(motor, idx_di, "ParameterSet")
        await ensure_variable(motor_parameter_set, idx_robotics, "MotorTemperature", 35.0 + i)
        await ensure_variable(motor_parameter_set, idx_robotics, "BrakeReleased", True)
        await ensure_variable(motor_parameter_set, idx_robotics, "EffectiveLoadRate", 30 + i)

        gear = await ensure_typed_object(
            server,
            power_train,
            NS_ROBOTICS,
            idx_robotics,
            f"Gear_{i}",
            "GearType",
            instantiate_optional=False,
        )
        await remove_placeholder_children(gear)
        await ensure_variable(gear, idx_di, "Manufacturer", "PLCM Drives", writable=False)
        await ensure_variable(gear, idx_di, "Model", "PrecisionGear", writable=False)
        await ensure_variable(gear, idx_di, "ProductCode", f"GR-{i:02d}", writable=False)
        await ensure_variable(gear, idx_di, "SerialNumber", f"GEAR-{instance_id:03d}-{i}", writable=False)
        gear_ratio = await find_child(gear, "GearRatio")
        if gear_ratio is not None:
            await ensure_variable(gear_ratio, 0, "Numerator", 100)
            await ensure_variable(gear_ratio, 0, "Denominator", 1)

        await ensure_variable(power_train, idx_vendor, "RatedTorqueNm", 12.5 + i)
    flange_load = await ensure_typed_object(
        server,
        motion_device,
        NS_ROBOTICS,
        idx_robotics,
        "FlangeLoad",
        "LoadType",
        instantiate_optional=False,
    )
    await ensure_variable(flange_load, idx_robotics, "Mass", 5.0)

    controller = await ensure_typed_object(
        server,
        controllers,
        NS_ROBOTICS,
        idx_robotics,
        f"Controller_{instance_id:03d}",
        "ControllerType",
        instantiate_optional=False,
    )
    await remove_placeholder_children(controller)
    await ensure_variable(controller, idx_di, "Manufacturer", "PLCM Controls", writable=False)
    await ensure_variable(controller, idx_di, "Model", "ValidationRobotController", writable=False)
    await ensure_variable(controller, idx_di, "ProductCode", "PLCM-RC-6", writable=False)
    await ensure_variable(
        controller,
        idx_di,
        "SerialNumber",
        f"CTRL-{instance_id:03d}",
        writable=False,
    )

    controller_parameters = await ensure_object(controller, idx_di, "ParameterSet")
    controller_state = await ensure_variable(
        controller_parameters,
        idx_vendor,
        "ControllerState",
        "Ready",
    )
    await ensure_variable(
        controller_parameters,
        idx_vendor,
        "ExecutionMode",
        "Automatic",
    )
    await ensure_variable(controller_parameters, idx_robotics, "Temperature", 38.0)

    software = await ensure_object(controller, idx_robotics, "Software")
    await remove_placeholder_children(software)
    runtime = await ensure_typed_object(
        server,
        software,
        NS_DI,
        idx_robotics,
        "RobotRuntime",
        "SoftwareType",
        instantiate_optional=False,
    )
    await remove_placeholder_children(runtime)
    await ensure_variable(runtime, idx_di, "Manufacturer", "PLCM Controls", writable=False)
    await ensure_variable(runtime, idx_di, "Model", "PLCM Motion Runtime", writable=False)
    await ensure_variable(runtime, idx_di, "SoftwareRevision", "1.0.0", writable=False)
    await ensure_variable(runtime, idx_vendor, "ServoCycleTimeMs", 4.0)

    await link_or_add_method(
        server,
        controller,
        idx_vendor,
        "Reset",
        reset,
        [],
        [ua.VariantType.String],
    )
    safety_state = await ensure_typed_object(
        server,
        safety_states,
        NS_ROBOTICS,
        idx_robotics,
        f"SafetyState_{instance_id:03d}",
        "SafetyStateType",
        instantiate_optional=False,
    )
    await remove_placeholder_children(safety_state)
    safety_parameters = await ensure_object(safety_state, idx_di, "ParameterSet")
    emergency_stop = await ensure_variable(
        safety_parameters,
        idx_robotics,
        "EmergencyStop",
        False,
    )
    protective_stop = await ensure_variable(
        safety_parameters,
        idx_robotics,
        "ProtectiveStop",
        False,
    )
    await ensure_variable(safety_parameters, idx_robotics, "OperationalMode", 2)
    safety_mode = await ensure_variable(
        safety_parameters,
        idx_vendor,
        "SafetyState",
        "Operational",
    )

    emergency_stop_functions = await ensure_object(
        safety_state,
        idx_robotics,
        "EmergencyStopFunctions",
    )
    await remove_placeholder_children(emergency_stop_functions)
    cabinet_estop = await ensure_typed_object(
        server,
        emergency_stop_functions,
        NS_ROBOTICS,
        idx_robotics,
        "CabinetEmergencyStop",
        "EmergencyStopFunctionType",
        instantiate_optional=False,
    )
    await remove_placeholder_children(cabinet_estop)
    await ensure_variable(cabinet_estop, idx_robotics, "Name", "Cabinet E-Stop", writable=False)
    await ensure_variable(cabinet_estop, idx_robotics, "Active", False)

    protective_stop_functions = await ensure_object(
        safety_state,
        idx_robotics,
        "ProtectiveStopFunctions",
    )
    await remove_placeholder_children(protective_stop_functions)
    cell_door_stop = await ensure_typed_object(
        server,
        protective_stop_functions,
        NS_ROBOTICS,
        idx_robotics,
        "CellDoorProtectiveStop",
        "ProtectiveStopFunctionType",
        instantiate_optional=False,
    )
    await remove_placeholder_children(cell_door_stop)
    await ensure_variable(cell_door_stop, idx_robotics, "Name", "Cell Door", writable=False)
    await ensure_variable(cell_door_stop, idx_robotics, "Enabled", True)
    await ensure_variable(cell_door_stop, idx_robotics, "Active", False)

    return {
        "axis_vars": axis_vars,
        "robot_state": robot_state,
        "speed_override": speed_override,
        "controller_state": controller_state,
        "emergency_stop": emergency_stop,
        "protective_stop": protective_stop,
        "safety_mode": safety_mode,
    }


async def build_machine_vision_server(
    server: Server,
    ns: Dict[str, int],
    instance_id: int,
    *,
    system_id: Optional[int] = None,
    source_slot: Optional[int] = None,
):
    idx_mv = ns[NS_MACHINE_VISION]
    idx_amcm = ns[NS_MACHINE_VISION_AMCM]
    idx_vendor = ns[f"{NS_VENDOR_BASE}:machine-vision"]

    if system_id is None:
        system_id = instance_id
    if source_slot is None:
        source_slot = instance_id

    is_top_camera = source_slot % 2 == 1
    camera_position = "Top" if is_top_camera else "Bottom"
    camera_id = f"{source_slot:02d}"
    image_sensor_name = f"{camera_position}Cam{camera_id}"
    lens_name = f"Lens{camera_position}{camera_id}"
    lamp_prefix = "RingLight" if is_top_camera else "Backlight"
    lamp_name = f"{lamp_prefix}{camera_position}{camera_id}"

    vision_system = await instantiate_object_type(
        server,
        server.nodes.objects,
        NS_MACHINE_VISION,
        idx_vendor,
        f"DualCameraVisionSystem_{system_id:03d}",
        "VisionSystemType",
        instantiate_optional=False,
    )
    await remove_placeholder_children(vision_system)

    vision_state_machine = await ensure_typed_object(
        server,
        vision_system,
        NS_MACHINE_VISION,
        idx_mv,
        "VisionStateMachine",
        "VisionStateMachineType",
        instantiate_optional=False,
    )
    vision_state = await ensure_variable(
        vision_state_machine,
        idx_mv,
        "CurrentState",
        ua.LocalizedText("Operational"),
    )

    vision_asset = await instantiate_object_type(
        server,
        server.nodes.objects,
        NS_MACHINE_VISION_AMCM,
        idx_vendor,
        f"DualCameraVisionSystem_{system_id:03d}_Assets",
        "VisionSystemAssetType",
        instantiate_optional=False,
    )
    await remove_placeholder_children(vision_asset)

    asset_identification = await find_child(vision_asset, "Identification")
    if asset_identification is not None:
        await set_child_value_if_present(
            asset_identification,
            "Manufacturer",
            ua.LocalizedText("PLCM Vision"),
        )
        await set_child_value_if_present(
            asset_identification,
            "Model",
            ua.LocalizedText("DualCameraVisionSystem"),
        )
        await set_child_value_if_present(
            asset_identification,
            "ProductCode",
            "DualCameraVisionSystem",
        )
        await set_child_value_if_present(
            asset_identification,
            "ProductInstanceUri",
            f"urn:plcm:machine-vision:{system_id:03d}",
        )
        await set_child_value_if_present(
            asset_identification,
            "SerialNumber",
            f"VIS-DUAL-{system_id:03d}",
        )
        await set_child_value_if_present(
            asset_identification,
            "YearOfConstruction",
            2026,
        )

    computing_devices = await ensure_typed_object(
        server,
        vision_asset,
        NS_MACHINE_VISION_AMCM,
        idx_amcm,
        "ComputingDevices",
        "VisionItemFolderType",
        instantiate_optional=False,
    )
    image_sensors = await ensure_typed_object(
        server,
        vision_asset,
        NS_MACHINE_VISION_AMCM,
        idx_amcm,
        "ImageSensors",
        "VisionItemFolderType",
        instantiate_optional=False,
    )
    lenses = await ensure_typed_object(
        server,
        vision_asset,
        NS_MACHINE_VISION_AMCM,
        idx_amcm,
        "Lenses",
        "VisionItemFolderType",
        instantiate_optional=False,
    )
    lamps = await ensure_typed_object(
        server,
        vision_asset,
        NS_MACHINE_VISION_AMCM,
        idx_amcm,
        "Lamps",
        "VisionItemFolderType",
        instantiate_optional=False,
    )

    for folder in [computing_devices, image_sensors, lenses, lamps]:
        await remove_placeholder_children(folder)

    main_ipc = await instantiate_object_type(
        server,
        computing_devices,
        NS_MACHINE_VISION_AMCM,
        idx_vendor,
        "MainIPC",
        "VisionComputingDeviceType",
        instantiate_optional=False,
    )
    image_sensor = await instantiate_object_type(
        server,
        image_sensors,
        NS_MACHINE_VISION_AMCM,
        idx_vendor,
        image_sensor_name,
        "VisionImageSensorType",
        instantiate_optional=False,
    )
    lens = await instantiate_object_type(
        server,
        lenses,
        NS_MACHINE_VISION_AMCM,
        idx_vendor,
        lens_name,
        "VisionLensType",
        instantiate_optional=False,
    )
    lamp = await instantiate_object_type(
        server,
        lamps,
        NS_MACHINE_VISION_AMCM,
        idx_vendor,
        lamp_name,
        "VisionLampType",
        instantiate_optional=False,
    )

    for component, serial_number in [
        (main_ipc, f"IPC-{system_id:03d}-{source_slot:02d}"),
        (image_sensor, f"CAM-{system_id:03d}-{source_slot:02d}"),
        (lens, f"LEN-{system_id:03d}-{source_slot:02d}"),
        (lamp, f"LMP-{system_id:03d}-{source_slot:02d}"),
    ]:
        await remove_placeholder_children(component)
        identification = await find_child(component, "Identification")
        if identification is not None:
            await set_child_value_if_present(
                identification,
                "Manufacturer",
                ua.LocalizedText("PLCM Vision"),
            )
            await set_child_value_if_present(
                identification,
                "SerialNumber",
                serial_number,
            )

    await add_reference_by_browse_name_if_available(
        server,
        vision_system,
        vision_asset,
        "RepresentsSameEntityAs",
    )
    await add_reference_by_browse_name_if_available(
        server,
        image_sensor,
        main_ipc,
        "TransmitsDataTo",
    )
    await add_reference_by_browse_name_if_available(
        server,
        lens,
        image_sensor,
        "HasOpticalPathTo",
    )
    await add_reference_by_browse_name_if_available(
        server,
        lamp,
        lens,
        "HasOpticalPathTo",
    )

    return {
        "vision_state": vision_state,
    }


async def build_nodeset_additive_manufacturing_server(
    server: Server,
    ns: Dict[str, int],
    instance_id: int,
):
    idx_di = ns[NS_DI]
    idx_machinery = ns[NS_MACHINERY]
    idx_am = ns[NS_ADDITIVE_MANUFACTURING]
    idx_vendor = ns[f"{NS_VENDOR_BASE}:additive-manufacturing"]

    machines = await ensure_object(server.nodes.objects, idx_machinery, "Machines")
    machine = await instantiate_object_type(
        server,
        machines,
        NS_ADDITIVE_MANUFACTURING,
        idx_vendor,
        f"AdditiveManufacturingSystem_{instance_id:03d}",
        "AdditiveManufacturingType",
    )
    await remove_placeholder_children(machine)

    identification = await ensure_object(machine, idx_machinery, "Identification")
    await ensure_variable(identification, idx_am, "AMTechnologyIdentifier", "PBF", writable=False)
    await ensure_variable(
        identification,
        idx_machinery,
        "Manufacturer",
        ua.LocalizedText("PLCM Additive"),
        writable=False,
    )
    await ensure_variable(
        identification,
        idx_di,
        "DeviceClass",
        "Additive Manufacturing machine",
        writable=False,
    )
    await ensure_variable(
        identification,
        idx_di,
        "Model",
        ua.LocalizedText("ValidationAMSystem"),
        writable=False,
    )
    await ensure_variable(
        identification,
        idx_di,
        "ProductCode",
        "ValidationAMSystem",
        writable=False,
    )
    await ensure_variable(
        identification,
        idx_machinery,
        "ProductInstanceUri",
        f"urn:plcm:additive:{instance_id:03d}",
        writable=False,
    )
    await ensure_variable(
        identification,
        idx_machinery,
        "SerialNumber",
        f"AM-{instance_id:03d}",
        writable=False,
    )
    await ensure_variable(identification, idx_machinery, "YearOfConstruction", 2026, writable=False)

    equipment = await ensure_object(machine, idx_am, "Equipment")
    feedstock_list = await ensure_object(equipment, idx_am, "Feedstock")
    await remove_placeholder_children(feedstock_list)
    feedstock = await instantiate_object_type(
        server,
        feedstock_list,
        NS_ADDITIVE_MANUFACTURING,
        idx_vendor,
        f"Feedstock_Powder_{instance_id:03d}",
        "FeedstockType",
    )
    await ensure_variable(feedstock, idx_am, "Cycle", 1, writable=False)
    await ensure_variable(feedstock, idx_am, "ExternalIdentifier", f"LOT-{instance_id:03d}", writable=False)
    await ensure_variable(feedstock, idx_am, "Function", 1, writable=False)
    await ensure_variable(feedstock, idx_am, "Identifier", f"PA12-{instance_id:03d}", writable=False)
    await ensure_variable(feedstock, idx_am, "Manufacturer", "PLCM Materials", writable=False)
    await ensure_variable(feedstock, idx_am, "Name", "PA12 Powder", writable=False)
    await ensure_variable(feedstock, idx_am, "ReadyForProduction", True, writable=False)
    await ensure_variable(feedstock, idx_am, "RemainingQuantity", 18.5)

    process = await ensure_path(machine, idx_am, ["Monitoring", "Process"])
    progress = await ensure_am_process_value(
        server,
        process,
        idx_am,
        idx_vendor,
        "BuildProgress",
        0.0,
        category=2,
    )
    chamber_temperature = await ensure_am_process_value(
        server,
        process,
        idx_am,
        idx_vendor,
        "ChamberTemperature",
        25.0,
        category=2,
    )
    layer_number = await ensure_am_process_value(
        server,
        process,
        idx_am,
        idx_vendor,
        "LayerNumber",
        0,
        category=2,
    )
    await ensure_am_process_value(
        server,
        process,
        idx_am,
        idx_vendor,
        "OxygenLevel",
        0.08,
        category=2,
        severity=1,
    )
    await ensure_am_process_value(
        server,
        process,
        idx_am,
        idx_vendor,
        "LaserPower",
        180.0,
        category=2,
    )
    await ensure_am_process_value(
        server,
        process,
        idx_am,
        idx_vendor,
        "ScanSpeed",
        1200.0,
        category=2,
    )

    health = await ensure_path(machine, idx_am, ["Monitoring", "Health"])
    await ensure_am_process_value(
        server,
        health,
        idx_am,
        idx_vendor,
        "FilterSaturation",
        12.0,
        category=0,
    )

    job_management = await ensure_path(
        machine,
        idx_am,
        ["MachineryBuildingBlocks", "JobManagement"],
    )
    job_order_control = await ensure_object(job_management, idx_am, "JobOrderControl")
    job_state = await ensure_variable(
        job_order_control,
        idx_am,
        "CurrentState",
        ua.LocalizedText("Idle"),
        writable=False,
    )
    await set_child_value_if_present(job_order_control, "MaxDownloadableJobOrders", 8)

    job_order_results = await ensure_object(job_management, idx_am, "JobOrderResults")
    await link_or_add_method(
        server,
        job_order_results,
        idx_am,
        "RequestJobResponseByJobOrderID",
        request_job_response,
        [],
        [ua.VariantType.String],
    )
    await link_or_add_method(
        server,
        job_order_results,
        idx_am,
        "RequestJobResponseByJobOrderState",
        request_job_response,
        [],
        [ua.VariantType.String],
    )

    return {
        "job_state": job_state,
        "progress": progress,
        "chamber_temperature": chamber_temperature,
        "layer_number": layer_number,
    }


async def build_nodeset_woodworking_server(
    server: Server,
    ns: Dict[str, int],
    instance_id: int,
):
    idx_machinery = ns[NS_MACHINERY]
    idx_ww = ns[NS_WOODWORKING]

    machines = await ensure_object(server.nodes.objects, idx_machinery, "Machines")

    machine = await instantiate_object_type(
        server,
        machines,
        NS_WOODWORKING,
        idx_ww,
        f"WwMachine_{instance_id:03d}",
        "WwMachineType",
    )
    await remove_child_if_present(machine, "MachineryBuildingBlocks")
    await remove_child_if_present(machine, "JobManagement")

    identification = await ensure_object(machine, idx_machinery, "Identification")
    await ensure_variable(
        identification,
        idx_machinery,
        "Manufacturer",
        ua.LocalizedText("PLCM Woodworking"),
        writable=False,
    )
    await ensure_variable(
        identification,
        idx_machinery,
        "DeviceClass",
        "EdgebandingMachine",
        writable=False,
    )
    await ensure_variable(
        identification,
        idx_machinery,
        "Model",
        ua.LocalizedText("ValidationWoodworkingCell"),
        writable=False,
    )
    await ensure_variable(
        identification,
        idx_machinery,
        "ProductInstanceUri",
        f"urn:plcm:woodworking:{instance_id:03d}",
        writable=False,
    )
    await ensure_variable(
        identification,
        idx_machinery,
        "SerialNumber",
        f"WW-{instance_id:03d}",
        writable=False,
    )
    await ensure_variable(
        identification,
        idx_machinery,
        "ProductCode",
        "ValidationWoodworkingCell",
        writable=False,
    )
    await ensure_variable(
        identification,
        idx_ww,
        "YearOfConstruction",
        2026,
        writable=False,
    )

    await ensure_object(
        machine,
        idx_ww,
        "ManufacturerSpecific",
    )

    overview = await ensure_path(machine, idx_ww, ["State", "Machine", "Overview"])
    current_state = await ensure_variable(overview, idx_ww, "CurrentState", 2, writable=False)
    current_mode = await ensure_variable(overview, idx_ww, "CurrentMode", 1, writable=False)

    flags = await ensure_path(machine, idx_ww, ["State", "Machine", "Flags"])
    machine_on = await ensure_variable(flags, idx_ww, "MachineOn", True, writable=False)
    machine_initialized = await ensure_variable(
        flags,
        idx_ww,
        "MachineInitialized",
        True,
        writable=False,
    )
    power_present = await ensure_variable(flags, idx_ww, "PowerPresent", True, writable=False)
    emergency = await ensure_variable(flags, idx_ww, "Emergency", False, writable=False)
    calibrated = await ensure_variable(flags, idx_ww, "Calibrated", True, writable=False)
    error = await ensure_variable(flags, idx_ww, "Error", False, writable=False)
    alarm = await ensure_variable(flags, idx_ww, "Alarm", False, writable=False)
    warning = await ensure_variable(flags, idx_ww, "Warning", False, writable=False)
    recipe_in_run = await ensure_variable(flags, idx_ww, "RecipeInRun", False, writable=False)
    energy_saving = await ensure_variable(flags, idx_ww, "EnergySaving", False, writable=False)

    values = await ensure_path(machine, idx_ww, ["State", "Machine", "Values"])
    axis_override = await ensure_variable(values, idx_ww, "AxisOverride", 100, writable=False)
    spindle_override = await ensure_variable(
        values,
        idx_ww,
        "SpindleOverride",
        100,
        writable=False,
    )
    feed_speed = await ensure_variable(values, idx_ww, "FeedSpeed", 15.0, writable=False)

    return {
        "feed_speed": feed_speed,
        "current_state": current_state,
        "current_mode": current_mode,
        "machine_on": machine_on,
        "machine_initialized": machine_initialized,
        "power_present": power_present,
        "emergency": emergency,
        "calibrated": calibrated,
        "error": error,
        "alarm": alarm,
        "warning": warning,
        "recipe_in_run": recipe_in_run,
        "energy_saving": energy_saving,
        "axis_override": axis_override,
        "spindle_override": spindle_override,
    }


async def build_generic_namespace_server(
    server: Server,
    ns: Dict[str, int],
    instance_id: int,
):
    """
    Generic validation server.

    Each generic server receives its own vendor namespace:
    urn:plcm:validation:generic:001
    urn:plcm:validation:generic:002
    ...

    This mode is useful for scalability tests with many different namespaces.
    """
    idx_di = ns[NS_DI]
    idx_generic = ns[f"{NS_VENDOR_BASE}:generic:{instance_id:03d}"]

    objects = server.nodes.objects

    device_set = await objects.add_object(idx_di, "DeviceSet")
    device = await device_set.add_object(
        idx_generic,
        f"GenericComponent_{instance_id:03d}",
    )

    await device.add_property(idx_di, "Manufacturer", "PLCM Validation")
    await device.add_property(idx_di, "Model", "GenericNamespaceComponent")
    await device.add_property(idx_di, "SerialNumber", f"GEN-{instance_id:03d}")

    parameter_set = await device.add_object(idx_di, "ParameterSet")

    status = await add_writable_variable(
        parameter_set,
        idx_generic,
        "Status",
        "Idle",
    )

    temperature = await add_writable_variable(
        parameter_set,
        idx_generic,
        "Temperature",
        25.0,
    )

    counter = await add_writable_variable(
        parameter_set,
        idx_generic,
        "Counter",
        0,
    )

    capabilities = await device.add_object(idx_generic, "Capabilities")

    await capabilities.add_method(
        idx_generic,
        "Reset",
        reset,
        [],
        [ua.VariantType.String],
    )

    process_data = await device.add_object(idx_generic, "ProcessData")

    process_value_a = await add_writable_variable(
        process_data,
        idx_generic,
        "ProcessValueA",
        0.0,
    )

    process_value_b = await add_writable_variable(
        process_data,
        idx_generic,
        "ProcessValueB",
        0.0,
    )

    return {
        "status": status,
        "temperature": temperature,
        "counter": counter,
        "process_value_a": process_value_a,
        "process_value_b": process_value_b,
    }


# =============================================================================
# Runtime Variable Updates
# =============================================================================

async def update_variables(domain: str, variables: Dict):
    start = time.time()

    while True:
        t = time.time() - start

        if domain == "robotics":
            for i, axis_var in enumerate(variables["axis_vars"]):
                value = math.sin(t * 0.2 + i) * 1.5
                await write_value_compatible(axis_var, value)

            state = "Running" if int(t) % 10 < 5 else "Idle"
            await write_value_compatible(variables["robot_state"], state)

            speed = 50.0 + 50.0 * abs(math.sin(t * 0.1))
            await write_value_compatible(variables["speed_override"], speed)

            controller_state = "Running" if state == "Running" else "Ready"
            await write_value_compatible(variables["controller_state"], controller_state)

            emergency_stop = int(t) % 60 == 0
            protective_stop = int(t) % 25 == 0 and not emergency_stop
            safety_mode = (
                "EmergencyStop"
                if emergency_stop
                else "ProtectiveStop"
                if protective_stop
                else "Operational"
            )

            await write_value_compatible(variables["emergency_stop"], emergency_stop)
            await write_value_compatible(variables["protective_stop"], protective_stop)
            await write_value_compatible(variables["safety_mode"], safety_mode)

        elif domain == "machine-vision":
            vision_state = (
                ua.LocalizedText("Operational")
                if int(t) % 20 < 17
                else ua.LocalizedText("Halted")
            )

            if variables.get("vision_state") is not None:
                await write_value_compatible(variables["vision_state"], vision_state)

        elif domain == "additive-manufacturing":
            progress = t % 100.0
            temperature = 25.0 + abs(math.sin(t * 0.05)) * 35.0
            layer_number = int(progress * 2)

            await write_value_compatible(variables["job_state"], "Running")
            await write_value_compatible(variables["progress"], progress)
            await write_value_compatible(variables["chamber_temperature"], temperature)
            await write_value_compatible(variables["layer_number"], layer_number)

        elif domain == "woodworking":
            alarm = int(t) % 55 == 0
            recipe_in_run = (not alarm) and int(t) % 12 < 8
            current_state = 4 if alarm else 3 if recipe_in_run else 2
            current_mode = 1
            axis_override = int(80 + abs(math.sin(t * 0.2)) * 20)
            spindle_override = int(75 + abs(math.cos(t * 0.15)) * 25)

            await write_value_compatible(variables["current_state"], current_state)
            await write_value_compatible(variables["current_mode"], current_mode)
            await write_value_compatible(variables["machine_on"], True)
            await write_value_compatible(variables["machine_initialized"], True)
            await write_value_compatible(variables["power_present"], True)
            await write_value_compatible(variables["emergency"], False)
            await write_value_compatible(variables["calibrated"], True)
            await write_value_compatible(variables["error"], alarm)
            await write_value_compatible(variables["warning"], (not alarm) and int(t) % 37 == 0)
            await write_value_compatible(variables["recipe_in_run"], recipe_in_run)
            await write_value_compatible(variables["energy_saving"], False)
            await write_value_compatible(variables["axis_override"], axis_override)
            await write_value_compatible(variables["spindle_override"], spindle_override)
            await write_value_compatible(variables["feed_speed"], 8.0 + abs(math.cos(t * 0.18)) * 14.0)
            await write_value_compatible(variables["alarm"], alarm)

        elif domain == "generic":
            await write_value_compatible(variables["status"],
                "Running" if int(t) % 10 < 5 else "Idle"
            )

            await write_value_compatible(variables["temperature"],
                25.0 + abs(math.sin(t * 0.1)) * 10.0
            )

            await write_value_compatible(variables["counter"], int(t))

            await write_value_compatible(variables["process_value_a"],
                abs(math.sin(t * 0.2)) * 100.0
            )

            await write_value_compatible(variables["process_value_b"],
                abs(math.cos(t * 0.2)) * 100.0
            )

        await asyncio.sleep(1.0)


# =============================================================================
# Server Runtime
# =============================================================================

async def run_source_server(
    config: SourceServerConfig,
    shuffle_namespaces: bool,
    advertise_ip: str,
    lds_url_future: Optional[asyncio.Future],
):
    server = Server()
    clamp_requested_session_timeout(server)
    await server.init()

    file_transfer = None
    if config.urdf_nodeset_path is not None:
        file_transfer = await import_enriched_urdf_fixture(server, config)
    else:
        await import_companion_nodesets(server, config.domain)

    path = f"/plcm/{config.domain}/{config.instance_id}/"

    endpoint = f"opc.tcp://{advertise_ip}:{config.port}{path}"

    application_uri = source_application_uri(config)

    server_name = (
        f"PLCM Validation Source Server - "
        f"{config.domain} #{config.instance_id:03d}"
    )
    if config.urdf_fixture_name is not None:
        server_name += f" ({config.urdf_fixture_name})"
    server.set_endpoint(endpoint)
    server.set_server_name(server_name)

    await server.set_application_uri(application_uri)

    ns = await register_namespaces(
        server=server,
        namespace_uris=config.namespace_uris,
        shuffle_namespaces=shuffle_namespaces,
    )
    ns[NS_DI] = await namespace_index(server, NS_DI)

    if config.urdf_nodeset_path is not None:
        variables = {}

    elif config.domain == "robotics":
        variables = await build_robotics_server(server, ns, config.instance_id)

    elif config.domain == "machine-vision":
        ns[NS_MACHINERY] = await namespace_index(server, NS_MACHINERY)
        ns[NS_MACHINE_VISION] = await namespace_index(server, NS_MACHINE_VISION)
        ns[NS_MACHINE_VISION_AMCM] = await namespace_index(
            server,
            NS_MACHINE_VISION_AMCM,
        )
        variables = await build_machine_vision_server(
            server,
            ns,
            config.instance_id,
            system_id=config.semantic_instance_id,
            source_slot=config.semantic_source_slot,
        )

    elif config.domain == "additive-manufacturing":
        ns[NS_MACHINERY] = await namespace_index(server, NS_MACHINERY)
        ns[NS_ADDITIVE_MANUFACTURING] = await namespace_index(
            server,
            NS_ADDITIVE_MANUFACTURING,
        )
        variables = await build_nodeset_additive_manufacturing_server(
            server,
            ns,
            config.instance_id,
        )

    elif config.domain == "woodworking":
        ns[NS_MACHINERY] = await namespace_index(server, NS_MACHINERY)
        ns[NS_WOODWORKING] = await namespace_index(server, NS_WOODWORKING)
        variables = await build_nodeset_woodworking_server(
            server,
            ns,
            config.instance_id,
        )

    elif config.domain == "generic":
        variables = await build_generic_namespace_server(server, ns, config.instance_id)

    else:
        raise ValueError(f"Unknown domain: {config.domain}")

    print("=" * 80)
    print(f"[START] {config.domain} #{config.instance_id:03d}")
    print(f"Endpoint bind:   {endpoint}")
    print(f"Endpoint public: opc.tcp://{advertise_ip}:{config.port}{path}")
    print(f"ApplicationUri:  {application_uri}")
    if config.urdf_fixture_name is not None:
        print(f"URDF fixture:    {config.urdf_fixture_name}")
        print(f"URDF NodeSet:    {config.urdf_nodeset_path}")
        print(f"URDF SHA-256:    {config.urdf_nodeset_sha256}")
    if config.domain == "machine-vision":
        semantic_id = config.semantic_instance_id or config.instance_id
        semantic_slot = config.semantic_source_slot or config.instance_id
        print(
            f"Semantic system: DualCameraVisionSystem_{semantic_id:03d} "
            f"(slot {semantic_slot:02d})"
        )
    print("Namespace mapping:")

    for uri, idx in ns.items():
        print(f"  ns={idx}: {uri}")

    print("=" * 80)

    registered_to_lds = False
    resolved_lds_url: Optional[str] = None
    lds_registration_task: Optional[asyncio.Task] = None

    async def register_with_lds_until_available():
        nonlocal registered_to_lds, resolved_lds_url

        if lds_url_future is None:
            return

        resolved_lds_url = normalize_lds_url(await asyncio.shield(lds_url_future))

        # Spread initial registration and renewal cycles aggressively across
        # instances to avoid LDS overload spikes on startup.
        stagger_seed = (
            sum(ord(ch) for ch in config.domain)
            + config.instance_id
            + config.port
        ) % 16
        initial_delay_s = LDS_REGISTRATION_STAGGER_STEP_S * stagger_seed
        if initial_delay_s > 0:
            await asyncio.sleep(initial_delay_s)

        attempt = 0
        while not registered_to_lds:
            attempt += 1
            try:
                gate = _get_lds_registration_gate()
                async with gate:
                    await asyncio.wait_for(
                        server.register_to_discovery(
                            resolved_lds_url,
                            period=LDS_REGISTRATION_PERIOD_S,
                        ),
                        timeout=LDS_REGISTRATION_TIMEOUT_S,
                    )
                registered_to_lds = True
                measurement_event(
                    "LDS REGISTERED",
                    (
                        f"application_uri={application_uri} "
                        f"endpoint={endpoint} lds={resolved_lds_url}"
                    ),
                )
                print(
                    f"[LDS] Registered at {resolved_lds_url}; "
                    f"renewal period: {LDS_REGISTRATION_PERIOD_S} s",
                    flush=True,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if attempt == 1 or attempt % 5 == 0:
                    measurement_event(
                        "LDS REGISTRATION FAILED",
                        (
                            f"application_uri={application_uri} attempt={attempt} "
                            f"lds={resolved_lds_url} error={exc!r}"
                        ),
                    )
                    print(
                        f"[LDS] Registration attempt {attempt} failed at "
                        f"{resolved_lds_url}: {exc!r}; retrying",
                        flush=True,
                    )
                backoff_s = min(30.0, 1.5 * (1.6 ** min(attempt, 8)))
                jitter_s = random.uniform(0.0, 1.0)
                await asyncio.sleep(backoff_s + jitter_s)

    async def checkout_from_discovery():
        nonlocal registered_to_lds

        print(f"[DISCOVERY] Checkout started for {application_uri}")

        # A planned shutdown explicitly removes the source from the LDS cache.
        if resolved_lds_url and registered_to_lds:
            try:
                gate = _get_lds_registration_gate()
                async with gate:
                    await asyncio.wait_for(
                        server.unregister_from_discovery(resolved_lds_url),
                        timeout=5.0,
                    )
                registered_to_lds = False
                print(f"[LDS] Unregistered from {resolved_lds_url}")
            except Exception as exc:
                print(f"[LDS] Unregister failed at {resolved_lds_url}: {exc!r}")

        # Give the unregister request a short time to leave the event loop.
        await asyncio.sleep(0.3)

        print(f"[DISCOVERY] Checkout finished for {application_uri}")

    async with server:
        try:
            measurement_event(
                "SOURCE LISTENING",
                (
                    f"application_uri={application_uri} endpoint={endpoint} "
                    f"urdf_payload={'true' if file_transfer is not None else 'false'} "
                    f"fixture={config.urdf_fixture_name or 'none'}"
                ),
            )
            if lds_url_future is not None:
                await asyncio.sleep((config.port % 10) * 0.15)
                lds_registration_task = asyncio.create_task(
                    register_with_lds_until_available()
                )

            await update_variables(config.domain, variables)

        finally:
            if lds_registration_task is not None:
                lds_registration_task.cancel()
                await asyncio.gather(
                    lds_registration_task,
                    return_exceptions=True,
                )

            # Wichtig: Cleanup läuft noch INNERHALB von async with server.
            cleanup_task = asyncio.create_task(checkout_from_discovery())

            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError:
                await cleanup_task
                raise

# =============================================================================
# Configuration Generation
# =============================================================================

def create_server_configs(
    base_port: int,
    replicas_per_domain: int = 1,
    num_servers: Optional[int] = None,
    domains: Optional[List[str]] = None,
    with_enriched_urdf: bool = False,
) -> List[SourceServerConfig]:
    configs: List[SourceServerConfig] = []

    if num_servers is not None:
        fixture_hashes: Dict[Path, str] = {}
        if with_enriched_urdf:
            for _, fixture_path in ENRICHED_URDF_FIXTURES:
                if not fixture_path.is_file():
                    raise FileNotFoundError(
                        f"Missing enriched URDF NodeSet: {fixture_path}"
                    )
                fixture_hashes[fixture_path] = sha256_file(fixture_path)

        for instance_id in range(1, num_servers + 1):
            if with_enriched_urdf:
                fixture_name, fixture_path = ENRICHED_URDF_FIXTURES[
                    (instance_id - 1) % len(ENRICHED_URDF_FIXTURES)
                ]
                configs.append(
                    SourceServerConfig(
                        domain="robotics-urdf",
                        instance_id=instance_id,
                        port=base_port + instance_id - 1,
                        namespace_uris=[],
                        urdf_fixture_name=fixture_name,
                        urdf_nodeset_path=fixture_path,
                        urdf_nodeset_sha256=fixture_hashes[fixture_path],
                    )
                )
                continue

            configs.append(
                SourceServerConfig(
                    domain="generic",
                    instance_id=instance_id,
                    port=base_port + instance_id - 1,
                    namespace_uris=[
                        NS_DI,
                        f"{NS_VENDOR_BASE}:generic:{instance_id:03d}",
                    ],
                )
            )

        return configs

    domain_templates = [
        (
            "robotics",
            [
                NS_DI,
                NS_ROBOTICS,
                f"{NS_VENDOR_BASE}:robotics",
            ],
        ),
        (
            "machine-vision",
            [
                NS_DI,
                NS_MACHINERY,
                NS_MACHINE_VISION,
                NS_MACHINE_VISION_AMCM,
                f"{NS_VENDOR_BASE}:machine-vision",
            ],
        ),
        (
            "additive-manufacturing",
            [
                NS_DI,
                NS_MACHINERY,
                NS_ADDITIVE_MANUFACTURING,
                f"{NS_VENDOR_BASE}:additive-manufacturing",
            ],
        ),
        (
            "woodworking",
            [
                NS_DI,
                NS_MACHINERY,
                NS_WOODWORKING,
            ],
        ),
    ]

    if domains:
        selected_domains = set(domains)
        known_domains = {domain for domain, _ in domain_templates}
        unknown_domains = selected_domains - known_domains
        if unknown_domains:
            raise ValueError(
                "Unknown predefined domain(s): "
                + ", ".join(sorted(unknown_domains))
            )
        domain_templates = [
            template for template in domain_templates
            if template[0] in selected_domains
        ]

    port = base_port

    for domain, namespace_uris in domain_templates:
        for instance_id in range(1, replicas_per_domain + 1):
            semantic_instance_id = None
            semantic_source_slot = None
            if domain == "machine-vision":
                semantic_instance_id, semantic_source_slot = machine_vision_semantic_mapping(
                    instance_id,
                    replicas_per_domain,
                )

            configs.append(
                SourceServerConfig(
                    domain=domain,
                    instance_id=instance_id,
                    port=port,
                    namespace_uris=namespace_uris,
                    semantic_instance_id=semantic_instance_id,
                    semantic_source_slot=semantic_source_slot,
                )
            )
            port += 1

    return configs


# =============================================================================
# Main
# =============================================================================

async def main():
    global LDS_MAX_CONCURRENT_REGISTRATIONS
    global LDS_REGISTRATION_STAGGER_STEP_S
    global _lds_registration_gate

    parser = argparse.ArgumentParser(
        description=(
            "Start multiple OPC UA source servers with different namespaces "
            "for Aggregation Server validation."
        )
    )

    parser.add_argument(
        "--base-port",
        type=int,
        default=4860,
        help="First TCP port used for the OPC UA source servers.",
    )

    parser.add_argument(
        "--replicas-per-domain",
        type=int,
        default=1,
        help=(
            "Number of replicas per predefined domain. "
            "Only used if --num-servers is not set."
        ),
    )

    parser.add_argument(
        "--num-servers",
        type=int,
        default=None,
        help=(
            "Starts N OPC UA servers. By default they use small generic fixtures; "
            "with --with-enriched-urdf they cycle through EVA, Franka, and UR5e. "
            "If this parameter is set, the predefined domain mode is disabled."
        ),
    )

    parser.add_argument(
        "--with-enriched-urdf",
        action="store_true",
        help=(
            "Opt-in configuration fixture: use the enriched EVA, Franka, and "
            "UR5e NodeSets (round-robin) for --num-servers. Without this flag, "
            "source_servers.py never imports URDF payloads."
        ),
    )

    parser.add_argument(
        "--domains",
        nargs="+",
        choices=(
            "robotics",
            "machine-vision",
            "additive-manufacturing",
            "woodworking",
        ),
        default=None,
        help=(
            "Start only the selected predefined domains. For an isolated "
            "two-system vision fixture use '--domains machine-vision "
            "--replicas-per-domain 4'."
        ),
    )

    parser.add_argument(
        "--shuffle-namespaces",
        action="store_true",
        help=(
            "Randomizes namespace registration order per server to test "
            "NamespaceArray robustness."
        ),
    )

    parser.add_argument(
        "--advertise-ip",
        type=str,
        default=None,
        help=(
            "IP-Adresse, die per mDNS/LDS-ME announced wird. "
            "Wenn nicht gesetzt, wird die LAN-IP automatisch bestimmt."
        ),
    )

    parser.add_argument(
        "--lds-url",
        type=str,
        default="auto",
        help=(
            "Use 'auto' (default) to discover an LDS-ME through mDNS, provide "
            "an explicit opc.tcp:// URL as an override, or use 'none' to "
            "disable LDS registration."
        ),
    )

    parser.add_argument(
        "--lds-discovery-timeout",
        type=float,
        default=5.0,
        help="Seconds to wait for each LDS-ME multicast discovery attempt.",
    )

    parser.add_argument(
        "--lds-registration-concurrency",
        type=int,
        default=LDS_MAX_CONCURRENT_REGISTRATIONS,
        help=(
            "Maximum simultaneous initial LDS registration calls "
            f"(default: {LDS_MAX_CONCURRENT_REGISTRATIONS})."
        ),
    )

    parser.add_argument(
        "--lds-registration-stagger-step",
        type=float,
        default=LDS_REGISTRATION_STAGGER_STEP_S,
        help=(
            "Seconds multiplied by the deterministic 0..15 source bucket "
            f"before initial LDS registration (default: "
            f"{LDS_REGISTRATION_STAGGER_STEP_S}). Use 0 for an unstaggered "
            "concurrency stress test."
        ),
    )

    parser.add_argument(
        "--allow-multiple-instances",
        action="store_true",
        help="Disable process lock and allow multiple source_servers.py instances.",
    )

    args = parser.parse_args()
    if args.num_servers is not None and args.domains:
        parser.error("--domains cannot be combined with --num-servers")
    if args.with_enriched_urdf and args.num_servers is None:
        parser.error("--with-enriched-urdf requires --num-servers")
    if args.lds_registration_concurrency <= 0:
        parser.error("--lds-registration-concurrency must be positive")
    if args.lds_registration_stagger_step < 0:
        parser.error("--lds-registration-stagger-step must be nonnegative")

    LDS_MAX_CONCURRENT_REGISTRATIONS = args.lds_registration_concurrency
    LDS_REGISTRATION_STAGGER_STEP_S = args.lds_registration_stagger_step
    _lds_registration_gate = None

    advertise_ip = args.advertise_ip or get_lan_ip()
    lds_option = args.lds_url.strip()

    configs = create_server_configs(
        base_port=args.base_port,
        replicas_per_domain=args.replicas_per_domain,
        num_servers=args.num_servers,
        domains=args.domains,
        with_enriched_urdf=args.with_enriched_urdf,
    )
    measurement_event(
        "SOURCE RUN START",
        (
            f"requested_server_count={len(configs)} base_port={args.base_port} "
            f"advertise_ip={advertise_ip} "
            f"with_enriched_urdf={'true' if args.with_enriched_urdf else 'false'} "
            f"lds_registration_concurrency={LDS_MAX_CONCURRENT_REGISTRATIONS} "
            f"lds_registration_stagger_step_seconds="
            f"{LDS_REGISTRATION_STAGGER_STEP_S}"
        ),
    )

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    lds_url_future: Optional[asyncio.Future]
    lds_discovery_task: Optional[asyncio.Task] = None
    if lds_option.lower() in {"", "none", "off", "disabled"}:
        lds_url_future = None
        print("[LDS] Registration disabled")
    elif lds_option.lower() in {"auto", "mdns"}:
        lds_discovery_task = asyncio.create_task(
            discover_lds_until_available(args.lds_discovery_timeout, advertise_ip)
        )
        lds_url_future = lds_discovery_task
        print("[LDS-ME/mDNS] Automatic LDS discovery enabled")
    elif lds_option.startswith("opc.tcp://"):
        explicit_lds = loop.create_future()
        explicit_lds.set_result(lds_option)
        lds_url_future = explicit_lds
        print(f"[LDS] Using explicit override: {lds_option}")
    else:
        parser.error("--lds-url must be 'auto', 'none', or an opc.tcp:// URL")

    instance_guard: Optional[SingleInstanceGuard] = None
    if not args.allow_multiple_instances:
        instance_guard = SingleInstanceGuard()
        if not instance_guard.acquire():
            print(
                "[STARTUP] Another source_servers.py instance is already running. "
                "Stop it first or use --allow-multiple-instances."
            )
            return

    try:
        loop.add_signal_handler(signal.SIGINT, stop_event.set)
        loop.add_signal_handler(signal.SIGTERM, stop_event.set)
    except NotImplementedError:
        pass

    tasks = [
    asyncio.create_task(
        run_source_server(
            config=config,
            shuffle_namespaces=args.shuffle_namespaces,
            advertise_ip=advertise_ip,
            lds_url_future=lds_url_future,
        )
    )
    for config in configs
    ]

    print()
    print(f"Started {len(tasks)} OPC UA source server task(s).")
    print("Press Ctrl+C to stop.")
    print()

    stop_waiter = asyncio.create_task(stop_event.wait())

    try:
        completed, _ = await asyncio.wait(
            [stop_waiter, *tasks],
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_waiter not in completed:
            completed_source = next(iter(completed))
            exception = completed_source.exception()
            if exception is not None:
                raise exception
            raise RuntimeError("A source-server task stopped unexpectedly")

    finally:
        print()
        print("Stopping servers...")

        stop_waiter.cancel()
        for task in tasks:
            task.cancel()

        await asyncio.gather(stop_waiter, *tasks, return_exceptions=True)

        if lds_discovery_task is not None and not lds_discovery_task.done():
            lds_discovery_task.cancel()
            await asyncio.gather(lds_discovery_task, return_exceptions=True)

        if instance_guard is not None:
            instance_guard.release()

        print("All servers stopped.")


if __name__ == "__main__":
    asyncio.run(main())
