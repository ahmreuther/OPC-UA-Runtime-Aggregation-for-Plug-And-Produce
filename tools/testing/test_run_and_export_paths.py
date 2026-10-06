# SPDX-FileCopyrightText: 2026 Adrian Reuther
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

"""Filesystem-contract tests for the semantic-integration exporter."""

from __future__ import annotations

import tempfile
import unittest
import io
import json
import os
from pathlib import Path
from unittest.mock import patch

import run_and_export as runner
from NodeXmlExporter import NodeXMLExporter
from XmlExporter import XmlExporter


class RunDirectoryTests(unittest.TestCase):
    def test_only_confirmed_namespace_zero_ghost_nodes_are_skippable(self):
        bad_node_unknown = type("BadNodeIdUnknown", (Exception,), {})()
        other_error = RuntimeError("failure")
        ns0_node = type("Node", (), {
            "nodeid": type("NodeId", (), {"NamespaceIndex": 0})()
        })()
        ns2_node = type("Node", (), {
            "nodeid": type("NodeId", (), {"NamespaceIndex": 2})()
        })()
        self.assertTrue(
            XmlExporter._is_nonexistent_namespace_zero_node(
                ns0_node, bad_node_unknown
            )
        )
        self.assertFalse(
            XmlExporter._is_nonexistent_namespace_zero_node(
                ns2_node, bad_node_unknown
            )
        )
        self.assertFalse(
            XmlExporter._is_nonexistent_namespace_zero_node(ns0_node, other_error)
        )


class StartupReadinessTests(unittest.TestCase):
    def test_subprocess_output_survives_legacy_console_encoding(self):
        buffer = io.BytesIO()
        console = io.TextIOWrapper(buffer, encoding="cp1252", errors="strict")
        with patch.object(runner.sys, "stdout", console):
            runner.print_subprocess_output("╭─ server started\n")
            console.flush()

        self.assertEqual(
            buffer.getvalue().decode("cp1252"),
            r"\u256d\u2500 server started" + os.linesep,
        )

    def test_server_endpoint_tcp_address_parses_ipv4_and_ipv6(self):
        self.assertEqual(
            runner.server_endpoint_tcp_address("opc.tcp://127.0.0.1:48400/path"),
            ("127.0.0.1", 48400),
        )
        self.assertEqual(
            runner.server_endpoint_tcp_address("opc.tcp://[::1]:48400"),
            ("::1", 48400),
        )

    def test_server_endpoint_tcp_address_rejects_invalid_endpoint(self):
        for endpoint in ("http://localhost:48400", "opc.tcp://localhost"):
            with self.subTest(endpoint=endpoint):
                with self.assertRaises(ValueError):
                    runner.server_endpoint_tcp_address(endpoint)

    def test_e01_endpoint_uses_local_or_explicit_aggregation_host(self):
        with patch.object(runner, "E01_AGGREGATION_HOST", "127.0.0.1"):
            self.assertEqual(
                runner.determine_server_endpoint({"port": 48400}),
                "opc.tcp://127.0.0.1:48400",
            )
        with patch.object(runner, "E01_AGGREGATION_HOST", "192.0.2.10"):
            self.assertEqual(
                runner.determine_server_endpoint({"port": 48400}),
                "opc.tcp://192.0.2.10:48400",
            )

    def test_wait_for_server_endpoint_stops_when_process_exits(self):
        process = type("Process", (), {"poll": lambda self: 7})()
        with patch.object(runner.socket, "create_connection") as connect:
            self.assertFalse(
                runner.wait_for_server_endpoint(
                    process, "opc.tcp://127.0.0.1:48400", 1.0
                )
            )
        connect.assert_not_called()

    def test_wait_for_server_endpoint_accepts_only_a_listening_server(self):
        process = type("Process", (), {"poll": lambda self: None})()
        with patch.object(runner.socket, "create_connection") as connect:
            self.assertTrue(
                runner.wait_for_server_endpoint(
                    process, "opc.tcp://127.0.0.1:48400", 1.0
                )
            )
        connect.assert_called_once_with(("127.0.0.1", 48400), timeout=0.25)


class ExportBrowseRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_transient_bad_unexpected_error_is_retried(self):
        bad_unexpected = type("BadUnexpectedError", (Exception,), {})

        class Node:
            nodeid = "ns=2;i=1"

            def __init__(self):
                self.calls = 0

            async def get_children(self, refs):
                self.calls += 1
                if self.calls < 3:
                    raise bad_unexpected()
                return ["child"]

        node = Node()
        exporter = NodeXMLExporter()
        children = await exporter._get_children_with_retry(
            node, retry_delays=(0, 0)
        )
        self.assertEqual(children, ["child"])
        self.assertEqual(node.calls, 3)

    async def test_non_transient_browse_error_is_not_retried(self):
        class Node:
            nodeid = "ns=2;i=1"
            calls = 0

            async def get_children(self, refs):
                self.calls += 1
                raise RuntimeError("fatal")

        node = Node()
        exporter = NodeXMLExporter()
        with self.assertRaises(RuntimeError):
            await exporter._get_children_with_retry(node, retry_delays=(0, 0))
        self.assertEqual(node.calls, 1)

    def test_completion_log_is_adapted_without_claiming_start_as_accepted(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "device_completions.log"
            path.write_text(
                "[2026-07-12 11:00:00.000] START urn:test:001\n"
                "[2026-07-12 11:00:01.000] DONE AGGREGATED  urn:test:001\n",
                encoding="utf-8",
            )
            events = runner.completion_events(path, {
                "hosts": [{"name": "urn:test:001", "address": "opc.tcp://source"}]
            })
            self.assertEqual([event["status"] for event in events], ["processing", "completed"])
            self.assertIsNone(events[0]["accepted"])
            self.assertTrue(events[1]["accepted"])
            self.assertEqual(events[1]["source_address"], "opc.tcp://source")

    def test_default_output_is_in_validation_raw_data(self):
        expected = (
            runner.WORKSPACE_ROOT
            / "Validation"
            / "integration"
            / "raw_data"
            / "semantic_integration"
        ).resolve()
        self.assertEqual(runner.SEMANTIC_INTEGRATION_ROOT, expected)

    def test_explicit_run_id_is_exclusive(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.object(runner, "SEMANTIC_INTEGRATION_ROOT", root):
                created = runner.create_run_directory("fixed-run")
                self.assertEqual(created, root / "fixed-run")
                with self.assertRaises(FileExistsError):
                    runner.create_run_directory("fixed-run")

    def test_runtime_inputs_are_snapshots(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            project = base / "project"
            run_dir = base / "run"
            project.mkdir()
            run_dir.mkdir()
            for filename in runner.RUNTIME_INPUT_FILES:
                (project / filename).write_text(
                    f'{{"source": "{filename}"}}\n', encoding="utf-8"
                )

            with patch.object(runner, "PROJECT_ROOT", project):
                snapshots = runner.snapshot_runtime_inputs(run_dir)

            original_snapshot = snapshots["config.json"].read_bytes()
            (project / "config.json").write_text("{}\n", encoding="utf-8")
            self.assertEqual(
                snapshots["config.json"].read_bytes(), original_snapshot
            )

    def test_local_opcua_dependency_provenance_hashes_actual_vendor_tree(self):
        with tempfile.TemporaryDirectory() as temporary:
            project = Path(temporary)
            vendor = project / "vendor" / "opcua"
            vendor.mkdir(parents=True)
            (project / "Cargo.toml").write_text(
                '[dependencies]\nopcua = { path = "vendor/opcua" }\n',
                encoding="utf-8",
            )
            (vendor / "Cargo.toml").write_text(
                '[package]\nname = "opcua"\nversion = "0.12.0"\n',
                encoding="utf-8",
            )

            with patch.object(runner, "PROJECT_ROOT", project):
                provenance = runner.opcua_dependency_provenance()

            self.assertEqual(provenance["source_type"], "local_path")
            self.assertEqual(provenance["tree"]["file_count"], 1)
            self.assertEqual(len(provenance["tree"]["sha256"]), 64)

    def test_discovery_provenance_records_network_path_and_fallback(self):
        with tempfile.TemporaryDirectory() as temporary:
            log_path = Path(temporary) / "server_console.log"
            log_path.write_text(
                "FindServers erfolgreich\n"
                "FindServersOnNetwork erfolgreich\n",
                encoding="utf-8",
            )
            network = runner.observed_discovery_provenance(log_path)
            self.assertEqual(
                network["active_mode"],
                "find_servers_plus_find_servers_on_network",
            )

            log_path.write_text(
                "FindServers erfolgreich\n"
                "FindServersOnNetwork nicht verfuegbar\n",
                encoding="utf-8",
            )
            fallback = runner.observed_discovery_provenance(log_path)
            self.assertEqual(
                fallback["active_mode"],
                "find_servers_with_registered_lds_fallback",
            )


class TypeIntegrityTests(unittest.TestCase):
    IRDI_URI = "http://opcfoundation.org/UA/Dictionary/IRDI"

    def _fixture(self, directory: Path) -> dict:
        nodeset_file = directory / "Opc.Ua.IRDI.NodeSet2.xml"
        nodeset_file.write_text(
            """<UANodeSet xmlns=\"http://opcfoundation.org/UA/2011/03/UANodeSet.xsd\">
  <NamespaceUris><Uri>http://opcfoundation.org/UA/Dictionary/IRDI</Uri></NamespaceUris>
  <UAObject NodeId=\"ns=1;i=1\" BrowseName=\"1:DictionaryEntry\" />
</UANodeSet>
""",
            encoding="utf-8",
        )
        return {
            "mappings": [{
                "namespace_uri": self.IRDI_URI,
                "model_uri": self.IRDI_URI,
                "nodeset_file": nodeset_file.name,
            }]
        }

    def test_irdi_model_without_type_nodes_is_valid_when_declared_nodes_exist(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            mapping = self._fixture(directory)
            nodes = {
                "ns=41;i=1": runner.XmlNode(
                    "ns=41;i=1", 41, "DictionaryEntry", "UAObject", None, []
                )
            }
            report = runner.build_type_integrity_report(
                nodes,
                {41: self.IRDI_URI},
                [{"url": self.IRDI_URI, "nsid": 41}],
                mapping,
                directory,
            )

        self.assertEqual(
            report["expected_type_count_by_required_namespace"][self.IRDI_URI], 0
        )
        self.assertEqual(report["missing_required_type_namespaces"], [])
        self.assertEqual(report["missing_required_model_namespaces"], [])
        self.assertTrue(report["passed"])

    def test_irdi_model_fails_when_a_declared_node_is_missing(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            mapping = self._fixture(directory)
            report = runner.build_type_integrity_report(
                {},
                {41: self.IRDI_URI},
                [{"url": self.IRDI_URI, "nsid": 41}],
                mapping,
                directory,
            )

        self.assertEqual(report["missing_required_type_namespaces"], [])
        self.assertEqual(report["missing_required_model_namespaces"], [self.IRDI_URI])
        self.assertFalse(report["passed"])


class QualifiedPathTests(unittest.TestCase):
    def test_duplicate_local_names_are_resolved_by_namespace_uri(self):
        nodes = {
            "i=85": runner.XmlNode("i=85", 0, "Objects", "UAObject", None, []),
            "ns=2;i=1": runner.XmlNode(
                "ns=2;i=1", 2, "Container", "UAObject", "i=85", []
            ),
            "ns=3;i=1": runner.XmlNode(
                "ns=3;i=1", 3, "Container", "UAObject", "i=85", []
            ),
            "ns=2;i=2": runner.XmlNode(
                "ns=2;i=2", 2, "Duplicate", "UAVariable", "ns=2;i=1", []
            ),
            "ns=3;i=2": runner.XmlNode(
                "ns=3;i=2", 3, "Duplicate", "UAVariable", "ns=3;i=1", []
            ),
        }
        namespaces = {
            0: "http://opcfoundation.org/UA/",
            2: "urn:test:a",
            3: "urn:test:b",
        }
        path = [
            {"namespace_uri": "http://opcfoundation.org/UA/", "name": "Objects"},
            {"namespace_uri": "urn:test:b", "name": "Container"},
            {"namespace_uri": "urn:test:b", "name": "Duplicate"},
        ]
        result = runner.resolve_browse_path(
            path, runner.build_name_index(nodes), nodes, namespaces
        )
        self.assertEqual(result.node.node_id, "ns=3;i=2")

    def test_unique_full_name_chain_documents_executor_namespace_projection(self):
        nodes = {
            "i=85": runner.XmlNode("i=85", 0, "Objects", "UAObject", None, []),
            "ns=2;i=1": runner.XmlNode(
                "ns=2;i=1", 2, "System", "UAObject", "i=85", []
            ),
            "ns=2;i=2": runner.XmlNode(
                "ns=2;i=2", 2, "Manufacturer", "UAVariable", "ns=2;i=1", []
            ),
        }
        namespaces = {
            0: runner.OPC_UA_NAMESPACE_URI,
            2: "urn:exported-parent",
        }
        path = [
            {"namespace_uri": runner.OPC_UA_NAMESPACE_URI, "name": "Objects"},
            {"namespace_uri": "urn:source-system", "name": "System"},
            {"namespace_uri": "urn:source-di", "name": "Manufacturer"},
        ]
        result = runner.resolve_browse_path(
            path, runner.build_name_index(nodes), nodes, namespaces
        )
        self.assertEqual(result.node.node_id, "ns=2;i=2")
        self.assertEqual(result.resolution_mode, "unique_local_name_projection")
        self.assertEqual(len(result.namespace_projection), 2)

    def test_ambiguous_local_name_projection_remains_unresolved(self):
        nodes = {
            "i=85": runner.XmlNode("i=85", 0, "Objects", "UAObject", None, []),
            "ns=2;i=1": runner.XmlNode(
                "ns=2;i=1", 2, "System", "UAObject", "i=85", []
            ),
            "ns=3;i=1": runner.XmlNode(
                "ns=3;i=1", 3, "System", "UAObject", "i=85", []
            ),
            "ns=2;i=2": runner.XmlNode(
                "ns=2;i=2", 2, "Value", "UAVariable", "ns=2;i=1", []
            ),
            "ns=3;i=2": runner.XmlNode(
                "ns=3;i=2", 3, "Value", "UAVariable", "ns=3;i=1", []
            ),
        }
        namespaces = {
            0: runner.OPC_UA_NAMESPACE_URI,
            2: "urn:actual-a",
            3: "urn:actual-b",
        }
        path = [
            {"namespace_uri": runner.OPC_UA_NAMESPACE_URI, "name": "Objects"},
            {"namespace_uri": "urn:source", "name": "System"},
            {"namespace_uri": "urn:source", "name": "Value"},
        ]
        result = runner.resolve_browse_path(
            path, runner.build_name_index(nodes), nodes, namespaces
        )
        self.assertIsNone(result.node)
        self.assertEqual(result.resolution_mode, "unresolved")

    def test_duplicate_source_paths_are_bound_by_source_application_uri(self):
        nodes = {
            "i=85": runner.XmlNode("i=85", 0, "Objects", "UAObject", None, []),
            "ns=2;i=1": runner.XmlNode(
                "ns=2;i=1", 2, "System", "UAObject", "i=85", []
            ),
            "ns=3;i=1": runner.XmlNode(
                "ns=3;i=1", 3, "System", "UAObject", "i=85", []
            ),
            "ns=2;i=2": runner.XmlNode(
                "ns=2;i=2", 2, "State", "UAVariable", "ns=2;i=1", []
            ),
            "ns=3;i=2": runner.XmlNode(
                "ns=3;i=2", 3, "State", "UAVariable", "ns=3;i=1", []
            ),
        }
        namespaces = {
            0: runner.OPC_UA_NAMESPACE_URI,
            2: "urn:vision:urn:source:001",
            3: "urn:vision:urn:source:002",
        }
        path = [
            {"namespace_uri": runner.OPC_UA_NAMESPACE_URI, "name": "Objects"},
            {"namespace_uri": "urn:vision", "name": "System"},
            {"namespace_uri": "urn:vision", "name": "State"},
        ]
        result = runner.resolve_browse_path(
            path,
            runner.build_name_index(nodes),
            nodes,
            namespaces,
            source_application_uri="urn:source:002",
        )
        self.assertEqual(result.node.node_id, "ns=3;i=2")
        self.assertEqual(
            result.resolution_mode,
            "source_scoped_namespace_projection",
        )

    def test_reference_kind_and_direction_must_match_exactly(self):
        source = runner.XmlNode(
            "ns=2;i=2", 2, "Leaf", "UAVariable", "ns=2;i=1", []
        )
        target = runner.XmlNode(
            "ns=2;i=1",
            2,
            "Container",
            "UAObject",
            "i=85",
            [("HasComponent", True, "ns=2;i=2")],
        )
        rule = {
            "reference_type": {
                "namespace_uri": "http://opcfoundation.org/UA/",
                "identifier": "i=47",
            },
            "is_forward": True,
        }
        namespaces = {0: "http://opcfoundation.org/UA/", 2: "urn:test"}
        self.assertTrue(
            runner.reference_semantics_match(rule, source, target, namespaces)
        )
        rule["is_forward"] = False
        self.assertFalse(
            runner.reference_semantics_match(rule, source, target, namespaces)
        )

    def test_q10_policy_and_qualified_source_identity_are_audited(self):
        target = [
            {"namespace_uri": runner.OPC_UA_NAMESPACE_URI, "name": "Objects"},
            {"namespace_uri": "urn:vision", "name": "Container"},
        ]
        source = target + [
            {"namespace_uri": "urn:vision", "name": "Camera"}
        ]
        rule = {
            "source_node": source,
            "target_node": target,
            "reference_type": {
                "namespace_uri": runner.OPC_UA_NAMESPACE_URI,
                "identifier": "i=35",
            },
            "is_forward": True,
            "source_id": "opc.tcp://127.0.0.1:4864/machine-vision/1",
            "source_node_id": {
                "namespace_uri": "urn:vision",
                "identifier": "i=42",
            },
            "source_reference_type": {
                "namespace_uri": runner.OPC_UA_NAMESPACE_URI,
                "identifier": "i=47",
            },
            "source_reference_is_forward": True,
            "reference_policy": runner.REFERENCE_TRANSFORMATION_POLICY,
            "merge_policy": "merge_at_qualified_target",
            "merge_key": target,
            "reference_semantics_verified": True,
        }
        report = runner.build_rule_semantics_report([rule])
        self.assertTrue(report["passed"])
        self.assertEqual(report["source_identity_collision_count"], 0)

        rule["source_reference_type"]["identifier"] = "i=40"
        report = runner.build_rule_semantics_report([rule])
        self.assertFalse(report["passed"])
        self.assertEqual(report["policy_violation_count"], 1)

    def test_rich_rules_project_to_deduplicated_legacy_executor_rules(self):
        legacy = {
            "target_node": ["Objects", "System"],
            "source_node": ["Objects", "System", "Camera"],
            "ref_type": ["HierarchicalReferences", "Organizes"],
            "is_forward": True,
        }
        first = {**legacy, "source_id": "opc.tcp://source-1"}
        second = {**legacy, "source_id": "opc.tcp://source-2"}
        self.assertEqual(
            runner.deduplicated_executor_projection([first, second]),
            [legacy],
        )

    def test_rich_rules_project_to_qualified_source_bound_executor_rules(self):
        rich = {
            "target_node": ["Objects", "System"],
            "source_node": ["Objects", "System", "Camera"],
            "target_node_qualified": [
                {"namespace_uri": runner.OPC_UA_NAMESPACE_URI, "name": "Objects"},
                {"namespace_uri": "urn:vision", "name": "System"},
            ],
            "source_node_qualified": [
                {"namespace_uri": runner.OPC_UA_NAMESPACE_URI, "name": "Objects"},
                {"namespace_uri": "urn:vision", "name": "System"},
                {"namespace_uri": "urn:vision", "name": "Camera"},
            ],
            "ref_type": ["HierarchicalReferences", "Organizes"],
            "is_forward": True,
            "source_id": "opc.tcp://127.0.0.1:4864/machine-vision/1",
            "source_node_id": {"namespace_uri": "urn:vision", "identifier": "i=42"},
            "reference_type": {
                "namespace_uri": runner.OPC_UA_NAMESPACE_URI,
                "identifier": "i=35",
            },
            "merge_policy": "merge_at_qualified_target",
            "merge_key": [
                {"namespace_uri": runner.OPC_UA_NAMESPACE_URI, "name": "Objects"},
                {"namespace_uri": "urn:vision", "name": "System"},
            ],
        }
        expected = {
            key: value
            for key, value in rich.items()
            if key in {
                "ref_type", "is_forward", "source_id", "source_node_id",
                "reference_type", "merge_policy", "merge_key",
            }
        }
        expected["target_node"] = rich["target_node_qualified"]
        expected["source_node"] = rich["source_node_qualified"]

        self.assertEqual(
            runner.executor_projection_schema([expected]),
            "qualified_source_bound_v1",
        )
        self.assertEqual(
            runner.deduplicated_executor_projection(
                [rich], "qualified_source_bound_v1"
            ),
            [expected],
        )

    def test_machine_vision_assignment_requires_two_disjoint_source_pairs(self):
        rules = []
        for replica, system_id in ((1, "001"), (2, "001"), (3, "002"), (4, "002")):
            rules.append({
                "source_id": f"opc.tcp://127.0.0.1:486{3 + replica}/machine-vision/{replica}",
                "target_node": [
                    {"namespace_uri": runner.OPC_UA_NAMESPACE_URI, "name": "Objects"},
                    {"namespace_uri": "urn:vision", "name": f"DualCameraVisionSystem_{system_id}"},
                ],
            })
        report = runner.build_machine_vision_assignment_report(rules)
        self.assertTrue(report["passed"])
        self.assertEqual(
            [system["source_count"] for system in report["systems"]],
            [2, 2],
        )

    def test_jsonl_preserves_events_and_latest_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "aggregation_results.jsonl"
            events = [
                {"source_name": "source", "status": "accepted"},
                {"source_name": "source", "status": "completed"},
            ]
            path.write_text(
                "".join(json.dumps(event) + "\n" for event in events),
                encoding="utf-8",
            )
            loaded = runner.load_jsonl(path)
            self.assertEqual(loaded, events)
            self.assertEqual(
                runner.latest_aggregation_results(loaded)[0]["status"],
                "completed",
            )


if __name__ == "__main__":
    unittest.main()
