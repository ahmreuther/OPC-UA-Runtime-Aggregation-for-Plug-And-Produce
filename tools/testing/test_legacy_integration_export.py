# SPDX-FileCopyrightText: 2026 Adrian Reuther
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

"""Regression checks for the Legacy artefacts currently matching Chapter 6."""

from __future__ import annotations

import json
import unittest

import run_and_export as runner


EXPORT_DIR = runner.PROJECT_ROOT / "address_space_exports"
XML_PATH = EXPORT_DIR / "address_space_2026-07-08_16-40-35.xml"
SUMMARY_PATH = EXPORT_DIR / "summary_2026-07-08T16-44-17.095713.json"


@unittest.skipUnless(XML_PATH.exists() and SUMMARY_PATH.exists(), "Legacy artefacts absent")
class LegacyIntegrationRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.summary = json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))

    def test_registered_hashes(self):
        self.assertEqual(
            runner.sha256_file(XML_PATH),
            "c2ddf91104d28ee29047921a78402361cc3878302c9ffc4b5a2884ebb95df203",
        )
        self.assertEqual(
            runner.sha256_file(SUMMARY_PATH),
            "c7f2255942dfbe42d2698910315ca70e7b37b5afc90c0140df8ee3f6275d66df",
        )

    def test_chapter_6_artifact_metrics(self):
        data = self.summary
        self.assertEqual(data["config"]["host_count"], 16)
        self.assertEqual(data["namespaces"]["count"], 15)
        self.assertEqual(data["nodeset_mapping"]["mapping_count"], 10)
        self.assertEqual(len(data["nodeset_mapping"]["unique_nodeset_files"]), 9)
        self.assertEqual(data["entry_points"]["total_entry_points"], 5)
        self.assertEqual(data["rules"]["count"], 1589)
        self.assertEqual(data["rules"]["unique_source_paths"], 1589)
        self.assertEqual(data["rules"]["unique_target_paths"], 397)
        self.assertEqual(data["rules"]["max_source_path_depth"], 10)
        self.assertEqual(data["rules"]["max_target_path_depth"], 9)
        self.assertEqual(data["address_space"]["total_node_count"], 16546)

    def test_namespace_count_and_resolution_limits(self):
        self.assertEqual(len(runner.parse_namespace_uris_from_xml(XML_PATH)), 105)
        unresolved_source = sum(
            1 for rule in self.summary["rules_enriched"]
            if not rule["source_resolved"]
        )
        unresolved_target = sum(
            1 for rule in self.summary["rules_enriched"]
            if not rule["target_resolved"]
        )
        self.assertEqual(unresolved_source, 70)
        self.assertEqual(unresolved_target, 0)

    def test_chapter_6_domain_rows(self):
        rows = {
            row["domain"]: (row["inst"], row["rules"], row["targets"])
            for row in self.summary["domain_table"]["rows"]
        }
        self.assertEqual(rows["Robotics"], (4, 625, 189))
        self.assertEqual(rows["Machine Vision"], (4, 515, 127))
        self.assertEqual(rows["Additive Manufacturing"], (4, 168, 52))
        self.assertEqual(rows["Woodworking"], (4, 278, 31))
        self.assertEqual(rows["Unbekannt / Sonstiges"], (None, 3, 2))
        self.assertEqual(
            self.summary["domain_table"]["total"],
            {"inst": 16, "rules": 1589, "targets": 397},
        )


if __name__ == "__main__":
    unittest.main()
