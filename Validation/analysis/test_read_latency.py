# SPDX-FileCopyrightText: 2026 Adrian Reuther
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

import csv
import json
import tempfile
import unittest
from pathlib import Path

import read_latency


class ReadLatencyAnalysisTests(unittest.TestCase):
    def test_linear_percentile(self):
        self.assertEqual(read_latency.percentile_linear([0, 10], 90), 9.0)
        self.assertEqual(read_latency.percentile_linear([1, 2, 3, 4, 5], 50), 3.0)

    def test_descriptive_statistics_use_sample_standard_deviation(self):
        result = read_latency.descriptive_statistics([1_000_000, 3_000_000])
        self.assertEqual(result["mean_ms"], 2.0)
        self.assertAlmostEqual(result["sample_std_dev_ms"], 2 ** 0.5)

    def test_analyze_complete_paired_run(self):
        manifest = {
            "run_id": "test-run",
            "scientific_validation_status": "passed",
            "protocol": {"samples_per_path": 2},
        }
        rows = [
            self.row(1, "direct", 1_000_000, "a"),
            self.row(1, "aggregated", 2_000_000, "a"),
            self.row(2, "aggregated", 4_000_000, "b"),
            self.row(2, "direct", 2_000_000, "b"),
        ]
        result = read_latency.analyze_rows(manifest, rows)
        self.assertEqual(result["analysis_status"], "complete")
        self.assertEqual(result["per_path"]["direct"]["mean_ms"], 1.5)
        self.assertEqual(result["per_path"]["aggregated"]["mean_ms"], 3.0)
        self.assertEqual(result["paired_overhead"]["mean_ms"], 1.5)

    def test_incomplete_run_is_rejected(self):
        manifest = {"run_id": "test-run", "protocol": {"samples_per_path": 1}}
        rows = [self.row(1, "direct", 1_000_000, "a")]
        with self.assertRaisesRegex(ValueError, "incomplete"):
            read_latency.analyze_rows(manifest, rows)

    @staticmethod
    def row(pair, path, latency, value_hash):
        return {
            "pair_index": str(pair),
            "path": path,
            "latency_ns": str(latency),
            "status_good": "true",
            "error_type": "",
            "value_sha256": value_hash,
        }


if __name__ == "__main__":
    unittest.main()
