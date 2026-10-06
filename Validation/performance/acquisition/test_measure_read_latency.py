# SPDX-FileCopyrightText: 2026 Adrian Reuther
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

import importlib.util
import sys
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("measure_read_latency.py")
SPEC = importlib.util.spec_from_file_location("measure_read_latency", MODULE_PATH)
measure_read_latency = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = measure_read_latency
SPEC.loader.exec_module(measure_read_latency)


class ReadLatencyAcquisitionTests(unittest.TestCase):
    def test_balanced_orders_are_reproducible(self):
        first = measure_read_latency.balanced_orders(11, 42)
        second = measure_read_latency.balanced_orders(11, 42)
        self.assertEqual(first, second)
        direct_first = sum(order[0] == "direct" for order in first)
        aggregated_first = len(first) - direct_first
        self.assertLessEqual(abs(direct_first - aggregated_first), 1)

    def test_value_serialization_is_deterministic(self):
        first = measure_read_latency.serialize_value({"b": 2, "a": [1, 2]})
        second = measure_read_latency.serialize_value({"a": [1, 2], "b": 2})
        self.assertEqual(first, second)

    def test_failed_read_prevents_pass(self):
        good = {
            "pair_index": 1,
            "path": "direct",
            "status_good": "true",
            "error_type": "",
            "value_sha256": "same",
        }
        failed = {
            "pair_index": 1,
            "path": "aggregated",
            "status_good": "false",
            "error_type": "TimeoutError",
            "value_sha256": "",
        }
        outcome = measure_read_latency.summarize_rows([good, failed], 1, True)
        self.assertEqual(outcome["scientific_validation_status"], "failed")


if __name__ == "__main__":
    unittest.main()
