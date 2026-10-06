# SPDX-FileCopyrightText: 2026 Adrian Reuther
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

"""Negative acceptance-gate checks and an independent fault-injector check."""
import asyncio
import importlib.util
from pathlib import Path
import unittest

from asyncua import Client, Server, ua

_path = Path(__file__).resolve().parents[2] / "scripts/test-onboarding-robustness.py"
_spec = importlib.util.spec_from_file_location("onboarding_robustness_harness", _path)
harness = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(harness)


class CleanupGateTests(unittest.TestCase):
    def test_rejects_residual_mapping(self):
        with self.assertRaisesRegex(AssertionError, "residual source state"):
            harness.clean_counts({"info_exists": False, "counts": {"references": 1, "worker_present": False}})

    def test_rejects_unjoined_worker(self):
        with self.assertRaisesRegex(AssertionError, "residual source state"):
            harness.clean_counts({"info_exists": False, "counts": {"references": 0, "worker_present": True}})

    def test_rejects_reanimated_status(self):
        with self.assertRaisesRegex(AssertionError, "residual source status"):
            harness.clean_counts({"info_exists": True, "counts": {"references": 0}})

    def test_rejects_absent_diagnostics(self):
        with self.assertRaisesRegex(AssertionError, "empty cleanup diagnostic"):
            harness.clean_counts({"info_exists": False, "counts": {}})


class FaultInjectorTests(unittest.IsolatedAsyncioTestCase):
    async def test_namespace_request_is_held_then_released(self):
        harness.install_namespace_fault_adapter()
        events = []
        fault = {"name": "unittest-only", "entered": asyncio.Event(),
                 "release": asyncio.Event(), "log": events.append}
        server = Server()
        await server.init()
        server.set_endpoint(f"opc.tcp://127.0.0.1:{harness.free_port()}/")
        server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
        server.iserver._ojies_robustness_fault = fault
        async with server:
            async with Client(server.endpoint.geturl(), timeout=3) as client:
                request = asyncio.create_task(client.get_namespace_array())
                try:
                    await asyncio.wait_for(fault["entered"].wait(), 2)
                    self.assertFalse(request.done(), "fault did not actually hold the request")
                    fault["release"].set()
                    namespaces = await asyncio.wait_for(request, 2)
                    self.assertIn("http://opcfoundation.org/UA/", namespaces)
                    self.assertEqual([event["event"] for event in events],
                                     ["namespace_read_held", "late_response_attempted", "late_response_returned"])
                finally:
                    fault["release"].set()
                    if not request.done():
                        request.cancel()
                    await asyncio.gather(request, return_exceptions=True)


if __name__ == "__main__":
    unittest.main()
