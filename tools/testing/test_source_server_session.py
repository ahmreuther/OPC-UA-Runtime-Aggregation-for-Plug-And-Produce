# SPDX-FileCopyrightText: 2026 Adrian Reuther
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tools.source_servers import (
    MAX_SESSION_TIMEOUT_MS,
    MIN_SESSION_TIMEOUT_MS,
    clamp_requested_session_timeout,
    create_server_configs,
    machine_vision_semantic_mapping,
    source_application_uri,
)


class _FakeSession:
    async def create_session(self, params, sockname=None):
        return params.RequestedSessionTimeout


class _FakeInternalServer:
    def create_session(self, *args, **kwargs):
        return _FakeSession()


class _FakeServer:
    def __init__(self):
        self.iserver = _FakeInternalServer()


class SessionTimeoutClampTests(unittest.IsolatedAsyncioTestCase):
    async def revised_timeout(self, requested_timeout):
        server = _FakeServer()
        clamp_requested_session_timeout(server)
        session = server.iserver.create_session()
        params = SimpleNamespace(RequestedSessionTimeout=requested_timeout)
        await session.create_session(params)
        return params.RequestedSessionTimeout

    async def test_short_session_is_raised_to_fixture_minimum(self):
        self.assertEqual(
            await self.revised_timeout(5_000),
            MIN_SESSION_TIMEOUT_MS,
        )

    async def test_session_inside_supported_range_is_unchanged(self):
        requested = 40 * 60 * 1000
        self.assertEqual(await self.revised_timeout(requested), requested)

    async def test_long_or_missing_session_is_bounded(self):
        self.assertEqual(
            await self.revised_timeout(2 * MAX_SESSION_TIMEOUT_MS),
            MAX_SESSION_TIMEOUT_MS,
        )
        self.assertEqual(
            await self.revised_timeout(0),
            MAX_SESSION_TIMEOUT_MS,
        )


class MachineVisionFixtureMappingTests(unittest.TestCase):
    def test_four_sources_form_exactly_two_two_source_systems(self):
        self.assertEqual(
            [machine_vision_semantic_mapping(index, 4) for index in range(1, 5)],
            [(1, 1), (1, 2), (2, 1), (2, 2)],
        )
        configs = create_server_configs(
            base_port=4864,
            replicas_per_domain=4,
            domains=["machine-vision"],
        )
        self.assertEqual(len(configs), 4)
        self.assertEqual([config.port for config in configs], [4864, 4865, 4866, 4867])
        self.assertEqual(
            [
                (config.semantic_instance_id, config.semantic_source_slot)
                for config in configs
            ],
            [(1, 1), (1, 2), (2, 1), (2, 2)],
        )


class UrdfFixtureSelectionTests(unittest.TestCase):
    def test_num_servers_remains_urdf_free_by_default(self):
        configs = create_server_configs(base_port=4860, num_servers=4)

        self.assertEqual([config.domain for config in configs], ["generic"] * 4)
        self.assertTrue(
            all(config.urdf_nodeset_path is None for config in configs)
        )
        self.assertTrue(
            all(config.urdf_fixture_name is None for config in configs)
        )

    @patch("tools.source_servers.Path.is_file", return_value=True)
    @patch("tools.source_servers.sha256_file", return_value="fixture-sha256")
    def test_enriched_urdf_fixtures_are_opt_in_and_round_robin(self, _hash, _is_file):
        configs = create_server_configs(
            base_port=4860,
            num_servers=5,
            with_enriched_urdf=True,
        )

        self.assertEqual([config.domain for config in configs], ["robotics-urdf"] * 5)
        self.assertEqual([config.port for config in configs], [4860, 4861, 4862, 4863, 4864])
        self.assertEqual(
            [config.urdf_fixture_name for config in configs],
            [
                "eva-automata",
                "franka-research-3",
                "ur5e",
                "eva-automata",
                "franka-research-3",
            ],
        )
        self.assertTrue(
            all(config.urdf_nodeset_path is not None for config in configs)
        )
        self.assertEqual(
            [config.urdf_nodeset_sha256 for config in configs],
            ["fixture-sha256"] * 5,
        )
        self.assertEqual(
            [source_application_uri(config) for config in configs],
            [
                "urn:plcm:robot-server:eva-automata-001",
                "urn:plcm:robot-server:franka-research-3-002",
                "urn:plcm:robot-server:ur5e-003",
                "urn:plcm:robot-server:eva-automata-004",
                "urn:plcm:robot-server:franka-research-3-005",
            ],
        )


if __name__ == "__main__":
    unittest.main()
