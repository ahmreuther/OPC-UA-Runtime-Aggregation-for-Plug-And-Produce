# SPDX-FileCopyrightText: 2026 Adrian Reuther
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

import unittest

from tools.source_servers import (
    LDS_MDNS_CAPABILITY,
    _service_has_capability,
    _service_info_to_discovery_url,
)


class FakeServiceInfo:
    def __init__(self, *, addresses, port=4840, path=b"", caps=b"LDS", server=""):
        self._addresses = addresses
        self.port = port
        self.server = server
        self.properties = {b"caps": caps, b"path": path}

    def parsed_addresses(self):
        return self._addresses


class MdnsLdsDiscoveryTests(unittest.TestCase):
    def test_detects_lds_capability_case_insensitively(self):
        info = FakeServiceInfo(addresses=["192.0.2.20"], caps=b"DA, lds")
        self.assertTrue(_service_has_capability(info, LDS_MDNS_CAPABILITY))

    def test_rejects_source_server_capabilities(self):
        info = FakeServiceInfo(addresses=["192.0.2.20"], caps=b"NA,DA,DI")
        self.assertFalse(_service_has_capability(info, LDS_MDNS_CAPABILITY))

    def test_builds_discovery_url_from_ipv4_record(self):
        info = FakeServiceInfo(
            addresses=["192.0.2.20"],
            port=4840,
            path=b"discovery",
        )
        self.assertEqual(
            _service_info_to_discovery_url(info),
            "opc.tcp://192.0.2.20:4840/discovery",
        )

    def test_brackets_ipv6_address(self):
        info = FakeServiceInfo(addresses=["2001:db8::1"], port=4840)
        self.assertEqual(
            _service_info_to_discovery_url(info),
            "opc.tcp://[2001:db8::1]:4840",
        )

    def test_prefers_address_on_source_server_network(self):
        info = FakeServiceInfo(
            addresses=["203.0.113.1", "198.51.100.155"],
            port=4840,
        )
        self.assertEqual(
            _service_info_to_discovery_url(info, "198.51.100.42"),
            "opc.tcp://198.51.100.155:4840",
        )


if __name__ == "__main__":
    unittest.main()
