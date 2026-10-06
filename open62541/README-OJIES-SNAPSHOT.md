# Provenance of the bundled open62541 snapshot

The base of this Local Discovery Server source snapshot was published in tag `v0.1.0` from original project revision `415bc89a7395a1c06b8e796bdca22a1f7e581bf3`. Its recorded project tree objects were:

| Path | Git tree object |
| --- | --- |
| `open62541/` | `fcada7e41692db9a80cbb245dfab3e2a5d02c5cc` |
| `open62541/deps/mdnsd/` | `2d33a1c21f38f001a354cdd0771d4ff1ad9b1912` |
| `open62541/deps/mqtt-c/` | `0f106359a25df72242b2967a5da2bb1bc2d1b74a` |
| `open62541/deps/nodesetLoader/` | `3a00ff0c81ff79c8d296a1abbcb04ac74d750b87` |
| `open62541/deps/ua-nodeset/` | `599945cfff917e6adbc4059995dc48a2cba0292b` |

These identify trees in the original project, not upstream commit identifiers. The embedded `CMakeLists.txt` reports `v1.5.0-rc2` and `unknown-commit`. The original upstream license files and source notices remain in place.

## Revision source changes

The revised publication incorporates five files from the research implementation inspected on 2026-10-06, whose source repository HEAD was `1aee44dd9f962f09668ce6ee2d5d429ec2fdef41`:

- `deps/mdnsd/libmdnsd/mdnsd.c`: bound serialized probing packets, space per-record probes, and compare typed DNS record identities.
- `src/server/ua_discovery_mdns.c`: match the exact service name when hash buckets collide and retain the `sockaddr_storage` caller buffer correction.
- `src/server/ua_services_discovery.c`: correct the final discovery-URL index used during deregistration.
- `examples/discovery/server_lds.c` and `ojies_lds_limits.h`: validate a finite secure-channel limit, with default 256 and accepted range 1 to 4096.

This is an updated software snapshot. It must not be identified as the exact historical LDS binary of every archived experiment. Each experiment's manifest identifies its executed version.

The no-socket regression programs are `tools/test_lds_limits.c`, `tools/test_lds_mdns_burst.c`, and `tools/test_lds_mdns_records.c`. The mDNS tests need the configured build's `src_generated/mdnsd_config.h` and `deps/mdnsd/libmdnsd/1035.c`. MinGW builds link `ws2_32`. These tests exercise parser/serializer and limit behavior, not live discovery or industrial robustness.

CMake build options remain `UA_BUILD_EXAMPLES=ON`, `UA_ENABLE_DISCOVERY=ON`, and `UA_ENABLE_DISCOVERY_MULTICAST=ON`.

Four nonessential upstream conference-paper/slide PDFs remain excluded because their redistribution rights were not established. Nested Git metadata, runtime state, caches, and build products are not included.
