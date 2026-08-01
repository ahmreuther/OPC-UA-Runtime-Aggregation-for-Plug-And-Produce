# Provenance of the bundled open62541 snapshot

This directory is the source snapshot used for the Local Discovery Server in the OJIES artifact. It was copied from original project revision:

`415bc89a7395a1c06b8e796bdca22a1f7e581bf3`

Within that revision, Git records the following tree objects:

| Path | Git tree object |
| --- | --- |
| `open62541/` | `fcada7e41692db9a80cbb245dfab3e2a5d02c5cc` |
| `open62541/deps/mdnsd/` | `2d33a1c21f38f001a354cdd0771d4ff1ad9b1912` |
| `open62541/deps/mqtt-c/` | `0f106359a25df72242b2967a5da2bb1bc2d1b74a` |
| `open62541/deps/nodesetLoader/` | `3a00ff0c81ff79c8d296a1abbcb04ac74d750b87` |
| `open62541/deps/ua-nodeset/` | `599945cfff917e6adbc4059995dc48a2cba0292b` |

These identifiers pin the exact trees stored by the original project. They are not assertions about the upstream repositories' commit identifiers. The imported snapshot contains no nested Git metadata, and its `CMakeLists.txt` identifies the embedded version as `v1.5.0-rc2` with `OPEN62541_VER_COMMIT` set to `unknown-commit`.

The public release differs from the recorded project tree only as follows:

- `src/server/ua_discovery_mdns.c` uses `sockaddr_storage` at the mDNS caller boundary, preventing a latent undersized-buffer write if that IPv4-only path is extended to IPv6;
- four nonessential conference-paper and slide PDFs were omitted because their redistribution rights were not established for this release;
- nested Git metadata, generated files, caches, and build outputs are absent.

The release root commit and tag `v0.1.0` pin the resulting public snapshot. The build was verified with:

- `UA_BUILD_EXAMPLES=ON`;
- `UA_ENABLE_DISCOVERY=ON`;
- `UA_ENABLE_DISCOVERY_MULTICAST=ON`.
