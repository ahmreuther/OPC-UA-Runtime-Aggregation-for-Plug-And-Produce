# Third-party notices

The root `LICENSE` applies only to project-authored source code offered in this release unless a file states otherwise. It does not replace or restrict third-party terms.

The separate `LICENSE_DATA.md` applies only to project-authored compact result files under `results/`.

## Bundled OPC UA for Rust fork

`opcua_agg_server/vendor/opcua/` contains version 0.12.0 of OPC UA for Rust with project-specific changes. It is distributed under MPL-2.0; its license is retained at `opcua_agg_server/vendor/opcua/LICENSE.md`.

The upstream test tree contains known, publicly distributed certificate/private-key fixtures named `our_private.pem` and `their_private.pem`. They are test material, not deployment credentials, and must never be used outside tests.

## Bundled open62541 snapshot

`open62541/` is the source snapshot used to build the Local Discovery Server, including project-specific changes to mDNS and LDS example code. The upstream MPL-2.0 and CC0 notices are retained as `open62541/LICENSE` and `open62541/LICENSE-CC0`. Exact original-project tree objects, the embedded version, known upstream-commit limitations, and public-release deviations are recorded in `open62541/README-OJIES-SNAPSHOT.md`.

The open62541 test suite retains three small upstream `tests/network_replay/*.pcap` fixtures. They are protocol-replay test inputs, not captures from the reported experiments.

## OPC Foundation NodeSets

The XML files under `nodesets/`, `opcua_agg_server/nodesets/`, and the open62541 dependency tree retain their embedded OPC Foundation MIT License 1.00 notices where applicable. The accompanying `nodeset_sources.json` files record exact SHA-256 values for every project-selected NodeSet. Exact upstream commits are included where retained; unresolved entries are explicitly marked as unpinned refs or repository-local snapshots.

## Modified FreeOPCUA XML exporter

`tools/testing/XmlExporter.py` contains modified portions of `asyncua.common.xmlexporter.XmlExporter` from the FreeOPCUA `opcua-asyncio` project. The exact copied revision was not retained; the method bodies most closely match the [`v0.9.98` implementation](https://github.com/FreeOpcUa/opcua-asyncio/blob/1c6ba81f9d0dd4d2612bfba4bd3e16fd824d46e0/asyncua/common/xmlexporter.py). The release runtime dependency is separately locked to `asyncua==2.0.1`.

The upstream portions and this modified file remain under the GNU Lesser General Public License v3 or later (`LGPL-3.0-or-later`), not the root PolyForm license. Project modifications by Adrian Reuther in July 2026 add progress reporting, exception handling, partial-tree cleanup, and explicit failure reporting. The unchanged upstream LGPL text is retained at `licenses/FreeOPCUA-LGPL-3.0-or-later.txt`; because LGPLv3 incorporates the terms of GPLv3, the full GPLv3 text is retained at `licenses/GPL-3.0.txt`.

## Package dependencies

Rust registry and Python package dependencies remain under their respective upstream licenses. `Cargo.lock` and the requirements files identify versions used by this release; no third-party package is relicensed by the root license.

Robot URDFs, meshes, vendor wheels, local WebSkillComposition source, and four nonessential conference-paper/slide PDFs from the upstream open62541 documentation tree were excluded because this release did not establish all redistribution rights.
