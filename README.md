# Runtime aggregation of heterogeneous OPC UA information models

Research software artifact for the manuscript “Runtime Aggregation of Heterogeneous OPC UA Information Models for Plug & Produce Systems”.

This self-contained release contains the Rust aggregation server, its local OPC UA dependency snapshot, the open62541 Local Discovery Server source used by the experiments, OPC Foundation NodeSets with provenance, public validation programs, compact result tables, and the curated measurements needed to audit E01–E05.

## Principal results represented here

- E01: all 16 emulated sources across four industrial domains were accepted and completed semantic aggregation; all 17 predefined criteria passed.
- E02: one joint run with three physical robots produced and validated three source-associated URDF bundles. This is a bounded functional demonstration with `n=1`.
- E03: 10,000 paired reads yielded means of 6.974 ms direct and 7.749 ms through the aggregator, corresponding to a mean paired overhead of 0.775 ms.
- E04/E05: 21 Raspberry-Pi/Linux runs covered 1, 5, 10, 25, 50, 75, and 100 requested sources with three repetitions each. Every run reached terminal aggregation for the requested count. The 100-source cases are stress observations, not validated operating points.

See `RESULTS.md` for scope and limitations and `results/` for the compact tables.

## Layout

- `opcua_agg_server/`: Rust server and bundled `opcua` 0.12.0 fork.
- `open62541/`: source snapshot used to build `discovery_server_lds`.
- `nodesets/` and `opcua_agg_server/nodesets/`: NodeSets and source/hash records.
- `tools/`: source-server and integration utilities.
- `Validation/analysis/`: E03–E05 analysis programs.
- `Validation/integration/raw_data/`, `Validation/configuration/runs/`, and `Validation/performance/raw_data/`: curated E01–E05 evidence and measurements.
- `Validation/performance/derived/`: publication-relevant derived numeric tables.
- `Validation/performance/acquisition/`: portable acquisition and PCAP post-processing utilities.
- `Validation/configuration/scripts/`: bounded URDF-bundle validator.
- `results/`: small, publication-relevant derived results only.
- `RESEARCH_DATA.md`, `DATA_DICTIONARY.md`, `RESEARCH_DATA_FILE_MANIFEST.csv`, and `RESEARCH_DATA_CHECKSUMS_SHA256.txt`: data scope, field definitions, inventory, and integrity records.

## Requirements

The release was verified on Windows with Rust 1.96.0, Python 3.13.3, CMake, Ninja, Visual Studio C++ Build Tools, and Perl. The Rust build also works from other supported platforms when the native dependencies of the bundled crates are available.

The bundled dependency tree contains paths longer than 260 characters. Git for Windows users should enable `core.longpaths` for clone and checkout operations.

## Build and test

From the repository root in PowerShell:

```powershell
$env:CARGO_TARGET_DIR = 'C:\tmp\ojies-cargo-target'
cargo test --release --locked --offline --manifest-path .\opcua_agg_server\Cargo.toml
```

The `--offline` form requires crates already present in the local Cargo cache. A fresh machine must fetch registry dependencies once; the modified `opcua` dependency itself is included.

Build the open62541 Local Discovery Server:

```powershell
cmake -S .\open62541 -B C:\tmp\ojies-lds-build -G Ninja `
  -DUA_BUILD_EXAMPLES=ON `
  -DUA_ENABLE_DISCOVERY=ON `
  -DUA_ENABLE_DISCOVERY_MULTICAST=ON
cmake --build C:\tmp\ojies-lds-build --target discovery_server_lds
```

For a local server configuration, copy `opcua_agg_server/config.example.json` to `opcua_agg_server/config.json`. Runtime-generated JSON, PKI, logs, and build products are ignored.

## Recompute the published tables

Clone tag `v0.1.0` into a disposable directory, verify `RESEARCH_DATA_CHECKSUMS_SHA256.txt`, and follow `REPRODUCIBILITY.md`. The required curated measurements are already present at the paths consumed by the analysis programs.

## Scope and dependencies

The physical robot models are not redistributed because their third-party redistribution rights were not established. E02 instead retains the bounded validation report and refers to WebSkillComposition v1.0.0 at <https://doi.org/10.5281/zenodo.17034716>.

Project-authored software is source-available under the PolyForm Noncommercial License 1.0.0. Noncommercial use, modification, and redistribution are permitted under that license. Commercial use requires a separate written license from the applicable copyright holder. Because commercial use is restricted, this is not an OSI-approved open-source license.

The root license does not apply to third-party components, compact result data, or the curated research dataset. Those materials remain under their separately identified terms; project-authored data under `results/` and the curated E01–E05 evidence listed in `RESEARCH_DATA_FILE_MANIFEST.csv` are covered by `LICENSE_DATA.md`. See `THIRD_PARTY_NOTICES.md`, `DATA_AVAILABILITY.md`, and `CITATION.cff` for the remaining boundaries.
