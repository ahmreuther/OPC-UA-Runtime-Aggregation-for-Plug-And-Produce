# Reproducibility guide

## Current public subset

Use a disposable checkout of the v0.2.0 source tag. First verify the data checksums, then run the offline current numerical audit:

```sh
python Validation/analysis/reproduce_current_release.py
```

The current verifier uses the Python standard library. It checks the selected hashes, recomputes latency distributions from unchanged measured observations, and recomputes resource and timing summaries over the complete specified performance windows. E01, E08, and E10 public projections allow checking reported counts and outcomes. They are not a complete rerun of semantic validation because the full private runtime artifacts are excluded.

The input cohort is `Validation/current_20261006/cohort.json`. Expected public summaries and provenance are under `results/current_20261006/`. Generated verification outputs are separate from the original measurements. See that directory's README for exact supported outputs and any plot scope. Never pool this cohort with the historical raw directories.

## Current source build

Use the checked in Rust toolchain and lockfile. On Windows, choose a short external target directory for the bundled dependency tree.

```powershell
$env:CARGO_TARGET_DIR = 'C:\tmp\ojies-cargo-target'
cargo test --release --locked --offline --manifest-path .\opcua_agg_server\Cargo.toml
```

The offline form requires cached registry dependencies. A fresh machine needs to fetch them first. The modified OPC UA dependency is included. Building or running the current software does not retroactively reproduce historical experimental binaries.

Build the Local Discovery Server using CMake with discovery and multicast discovery enabled. Component notices record project modifications. Example:

```sh
cmake -S open62541 -B verification-output/lds -DUA_BUILD_EXAMPLES=ON -DUA_ENABLE_DISCOVERY=ON -DUA_ENABLE_DISCOVERY_MULTICAST=ON
cmake --build verification-output/lds --target discovery_server_lds
```

No live laboratory access, source startup, or robot movement is needed for the offline numerical checks. Live experiment reproduction requires a separate deployment and the relevant model fixtures or robot assets.

## Historical v0.1.0 instructions

The instructions below describe the earlier July public subset. Use the original v0.1.0 tag when reproducing that version. The old plotting programs and fixed windows must not be interpreted as the current paper's performance analysis.


### 1. Verify the source build

Use Rust 1.96.0 and an external target directory on Windows:

```powershell
$env:CARGO_TARGET_DIR = 'C:\tmp\ojies-cargo-target'
cargo test --release --locked --offline --manifest-path .\opcua_agg_server\Cargo.toml
```

If the Cargo registry cache is empty, omit `--offline` once to fetch registry crates. The modified `opcua` source is bundled at `opcua_agg_server/vendor/opcua/`.

Build the Local Discovery Server:

```powershell
cmake -S .\open62541 -B C:\tmp\ojies-lds-build -G Ninja `
  -DUA_BUILD_EXAMPLES=ON `
  -DUA_ENABLE_DISCOVERY=ON `
  -DUA_ENABLE_DISCOVERY_MULTICAST=ON
cmake --build C:\tmp\ojies-lds-build --target discovery_server_lds
```

### 2. Prepare a disposable analysis checkout

Use a disposable clone of combined release tag `v0.1.0` and verify `RESEARCH_DATA_CHECKSUMS_SHA256.txt` before use. The curated `Validation/` evidence and measurements are already present beside the analysis programs; no second repository or overlay step is required.

Create the analysis environment:

```powershell
python -m venv .\Validation\analysis\.venv
.\Validation\analysis\.venv\Scripts\python.exe -m pip install -r .\Validation\analysis\requirements-lock.txt
```

The complete direct and transitive package set was verified in a fresh Python 3.13.3 environment and is pinned in `requirements-lock.txt`. Platform wheels are not bundled, so a fresh installation requires package-index access.

### 3. Run regression tests

```powershell
.\Validation\analysis\.venv\Scripts\python.exe -m unittest discover -s .\Validation\analysis -p 'test_*.py'
.\Validation\analysis\.venv\Scripts\python.exe -m unittest discover -s .\Validation\performance\acquisition -p 'test_*.py'
```

The source-server utility tests use the separate top-level requirements:

```powershell
python -m venv .\.venv
.\.venv\Scripts\python.exe -m pip install -r .\requirements-lock.txt
.\.venv\Scripts\python.exe -m unittest discover -s .\tools\testing -p 'test_*.py'
```

In the curated release, four tests that require deliberately excluded legacy artifacts are expected to report `skipped`; all portable utility tests must pass.

### 4. Recompute E03

```powershell
$run = '.\Validation\performance\raw_data\read_latency\20260717T081221.315204Z-e03-53712-eva-table4-r1'
.\Validation\analysis\.venv\Scripts\python.exe .\Validation\analysis\read_latency.py $run
```

Compare the regenerated `table4_summary.csv` with `results/E03/table4_summary.csv`.

### 5. Recompute E04 and E05

```powershell
$env:MPLBACKEND = 'Agg'
$env:OJIES_FIGURE_DIR = (Resolve-Path .).Path + '\verification-output\figures'
.\Validation\analysis\.venv\Scripts\python.exe .\Validation\analysis\ressourcen_platform.py
.\Validation\analysis\.venv\Scripts\python.exe .\Validation\analysis\performance_outcomes.py --figure all
```

Compare the regenerated numeric CSV files under `Validation/performance/derived/` with `results/E04/` and `results/E05/`. Generated figures are secondary visualizations; the CSV files are the comparison targets.

### 6. Limits of reproduction

E01 and E02 depend on emulated or physical OPC UA sources and, for E02, a separately archived target application. Their curated records support result auditing but do not make the live hardware run self-contained. E04/E05 experimental packet captures and repeated runtime snapshots were excluded because the retained CSVs, manifests, and structured timing records are sufficient for the published analyses.
