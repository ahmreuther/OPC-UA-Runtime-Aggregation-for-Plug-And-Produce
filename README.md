# Runtime Aggregation of Heterogeneous OPC UA Information Models

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.23190835.svg)](https://doi.org/10.5281/zenodo.23190835)

Research software and data accompanying **Runtime Aggregation of Heterogeneous OPC UA Information Models for Plug And Produce Systems** by Adrian Reuther, Niklas Bönisch, and Benjamin Schleich.

The framework discovers OPC UA source servers and derives structural mappings from supported information models during operation. It integrates their information into a shared address space, providing a single OPC UA access point for client applications. Applications can retrieve source associated configuration artifacts and transform them for their own requirements.

## Components

- **Discovery:** identifies available sources through an OPC UA Local Discovery Server with Multicast Extension.
- **Rule generation:** derives mappings from supported NodeSets and source structures while preserving source identity and namespace context.
- **Runtime aggregation:** applies the mappings to a shared address space. Sources are integrated serially while further registrations can be received.
- **Configuration artifact provisioning:** makes source associated URDF bundles available to a consuming application.

The repository includes the Rust aggregation server, its bundled OPC UA dependency, the Local Discovery Server source, public OPC Foundation NodeSets, analysis programs, and curated experimental data.

## Repository layout

| Path | Contents |
| --- | --- |
| `opcua_agg_server/` | Aggregation server and bundled OPC UA library |
| `open62541/` | Local Discovery Server and documented component modifications |
| `nodesets/` | OPC Foundation NodeSets and provenance |
| `tools/`, `scripts/` | Utilities and test support |
| `Validation/current_20261006/` | Selected experimental records and the explicit analysis cohort |
| `Validation/analysis/reproduce_current_release.py` | Numerical verification of the selected public data |
| `results/current_20261006/` | Results and export provenance |

`RESULTS.md` describes the findings and their scope. `EXPERIMENT_VERSIONS.md` identifies the software used in the experiments. `RESEARCH_DATA.md` explains the available data and distinguishes the selected analysis cohort from retained supplementary records.

## Build and numerical verification

Clone the repository with long path support when using Git for Windows:

```sh
git -c core.longpaths=true clone https://github.com/ahmreuther/OPC-UA-Runtime-Aggregation-for-Plug-And-Produce.git
cd OPC-UA-Runtime-Aggregation-for-Plug-And-Produce
```

Follow `REPRODUCIBILITY.md` for build requirements and instructions. Start with `opcua_agg_server/config.example.json` for a local configuration.

The public numerical verification requires Python 3.10 or newer and uses only the standard library:

```sh
python Validation/analysis/reproduce_current_release.py
```

It checks the selected input hashes and recalculates numerical summaries without laboratory access. Results are written to `results/current_20261006/reproduced/`. Original measurements are not modified. `RESEARCH_DATA_FILE_MANIFEST.csv` and `RESEARCH_DATA_CHECKSUMS_SHA256.txt` provide the data inventory and checksums.

## Evaluation scope

The evaluation covers structural integration of emulated sources from four industrial domains, configuration artifact provisioning for three physical robots, direct and aggregated Read latency, integration performance, and controlled supervision and registration scenarios. The physical configuration experiment ends at URDF reconstruction and delivery. It does not validate robot motion or task execution.

The public subset supports numerical reproduction and inspection of compact audit results. Complete semantic and lifecycle validation requires runtime artifacts that are not distributed here. See `Validation/current_20261006/README.md` for the available checks and their limits.

## Citation and archive

The software and research data snapshot is archived on Zenodo:

**A. Reuther, N. Bönisch, and B. Schleich, OPC UA Runtime Aggregation for Plug And Produce, 2026. DOI: [10.5281/zenodo.23190835](https://doi.org/10.5281/zenodo.23190835).**

Use this version DOI when citing the archived software and data. `CITATION.cff` provides machine readable citation metadata. The separately archived target application is [WebSkillComposition v1.0.0](https://doi.org/10.5281/zenodo.17034716).

## Licensing and distributed material

Project authored software is available under PolyForm Noncommercial 1.0.0. Commercial use requires a separate license. This is not an OSI approved open source license. The research data identified in the data manifest are licensed under CC BY 4.0. Third party components retain their original terms. See `LICENSE`, `LICENSE_DATA.md`, and `THIRD_PARTY_NOTICES.md`.

Robot URDFs, meshes, enriched robot NodeSets, experimental packet captures, deployment credentials, private endpoints, and unredacted runtime copies are excluded. Keep local runtime configuration, deployment keys, logs, and generated mapping state outside version control.
