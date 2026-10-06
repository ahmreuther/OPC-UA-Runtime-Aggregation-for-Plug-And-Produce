# Runtime aggregation of heterogeneous OPC UA information models

Software and curated research evidence for “Runtime Aggregation of Heterogeneous OPC UA Information Models for Plug And Produce Systems” by Adrian Reuther, Niklas Bönisch, and Benjamin Schleich.

This revision, **v0.2.0**, accompanies the revised manuscript. It contains the current aggregation implementation, its bundled OPC UA dependency, the Local Discovery Server source, public utilities, and curated evidence from the revised evaluation. Experimental source revisions are recorded separately. The current source must not be treated as the binary used in every historical experiment.

## Current evidence

- **Structural integration:** three independently initialized local runs, each with 16 emulated sources across Robotics, Machine Vision, Additive Manufacturing, and Woodworking. All 17 semantic criteria passed in each run.
- **Physical configuration:** one joint run with three robots, validating reconstruction and delivery of source associated URDF bundles. The scope ends at artifact provisioning.
- **Read latency:** three separately initialized sessions, each with 10,000 direct and aggregated pairs. All 60,000 measured Reads succeeded and all paired values agreed. The physical source servers and network were retained between sessions.
- **Performance:** 21 complete runs, three at each source population 1, 5, 10, 25, 50, 75, and 100. These are the complete recordings from 37 documented attempts. They evaluate incremental rule maintenance and include final integration and subsequent observation.
- **Supervision and concurrent registration:** local supervision tests and separate application trials. All three 100 source trials completed integration and passed live semantic checks, but only two passed every trial criterion. All three 24 source interruption trials passed.

See `RESULTS.md` for interpretation and `Validation/current_20261006/cohort.json` for the explicit current selection. Earlier E01 and E03 to E05 files remain as historical evidence. The original E02 record remains the current physical configuration evidence. Historical and new cohorts are not pooled or silently relabelled.

## Layout

| Path | Purpose |
| --- | --- |
| `opcua_agg_server/` | Rust aggregation server and bundled OPC UA fork |
| `open62541/` | Local Discovery Server source and documented local changes |
| `nodesets/` | Public OPC Foundation NodeSets and provenance |
| `tools/`, `scripts/` | Portable utilities and test support |
| `Validation/current_20261006/` | Selected current numerical records and public result projections |
| `Validation/analysis/reproduce_current_release.py` | Offline numerical verification of the current public subset |
| `results/current_20261006/` | Current summaries and export provenance |
| Other `Validation/` and `results/E01` to `results/E05` paths | Retained historical public evidence and programs |

`RESEARCH_DATA_FILE_MANIFEST.csv` defines the curated data boundary. `RESEARCH_DATA_CHECKSUMS_SHA256.txt` verifies it. `EXPERIMENT_VERSIONS.md` separates current code, recorded experimental revisions, and public transformations.

## Build and verify

Git for Windows users should clone with long path support:

```sh
git -c core.longpaths=true clone https://github.com/ahmreuther/OPC-UA-Runtime-Aggregation-for-Plug-And-Produce.git
```

Follow `REPRODUCIBILITY.md` for the source build and numerical checks. Verification runs locally and does not require connecting to the laboratory. Running live source servers or reproducing physical experiments is a separate activity.

Runtime configuration, certificates, keys, logs, and generated mapping state do not belong in version control. Start with `opcua_agg_server/config.example.json` for a local configuration.

## Citation and archive

The previous deposited artifact is [version v0.1.0 on Zenodo](https://doi.org/10.5281/zenodo.21756816). It does **not** archive the changes in v0.2.0. The existing [version family](https://doi.org/10.5281/zenodo.21756815) should be continued by depositing this release as a new version of the same Zenodo record. Its DOI is assigned by Zenodo and must be verified after publication.

Use the DOI of the exact archived version in the manuscript. A Git push or a mirrored branch alone does not update the existing Zenodo deposit. No DOI for v0.2.0 is invented in this repository. `CITATION.cff` identifies the source version in the meantime.

## Scope and licensing

The public evidence supports the explicitly stated checks. It does not reproduce the complete private semantic snapshots or the physical robot deployment. Robot URDFs, meshes, enriched robot NodeSets, experimental packet captures, deployment credentials, private endpoints, and unredacted runtime copies are excluded. E02 refers to the separately archived [WebSkillComposition v1.0.0](https://doi.org/10.5281/zenodo.17034716).

Project authored software is source available under PolyForm Noncommercial 1.0.0. Commercial use requires a separate license. This is not an OSI approved open source license. The research data identified in the data manifest are covered by CC BY 4.0. Third party components retain their original terms. See `LICENSE`, `LICENSE_DATA.md`, and `THIRD_PARTY_NOTICES.md`.
