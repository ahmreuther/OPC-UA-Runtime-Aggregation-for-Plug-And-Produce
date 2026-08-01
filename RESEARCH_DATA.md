# Research Data for Runtime Aggregation of Heterogeneous OPC UA Information Models for Plug & Produce Systems

Authors: Adrian Reuther, Niklas Bönisch, Benjamin Schleich  
Affiliation: Technical University of Darmstadt  
Dataset version: 0.1.0

This curated dataset accompanies the OJIES manuscript and is included in the same RWTH GitLab repository and immutable release tag as the research software. It contains only the measurements, public manifests, structured timing evidence, and derived tables needed to audit the principal E01–E05 results.

Repository: <https://git.rwth-aachen.de/ai-in-production/publications/ieee-ojies-2026>

## Contents

- `Validation/integration/raw_data/semantic_integration/20260729T121702.782581Z-9b66527e/`: compact E01 outcome, domain and rule tables, public manifest, and per-source aggregation events.
- `Validation/configuration/runs/20260716T155244.8161167Z-e02-live/`: bounded E02 validation report and public manifest. Robot assets are not included.
- `Validation/performance/raw_data/read_latency/20260717T081221.315204Z-e03-53712-eva-table4-r1/`: 10,000 paired direct/aggregated Read observations, outcome, and redacted manifest.
- `Validation/performance/raw_data/resource_usage/pi5_linux_distributed/`: 21 resource CSV files, reduced manifests, and timing-only aggregation logs for E04/E05.
- `Validation/performance/derived/`: the numeric tables reported or plotted in the paper.
- `RESEARCH_DATA_FILE_MANIFEST.csv` and `RESEARCH_DATA_CHECKSUMS_SHA256.txt`: file inventory and SHA-256 integrity information.

## Public transformations

The numerical measurement files are unchanged.

- E01 `summary.json`: one private aggregation-host address was replaced by `127.0.0.1`; a `public_transform` record documents the change.
- E01/E02: new public manifests retain scientific status and result fields but omit machine-specific paths, private endpoints, and serial numbers.
- E03 `manifest.json`: operator, hostname, endpoint hosts, and absolute local paths were redacted. `measurements.csv` and `outcome.json` are unchanged.
- E04/E05 manifests: topology addresses were replaced by role labels. Timing logs retain only structured `OJIES_PHASE_TIMING` records. The resource-summary JSON redacts the TCP capture filter while preserving all numerical fields.

## Deliberate exclusions

The package excludes robot URDFs and meshes without established redistribution rights, enriched robot NodeSets, experimental packet captures, repeated runtime-state snapshots, deployment PKI, private keys, verbose logs, and unrelated working-tree patches. The retained CSVs and timing records are sufficient for the published numerical analyses.

The E02 target application is archived separately as WebSkillComposition v1.0.0: <https://doi.org/10.5281/zenodo.17034716>.

## Reuse and analysis

Check out combined release tag `v0.1.0` in a disposable clone and verify `RESEARCH_DATA_CHECKSUMS_SHA256.txt` first. Then follow `REPRODUCIBILITY.md`; the data already occupy the paths consumed by the analysis programs.

The 100-source E04/E05 records are bounded stress observations. They establish eventual completion in the evaluated serialized path, not a recommended operating point, arbitrary scalability, or concurrent aggregation.

## License

Except where a file retains a more specific third-party notice, this dataset is licensed under Creative Commons Attribution 4.0 International. See `LICENSE_DATA.md`.
