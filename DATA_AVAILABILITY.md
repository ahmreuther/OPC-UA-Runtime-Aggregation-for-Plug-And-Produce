# Data availability

The included research dataset is titled:

> Research Data for Runtime Aggregation of Heterogeneous OPC UA Information Models for Plug & Produce Systems

The dataset is included directly in this repository and the same immutable `v0.1.0` tag as the software. It contains curated E01–E05 evidence, public manifests, 10,000 paired E03 observations, the 21-run E04/E05 matrix, structured phase-timing records, derived tables, a data dictionary, and SHA-256 integrity records. No separate data repository or overlay step is required.

`RESEARCH_DATA_FILE_MANIFEST.csv` defines the dataset boundary and `RESEARCH_DATA_CHECKSUMS_SHA256.txt` verifies its files. Add the article DOI after publication. If a separate archival data DOI is later minted from this immutable combined tag, add that DOI to the GitLab release description without rewriting the tag.

Excluded material comprises robot URDFs and meshes without confirmed redistribution rights, enriched robot NodeSets, private laboratory endpoints, serial numbers, experimental packet captures, repeated runtime-state snapshots, deployment keys, verbose logs, and unrelated working-tree state.

E02's target application is archived separately as WebSkillComposition v1.0.0: <https://doi.org/10.5281/zenodo.17034716>.
