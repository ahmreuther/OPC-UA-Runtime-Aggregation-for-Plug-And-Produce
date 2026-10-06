# Data availability

This repository combines source code and curated evidence for the revised OJIES study. The new current selection is defined by `Validation/current_20261006/cohort.json`. The unchanged historical public records remain available, including the one physical E02 configuration run.

`RESEARCH_DATA_FILE_MANIFEST.csv` inventories the public data and associated documentation. `RESEARCH_DATA_CHECKSUMS_SHA256.txt` verifies their bytes. `results/current_20261006/export_manifest.json` documents the current export and distinguishes unchanged numerical records from projections of private manifests.

The public subset omits robot URDFs and meshes, enriched robot NodeSets, experimental packet captures, deployment credentials, private infrastructure identifiers, large runtime snapshots, and unrelated worktree state. These exclusions mean that the public subset permits numerical recomputation and audit of reported validation results, not a complete reconstruction of every semantic or physical experiment.

The target application for E02 is separately archived as [WebSkillComposition v1.0.0](https://doi.org/10.5281/zenodo.17034716). The combined software and data snapshot is archived as [Zenodo v0.2.0](https://doi.org/10.5281/zenodo.23190835). Its ZIP is the verified archive of the immutable v0.2.0 source tag.
