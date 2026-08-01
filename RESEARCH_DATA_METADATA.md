# Research data metadata

Use these values for the research-data component of the combined software-and-data release.

- Project path: `ai-in-production/publications/ieee-ojies-2026`
- Project URL: <https://git.rwth-aachen.de/ai-in-production/publications/ieee-ojies-2026>
- Title: Research Data for Runtime Aggregation of Heterogeneous OPC UA Information Models for Plug & Produce Systems
- Description: Curated E01–E05 evidence and measurements for the OJIES 2026 runtime-aggregation study.
- Resource type: Dataset
- Combined release tag: `v0.1.0`
- Publication year: 2026
- Creators:
  - Adrian Reuther — Technical University of Darmstadt
  - Niklas Bönisch — Technical University of Darmstadt
  - Benjamin Schleich — Technical University of Darmstadt
- Language: English
- Collection dates: 2026-07-16 to 2026-07-30
- License: Creative Commons Attribution 4.0 International
- Project visibility: use the existing GitLab project setting and publish only after rights, security, and metadata checks are complete.
- Topics: `opc-ua`, `plug-and-produce`, `semantic-integration`, `industrial-automation`, `research-data`

The repository contains a 16-source cross-domain semantic-integration run, one bounded three-robot configuration run, 10,000 paired direct/aggregated Read observations, and a 21-run Raspberry-Pi/Linux source-count matrix up to 100 requested sources. Private infrastructure data, deployment keys, experimental packet captures, redundant runtime states, and robot assets without confirmed redistribution rights are excluded.

## Related resources

- Combined software and data release: <https://git.rwth-aachen.de/ai-in-production/publications/ieee-ojies-2026>, tag `v0.1.0`.
- Article DOI: add after acceptance/publication.
- E02 target application: WebSkillComposition v1.0.0, <https://doi.org/10.5281/zenodo.17034716>.
- Archival data DOI: add only if the immutable data tag is later deposited in a DOI-minting repository.

## Release workflow

1. Push the single combined root commit as `main` and tag `v0.1.0`.
2. Verify `RESEARCH_DATA_CHECKSUMS_SHA256.txt` in a fresh clone.
3. Create one GitLab release from `v0.1.0` for software and research data.
4. Add the article or archival data DOI later without rewriting the immutable tag.
