# Public validation analyses

This release retains only the analyses used for the current E03–E05 evidence:

- `read_latency.py`: recomputes the E03 paired-read statistics.
- `ressourcen_platform.py`: recomputes E04 resource summaries and figures.
- `performance_outcomes.py`: recomputes E05 onboarding outcomes and processing-phase figures.
- `measurement_selection.py` and `plot_paths.py`: shared selection, integrity, and output-path logic.
- `test_read_latency.py`: regression tests for the latency analysis.

The curated raw inputs are included in this repository at the paths consumed by the analysis programs. Use a disposable clone of release tag `v0.1.0`, verify `RESEARCH_DATA_CHECKSUMS_SHA256.txt`, and follow the repository-root `REPRODUCIBILITY.md`.

`requirements-lock.txt` records the complete package set used during clean release verification. The broader `requirements.txt` remains suitable when exact versions are not required.
