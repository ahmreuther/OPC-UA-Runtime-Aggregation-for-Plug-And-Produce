# Current numerical results, 6 October 2026

`summary.json` is the checked numerical summary for the current curated cohort. Recreate it from the repository root with:

```text
python Validation/analysis/reproduce_current_release.py
```

The recalculated file is written to `reproduced/summary.json`. It is numerically identical to the supplied summary. No external packages or laboratory services are required. Inputs, methods and limits are explained in `Validation/current_20261006/README.md` and explicitly listed in `Validation/current_20261006/cohort.json`.

The summary contains three E01, E03 and E08 runs, E09 with 21 complete recordings from 37 documented attempts, and six E10 trials. E09 calculations use complete unsmoothed recordings and three independent runs per source count. The paper's 750-second view is only a display choice. B100 completed 100 integrations in all three trials, but passed all acceptance criteria in two; F24 passed in all three.

Current numerical reproduction is distinct from a rerun of the full semantic or lifecycle validators. Compact audit conclusions, packet-exact network subwindows and phase-event reconstruction have explicit limitations in the data README. Current paper plots are not recreated by this entry point. The other analysis programs and result folders retain their historical release status.

`export_manifest.json` contains byte-level input/code/documentation hashes, source provenance and transformations. The separately listed derived-result hash covers `summary.json`; the manifest excludes its own hash to avoid self-reference.
