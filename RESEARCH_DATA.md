# Curated research data

Authors: Adrian Reuther, Niklas Bönisch, and Benjamin Schleich. Dataset version: 0.2.0.

The release distinguishes the new evaluation cohorts from the earlier subset retained from v0.1.0. The original E02 record remains the current physical configuration evidence. Earlier E01 and E03 to E05 records are historical. Use the explicit current cohort at `Validation/current_20261006/cohort.json` and the current summary at `results/current_20261006/summary.json`. Do not gather every matching CSV from the repository and pool it into one experiment.

## Current selection

The current export covers three local semantic integration repetitions, three network latency sessions, local supervision results, 21 complete incremental performance recordings from the 37 attempt campaign, and the concurrent registration and interruption trials. The original single physical configuration run remains the E02 evidence. See `RESULTS.md` for the exact repetition counts and validity boundaries.

## Public transformations

Numerical measurement files included as unchanged raw records preserve their original bytes. Reduced manifests and result projections retain the selected scientific fields and hash provenance, while omitting private endpoint addresses, hostnames, local paths, and unrelated runtime data. Their filenames and export manifest identify them as public projections. A public projection is not a byte identical copy of its private source. It does not establish omitted checks independently.

Historical data and their original scientific scope remain distinguishable. All selected current files and their transformations are indexed in `results/current_20261006/export_manifest.json`. The combined data inventory and checksums cover the complete release data boundary.

## Exclusions and reuse

Robot assets, enriched robot NodeSets, packet captures from the experiments, credentials, and complete runtime snapshots are deliberately excluded. Public protocol replay fixtures in the bundled upstream source remain separately documented test inputs. The numerical verification program is an offline audit of the released subset, not a replacement for live experimentation or complete semantic validation.

Project authored data and documentation identified by the release manifest retain the CC BY 4.0 terms in `LICENSE_DATA.md`. Third party terms remain unchanged. The source code is covered by its separate notices.
