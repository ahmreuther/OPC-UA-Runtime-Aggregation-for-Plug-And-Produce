# Principal results and limits

This file summarizes only results backed by the curated run records. The compact source tables are in `results/`; the raw measurements and public manifests are in the companion data package.

## E01 — cross-domain semantic integration

Run `20260729T121702.782581Z-9b66527e` used 16 emulated sources from robotics, machine vision, additive manufacturing, and woodworking. All 16 were discovered, accepted, and completed aggregation. The run generated 2,756 rules and passed all 17 predefined acceptance criteria, including type integrity, namespace-aware rule semantics, source-identity separation, and the machine-vision assignment contract.

This supports semantic integration for the evaluated models and fixture topology. It does not establish compatibility with arbitrary information models.

## E02 — three physical robots

Run `20260716T155244.8161167Z-e02-live` was one joint functional run with EVA Automata, Franka Research 3, and UR5e. The target application created one source-associated URDF bundle per robot. The bounded validation checked 55 files, 52 distinct mesh references, XML parsing, parent/child resolution, local mesh containment, HTTP delivery, and hash equality.

The result has `n=1`. No claim of repeatability, arbitrary-robot support, kinematic correctness, skill execution, or robot control is made. Robot models and meshes are not redistributed.

## E03 — paired Read latency

The run contains 10,000 randomized paired observations:

| Path | Mean (ms) | Median (ms) | P90 (ms) | P99 (ms) |
| --- | ---: | ---: | ---: | ---: |
| Direct | 6.974 | 6.534 | 7.437 | 13.664 |
| Aggregated | 7.749 | 6.843 | 7.745 | 51.516 |

The mean paired overhead was 0.775 ms. These are measurements of one hardware/software setup and workload; the higher aggregated-path tail must not be generalized beyond that scope.

## E04/E05 — distributed source-count matrix

The curated matrix `pdist-pilinux-20260730T091252Z-89617` contains three runs at each requested count 1, 5, 10, 25, 50, 75, and 100, for 21 runs in total. Each run retained 2,000 resource samples, and each reached the requested source-listening, LDS-registration, and terminal-aggregation count.

At 100 requested sources, total processing took 6,882.304–7,068.422 s across the three runs, or 68.823–70.684 s per source. Rule generation accounted for about 85% of those totals; this percentage is a derived interpretation of the phase table. The result demonstrates eventual completion under the evaluated serialized provisioning path. It is not a recommended operating point or a concurrency result.

For the fixed 0–1,800 s resource-analysis window, high-count scenarios have reduced simultaneous repetition coverage because processing continued beyond the window. Consult the coverage columns in `results/E04/resource_usage_summary.csv`; do not interpret the 100-source curve as a three-run pointwise mean throughout.
