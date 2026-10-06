# Current experimental subset, 6 October 2026

This directory adds the evidence used in the revised manuscript. It does not replace or pool the historical E01–E05 release data. The explicit membership list is `cohort.json`; analysis never selects an unrestricted tree of CSV files.

From the repository root, run with Python 3.10 or newer:

```text
python Validation/analysis/reproduce_current_release.py
```

No external Python package, network service or laboratory device is required. The program verifies export checksums and recalculates numerical results into `results/current_20261006/reproduced/summary.json`. Use `--verify-only` to calculate without writing, or `--output-dir PATH` for a different output directory. It never writes to a manuscript directory.

| Experiment | Included observations | What the public entry point can reproduce |
|---|---|---|
| E01 | Three independent local runs; original domain tables and compact projected semantic audit results | Domain-table sums and consistency of the retained audit counts. Full semantic validation needs the excluded source models, generated rules and address spaces. |
| E03 | Three complete sessions, each with 10,000 paired direct and aggregated Reads after 1,000 warm-up Reads per path | Means, medians, sample standard deviations, P90/P99, paired overhead, successful-Read counts and paired value agreement from the original measurement rows. |
| E08 | Three local synthetic-source runs, each with five completed healthy integrations, and a separate process-termination test | 181 successful recorded reference Reads in total, per-run counts and consistency with projected recorded terminal outcomes. This subset does not rerun the lifecycle supervisor. |
| E09 | 21 complete resource recordings, three each at 1, 5, 10, 25, 50, 75 and 100 sources; a register of all 37 attempts; original per-run timing table | Complete-recording RSS peaks and network-rate integrals, CPU means over complete intervals within acquisition-to-terminal windows, and mean/sample standard deviation across three runs. |
| E10 | Three B100 and three F24 projected audit reports | Reported integration and acceptance counts. B100 integrated all 100 sources in all three trials, but only two trials passed the full acceptance criteria. F24 passed in all three trials. Full validator reruns require excluded runtime artifacts. |

The earlier E03 acquisition interrupted by client standby is documented in the cohort, including the hash and size of its retained original measurement file. It is excluded as a whole. Its failed measurement rows are not distributed here. The three analyzed sessions are from the subsequent predefined block, not selected successful rows from the interrupted attempt.

For E09, all 37 attempts remain represented. The original campaign produced five complete and sixteen unsuccessful attempts, followed by a predefined finite extension of sixteen complete attempts. The selected 21 recordings therefore contain 798 completed source integrations. Acquisition continued for at least 2,000 seconds per recording. The paper's 750-second resource view, mean of three runs and centered 31-second display smoothing do not truncate or smooth the numerical analysis. The full CSV recordings are included.

CPU and RSS describe the aggregation and discovery-service processes on the measured host, not the separate source host or the entire machine. The legacy resource CSV column `net_in_mbps` contains MiB/s, despite its name. The full-recording network integral is reproducible from these intervals. Packet-exact network totals for the shorter acquisition-to-terminal window remain explicitly marked **reported audit results**: the packet captures are excluded, so these exact boundary totals cannot be reconstructed from the public interval CSV alone. Likewise, per-run phase totals can be summarized from `run_timing.csv`, while reconstructing them from individual original log events requires the excluded logs. Rule-commit time is part of other processing, not an extra term to add to the phase sum.

E08 process termination requires an external restart and does not demonstrate continuous availability. E10 B100's third trial was rejected after a discovery failure despite completing all 100 integrations and live checks; its offline semantic validation did not finish. F24 used a controlled Browse failure and a same-generation retry; the outer integration deadline did not expire. These observations do not establish arbitrary-failure recovery or industrial reliability.

`Validation/analysis/current_20261006/read_latency.py` preserves the current E03 statistical implementation, with only portable default output paths and a license header added. It can analyze an individual included E03 run:

```text
python Validation/analysis/current_20261006/read_latency.py Validation/current_20261006/E03/20260930T105757.188826Z-e03-67688-rev040-b02-eva-r1
```

Its default output is under `results/current_20261006/read_latency/`, and it refuses to overwrite an existing per-run result directory. Existing programs outside this current profile remain the historical release programs. This update provides current numerical reproduction, not full recreation of the current paper plots or the complete paper.

`results/current_20261006/export_manifest.json` records every exported file, original repository-relative source paths, SHA-256 hashes and transformations. Original numerical CSV files are copied byte for byte. JSON reports and manifests use selected fields; private endpoints, host-specific paths and operator information are omitted or replaced by generic roles. Original source paths in the provenance are identifiers for locally retained evidence and do not imply those original artifacts are included in this release.

Excluded materials include packet captures, runtime archives and copies, deployment PKI, private credentials, robot URDF/meshes and enriched models, full generated-rule/address-space dumps, verbose logs, review documents and private research notes. The compact audit reports retain scope and failure distinctions but do not substitute for independent validation of those excluded artifacts.
