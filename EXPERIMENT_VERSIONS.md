# Experiment and source provenance

## Revised release v0.2.0

The current software is exported from the working implementation on 6 October 2026, preserving the public release boundary and third party notices. It includes incremental rule maintenance and the later admission and supervision implementation. Public source modifications needed for portability and redaction are distinct from experimental revisions. Source hashes and any deviations are documented in the release preparation audit and relevant component notices.

The authoritative current public selection is `Validation/current_20261006/cohort.json`. Its result projections and export manifest preserve available original run IDs, relevant hashes, and explicit limitations.

| Evidence | Recorded scope |
| --- | --- |
| Current E01 | Three independently initialized local runs of the same 16 source fixtures |
| E02 | Original single physical configuration run from July, unchanged scope |
| Current E03 | Three separately initialized latency sessions in one retained physical deployment |
| E08 | Local supervision and separate controlled process exit tests |
| E09 | Earlier incremental implementation, 21 complete recordings selected from 37 attempts |
| E10 | Later admission implementation, three B100 trials with two full passes and three F24 trials with three full passes |

No claim is made that building the current release recreates every recorded historical binary. In particular, the E09 timing data are not measurements of the later admission implementation. The compact public evidence does not include every original model export, runtime state, or deployment inventory.

## Historical v0.1.0 provenance

The following describes the retained original release and its July experiments, not the current experimental cohorts.


The original v0.1.0 release was a clean-history publication assembled from original repository commit `415bc89a7395a1c06b8e796bdca22a1f7e581bf3`. Public-only changes are documentation and licensing metadata, compact result copies, removal of generated/cache/lab-specific files, replacement of the private default aggregation address with `127.0.0.1`, and allocation of an IPv6-capable `sockaddr_storage` buffer in the bundled open62541 mDNS caller. The latter removes a latent overflow if that currently IPv4-only path is later extended to IPv6; it does not change the evaluated IPv4 behavior.

The v0.1.0 release commit is not the historical commit that executed every experiment. The run manifests remain authoritative:

| Experiment | Canonical run or matrix | Recorded implementation |
| --- | --- | --- |
| E01 | `20260729T121702.782581Z-9b66527e` | Base commit `a056c4f756da710e351b4a7f5ffe961aef3dd9b1`, dirty worktree; the relevant runner changes were subsequently committed. Bundled `opcua` tree SHA-256 `b74ecbb10c056a5ab83fb5c38a265a7c7279d5211c7af86cc09bbd54c386eebe`; executed binary SHA-256 `00951ca298c2007c5c3fcd83f15976ea64d11f0113b0ba005bfc97a07d1dbde5`. |
| E02 | `20260716T155244.8161167Z-e02-live` | Aggregation-server commit `b799773c32d19356bac8de1643a2a917e36497e6`; target transformer identified by SHA-256 in the public manifest and by its separate archive. |
| E03 | `20260717T081221.315204Z-e03-53712-eva-table4-r1` | Clean aggregation-server commit `b799773c32d19356bac8de1643a2a917e36497e6`. |
| E04/E05 | `pdist-pilinux-20260730T091252Z-89617` | Source repository commit `415bc89a7395a1c06b8e796bdca22a1f7e581bf3`, recorded per run. |

E01's full dirty-worktree patch was not redistributed because it also captured large generated runtime files. The two source-runner files changed at that time are present in their later committed form. Therefore E01 is auditable from the run evidence but is not claimed to be a byte-identical source rebuild from this release.

NodeSet SHA-256 values and source status are recorded in `nodesets/nodeset_sources.json` and `opcua_agg_server/nodesets/nodeset_sources.json`. Exact upstream commits are included only where the original project retained them; unpinned refs and repository-only snapshots are explicitly marked.
