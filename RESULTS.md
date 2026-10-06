# Results represented by v0.2.0

The current public subset is selected in `Validation/current_20261006/cohort.json`. Numerical observations are retained unchanged. Public result projections are explicitly distinguished from full raw runtime snapshots. The original release and its historical results remain accessible at tag `v0.1.0`.

## Structural integration

Three independently initialized local Windows runs each used the same 16 emulated sources from four domains. All 17 semantic criteria passed. Every run produced 2756 detailed and executable mappings, distributed as 612 Robotics, 44 Machine Vision, 1836 Additive Manufacturing, and 264 Woodworking mappings. Differences reflect the instantiated structures and traversal policy. They do not rank Companion Specifications or establish comparative domain performance. The compact public records report these semantic validation outcomes, but do not permit a complete rerun of the semantic validator without the deliberately excluded model and runtime artifacts.

## Physical configuration

The unchanged E02 record is one joint run with three physical robots. It checked 55 generated files and 52 mesh references and verified unchanged delivery of generated bundles to the target frontend. The robots are three cases within one run, not three independent repetitions. The result ends at artifact provisioning and does not validate kinematics, robot motion, or configuration of other device classes. Robot assets are not redistributed.

## Read latency

Three fresh aggregation, discovery, and client initializations each recorded 10,000 randomized direct and aggregated Read pairs after 1000 warmup Reads per path. All 60,000 measured Reads succeeded, with matching values in every pair. Mean paired additional latency was 0.79, 0.61, and 1.02 ms. Aggregated P99 was larger in every session. Source servers and network were retained, so the results characterize repeated sessions in one deployment, not independent hardware environments or guaranteed latency bounds. An earlier standby interrupted attempt remains distinct and is not pooled with this cohort.

## Performance and resource use

The evaluated incremental implementation has 21 complete recordings, with three at each source count 1, 5, 10, 25, 50, 75, and 100. The 798 integrated source instances are 3 × (1 + 5 + 10 + 25 + 50 + 75 + 100). These records comprise five completed initial attempts and sixteen completions from a predefined continuation. Sixteen unsuccessful initial attempts remain documented separately within the 37 attempt campaign.

At 100 sources, elapsed time from recording start to final integration was 562.68 ± 10.47 s, including startup and discovery. Recorded processing phases totaled 340.25 ± 9.54 s. Mean rule generation and final rule storage were 108.71 s and 130.29 s, respectively. These are different timing windows and must not be substituted for one another. Uncertainty denotes sample standard deviation across three runs.

Mean maximum resident memory, summed over the aggregation and discovery processes, was 4911.3 ± 103.1 MiB at 100 sources. Internal allocation shares and leak absence were not established. The resource recordings cover final integration and subsequent observation. The manuscript's 750 s plotting window is only a display choice after the integrations had completed. Numerical summaries use the full specified recording windows. No matched algorithmic speedup or general capacity limit follows from these measurements.

## Supervision and concurrent registration

Three local synthetic supervision runs each completed five healthy integrations. All 181 sampled reference Reads succeeded. Separate blocked worker tests produced controlled process termination in all three repetitions. This is a service interruption requiring external restart, not continuous availability.

All three application trials with 100 overlapping direct registration calls completed all 100 integrations and passed live semantic checks. **Two of the three trials passed all acceptance criteria.** One was rejected because of a discovery handshake timeout after integration. The recorded outcome is not replaced or relabelled. Registration overlap is not simultaneous packet arrival or a test of mDNS advertisement bursts.

Three further trials used 24 sources and admission capacity 4 with a controlled Browse interruption. Each recorded one failed attempt, confirmed termination and cleanup before subsequent integration, successful retry, and completion of all requested sources. All three trials passed. The failure occurred before the configured outer attempt deadline. These trials do not demonstrate that deadline expiring or arbitrary failure recovery.

## Implementation attribution

The current code includes the later admission and supervision mechanism. The performance cohort evaluated the earlier incremental implementation. Current source, historical binaries, and separately initialized cohorts are not interchangeable. See the public cohort provenance and `EXPERIMENT_VERSIONS.md`.
