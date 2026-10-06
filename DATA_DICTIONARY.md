# Data dictionary

## Current cohort

`Validation/current_20261006/cohort.json` explicitly selects the current public experiment records. Public result projections retain their original run association and distinguish experiment outcome, integration completion, and full trial acceptance. Unchanged numerical CSVs retain their original field names and units. The current offline verifier documents which fields enter each statistic.

Latencies are measured in nanoseconds in the source records and reported in milliseconds. Performance elapsed time begins at recording start, while phase timing sums instrumented processing phases. These are different windows. Memory is the combined resident set size of the aggregation and discovery processes. Across run error bounds use sample standard deviation. The display limit of 750 seconds is not the numerical resource measurement window.

The current `results/current_20261006/README.md` and export manifest document the selected files and public transformations. Checksums identify the bytes distributed here, not omitted private originals.

## Retained historical field definitions

The following definitions concern the original public E01 to E05 records, which remain separate from the current cohort.


## E01 semantic-integration evidence

- `summary.json`: aggregate counts, rule semantics, type integrity, and acceptance results. The private server host was replaced by loopback.
- `manifest_public.json`: run identity, timestamps, implementation hashes, validation status, and declared public-subset exclusions.
- `domain_table.csv`: per-domain source, namespace, entry-point, rule, completion, and acceptance counts.
- `machine_vision_assignment.csv`: assignment checks for the two semantic machine-vision systems.
- `rules_per_namespace.csv` and `rules_per_namespace_pair.csv`: rule-count distributions.
- `inputs/aggregation_results.jsonl`: structured per-source start and terminal events.
- `COMPLETED`, `VALIDATED`: zero-length state markers from the canonical run.

## E02 configuration evidence

- `validation_report.md`: human-readable bounded validation result.
- `manifest_public.json`: machine-independent run metadata, result counts, acceptance status, limitations, and reference to the separately archived target application.

## E03 Read latency

Each row in `measurements.csv` is one position in a randomized direct/aggregated pair.

- `pair_index`: one-based pair identifier.
- `position_in_pair`: first or second request within the pair.
- `order`: randomized order label.
- `path`: `direct` or `aggregated`.
- `observed_at_utc`: wall-clock observation time.
- `start_monotonic_ns`, `end_monotonic_ns`, `latency_ns`: monotonic timing fields.
- `status_code`, `status_good`: OPC UA status.
- `value_type`, `value_sha256`, `value_json`: returned-value integrity fields.
- `error_type`, `error_message`: empty for successful observations.

`outcome.json` records completion and quality gates. The derived directory contains CSV, JSON, and LaTeX summaries.

## E04/E05 distributed matrix

CSV naming follows `<repetition>n<requested-source-count>.csv`, for example `2n50.csv`.

- `timestamp`: UTC sample time.
- `rust_pid`, `cpp_pid`: aggregation-server and LDS process identifiers in the run namespace.
- `cpu_cores_in_use`: interval CPU-time delta divided by elapsed wall time.
- `total_memory_mb`: combined resident memory of aggregation server and LDS.
- `net_in_mbps`: bidirectional captured TCP/IP volume per interval in MiB/s despite the historical column name.
- `sample_interval_seconds_actual`: actual interval duration.
- `elapsed_seconds`: seconds from measurement start.

Each reduced manifest records requested count, repetition, exact observed listening/registration/terminal counts, data hashes, measurement scope, fixture hashes, and the path/hash of its timing-only log.

Derived files:

- `resource_usage_summary.csv/json`: E04 numeric summaries and coverage diagnostics.
- `onboarding_outcomes.csv`: E05 requested and observed source counts per run.
- `processing_phase_times.csv`: rule generation, aggregation, other processing, total processing, and per-source total by run.
