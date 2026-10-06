# Changes in v0.2.0

This version accompanies the revised OJIES manuscript. It preserves the v0.1.0 tag and keeps earlier public evidence distinguishable from the revised evaluation.

## Software

- Incremental maintenance of detailed and executable rule collections avoids rewriting the retained collection for ordinary additions.
- Discovery observation is separated from bounded admission to a serial integration worker. This supports overlapping source registration without competing integration writers.
- Integration supervision and cancellation do not depend on acquiring the integration mutex. Safe continuation requires confirmed termination and cleanup. Otherwise, controlled process termination interrupts the service.
- The Local Discovery Server and the bundled OPC UA dependency include the corresponding robustness corrections. Third party license notices and the public test fixture boundary are preserved.

These are implementation changes. A controlled speedup, unrestricted concurrency, or continuous availability is not inferred from them.

## Evidence

- Three independently initialized local structural integration runs replace the single run as the current integration cohort.
- Three separately initialized network latency sessions replace the historical session as the current latency cohort.
- Twenty-one complete incremental performance recordings are selected from the documented 37 attempt campaign. All include final integration and subsequent observation.
- Local supervision results and concurrent registration and interruption trials extend the tested conditions. All three 100 source trials completed integration, but two passed every trial criterion. All three 24 source interruption trials passed.
- The physical configuration evidence remains one joint run with three robots, ending at artifact provisioning.

The new offline verifier recomputes numerical summaries from the released subset. Public projections of semantic and lifecycle results remain auditable summaries, not substitutes for excluded complete runtime artifacts. See `REPRODUCIBILITY.md`, the current cohort, and `RESULTS.md`.

## Archiving

The existing Zenodo DOI `10.5281/zenodo.21756816` identifies the earlier deposit. Archive v0.2.0 as a new version of that existing record, preserving its version family `10.5281/zenodo.21756815`. Use the newly assigned version DOI for this release in the revised paper only after the deposit is published and verified.
