# Local regression harnesses

These portable harnesses accompany the Rust examples under `opcua_agg_server/examples/`. They default to isolated loopback fixtures and do not discover or register with plant equipment. Running them is a separate, explicit activity. Merely building or importing their modules does not run an experiment.

- `test-onboarding-robustness.py` requires explicitly supplied compiled example executables and writes new run directories under this repository's `Validation/integration/raw_data/onboarding_robustness/`, unless `--output-root` overrides it. It audits the lower-server lifecycle, not the complete discovery and rule-generation pipeline.
- `test-aggregation-subscriptions.py` requires an explicit target mode and rejects non-loopback endpoint URLs. Its optional synthetic source-service adapter is test-only and must not be described as an implemented source feature or a complete OPC UA service qualification.

Both require the Python dependencies declared at the repository root. Use `--help` to inspect parameters before an explicitly authorized run. Generated evidence is not part of the source export. The local publication preparation runs unit and compilation checks only.

The bundled OPC UA library is not a Cargo workspace member. Its own development dependencies are pinned in `opcua_agg_server/vendor/opcua/Cargo.lock`. Test it through that manifest rather than `cargo test -p opcua` from the application. On the documented Windows toolchain, use the same conservative `profile.release.package.opcua.opt-level=1` override as the application, and enable `vendored-openssl` when no system OpenSSL development package is configured.
