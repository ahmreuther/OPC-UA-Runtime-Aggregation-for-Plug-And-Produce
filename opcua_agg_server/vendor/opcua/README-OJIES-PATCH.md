# OJIES local OPC UA patch

This directory vendors only the `opcua` library crate from
`https://git.rwth-aachen.de/acor-working-group/opcua-agg-server.git`, branch
`aggregation_server`, commit `05c1eb26be06ab96c618ee7afa4ec2b6f7056542`.
The upstream MPL-2.0 license is preserved in `LICENSE.md`.

The local patch is required because the upstream instance-rule API stores and
resolves every browse-path segment by local name only. The OJIES implementation
adds:

- qualified `{namespace_uri, name, identifier?}` path elements with legacy
  string input; the optional namespace-independent identifier selects one of
  several equal-name target siblings;
- a source endpoint and stable source-node identity per generated rule;
- an explicit `merge_at_qualified_target` policy and exact merge key;
- an exact source reference-type identity and direction;
- ambiguity rejection for legacy local-name paths;
- exact target-NodeId resolution when a qualified target segment carries an
  identifier, while still checking its namespace URI and BrowseName;
- complete Browse/BrowseNext consumption with service-error propagation; and
- `FindServersOnNetwork` request/response support in the vendored message
  dispatcher so the Aggregation Server can consume LDS-ME mDNS records.

The application writes this qualified schema directly to
`rules_executor.json`. `source_id` binds a rule to one `LowerServer`,
`source_node_id` verifies the node reached by the qualified source path, and a
target `identifier` disambiguates equal-name local siblings. This is required
for physical Robotics servers that all expose `MotionDevice_Generic`,
`Controller_Generic`, and `SafetyState_Generic` but use different source
NodeIds. Legacy string-only rule files remain readable, but they neither select
a source endpoint nor permit ambiguous sibling resolution.

## Physical-robot verification

The 2026-07-14 ARM64 deployment exercised this code against three physical-robot
source servers simultaneously. EVA Automata, UR5e, and Franka Research 3 exposed
the same Robotics instance BrowseNames and DisplayNames but different source
NodeIds. The generator produced 1,068 rich rules and 1,068 source-bound executor
rules instead of the former 517-rule local-name projection. Live browsing of the
aggregated server found three distinct siblings under each of `Controllers`,
`MotionDevices`, and `SafetyStates`. A recursive client check completed 88
delegated variable reads without an error, including robot-specific manufacturer,
model, and serial-number values for all three motion devices.

This check demonstrates the executor behavior in the deployed laboratory setup.
The observed generated target NodeIds are runtime-local and are not portable
configuration values.

Additional project-specific client changes expose the synchronous request
timeout and decoding limits through `ClientBuilder`, propagate them into every
session, and configure aggregation lower-server clients for a 60 s request
timeout, 16 MiB ByteStrings, and 20 MiB messages. This is required because the
physical-robot NodeSets model meshes as `Content` ByteString properties and the
largest currently versioned mesh is 7,079,719 bytes, above the upstream
65,535-byte default. The Aggregation Server only transports these variables;
target-side URDF reconstruction remains in WebSkillComposition.

The affected client files are `src/client/{builder.rs,client.rs,config.rs}` and
`src/client/session/{session.rs,session_state.rs}`. The upper-server limits are
set by the application builder, while the lower-server limits are set in
`src/server/aggregation_server/aggregation_server.rs`.

`src/core/comms/message_writer.rs` additionally enforces the negotiated send
buffer as the maximum encoded chunk size. Without this correction, the writer
passed an unlimited chunk size to the chunker and emitted a 120,561-byte mesh
ReadResponse as one chunk even though the peer advertised a 65,535-byte receive
buffer. The application regression suite covers this exact response size and
requires multiple `MSG` chunks at or below the negotiated boundary.

## Validation boundary

The validated immutable E01 run
`Validation/integration/raw_data/semantic_integration/20260713T180111.498542Z-b594fcf0/`
did not compile this directory. It used the Git dependency at the pinned upstream
commit and the explicit LDS-ME/`FindServers` path. Because this vendor patch also
changes the aggregation executor, its successful unit tests and build are not a
substitute for the complete E01 acceptance protocol. The historical E01 result
remains valid for its recorded binary; this vendor variant must produce a new
16-source `VALIDATED` bundle before it can be described as E01-regression-validated.
