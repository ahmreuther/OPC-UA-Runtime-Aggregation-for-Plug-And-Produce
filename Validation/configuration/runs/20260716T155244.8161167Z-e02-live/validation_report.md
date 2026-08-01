# E02 three-robot validation report

- Run ID: `20260716T155244.8161167Z-e02-live`
- Systems: EVA Automata, Franka Research 3, and UR5e
- Repetitions: one joint functional run (`n=1`)
- Outcome: `passed`
- Scientific status: `passed` for the bounded E02 scope

The WebSkillComposition transformer connected to the unified Aggregation Server endpoint and created fresh source-associated bundles for all three physical robot source trees. The process exited with code 0. No oversized FileType response, connection closure, missing source tree, or partial publication occurred.

The earlier URDF element-case defect was corrected before this run. Seventeen focused backend tests pass, including the regression that maps OPC UA `Visuals` and `Collisions` folders to the case-sensitive URDF elements `visual` and `collision`.

## Per-robot results

| Robot | Links | Joints | Mesh references | Files | Bytes | URDF SHA-256 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| EVA | 10 | 9 | 18 | 19 | 6,839,282 | `0234131742ea95b536f8e2fed482ac73984052f92c5d4aded599a13de226ba80` |
| Franka | 14 | 13 | 20 | 21 | 27,456,031 | `5c406e1709e4c09a0db24ca7bfb98e6a36b0c6183f93edeffa0fd026e0afacf1` |
| UR5e | 13 | 12 | 14 | 15 | 9,722,370 | `7f0cb2338449870ae8dcfce606328bde3132e72db6a1cfd8895ba63cf107d244` |

All three URDF files are XML-parseable. Every joint parent/child reference resolves. All 52 distinct mesh references remain within their source-associated bundle, exist locally, and point to XML-parseable DAE files. No nonstandard `Visual` or `Collision` element remains. The output comprises 55 files and 44,017,683 bytes in total.

The backend list endpoint returned exactly EVA, Franka, and UR5e. All 55 URDF/mesh resources were then retrieved through the backend's `/urdfs/...` HTTP path. Every request returned HTTP 200, and every response SHA-256 matched the corresponding file in `frontend/public/urdf/`.

The operator immediately confirmed that the URDFs and robot models appeared in the frontend after this joint run. This manual observation closes the visible-model acceptance criterion, although no screenshot was retained. The operator also reported correct configuration of kinematics and skill-based control; that observation is recorded but does not contribute to E02 because those functions are outside the OJIES validation scope.

## Acceptance result

All seven protocol criteria D1--D7 pass: source availability, unified aggregated access, target-side extraction, source association, fresh cache population, frontend availability, and operator-confirmed visualization. The Aggregation Server only exposed and delegated the unified OPC UA address space; extraction, caching, validation, and frontend preparation remained in WebSkillComposition.

This result is a bounded feasibility demonstration for one joint three-robot run. It does not establish repeatability, long-term reliability, compatibility with arbitrary robots, or renewed validation of kinematics, skills, and robot control. Exact live container and NodeSet hashes were not read back from the physical hosts; the manifest records the clean local deployment revisions and workspace NodeSet hashes separately.
