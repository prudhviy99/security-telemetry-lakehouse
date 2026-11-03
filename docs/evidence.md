# Validation evidence

Baseline: [`e4ba9244666ac6dd67d83866b5084bc0ebd257d8`](https://github.com/prudhviy99/security-telemetry-lakehouse/commit/e4ba9244666ac6dd67d83866b5084bc0ebd257d8).
Implementation and local verification date: 2026-11-02 UTC.

## What changed

- Reject duration overflow, signed-64-bit version overflow, malformed timezone offsets, and timestamps outside Python's UTC range.
- Separate local Delta paths from Unity Catalog managed tables; preserve cloud URI strings and use workspace Spark in native notebooks.
- Pass notebook parameters explicitly; share one actual job run ID across the four serverless tasks.
- Persist checkpoint identities and reject unsafe checkpoint reuse/loss; document isolated rebuilds.
- Correct Docker's Python version and add required-Spark CI plus a container integration smoke.
- Resolve local warehouse paths before Ivy startup and align Python/worker timestamp conversion with UTC.
- Add a deployable bundle and real, opt-in workspace acceptance runner.

## Current local verification

Python 3.12.14, Java Temurin 21.0.11, PySpark 4.2.0, delta-spark 4.4.1,
pytest 8.3.5 on macOS arm64. The local venv is isolated from system Python.

The local Spark startup smoke produced:

```text
SPARK_SMOKE 4.2.0 3
```

The default Ivy download path could not use this session's JVM network setup.
The two official Maven Central Delta JARs were downloaded through the inherited
HTTP proxy and loaded directly on the local JVM classpath using
`TELEMETRY_DELTA_JARS`. Java/Python dependencies and Spark/Delta versions are the
same pins as CI. No cloud APIs or managed Databricks runtime were substituted.

```bash
python -m pytest -m 'not spark' -q --junitxml=validation/lightweight.xml
python -m pytest -m spark --require-spark-tests -vv --junitxml=validation/spark.xml
```

The final local result is **45 lightweight tests passed** plus **15 Spark/Delta
integration tests passed** (22m 51.76s), with no skips in the integration run.
Machine-readable results are in [validation-2026-11-02.json](validation-2026-11-02.json). The tests include all 13 original Spark cases plus numeric and
timestamp quarantine and unsafe checkpoint reset / isolated reference rebuild.
The reference assertions cover 105 raw occurrences, 102 valid, 3 invalid,
101 ledger versions, 100 READY events, and reconciliation of all three gold marts.

Additional checks: Python compilation, wheel build, notebook parameter/dispatch
checks, required-Spark skip-gate checks, and Docker Compose configuration parsing.
The Databricks CLI used for static schema inspection is v1.23.0. Static validation
is not a successful workspace deployment.

## CI evidence

The original [Actions run](https://github.com/prudhviy99/security-telemetry-lakehouse/actions/runs/41385029569)
reported **26 passed, 1 skipped**, because it installed only dependency-light
extras. The original author's **39 passed** report is preserved separately in
[historical evidence](evidence-original.md); those are different evidence sources.

The updated workflow runs independent lightweight, Spark, and Docker smoke jobs, with Java 21
and Python 3.12. The Spark job explicitly requires integration execution and fails
if the selected tests skip. Logs and JUnit reports are uploaded per commit.
[CI run 41389013563](https://github.com/prudhviy99/security-telemetry-lakehouse/actions/runs/41389013563)
completed successfully on implementation commit
`e5b758bd04b7b8c3194c415a63ea69874419f8b7`:

| Job | Measured result |
| --- | --- |
| Lightweight | 45 passed; 1 Spark module skipped because this job omits Spark dependencies. |
| Required Spark integration | 15 passed, no skipped integration tests; 1809.39 seconds. |
| Docker build and integration smoke | Image build passed; selected Spark/Delta test passed in 94.37 seconds. |

The final follow-up commit changes documentation only and uses `[skip ci]` to
avoid rerunning unchanged executable/test/build files. The linked run and JSON
identify the exact code commit that was executed. The draft PR is
[PR #1](https://github.com/prudhviy99/security-telemetry-lakehouse/pull/1).

## Still requires external execution

- A Databricks workspace identity and OAuth login; no profile was available in this session.
- Bundle validate/deploy/run against that workspace, real Auto Loader ingestion,
  managed-table MERGEs, persistent checkpoints, and all cloud acceptance checks.
- Actual Lakeflow job/run IDs, measured cloud counts, and the cloud result JSON.
- Unity Catalog permissions verification; a second-principal denial test remains
  explicitly untested if the account cannot supply another identity.

The local Docker daemon is unavailable to this session. The image was instead
built and run by the Docker smoke job on GitHub Actions; its recorded outcome
is included in the validation JSON and linked CI run.

No Databricks result, deployment, permission grant, throughput improvement, or
production-scale claim is made. The project collects small batches and touched
version histories on the driver, and rebuilds gold tables independently.


## Follow-up verification and workspace handoff

On November 1, 2026 Pacific (November 2 UTC), the existing GitHub Actions jobs
were inspected directly: `contract-tests`, `spark-tests`, and `docker-smoke`
all reported `success`. Comparing the implementation commit with PR head
`62791c6` confirmed that only `docs/databricks.md`, `docs/evidence.md`, and the
validation JSON changed after the tested executable code.

The signed-in Free Edition workspace was reached. Its console displayed:
"Automated browser control detected. The Databricks console doesn't support
automated browser control. Continue manually, or connect your agent through
Genie One MCP." No Databricks connector or authenticated CLI profile was
available in the follow-up session. Consequently no schema, volume, grant,
job deployment, or workspace run is claimed from that session. The README now
links the existing completion branch and the supported OAuth/CLI setup path.

This follow-up changes documentation only. Existing measured test totals remain
attributed to the linked implementation CI run; they are not new cloud results.


## Supported terminal deployment helper

The CLI is now installed (v1.23.0), and a `TELEMETRY` profile points to the workspace. In the follow-up agent session, `current-user me`, profile validation, and bundle validation report `no cached credentials`; `auth describe` identifies secure OS-keyring storage. A direct request to the workspace's public OAuth discovery URL also fails with `Could not resolve host`. These observations do not establish that the user's normal-terminal login failed; they establish that this agent environment cannot execute authenticated workspace operations.

`scripts/deploy_databricks.py` provides a supported normal-terminal handoff without exporting tokens or changing credential storage. It performs catalog selection, dedicated schema/managed-volume setup, byte-verified immutable uploads, bundle build/deploy, reference/no-op runs, and cloud acceptance. Extended recovery checks are opt-in. It writes actual CLI output and a source/input summary, while leaving measured cloud counts to the acceptance report.

Six dependency-free tests passed with `python3 -m unittest tests.test_deploy_helper -v`. They cover actual-response catalog selection, rejection of an invented catalog, unchanged replay parameters, immutable-file reuse, mismatched-input rejection, and propagation of a remote-read failure. Help output, Python syntax, and whitespace checks passed. No new Spark or Databricks execution is claimed for this helper change; pipeline/runtime/container sources remain the versions covered by the earlier CI evidence.


## First real workspace run: serverless refresh failure

The normal-terminal helper log `databricks-deploy-20261103T045515Z.log` and its
JSON summary record successful bundle validation and deployment: both jobs were
created and 48 bundle files were uploaded. The selected catalog is `workspace`,
with the dedicated `security_telemetry_demo` schema. The helper recorded input
SHA-256 values `442b9499cc4b4943118990866e07df3263bff57a45bd59c9dce654372b647c01`
(events) and `177c90ebb0fe3c0f2114c8175b83e2c5eb62ead9a6d4569d6bdb33feac113a9c`
(customers), and local source commit `51b4d59` with no tracked modifications.

[Pipeline run 837260759008916](https://dbc-03193c06-bea4.cloud.databricks.com/jobs/475857202225789/runs/837260759008916?o=7474656704611080)
started on November 2, 2026 at 20:55 Pacific and failed at 20:59 in `silver`.
The exception was `NOT_SUPPORTED_WITH_SERVERLESS: REFRESH TABLE is not supported
on serverless compute` from `run_dimension`, after writing the customer snapshot.
Gold and validation tasks were skipped. Replay and acceptance commands were not
reached. No successful cloud publication or final reference metrics are claimed.

Version 0.1.1 restricts both dimension and gold `REFRESH TABLE` calls to the local
backend and pins both job dependencies to the new wheel filename. Three narrow
write-stage regressions pass, together with six deployment-helper tests. Running
the same regressions against the pre-fix source reproduces both unsupported
serverless refresh failures. These are dependency-free boundary tests; they do
not substitute for the pending workspace retry or full Spark execution of the fix.
The existing schema, source files, stream generation, and checkpoints are retained.


## Successful 0.1.1 pipeline and basic acceptance; extended harness failure

The same helper log/summary `databricks-deploy-20261103T055620Z` records:

| Run | Result | Pacific time / duration |
| --- | --- | --- |
| Pipeline `673561808875428` | SUCCESS | Nov 2, 21:56:40–21:59:46; 3m 6s |
| Pipeline rerun `629043682032596` | SUCCESS | Nov 2, 21:59:50–22:02:26; 2m 36s |
| Basic acceptance `363703603959324` | SUCCESS | Nov 2, 22:02:29–22:14:10; 11m 41s |
| Extended acceptance `143069489210937` | FAILED | Nov 2, 22:14:12–22:59:18; 45m 6s |

The extended error trace contains the deliberately injected failure at each of `classification`, `quarantine_invalid`, `ledger`, `current`, `quarantine_conflict`, and `audit`. Subsequent notebook attempts report `Acceptance namespace already exists`. These are observed errors; a complete extended pass is not claimed. The basic job's measured JSON has not yet been downloaded into the repository by the agent.

All three [CI jobs for the 0.1.1 fix](https://github.com/prudhviy99/security-telemetry-lakehouse/actions/runs/41585455038) completed successfully. Version 0.1.2 leaves the pipeline transformations unchanged and adjusts acceptance orchestration/reporting: disable automatic optimization retries, add a separate strict report-verification leaf, retain attempt reports, collect real Jobs API/report evidence, and resume verified prior successes.

Twenty-three dependency-free tests pass for the write boundary, deployment safeguards, exact report verification, expected-vs-unexpected task failures, and resume execution. A direct source comparison against the recorded clean `d7e443d` checkout confirms that the pipeline inputs/code are eligible for resume (versioned wheel references are normalized). The 0.1.2 workspace retry remains pending. Unit tests and static schema checks do not establish that the new cloud job has run.
