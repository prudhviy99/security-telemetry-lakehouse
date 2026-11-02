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
