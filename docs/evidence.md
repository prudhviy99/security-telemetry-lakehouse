# Validation evidence

Baseline: [`e4ba9244666ac6dd67d83866b5084bc0ebd257d8`](https://github.com/prudhviy99/security-telemetry-lakehouse/commit/e4ba9244666ac6dd67d83866b5084bc0ebd257d8).
Implementation and local verification date: 2026-11-02 UTC.

## What changed

- Reject duration overflow, signed-64-bit version overflow, malformed timezone offsets, and timestamps outside Python's UTC range.
- Separate local Delta paths from Unity Catalog managed tables; preserve cloud URI strings and use workspace Spark in native notebooks.
- Pass notebook parameters explicitly; share one actual job run ID across the four serverless tasks.
- Persist checkpoint identities and reject unsafe checkpoint reuse/loss; document isolated rebuilds.
- Correct Docker's Python version and add an independent required-Spark CI job.
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

Final suite results are recorded in `docs/validation-2026-11-02.json` after the
suite completes. The tests include all 13 original Spark cases plus numeric and
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

The updated workflow runs independent lightweight and Spark jobs, with Java 21
and Python 3.12. The Spark job explicitly requires integration execution and fails
if the selected tests skip. Logs and JUnit reports are uploaded per commit.
A configured workflow is not a passing CI result; inspect the checks on the
implementation pull request for the actual outcome.

## Still requires external execution

- A Databricks workspace identity and OAuth login; no profile was available in this session.
- Bundle validate/deploy/run against that workspace, real Auto Loader ingestion,
  managed-table MERGEs, persistent checkpoints, and all cloud acceptance checks.
- Actual Lakeflow job/run IDs, measured cloud counts, and the cloud result JSON.
- Unity Catalog permissions verification; a second-principal denial test remains
  explicitly untested if the account cannot supply another identity.
- Docker image build/run: Docker is installed, but this session cannot access its daemon.

No Databricks result, deployment, permission grant, throughput improvement, or
production-scale claim is made. The project collects small batches and touched
version histories on the driver, and rebuilds gold tables independently.
