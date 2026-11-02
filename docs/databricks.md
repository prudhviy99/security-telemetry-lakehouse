# Run the project in Databricks

The local implementation runs Spark and Delta. A real Databricks run is still
needed to verify Auto Loader, serverless Spark Connect, Unity Catalog permissions,
and Lakeflow Jobs together. No cloud success is claimed by this document or by
building the bundle. Use synthetic data and a dedicated demo schema.

## 1. Create a free learning workspace

Start at the official [Databricks Free Edition guide](https://docs.databricks.com/aws/en/getting-started/free-edition)
and choose **Free Edition**, not a paid account or time-limited trial. You do not
need to create an AWS account for this route. Open the resulting workspace and
copy its actual `https://...` workspace URL.

Free Edition has serverless compute and usage limits. This bundle has no classic
cluster definitions, schedule, or external cloud-storage resources. It runs one
task at a time. Quota exhaustion can stop a run; wait for quota recovery instead
of changing to paid compute. Feature availability is still verified by the run,
not assumed from the free-account label. See the official
[Free Edition limits](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations).

## 2. Choose storage names in the workspace

In Catalog Explorer, find a catalog in which your identity can create schemas.
Do not guess `main`: use the name actually shown. In a SQL notebook run the
following, replacing `YOUR_CATALOG` everywhere with that name:

```sql
CREATE SCHEMA IF NOT EXISTS YOUR_CATALOG.security_telemetry_demo;
CREATE VOLUME IF NOT EXISTS YOUR_CATALOG.security_telemetry_demo.pipeline_files;
```

The volume holds input files, checkpoints, and acceptance reports. Output Delta
tables are **managed tables** in Unity Catalog, created by name; the code does
not register files inside a volume as tables. Ordinary runs retain the same
schema, generation, and checkpoints.

The job runs as the deploying identity by default. That identity needs catalog
and schema usage, table/view creation, and volume read/write permissions. The
acceptance job additionally creates isolated schemas under the same catalog, so
it requires `CREATE SCHEMA` there. A catalog/schema owner can normally perform
these demo operations. If a shared workspace denies a permission, ask its owner
for the specific privilege; do not grant broad public access.

## 3. Authenticate the checkout terminal

Install the current official Databricks CLI. On macOS with Homebrew:

```bash
brew install databricks/tap/databricks
databricks version
databricks auth login --host https://YOUR_ACTUAL_WORKSPACE_HOST --profile TELEMETRY
databricks current-user me --profile TELEMETRY
databricks catalogs list --profile TELEMETRY
```

The login opens a browser. Complete it yourself. Do not paste tokens or
credentials into a chat, source code, or a report. The workspace host is the
workspace URL, not an account-console URL. Other install methods and the OAuth
flow are in the [CLI install](https://docs.databricks.com/aws/en/dev-tools/cli/install)
and [authentication](https://docs.databricks.com/aws/en/dev-tools/cli/authentication)
documentation. A CLI executable alone does not mean authentication is configured. A browser
already signed in to the workspace does not authenticate this terminal either.
If the console displays "The Databricks console doesn't support automated
browser control", use this supported OAuth/CLI route for agent work. Complete
the browser OAuth prompt yourself; keep credentials out of the repository.
After login, `catalogs list` supplies the actual catalog names for Step 2.

If this checkout already has `.tools/databricks`, it is a local CLI executable;
you may use that path in place of `databricks`. It is not a credential.

## 4. Upload the committed reference fixtures

Run these commands from the repository root. Replace `YOUR_CATALOG` in the
commands; leave the reference files unchanged. The acceptance runner checks the
105 event rows and 12 customer rows, then asserts the expected pipeline counts.

```bash
databricks fs mkdirs dbfs:/Volumes/YOUR_CATALOG/security_telemetry_demo/pipeline_files/reference/events --profile TELEMETRY
databricks fs cp data/events.jsonl dbfs:/Volumes/YOUR_CATALOG/security_telemetry_demo/pipeline_files/reference/events/events.jsonl --profile TELEMETRY
databricks fs cp data/customers.jsonl dbfs:/Volumes/YOUR_CATALOG/security_telemetry_demo/pipeline_files/reference/customers.jsonl --profile TELEMETRY
```

These uploads intentionally do not use `--overwrite`. Once Auto Loader has seen
an input file, retain it unchanged. Later deliveries use new filenames. If the
same fixture is already there, verify it rather than replacing it.

## 5. Validate and deploy the bundle

Activate your local Python environment first, so `python -m pip wheel` is
available. The artifact build installs no local Spark dependencies on Databricks.

```bash
source .venv/bin/activate
databricks bundle validate -t dev --profile TELEMETRY --var catalog=YOUR_CATALOG
databricks bundle deploy -t dev --profile TELEMETRY --var catalog=YOUR_CATALOG
```

The bundle creates two on-demand jobs:

| Resource key | Purpose |
| --- | --- |
| `telemetry_pipeline` | Four serial tasks: bronze, silver plus dimension, gold, validate and publish. |
| `telemetry_acceptance` | Isolated, measured workspace checks using tiny fixtures and the uploaded reference data. |

All four pipeline tasks receive the same actual `{{job.run_id}}`. The stable
stream identities derive from catalog, schema, stream name, and `v1` generation;
they do not change every time the job runs. Both jobs have
`max_concurrent_runs: 1`. Do not start notebook copies or another job against the
same pipeline targets concurrently.

The job environment installs `dist/*.whl` from the bundle artifact. It does not
install `.[spark]`, `pyspark`, or `delta-spark`: Databricks supplies those runtime
components. Current Databricks supports a job environment override for notebook
tasks. See [serverless dependency configuration](https://docs.databricks.com/aws/en/compute/serverless/dependencies)
and [bundle library dependencies](https://docs.databricks.com/aws/en/dev-tools/bundles/library-dependencies).

Environment version `5` is the default. If your workspace does not offer it,
select a supported serverless version compatible with this package's Python
requirement and pass `--var catalog=YOUR_CATALOG,environment_version=VERSION` to
both validation and deployment. Then run acceptance; choosing another runtime
is not proof of compatibility. Do not add local Spark packages as a workaround.

## 6. Run the four-task pipeline

```bash
databricks bundle run -t dev --profile TELEMETRY --var catalog=YOUR_CATALOG telemetry_pipeline
```

Open the run URL printed by the CLI. Check that every task succeeded. In the
`validate_publish` task output, find `PIPELINE_SUMMARY`. For the untouched
reference fixture, expected counts are raw `105`, valid `102`, invalid `3`,
ledger versions `101`, READY `100`, duplicate deliveries `1`, superseded
versions `1`, conflicts `0`, and quarantine rows `3`. Each gold table's sum of
`accepted_event_count` should be `100`. These are expectations until your
workspace actually reports them.

Run the same command again without changing the source or checkpoints. The
business results and bronze count should stay unchanged. Save both run URLs and
task outputs. The job ID/run ID in the UI must match the recorded run context.

To exercise a failure through the actual four-task job, deliberately fail its
final validation task:

```bash
databricks bundle run -t dev --profile TELEMETRY --var catalog=YOUR_CATALOG telemetry_pipeline --params force_gold_assertion_failure=true
```

Expect `validate_publish` to fail and no publication row for that failed run ID.
Then run the ordinary command again (the default flag is false), preserving the
same checkpoints. Verify the retry succeeds without increasing bronze or logical
event counts. This checks task orchestration in addition to the acceptance
module's function-level failure tests.

The pipeline reads raw text using Auto Loader and uses the finite
`AvailableNow` trigger. On serverless, this avoids unsupported continuously
running triggers. It uses the workspace's existing `spark` session and Spark
Connect compatible transformations; no local Spark session, JVM bridge, or
driver file path is assumed. See [serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations)
and [Auto Loader production guidance](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/production).

## 7. Run cloud acceptance and recovery checks

Start with the smaller smoke run:

```bash
databricks bundle run -t dev --profile TELEMETRY --var catalog=YOUR_CATALOG telemetry_acceptance
```

It verifies the reference counts and gold sums, replay, receipt-independent
identity, a late revision moving an event between historical days, a conflict
arriving in a later batch, higher-version conflict resolution, unknown-customer
retention, duplicate-customer rejection, and failure blocking publication.

Then, when quota permits, run the extended checks and all six sink failures:

```bash
databricks bundle run -t dev --profile TELEMETRY --var catalog=YOUR_CATALOG telemetry_acceptance --params include_extended_checks=true,include_failure_injection=true
```

The extended run checks lower-after-higher arrival, all three conflict arrival
orders, six numeric/timestamp storage boundaries, additive/wrong-type fields,
and an isolated `v2` rebuild. It also injects a failure immediately after each
silver sink has committed and retries with the same checkpoints, with failure
injection disabled. Every recovered business snapshot must match an
uninterrupted run. It rejects changing a checkpoint against populated targets.

Acceptance writes a JSON report to:

```text
/Volumes/YOUR_CATALOG/security_telemetry_demo/pipeline_files/acceptance/ACTUAL_JOB_RUN_ID/result.json
```

It prints `CLOUD_CHECK` only after an assertion passes. A failure writes a
`failed` report and raises the error, so the Lakeflow task fails too. The report
records the real Lakeflow run ID, runtime Spark version, isolated schema names,
measured checks, and `not_run` items. Per-phase audit IDs inside the isolated
schemas are qualified with that real run ID; phases are not separate Lakeflow
job runs. Reports remain evidence of exactly the checks they contain.

Retrieve your actual report (replace the run ID):

```bash
databricks fs cp dbfs:/Volumes/YOUR_CATALOG/security_telemetry_demo/pipeline_files/acceptance/ACTUAL_JOB_RUN_ID/result.json docs/cloud-acceptance-ACTUAL_JOB_RUN_ID.json --profile TELEMETRY
```

The acceptance job creates new schemas named
`stl_acceptance_ACTUAL_JOB_RUN_ID_CASE`. It never deletes existing tables or
checkpoints. Preserve them until you have reviewed the evidence. See
[recovery.md](recovery.md) for recovery and cleanup boundaries.

## 8. Record the remaining evidence honestly

Cloud acceptance does not test a second identity's permissions, run the tiny
fixture's analysis-SQL expectations in the workspace, or benchmark a larger
cloud workload. Those are explicitly listed in `not_run`. The local SQL tests
and local benchmark are separate evidence; they cannot establish cloud results.

For permission verification in a workspace with a second principal, have its
owner grant that principal `USE CATALOG`, `USE SCHEMA`, and `SELECT` on the
published gold tables only. Sign in as that principal and prove a gold read
succeeds while a write and a raw-volume read are denied. Do not perform a write
against real business data. Record commands, actual errors, identity, and time.
If Free Edition cannot provide that second identity/admin capability, leave
the permission check pending; a single user's successful query is insufficient.

For an interview evidence record, include commit SHA, workspace/runtime, job
run URLs and IDs, input counts/bytes, measured counts, report path, and observed
failures/retries. Keep unexecuted checks marked pending. A successful bundle
validation confirms configuration resolution; only completed workspace runs
confirm Databricks behavior.
