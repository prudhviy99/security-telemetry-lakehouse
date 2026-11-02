# Evidence

Local Spark and Delta run of the security telemetry lakehouse. No Databricks workspace was available, so nothing below is a Databricks job run, notebook run, or Unity Catalog grant. There are no secrets in this file.

## Runtime

| Piece | Version |
| --- | --- |
| OS | Ubuntu 24.04.5 |
| Python | 3.12.3 |
| Java | OpenJDK 21.0.12.1 |
| PySpark | 4.2.0 |
| delta-spark | 4.4.1 |
| pytest | 8.3.5 |

`configure_spark_with_delta_pip` resolved `io.delta:delta-spark_4.2_2.13:4.4.1` from Maven Central on the first Spark submit. Later submits reused the Ivy cache.

Docker Compose is defined in `docker-compose.yml` and pins the same Python, Java, PySpark, and Delta versions. `docker` is not installed in the environment that produced this file, so `docker compose run` was not executed. The commands below are the pinned local install.

## Commands

```bash
python3 -m telemetry.generate --scenario reference --out-dir data
wc -l -c data/events.jsonl data/customers.jsonl
python3 -m pytest -q --tb=line
python3 -m telemetry.orchestrator \
  --events data/events.jsonl \
  --customers data/customers.jsonl \
  --warehouse /tmp/stl-evidence/warehouse \
  --namespace security_telemetry_demo \
  --job-run-id local-evidence-reference \
  --bronze-checkpoint /tmp/stl-evidence/checkpoints/bronze \
  --silver-checkpoint /tmp/stl-evidence/checkpoints/silver
python3 -m telemetry.generate --scenario volume --out-dir /tmp/telemetry-volume
python3 -m telemetry.measure \
  --events /tmp/telemetry-volume/landing \
  --customers /tmp/telemetry-volume/customers.jsonl \
  --warehouse /tmp/telemetry-measure/warehouse \
  --namespace security_telemetry_measure
```

`local-evidence-reference` is an id chosen for this local orchestrator run. It is not a Databricks run id.

## Fixture

```text
  105 28048 data/events.jsonl
   12  2028 data/customers.jsonl
```

Reference scenario seed `20261101`. 105 raw lines, 12 customers. The three invalid lines are a missing `event_id`, event type `reboot`, and an `event_ts` ten minutes ahead of `received_ts`.

## Tests

```text
python3 -m pytest -q --tb=line
.......................................                                  [100%]
39 passed in 1244.71s (0:20:44)
EXIT:0
```

26 of those tests do not start Spark. 13 are Spark/Delta tests (`tests/test_spark_e2e.py`), including MERGE, checkpoints, late events, and the publication marker. GitHub Actions runs `pytest -m "not spark"` only.

One earlier Spark run failed while reading the `silver_events` view as a Delta path. The helper was changed to read the catalog view. `test_matrix_08_duplicate_customer_blocks_and_unknown_stays` then passed alone (`1 passed in 106.44s`), and the full 39-test run above is after that fix.

## Reference pipeline

Orchestrator exit code 0. Startup printed Ivy resolution and `NativeCodeLoader` / log4j warnings. The measured summary line was:

```text
PIPELINE_SUMMARY {"conflict_events": 0, "duplicate_delivery_rows": 1, "invalid_rows": 3, "job_run_id": "local-evidence-reference", "ledger_versions": 101, "namespace": "security_telemetry_demo", "publication_rows": 1, "quarantine_rows": 3, "raw_rows": 105, "ready_events": 100, "superseded_versions": 1, "valid_rows": 102}
```

Valid occurrences (102) plus invalid occurrences (3) equal raw bronze rows (105). READY events are 100. The two extra valid occurrences are one repeated delivery and one superseded version. They are not conflicts.

Quarantine reasons from `quarantine_events`:

```text
+-------------------------------+-----+
|quality_reason                 |count|
+-------------------------------+-----+
|event_ts_ahead_of_received_ts  |1    |
|missing_required_field:event_id|1    |
|unsupported_event_type         |1    |
+-------------------------------+-----+
```

`event-0010` was ingested as `2026-06-02T03:30:00+05:00` and stored as UTC `2026-06-01 22:30:00`, state `READY`. The unknown customer landed in the cohort mart as `UNKNOWN` / `UNKNOWN` with `accepted_event_count` 1.

Gold sums of `accepted_event_count` were 100 on `gold_device_day`, `gold_product_version_day`, and `gold_signup_cohort_day`, matching 100 READY events.

Publication marker (one row, reconciliation `passed`):

```text
publication_id           local-evidence-reference
namespace                security_telemetry_demo
published_at             2026-11-02 00:43:38.664524
ready_event_count        100
device_day_event_sum     100
product_version_event_sum 100
cohort_event_sum         100
reconciliation           passed
```

## Failure injection and assertion failure

Both are Spark tests in the 39 passed run, not a second hand-written log.

- `test_matrix_06_failure_after_each_sink_recovers` raises after `classification`, `quarantine_invalid`, `ledger`, `current`, `quarantine_conflict`, and `audit`, then resumes with the same silver checkpoint. Ledger, current state, quarantine, and gold matched an uninterrupted run of the same input.
- `test_matrix_12_failed_gold_assertion_does_not_advance_marker` publishes once (`publication_id` `publish-ok`), then reruns with the forced gold assertion. The marker stays `publish-ok`.

Duplicate customer ids block publication and do not write `gold_device_day` (`test_matrix_08`). A higher version after a `CONFLICT` becomes `READY` (`test_matrix_05`). Identical files with the same checkpoints do not change bronze, the ledger, current state, quarantine, or gold (`test_matrix_01`).

## Larger local run

`scenarios/volume.json`: seed `20261101`, 10,000 logical events, 200 customers, 4 JSONL files, 2,674,008 bytes. No invalid rows, so READY count equals raw count. Cold time includes JVM startup in that process. The warm rerun uses the same checkpoints.

```text
input_rows: 10000
input_bytes: 2674008
file_count: 4
pure_ready_events: 10000
spark_ready_events: 10000
ready_matches_contract: True
cold_pipeline_seconds: 73.928
warm_rerun_seconds: 28.756
warm_rerun_ready_events: 10000
gold_rebuild_shuffle_1_seconds: 2.711
gold_rebuild_shuffle_8_seconds: 8.874
```

The single change was `spark.sql.shuffle.partitions` on a warm gold rebuild, from 1 to 8. Eight partitions was slower on this machine for this data. No throughput target or improvement is claimed.

Physical plan at 1 shuffle partition:

```text
== Physical Plan ==
*(2) HashAggregate(keys=[event_date#36521, customer_id#36535, device_id#36536], functions=[count(1)])
+- Exchange hashpartitioning(event_date#36521, customer_id#36535, device_id#36536, 1), ENSURE_REQUIREMENTS, [plan_id=8676]
   +- *(1) HashAggregate(keys=[event_date#36521, customer_id#36535, device_id#36536], functions=[partial_count(1)])
      +- *(1) Project [cast(event_ts#36534 as date) AS event_date#36521, customer_id#36535, device_id#36536]
         +- *(1) Filter (isnotnull(state#36530) AND (state#36530 = READY))
            +- *(1) ColumnarToRow
               +- FileScan parquet spark_catalog.security_telemetry_measure.silver_current[state#36530,event_ts#36534,customer_id#36535,device_id#36536] Batched: true, DataFilters: [isnotnull(state#36530), (state#36530 = READY)], Format: Parquet, Location: PreparedDeltaFileIndex(1 paths)[file:/tmp/telemetry-measure/warehouse/security_telemetry_measure/..., PartitionFilters: [], PushedFilters: [IsNotNull(state), EqualTo(state,READY)], ReadSchema: struct<state:string,event_ts:timestamp,customer_id:string,device_id:string>
```

The 8-partition plan is the same shape with `hashpartitioning(..., 8)`.

After the measure process printed that summary it exited 0. Stderr also showed `BrokenPipeError` from a PySpark worker during shutdown. The counts above were printed before that.

## Local job export

This is the orchestrator task list. `databricks_job_id` is null on purpose.

```json
{
  "name": "security_telemetry_daily",
  "format": "local-lakeflow-equivalent",
  "max_concurrent_runs": 1,
  "trigger": "availableNow",
  "databricks_job_id": null,
  "parameters": {
    "catalog": "local",
    "namespace": "security_telemetry_demo",
    "events_path": "data/events.jsonl",
    "customers_path": "data/customers.jsonl",
    "warehouse_path": "warehouse",
    "bronze_checkpoint": "checkpoints/bronze",
    "silver_checkpoint": "checkpoints/silver",
    "use_auto_loader": false,
    "source": "spark structured streaming text files with Trigger.AvailableNow"
  },
  "tasks": [
    {"task_key": "bronze", "depends_on": [], "entry": "notebooks/01_bronze.py"},
    {"task_key": "silver_dimension", "depends_on": ["bronze"], "entry": "notebooks/02_silver.py"},
    {"task_key": "gold", "depends_on": ["silver_dimension"], "entry": "notebooks/03_gold_validate.py", "parameters": {"stage": "gold"}},
    {"task_key": "validate", "depends_on": ["gold"], "entry": "notebooks/03_gold_validate.py", "parameters": {"stage": "validate"}}
  ]
}
```

## Not executed

- Databricks workspace, Lakeflow job submission, and any Databricks run id
- Auto Loader (`cloudFiles`). Local bronze uses the text file source and `Trigger.AvailableNow`
- Unity Catalog volumes, grants, and a second read-only principal
- `docker compose run` (Docker is not installed here)
- Spec section 9 anomaly / modeling extension
