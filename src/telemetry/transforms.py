"""Bronze, silver, dimension, gold, and publication on Spark and Delta Lake.

Silver is checkpointed Structured Streaming. Each foreachBatch validates,
canonicalizes, and MERGEs. Current state for every touched event id is
recomputed from all stored versions. Gold is a full rebuild from the READY
view and is published only after reconciliation. There is no multi-table
transaction: the publication marker is written last and is not atomic with
the three gold tables.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from telemetry.config import PipelineConfig
from telemetry.contracts import (
    UNKNOWN_SEGMENT,
    CanonicalEvent,
    classify_raw_line,
    conflict_evidence,
    current_states,
    dedupe_versions,
    raw_payload_hash,
    validate_customer_snapshot,
)
from telemetry.locking import SingleRunLock

BRONZE_SCHEMA = StructType(
    [
        StructField("bronze_row_id", StringType(), False),
        StructField("raw_json", StringType(), False),
        StructField("source_file", StringType(), True),
        StructField("loaded_at", TimestampType(), False),
        StructField("stream_id", StringType(), False),
        StructField("batch_id", LongType(), False),
    ]
)

CLASSIFICATION_SCHEMA = StructType(
    [
        StructField("bronze_row_id", StringType(), False),
        StructField("raw_json", StringType(), False),
        StructField("source_file", StringType(), True),
        StructField("status", StringType(), False),
        StructField("quality_reason", StringType(), True),
        StructField("detail", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("event_version", LongType(), True),
        StructField("payload_hash", StringType(), True),
        StructField("received_ts", TimestampType(), True),
    ]
)

QUARANTINE_SCHEMA = StructType(
    [
        StructField("raw_payload_hash", StringType(), False),
        StructField("quality_reason", StringType(), False),
        StructField("raw_json", StringType(), False),
        StructField("source_file", StringType(), True),
        StructField("detail", StringType(), True),
        StructField("first_seen_run", StringType(), False),
        StructField("first_seen_at", TimestampType(), False),
    ]
)

VERSION_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_version", LongType(), False),
        StructField("payload_hash", StringType(), False),
        StructField("received_ts", TimestampType(), False),
        StructField("event_ts", TimestampType(), False),
        StructField("customer_id", StringType(), False),
        StructField("device_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("product_version", StringType(), False),
        StructField("scan_status", StringType(), True),
        StructField("duration_ms", DoubleType(), True),
        StructField("raw_json", StringType(), False),
        StructField("source_file", StringType(), True),
    ]
)

CURRENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_version", LongType(), False),
        StructField("state", StringType(), False),
        StructField("payload_hash", StringType(), True),
        StructField("payload_count", LongType(), False),
        StructField("received_ts", TimestampType(), True),
        StructField("event_ts", TimestampType(), True),
        StructField("customer_id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_version", StringType(), True),
        StructField("scan_status", StringType(), True),
        StructField("duration_ms", DoubleType(), True),
    ]
)

CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("signup_date", StringType(), False),
        StructField("signup_month", StringType(), False),
        StructField("plan_tier", StringType(), False),
        StructField("country_code", StringType(), False),
        StructField("acquisition_channel", StringType(), False),
        StructField("updated_at", TimestampType(), False),
    ]
)

AUDIT_SCHEMA = StructType(
    [
        StructField("stream_id", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("run_kind", StringType(), False),
        StructField("status", StringType(), False),
        StructField("completed_at", TimestampType(), False),
        StructField("raw_rows", LongType(), True),
        StructField("valid_rows", LongType(), True),
        StructField("invalid_rows", LongType(), True),
        StructField("details", StringType(), True),
    ]
)

PUBLICATION_SCHEMA = StructType(
    [
        StructField("publication_id", StringType(), False),
        StructField("namespace", StringType(), False),
        StructField("published_at", TimestampType(), False),
        StructField("ready_event_count", LongType(), False),
        StructField("device_day_event_sum", LongType(), False),
        StructField("product_version_event_sum", LongType(), False),
        StructField("cohort_event_sum", LongType(), False),
        StructField("reconciliation", StringType(), False),
    ]
)

SINK_ORDER = (
    "classification",
    "quarantine_invalid",
    "ledger",
    "current",
    "quarantine_conflict",
    "audit",
)


class InjectedSinkFailure(RuntimeError):
    def __init__(self, sink: str):
        super().__init__(f"injected failure after sink {sink}")
        self.sink = sink


class DimensionPublicationBlocked(RuntimeError):
    def __init__(self, reasons: list[str]):
        super().__init__("customer snapshot blocked publication: " + "; ".join(reasons))
        self.reasons = reasons


class GoldAssertionError(RuntimeError):
    def __init__(self, failures: list[str]):
        super().__init__("gold reconciliation failed: " + "; ".join(failures))
        self.failures = failures


class FailurePlan:
    """Pop sink names as each write commits. The same object must be reused on resume."""

    def __init__(self, sinks: list[str]):
        self.sinks = sinks
        self.pending: str | None = None

    def after(self, sink: str) -> None:
        if self.sinks and self.sinks[0] == sink:
            self.pending = self.sinks.pop(0)
            raise InjectedSinkFailure(self.pending)


def _plan(config: PipelineConfig) -> FailurePlan:
    plan = getattr(config, "_failure_plan", None)
    if plan is None:
        plan = FailurePlan(config.fail_after_sinks)
        setattr(config, "_failure_plan", plan)
    return plan


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _location(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "")


def _checkpoint_is_outside_input(checkpoint: Path, landing: Path) -> None:
    checkpoint_resolved = checkpoint.resolve()
    landing_resolved = landing.resolve()
    if checkpoint_resolved == landing_resolved or landing_resolved in checkpoint_resolved.parents:
        raise RuntimeError(
            f"checkpoint {checkpoint_resolved} must be outside the input directory {landing_resolved}"
        )


def ensure_database(spark: SparkSession, config: PipelineConfig) -> None:
    if config.catalog in {"local", "spark_catalog"}:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.namespace}")
    else:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.catalog}.{config.namespace}")


def ensure_table(
    spark: SparkSession,
    config: PipelineConfig,
    table: str,
    schema: StructType,
    *,
    append_only: bool = False,
) -> None:
    ensure_database(spark, config)
    path = config.table_path(table)
    path.parent.mkdir(parents=True, exist_ok=True)
    qualified = config.qualified(table)
    if not DeltaTable.isDeltaTable(spark, str(path)):
        # Bronze is a streaming source, so its history must be append-only.
        mode = "append" if append_only else "overwrite"
        spark.createDataFrame([], schema).write.format("delta").mode(mode).save(str(path))
    spark.sql(f"CREATE TABLE IF NOT EXISTS {qualified} USING DELTA LOCATION '{_location(path)}'")


def _frame(spark: SparkSession, rows: list[dict], schema: StructType) -> DataFrame:
    frame = spark.createDataFrame(rows, schema=schema)
    columns = [field.name for field in schema.fields]
    return frame.select(*columns)


def _merge(spark: SparkSession, config: PipelineConfig, table: str, frame: DataFrame, keys: list[str], matched_update: dict | None) -> None:
    if frame.limit(1).count() == 0:
        return
    deduped = frame.dropDuplicates(keys)
    target = DeltaTable.forPath(spark, str(config.table_path(table)))
    condition = " AND ".join(f"target.{key} = source.{key}" for key in keys)
    builder = target.alias("target").merge(deduped.alias("source"), condition)
    if matched_update is None:
        builder = builder.whenNotMatchedInsertAll()
    else:
        builder = builder.whenMatchedUpdate(set=matched_update).whenNotMatchedInsertAll()
    builder.execute()


def _version_from_spark(row) -> CanonicalEvent:
    duration = None if row.duration_ms is None else Decimal(str(row.duration_ms))
    return CanonicalEvent(
        event_id=row.event_id,
        event_version=int(row.event_version),
        customer_id=row.customer_id,
        device_id=row.device_id,
        event_ts=_aware(row.event_ts),
        received_ts=_aware(row.received_ts),
        event_type=row.event_type,
        product_version=row.product_version,
        scan_status=row.scan_status,
        duration_ms=duration,
        payload_hash=row.payload_hash,
        raw_json=row.raw_json,
        source_file=row.source_file,
    )


def _event_to_version_row(event: CanonicalEvent) -> dict:
    return {
        "event_id": event.event_id,
        "event_version": int(event.event_version),
        "payload_hash": event.payload_hash,
        "received_ts": event.received_ts,
        "event_ts": event.event_ts,
        "customer_id": event.customer_id,
        "device_id": event.device_id,
        "event_type": event.event_type,
        "product_version": event.product_version,
        "scan_status": event.scan_status,
        "duration_ms": None if event.duration_ms is None else float(event.duration_ms),
        "raw_json": event.raw_json,
        "source_file": event.source_file,
    }


def _state_to_row(state: dict) -> dict:
    return {
        "event_id": state["event_id"],
        "event_version": int(state["event_version"]),
        "state": state["state"],
        "payload_hash": state["payload_hash"],
        "payload_count": int(state["payload_count"]),
        "received_ts": state["received_ts"],
        "event_ts": state["event_ts"],
        "customer_id": state["customer_id"],
        "device_id": state["device_id"],
        "event_type": state["event_type"],
        "product_version": state["product_version"],
        "scan_status": state["scan_status"],
        "duration_ms": state["duration_ms"],
    }


def refresh_silver_events(spark: SparkSession, config: PipelineConfig) -> None:
    current = config.qualified("silver_current")
    view = config.qualified("silver_events")
    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {view} AS
        SELECT
            event_id,
            event_version,
            payload_hash,
            received_ts,
            event_ts,
            CAST(event_ts AS DATE) AS event_date,
            customer_id,
            device_id,
            event_type,
            product_version,
            scan_status,
            duration_ms
        FROM {current}
        WHERE state = 'READY'
        """
    )


def _prepare_tables(spark: SparkSession, config: PipelineConfig) -> None:
    ensure_table(spark, config, "bronze_events", BRONZE_SCHEMA, append_only=True)
    ensure_table(spark, config, "bronze_row_classification", CLASSIFICATION_SCHEMA)
    ensure_table(spark, config, "quarantine_events", QUARANTINE_SCHEMA)
    ensure_table(spark, config, "silver_event_versions", VERSION_SCHEMA)
    ensure_table(spark, config, "silver_current", CURRENT_SCHEMA)
    ensure_table(spark, config, "dim_customer", CUSTOMER_SCHEMA)
    ensure_table(spark, config, "pipeline_run_audit", AUDIT_SCHEMA)
    ensure_table(spark, config, "successful_publication", PUBLICATION_SCHEMA)
    refresh_silver_events(spark, config)


def _await_query(query, plan: FailurePlan) -> None:
    try:
        query.awaitTermination()
    except Exception as exc:
        pending = plan.pending
        plan.pending = None
        try:
            if query.isActive:
                query.stop()
        except Exception:
            pass
        if pending:
            raise InjectedSinkFailure(pending) from exc
        raise
    finally:
        try:
            if query.isActive:
                query.stop()
        except Exception:
            pass


def run_bronze(spark: SparkSession, config: PipelineConfig) -> None:
    landing, pattern = config.landing_dir_and_glob()
    _checkpoint_is_outside_input(config.bronze_checkpoint, landing)
    _prepare_tables(spark, config)
    plan = _plan(config)
    if config.use_auto_loader:
        # Databricks Auto Loader path. Not used by the recorded local runs.
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "text")
            .option("cloudFiles.schemaLocation", str(config.bronze_checkpoint / "schema"))
        )
    else:
        reader = spark.readStream.format("text")
    source = (
        reader.option("pathGlobFilter", pattern)
        .load(str(landing))
        .select(F.col("value").alias("raw_json"), F.col("_metadata.file_path").alias("source_file"))
    )

    def handle(batch: DataFrame, batch_id: int) -> None:
        if batch.limit(1).count() == 0:
            return
        loaded_at = _utcnow()
        rows = batch.select("raw_json", "source_file").collect()
        seen: dict[tuple[str, str], int] = {}
        payload = []
        for row in rows:
            key = (row.source_file or "", row.raw_json)
            occurrence = seen.get(key, 0)
            seen[key] = occurrence + 1
            identity = f"{config.bronze_stream_id}|{batch_id}|{key[0]}|{occurrence}|{row.raw_json}"
            payload.append(
                {
                    "bronze_row_id": raw_payload_hash(identity),
                    "raw_json": row.raw_json,
                    "source_file": row.source_file,
                    "loaded_at": loaded_at,
                    "stream_id": config.bronze_stream_id,
                    "batch_id": int(batch_id),
                }
            )
        frame = _frame(spark, payload, BRONZE_SCHEMA)
        (
            frame.write.format("delta")
            .mode("append")
            .option("txnAppId", config.bronze_stream_id)
            .option("txnVersion", str(batch_id))
            .save(str(config.table_path("bronze_events")))
        )

    config.bronze_checkpoint.mkdir(parents=True, exist_ok=True)
    query = (
        source.writeStream.foreachBatch(handle)
        .option("checkpointLocation", str(config.bronze_checkpoint))
        .trigger(availableNow=True)
        .queryName(f"{config.namespace}_bronze")
        .start()
    )
    _await_query(query, plan)


def apply_silver_batch(spark: SparkSession, config: PipelineConfig, batch: DataFrame, batch_id: int) -> None:
    """Validate one bronze micro-batch and MERGE ledger, quarantine, and current state.

    Writes are individually idempotent. The audit completion row is last.
    A failure after any earlier sink must be safe to resume with the same batch.
    """
    plan = _plan(config)
    if batch.limit(1).count() == 0:
        return
    collected = batch.select("bronze_row_id", "raw_json", "source_file").collect()
    classified = [(row, classify_raw_line(row.raw_json, row.source_file)) for row in collected]
    run_id = f"{config.silver_stream_id}:{batch_id}"
    now = _utcnow()

    class_rows = []
    invalid_rows = []
    valid_events: list[CanonicalEvent] = []
    for bronze, item in classified:
        event = item.event
        class_rows.append(
            {
                "bronze_row_id": bronze.bronze_row_id,
                "raw_json": item.raw_json,
                "source_file": item.source_file,
                "status": item.status,
                "quality_reason": item.quality_reason,
                "detail": item.detail,
                "event_id": None if event is None else event.event_id,
                "event_version": None if event is None else int(event.event_version),
                "payload_hash": None if event is None else event.payload_hash,
                "received_ts": None if event is None else event.received_ts,
            }
        )
        if item.status == "invalid":
            invalid_rows.append(
                {
                    "raw_payload_hash": item.raw_payload_hash,
                    "quality_reason": item.quality_reason,
                    "raw_json": item.raw_json,
                    "source_file": item.source_file,
                    "detail": item.detail,
                    "first_seen_run": run_id,
                    "first_seen_at": now,
                }
            )
        elif event is not None:
            valid_events.append(event)

    _merge(
        spark,
        config,
        "bronze_row_classification",
        _frame(spark, class_rows, CLASSIFICATION_SCHEMA),
        ["bronze_row_id"],
        None,
    )
    plan.after("classification")

    _merge(
        spark,
        config,
        "quarantine_events",
        _frame(spark, invalid_rows, QUARANTINE_SCHEMA),
        ["raw_payload_hash", "quality_reason"],
        None,
    )
    plan.after("quarantine_invalid")

    deduped = dedupe_versions(valid_events)
    if deduped:
        version_frame = _frame(spark, [_event_to_version_row(event) for event in deduped], VERSION_SCHEMA)
        target = DeltaTable.forPath(spark, str(config.table_path("silver_event_versions")))
        (
            target.alias("target")
            .merge(
                version_frame.dropDuplicates(["event_id", "event_version", "payload_hash"]).alias("source"),
                "target.event_id = source.event_id AND target.event_version = source.event_version AND target.payload_hash = source.payload_hash",
            )
            .whenMatchedUpdate(
                condition="source.received_ts < target.received_ts",
                set={
                    "received_ts": "source.received_ts",
                    "raw_json": "source.raw_json",
                    "source_file": "source.source_file",
                },
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    plan.after("ledger")

    touched = sorted({event.event_id for event in valid_events})
    stored: list[CanonicalEvent] = []
    states: list[dict] = []
    if touched:
        id_frame = spark.createDataFrame([(event_id,) for event_id in touched], ["event_id"])
        stored_rows = (
            spark.read.format("delta")
            .load(str(config.table_path("silver_event_versions")))
            .join(F.broadcast(id_frame), "event_id", "inner")
            .collect()
        )
        stored = [_version_from_spark(row) for row in stored_rows]
        states = current_states(stored)
        current_frame = _frame(spark, [_state_to_row(state) for state in states], CURRENT_SCHEMA)
        (
            DeltaTable.forPath(spark, str(config.table_path("silver_current")))
            .alias("target")
            .merge(current_frame.dropDuplicates(["event_id"]).alias("source"), "target.event_id = source.event_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    plan.after("current")

    conflict_rows = []
    if touched:
        for event in conflict_evidence(stored):
            conflict_rows.append(
                {
                    "raw_payload_hash": raw_payload_hash(event.raw_json),
                    "quality_reason": "conflicting_version_payload",
                    "raw_json": event.raw_json,
                    "source_file": event.source_file,
                    "detail": f"event_id={event.event_id} version={event.event_version} has multiple canonical payloads",
                    "first_seen_run": run_id,
                    "first_seen_at": now,
                }
            )
    _merge(
        spark,
        config,
        "quarantine_events",
        _frame(spark, conflict_rows, QUARANTINE_SCHEMA),
        ["raw_payload_hash", "quality_reason"],
        None,
    )
    plan.after("quarantine_conflict")

    refresh_silver_events(spark, config)
    ready_touched = 0
    conflict_touched = 0
    if touched:
        for state in states:
            if state["state"] == "READY":
                ready_touched += 1
            elif state["state"] == "CONFLICT":
                conflict_touched += 1
    audit_row = {
        "stream_id": config.silver_stream_id,
        "batch_id": str(batch_id),
        "run_kind": "ingestion_batch",
        "status": "COMPLETED",
        "completed_at": _utcnow(),
        "raw_rows": len(classified),
        "valid_rows": len(valid_events),
        "invalid_rows": len(invalid_rows),
        "details": json.dumps(
            {
                "touched_event_ids": len(touched),
                "ready_touched": ready_touched,
                "conflict_touched": conflict_touched,
            },
            sort_keys=True,
        ),
    }
    _merge(
        spark,
        config,
        "pipeline_run_audit",
        _frame(spark, [audit_row], AUDIT_SCHEMA),
        ["stream_id", "batch_id", "run_kind"],
        None,
    )
    plan.after("audit")


def run_silver(spark: SparkSession, config: PipelineConfig) -> None:
    landing, _pattern = config.landing_dir_and_glob()
    _checkpoint_is_outside_input(config.silver_checkpoint, landing)
    _prepare_tables(spark, config)
    plan = _plan(config)
    source = (
        spark.readStream.format("delta")
        .option("startingVersion", "0")
        .option("ignoreDeletes", "true")
        .load(str(config.table_path("bronze_events")))
    )
    config.silver_checkpoint.mkdir(parents=True, exist_ok=True)

    def handle(batch: DataFrame, batch_id: int) -> None:
        apply_silver_batch(spark, config, batch, batch_id)

    query = (
        source.writeStream.foreachBatch(handle)
        .option("checkpointLocation", str(config.silver_checkpoint))
        .trigger(availableNow=True)
        .queryName(f"{config.namespace}_silver")
        .start()
    )
    _await_query(query, plan)
    refresh_silver_events(spark, config)


def run_dimension(spark: SparkSession, config: PipelineConfig) -> None:
    """Replace dim_customer with the validated snapshot, or block publication."""
    _prepare_tables(spark, config)
    lines = config.customers_path.read_text(encoding="utf-8").splitlines()
    customers, errors = validate_customer_snapshot(lines)
    if errors or customers is None:
        _write_job_audit(spark, config, "DIMENSION_BLOCKED", {"errors": errors})
        raise DimensionPublicationBlocked(errors)
    rows = [
        {
            "customer_id": customer.customer_id,
            "signup_date": customer.signup_date.isoformat(),
            "signup_month": customer.signup_month,
            "plan_tier": customer.plan_tier,
            "country_code": customer.country_code,
            "acquisition_channel": customer.acquisition_channel,
            "updated_at": customer.updated_at,
        }
        for customer in customers
    ]
    path = config.table_path("dim_customer")
    _frame(spark, rows, CUSTOMER_SCHEMA).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(path))
    spark.sql(f"CREATE TABLE IF NOT EXISTS {config.qualified('dim_customer')} USING DELTA LOCATION '{_location(path)}'")
    spark.sql(f"REFRESH TABLE {config.qualified('dim_customer')}")


def _gold_select(scan_alias: str = "") -> str:
    prefix = f"{scan_alias}." if scan_alias else ""
    event_type = f"{prefix}event_type"
    scan_status = f"{prefix}scan_status"
    duration = f"{prefix}duration_ms"
    scan = f"CASE WHEN {event_type} = 'scan' THEN 1 ELSE 0 END"
    failed = f"CASE WHEN {event_type} = 'scan' AND {scan_status} = 'failed' THEN 1 ELSE 0 END"
    threat = f"CASE WHEN {event_type} = 'threat_detected' THEN 1 ELSE 0 END"
    return f"""
        COUNT(*) AS accepted_event_count,
        SUM({scan}) AS scan_count,
        SUM({failed}) AS failed_scan_count,
        SUM({threat}) AS threat_event_count,
        AVG(CASE WHEN {event_type} = 'scan' THEN {duration} END) AS avg_scan_duration_ms,
        CASE
            WHEN SUM({scan}) = 0 THEN NULL
            ELSE CAST(SUM({failed}) AS DOUBLE) / SUM({scan})
        END AS scan_failure_rate
    """


def _overwrite_sql(spark: SparkSession, config: PipelineConfig, table: str, select_sql: str) -> None:
    frame = spark.sql(select_sql)
    path = config.table_path(table)
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(path))
    qualified = config.qualified(table)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {qualified} USING DELTA LOCATION '{_location(path)}'")
    spark.sql(f"REFRESH TABLE {qualified}")


def run_gold(spark: SparkSession, config: PipelineConfig) -> None:
    """Full rebuild from READY silver events. Historical dates change when late facts arrive."""
    refresh_silver_events(spark, config)
    events = config.qualified("silver_events")
    customers = config.qualified("dim_customer")
    _overwrite_sql(
        spark,
        config,
        "gold_device_day",
        f"""
        SELECT event_date, customer_id, device_id, {_gold_select()}
        FROM {events}
        GROUP BY event_date, customer_id, device_id
        """,
    )
    _overwrite_sql(
        spark,
        config,
        "gold_product_version_day",
        f"""
        SELECT
            event_date,
            product_version,
            COUNT(DISTINCT customer_id, device_id) AS active_device_count,
            {_gold_select()}
        FROM {events}
        GROUP BY event_date, product_version
        """,
    )
    _overwrite_sql(
        spark,
        config,
        "gold_signup_cohort_day",
        f"""
        SELECT
            e.event_date AS event_date,
            COALESCE(c.signup_month, '{UNKNOWN_SEGMENT}') AS signup_month,
            COALESCE(c.plan_tier, '{UNKNOWN_SEGMENT}') AS plan_tier,
            COUNT(DISTINCT e.customer_id) AS active_customer_count,
            COUNT(DISTINCT e.customer_id, e.device_id) AS active_device_count,
            {_gold_select("e")}
        FROM {events} e
        LEFT JOIN {customers} c
          ON e.customer_id = c.customer_id
        GROUP BY e.event_date, COALESCE(c.signup_month, '{UNKNOWN_SEGMENT}'), COALESCE(c.plan_tier, '{UNKNOWN_SEGMENT}')
        """,
    )


def reconciliation_failures(spark: SparkSession, config: PipelineConfig) -> list[str]:
    failures: list[str] = []
    ready = spark.table(config.qualified("silver_events")).count()
    grains = {
        "gold_device_day": "event_date, customer_id, device_id",
        "gold_product_version_day": "event_date, product_version",
        "gold_signup_cohort_day": "event_date, signup_month, plan_tier",
    }
    for table, grain in grains.items():
        qualified = config.qualified(table)
        duplicate_grains = spark.sql(
            f"SELECT {grain}, COUNT(*) AS c FROM {qualified} GROUP BY {grain} HAVING COUNT(*) > 1"
        ).count()
        if duplicate_grains:
            failures.append(f"{table} grain is not unique ({duplicate_grains} duplicate keys)")
        total = spark.sql(f"SELECT COALESCE(SUM(accepted_event_count), 0) AS total FROM {qualified}").collect()[0].total
        if int(total) != int(ready):
            failures.append(f"{table} accepted_event_count sum {int(total)} != READY silver events {int(ready)}")
        broken = spark.sql(
            f"""
            SELECT COUNT(*) AS c
            FROM {qualified}
            WHERE (scan_count = 0 AND (scan_failure_rate IS NOT NULL OR avg_scan_duration_ms IS NOT NULL))
               OR (scan_count > 0 AND (scan_failure_rate IS NULL OR avg_scan_duration_ms IS NULL))
               OR (
                    scan_count > 0
                    AND ABS(scan_failure_rate - (CAST(failed_scan_count AS DOUBLE) / scan_count)) > 1e-9
                  )
            """
        ).collect()[0].c
        if int(broken):
            failures.append(f"{table} has {int(broken)} rows breaking the scan rate or duration rule")
    raw = spark.table(config.qualified("bronze_events")).count()
    classified = spark.table(config.qualified("bronze_row_classification")).count()
    if int(raw) != int(classified):
        failures.append(f"bronze rows {int(raw)} != classified rows {int(classified)}")
    counts = spark.sql(
        f"""
        SELECT
            SUM(CASE WHEN status = 'valid' THEN 1 ELSE 0 END) AS valid_rows,
            SUM(CASE WHEN status = 'invalid' THEN 1 ELSE 0 END) AS invalid_rows
        FROM {config.qualified("bronze_row_classification")}
        """
    ).collect()[0]
    valid_rows = int(counts.valid_rows or 0)
    invalid_rows = int(counts.invalid_rows or 0)
    if valid_rows + invalid_rows != int(raw):
        failures.append("valid plus invalid occurrences do not equal raw bronze rows")
    duplicate_customers = spark.sql(
        f"""
        SELECT customer_id
        FROM {config.qualified("dim_customer")}
        GROUP BY customer_id
        HAVING COUNT(*) > 1
        """
    ).count()
    if duplicate_customers:
        failures.append("dim_customer has duplicate customer ids")
    if config.force_gold_assertion_failure:
        failures.append("intentional gold assertion failure")
    return failures


def _write_job_audit(spark: SparkSession, config: PipelineConfig, status: str, details: dict) -> None:
    _prepare_tables(spark, config)
    row = {
        "stream_id": "job",
        "batch_id": config.job_run_id,
        "run_kind": "job_run",
        "status": status,
        "completed_at": _utcnow(),
        "raw_rows": details.get("raw_rows"),
        "valid_rows": details.get("valid_rows"),
        "invalid_rows": details.get("invalid_rows"),
        "details": json.dumps(details, sort_keys=True, default=str),
    }
    # Latest job status replaces an earlier failed attempt with the same local run id.
    frame = _frame(spark, [row], AUDIT_SCHEMA)
    target = DeltaTable.forPath(spark, str(config.table_path("pipeline_run_audit")))
    (
        target.alias("target")
        .merge(
            frame.alias("source"),
            "target.stream_id = source.stream_id AND target.batch_id = source.batch_id AND target.run_kind = source.run_kind",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_validation(spark: SparkSession, config: PipelineConfig) -> list[str]:
    """Reconcile gold, then advance the publication marker only when every assertion passes."""
    failures = reconciliation_failures(spark, config)
    quality = quality_counts(spark, config)
    if failures:
        _write_job_audit(spark, config, "VALIDATION_FAILED", {"failures": failures, **quality})
        raise GoldAssertionError(failures)
    ready = int(quality["ready_events"])
    sums = {
        table: int(
            spark.sql(f"SELECT COALESCE(SUM(accepted_event_count), 0) AS total FROM {config.qualified(table)}")
            .collect()[0]
            .total
        )
        for table in ("gold_device_day", "gold_product_version_day", "gold_signup_cohort_day")
    }
    _write_job_audit(spark, config, "COMPLETED", quality)
    publication = {
        "publication_id": config.job_run_id,
        "namespace": config.namespace,
        "published_at": _utcnow(),
        "ready_event_count": ready,
        "device_day_event_sum": sums["gold_device_day"],
        "product_version_event_sum": sums["gold_product_version_day"],
        "cohort_event_sum": sums["gold_signup_cohort_day"],
        "reconciliation": "passed",
    }
    _merge(
        spark,
        config,
        "successful_publication",
        _frame(spark, [publication], PUBLICATION_SCHEMA),
        ["publication_id"],
        None,
    )
    return failures


def quality_counts(spark: SparkSession, config: PipelineConfig) -> dict:
    """Separate raw, invalid, duplicate, revision, conflict, and READY counts."""
    row = spark.sql(
        f"""
        SELECT
            (SELECT COUNT(*) FROM {config.qualified("bronze_events")}) AS raw_rows,
            (SELECT COUNT(*) FROM {config.qualified("bronze_row_classification")} WHERE status = 'invalid') AS invalid_rows,
            (SELECT COUNT(*) FROM {config.qualified("bronze_row_classification")} WHERE status = 'valid') AS valid_rows,
            (
                SELECT COUNT(*) FROM (
                    SELECT event_id, event_version, payload_hash
                    FROM {config.qualified("bronze_row_classification")}
                    WHERE status = 'valid'
                    GROUP BY event_id, event_version, payload_hash
                )
            ) AS distinct_valid_payloads,
            (SELECT COUNT(*) FROM {config.qualified("silver_event_versions")}) AS ledger_versions,
            (
                SELECT COUNT(*)
                FROM {config.qualified("silver_event_versions")} versions
                JOIN {config.qualified("silver_current")} current
                  ON versions.event_id = current.event_id
                WHERE versions.event_version < current.event_version
            ) AS superseded_versions,
            (SELECT COUNT(*) FROM {config.qualified("silver_current")} WHERE state = 'CONFLICT') AS conflict_events,
            (SELECT COUNT(*) FROM {config.qualified("silver_current")} WHERE state = 'READY') AS ready_events,
            (SELECT COUNT(*) FROM {config.qualified("quarantine_events")}) AS quarantine_rows
        """
    ).collect()[0]
    valid_rows = int(row.valid_rows or 0)
    distinct_payloads = int(row.distinct_valid_payloads or 0)
    return {
        "raw_rows": int(row.raw_rows or 0),
        "invalid_rows": int(row.invalid_rows or 0),
        "valid_rows": valid_rows,
        "duplicate_delivery_rows": valid_rows - distinct_payloads,
        "ledger_versions": int(row.ledger_versions or 0),
        "superseded_versions": int(row.superseded_versions or 0),
        "conflict_events": int(row.conflict_events or 0),
        "ready_events": int(row.ready_events or 0),
        "quarantine_rows": int(row.quarantine_rows or 0),
    }


def execute(spark: SparkSession, config: PipelineConfig) -> dict:
    """Bronze, then silver and the dimension, then gold, then validation. One run at a time."""
    config.warehouse.mkdir(parents=True, exist_ok=True)
    with SingleRunLock(config.warehouse / "pipeline.lock"):
        run_bronze(spark, config)
        run_silver(spark, config)
        run_dimension(spark, config)
        run_gold(spark, config)
        run_validation(spark, config)
        summary = quality_counts(spark, config)
        summary["job_run_id"] = config.job_run_id
        summary["namespace"] = config.namespace
        summary["publication_rows"] = spark.table(config.qualified("successful_publication")).count()
        return summary
