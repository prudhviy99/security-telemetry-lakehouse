"""Local orchestrator with the same task order as the Lakeflow job export."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping
from pathlib import Path

from telemetry.config import DEFAULT_NAMESPACE, PipelineConfig
from telemetry.job import job_definition
from telemetry.locking import SingleRunLock


def config_from_env(parameters: Mapping[str, str] | None = None) -> PipelineConfig:
    """Read explicit notebook widgets or environment variables.

    A notebook passes its widget dictionary here. Imported modules do not share
    notebook globals such as dbutils. Explicit values take precedence over the
    environment, and cloud values are kept as strings until the config adapter.
    """
    parameters = {} if parameters is None else parameters

    def param(name: str, env: str, default: str = "") -> str:
        return str(parameters.get(name, os.environ.get(env, default)))

    def flag(name: str, env: str, default: str = "false") -> bool:
        value = param(name, env, default).strip().lower()
        if value not in {"true", "false"}:
            raise ValueError(f"{name} must be true or false")
        return value == "true"

    backend = param("backend", "TELEMETRY_BACKEND", "local")
    cloud = backend == "databricks"
    bronze_checkpoint = param("bronze_checkpoint", "TELEMETRY_BRONZE_CHECKPOINT", "" if cloud else "checkpoints/bronze")
    silver_checkpoint = param("silver_checkpoint", "TELEMETRY_SILVER_CHECKPOINT", "" if cloud else "checkpoints/silver")
    return PipelineConfig(
        events_path=param("events_path", "TELEMETRY_EVENTS", "" if cloud else "data/events.jsonl"),
        customers_path=param("customers_path", "TELEMETRY_CUSTOMERS", "" if cloud else "data/customers.jsonl"),
        warehouse=param("warehouse_path", "TELEMETRY_WAREHOUSE", "warehouse"),
        namespace=param("namespace", "TELEMETRY_NAMESPACE", "" if cloud else DEFAULT_NAMESPACE),
        catalog=param("catalog", "TELEMETRY_CATALOG", "" if cloud else "local"),
        backend=backend,
        bronze_checkpoint=bronze_checkpoint or None,
        silver_checkpoint=silver_checkpoint or None,
        use_auto_loader=flag("use_auto_loader", "TELEMETRY_USE_AUTO_LOADER", "true" if cloud else "false"),
        stream_generation=param("stream_generation", "TELEMETRY_STREAM_GENERATION", "v1"),
        bronze_stream_id=param("bronze_stream_id", "TELEMETRY_BRONZE_STREAM_ID", "bronze_events"),
        silver_stream_id=param("silver_stream_id", "TELEMETRY_SILVER_STREAM_ID", "silver_events"),
        job_run_id=param("job_run_id", "TELEMETRY_JOB_RUN_ID"),
        force_gold_assertion_failure=flag("force_gold_assertion_failure", "TELEMETRY_FORCE_GOLD_ASSERTION_FAILURE"),
        fail_after_sinks=[item.strip() for item in param("fail_after_sinks", "TELEMETRY_FAIL_AFTER_SINKS").split(",") if item.strip()],
        shuffle_partitions=int(param("shuffle_partitions", "TELEMETRY_SHUFFLE_PARTITIONS", "1")),
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the local security telemetry lakehouse")
    parser.add_argument("--events", type=Path, default=Path("data/events.jsonl"))
    parser.add_argument("--customers", type=Path, default=Path("data/customers.jsonl"))
    parser.add_argument("--warehouse", type=Path, default=Path("warehouse"))
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    parser.add_argument("--catalog", default="local")
    parser.add_argument("--bronze-checkpoint", type=Path, default=Path("checkpoints/bronze"))
    parser.add_argument("--silver-checkpoint", type=Path, default=Path("checkpoints/silver"))
    parser.add_argument("--job-run-id", default="")
    parser.add_argument("--stream-generation", default="v1", help="Stable for retries; change only for an isolated rebuild")
    parser.add_argument("--bronze-stream-id", default="bronze_events")
    parser.add_argument("--silver-stream-id", default="silver_events")
    parser.add_argument("--shuffle-partitions", type=int, default=1)
    parser.add_argument("--use-auto-loader", action="store_true")
    parser.add_argument("--force-gold-assertion-failure", action="store_true")
    parser.add_argument("--print-job", action="store_true", help="Print the local job export and exit")
    args = parser.parse_args(argv)
    if args.print_job:
        print(json.dumps(job_definition(), indent=2))
        return
    if args.use_auto_loader or args.catalog not in {"local", "spark_catalog"}:
        parser.error("This CLI starts local Spark. Use the Databricks bundle for Auto Loader and Unity Catalog.")

    # Spark imports stay inside the command so contract tests do not need PySpark.
    from telemetry.spark_session import get_spark
    from telemetry.transforms import execute

    config = PipelineConfig(
        events_path=args.events,
        customers_path=args.customers,
        warehouse=args.warehouse,
        namespace=args.namespace,
        catalog=args.catalog,
        bronze_checkpoint=args.bronze_checkpoint,
        silver_checkpoint=args.silver_checkpoint,
        use_auto_loader=args.use_auto_loader,
        job_run_id=args.job_run_id,
        stream_generation=args.stream_generation,
        bronze_stream_id=args.bronze_stream_id,
        silver_stream_id=args.silver_stream_id,
        force_gold_assertion_failure=args.force_gold_assertion_failure,
        shuffle_partitions=args.shuffle_partitions,
    )
    spark = get_spark(config.warehouse, shuffle_partitions=config.shuffle_partitions)
    summary = execute(spark, config)
    print("PIPELINE_SUMMARY " + json.dumps(summary, sort_keys=True))


__all__ = ["SingleRunLock", "config_from_env", "main"]


if __name__ == "__main__":
    main()
