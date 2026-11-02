"""Local orchestrator with the same task order as the Lakeflow job export."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from telemetry.config import DEFAULT_NAMESPACE, PipelineConfig
from telemetry.job import job_definition
from telemetry.locking import SingleRunLock


def config_from_env() -> PipelineConfig:
    def param(name: str, default: str) -> str:
        dbutils = globals().get("dbutils")
        if dbutils is not None:
            try:
                dbutils.widgets.text(name, default)
                value = dbutils.widgets.get(name)
                if value:
                    return value
            except Exception:
                pass
        return os.environ.get(name, default)

    catalog = param("catalog", param("TELEMETRY_CATALOG", "local"))
    namespace = param("namespace", param("TELEMETRY_NAMESPACE", DEFAULT_NAMESPACE))
    return PipelineConfig(
        events_path=Path(param("events_path", param("TELEMETRY_EVENTS", "data/events.jsonl"))),
        customers_path=Path(param("customers_path", param("TELEMETRY_CUSTOMERS", "data/customers.jsonl"))),
        warehouse=Path(param("warehouse_path", param("TELEMETRY_WAREHOUSE", "warehouse"))),
        namespace=namespace,
        catalog=catalog,
        bronze_checkpoint=Path(param("bronze_checkpoint", param("TELEMETRY_BRONZE_CHECKPOINT", "checkpoints/bronze"))),
        silver_checkpoint=Path(param("silver_checkpoint", param("TELEMETRY_SILVER_CHECKPOINT", "checkpoints/silver"))),
        use_auto_loader=param("use_auto_loader", param("TELEMETRY_USE_AUTO_LOADER", "false")).lower() == "true",
        job_run_id=param("job_run_id", param("TELEMETRY_JOB_RUN_ID", "")),
        force_gold_assertion_failure=param(
            "force_gold_assertion_failure",
            param("TELEMETRY_FORCE_GOLD_ASSERTION_FAILURE", "false"),
        ).lower()
        == "true",
        shuffle_partitions=int(param("shuffle_partitions", param("TELEMETRY_SHUFFLE_PARTITIONS", "1"))),
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
    parser.add_argument("--shuffle-partitions", type=int, default=1)
    parser.add_argument("--use-auto-loader", action="store_true")
    parser.add_argument("--force-gold-assertion-failure", action="store_true")
    parser.add_argument("--print-job", action="store_true", help="Print the local job export and exit")
    args = parser.parse_args(argv)
    if args.print_job:
        print(json.dumps(job_definition(), indent=2))
        return

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
        force_gold_assertion_failure=args.force_gold_assertion_failure,
        shuffle_partitions=args.shuffle_partitions,
    )
    spark = get_spark(config.warehouse, shuffle_partitions=config.shuffle_partitions)
    summary = execute(spark, config)
    print("PIPELINE_SUMMARY " + json.dumps(summary, sort_keys=True))


__all__ = ["SingleRunLock", "config_from_env", "main"]


if __name__ == "__main__":
    main()
