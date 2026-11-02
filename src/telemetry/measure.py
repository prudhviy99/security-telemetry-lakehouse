"""Time one larger deterministic run and one isolated gold rebuild change."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from telemetry.config import PipelineConfig
from telemetry.contracts import reconcile_lines


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Measure a local Spark run of a generated fixture")
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--customers", type=Path, required=True)
    parser.add_argument("--warehouse", type=Path, default=Path("warehouse-measure"))
    parser.add_argument("--namespace", default="security_telemetry_measure")
    args = parser.parse_args(argv)

    event_files = sorted(path for path in args.events.glob("*.jsonl") if path.is_file()) if args.events.is_dir() else [args.events]
    raw_lines = []
    for path in event_files:
        raw_lines.extend(path.read_text(encoding="utf-8").splitlines())
    expected = reconcile_lines(raw_lines)
    bytes_in = sum(path.stat().st_size for path in event_files)

    from telemetry.spark_session import get_spark
    from telemetry.transforms import execute, run_gold

    config = PipelineConfig(
        events_path=args.events,
        customers_path=args.customers,
        warehouse=args.warehouse,
        namespace=args.namespace,
        bronze_checkpoint=args.warehouse.parent / "checkpoints-measure" / "bronze",
        silver_checkpoint=args.warehouse.parent / "checkpoints-measure" / "silver",
        job_run_id="measure-initial",
        shuffle_partitions=1,
    )
    spark = get_spark(args.warehouse, shuffle_partitions=1)
    started = time.perf_counter()
    summary = execute(spark, config)
    cold_seconds = time.perf_counter() - started

    config.job_run_id = "measure-rerun"
    rerun_started = time.perf_counter()
    rerun = execute(spark, config)
    rerun_seconds = time.perf_counter() - rerun_started

    spark.conf.set("spark.sql.shuffle.partitions", "1")
    gold_started = time.perf_counter()
    run_gold(spark, config)
    gold_partitions_1 = time.perf_counter() - gold_started
    plan_1 = spark.sql(
        f"EXPLAIN SELECT event_date, customer_id, device_id, COUNT(*) c FROM {config.qualified('silver_events')} GROUP BY event_date, customer_id, device_id"
    ).collect()[0][0]

    spark.conf.set("spark.sql.shuffle.partitions", "8")
    gold_started = time.perf_counter()
    run_gold(spark, config)
    gold_partitions_8 = time.perf_counter() - gold_started
    plan_8 = spark.sql(
        f"EXPLAIN SELECT event_date, customer_id, device_id, COUNT(*) c FROM {config.qualified('silver_events')} GROUP BY event_date, customer_id, device_id"
    ).collect()[0][0]

    report = {
        "input_rows": len(raw_lines),
        "input_bytes": bytes_in,
        "file_count": len(event_files),
        "pure_ready_events": expected["ready_events"],
        "spark_ready_events": summary["ready_events"],
        "ready_matches_contract": summary["ready_events"] == expected["ready_events"],
        "cold_pipeline_seconds": round(cold_seconds, 3),
        "warm_rerun_seconds": round(rerun_seconds, 3),
        "warm_rerun_ready_events": rerun["ready_events"],
        "gold_rebuild_shuffle_1_seconds": round(gold_partitions_1, 3),
        "gold_rebuild_shuffle_8_seconds": round(gold_partitions_8, 3),
        "explain_shuffle_1": plan_1,
        "explain_shuffle_8": plan_8,
        "note": "Cold time includes JVM startup for this process. Shuffle settings are compared on warm gold rebuilds only. No target throughput is claimed.",
    }
    print("MEASURE_SUMMARY " + json.dumps(report))


if __name__ == "__main__":
    main()
