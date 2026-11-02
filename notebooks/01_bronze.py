# Databricks notebook source
# Bronze: append immutable JSONL lines with Trigger.AvailableNow.
#
# Local default: Spark structured streaming file source.
# Databricks later: set use_auto_loader / TELEMETRY_USE_AUTO_LOADER=true.
# Recorded runs in this repository use the local file source. A workspace
# Auto Loader run has not been executed.

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from telemetry.orchestrator import config_from_env  # noqa: E402
from telemetry.spark_session import get_spark  # noqa: E402
from telemetry.transforms import run_bronze  # noqa: E402


def main() -> None:
    config = config_from_env()
    spark = get_spark(config.warehouse, shuffle_partitions=config.shuffle_partitions)
    run_bronze(spark, config)


if __name__ == "__main__":
    main()
