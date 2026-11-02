# Databricks notebook source
# Silver and the Type 1 customer dimension.
#
# Silver reads append-only bronze with its own checkpoint. foreachBatch
# validates, MERGEs the version ledger, and recomputes current state from
# every stored version of each touched event id. Duplicate customer ids
# block dimension publication.

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from telemetry.orchestrator import config_from_env  # noqa: E402
from telemetry.spark_session import get_spark  # noqa: E402
from telemetry.transforms import run_dimension, run_silver  # noqa: E402


def main() -> None:
    config = config_from_env()
    spark = get_spark(config.warehouse, shuffle_partitions=config.shuffle_partitions)
    run_silver(spark, config)
    run_dimension(spark, config)


if __name__ == "__main__":
    main()
