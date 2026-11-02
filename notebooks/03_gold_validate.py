# Databricks notebook source
# Gold full rebuild and publication validation.
#
# stage=gold rebuilds marts from the READY view.
# stage=validate reconciles grains and rates, then writes the publication
# marker. An assertion failure does not advance that marker. The marker does
# not make the three gold tables atomically visible.

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from telemetry.orchestrator import config_from_env  # noqa: E402
from telemetry.spark_session import get_spark  # noqa: E402
from telemetry.transforms import run_gold, run_validation  # noqa: E402


def main() -> None:
    config = config_from_env()
    spark = get_spark(config.warehouse, shuffle_partitions=config.shuffle_partitions)
    stage = os.environ.get("TELEMETRY_STAGE", "all")
    dbutils = globals().get("dbutils")
    if dbutils is not None:
        try:
            dbutils.widgets.text("stage", stage)
            stage = dbutils.widgets.get("stage") or stage
        except Exception:
            pass
    if stage in {"gold", "all"}:
        run_gold(spark, config)
    if stage in {"validate", "all"}:
        run_validation(spark, config)


if __name__ == "__main__":
    main()
