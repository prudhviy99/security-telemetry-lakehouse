"""One local Spark session with Delta Lake."""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession

_SESSION: SparkSession | None = None


def get_spark(warehouse: Path, app_name: str = "security-telemetry-lakehouse", shuffle_partitions: int = 1) -> SparkSession:
    """Return the process-wide session.

    Spark allows one context per process. The warehouse directory is fixed by
    the first caller; table paths are still chosen per pipeline run.
    """
    global _SESSION
    if _SESSION is not None:
        _SESSION.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
        return _SESSION

    warehouse = Path(warehouse)
    warehouse.mkdir(parents=True, exist_ok=True)
    derby = warehouse / "derby"
    local = warehouse / "spark-local"
    ivy = warehouse / "ivy"
    for path in (derby, local, ivy):
        path.mkdir(parents=True, exist_ok=True)

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse / "metastore"))
        .config("spark.local.dir", str(local))
        .config("spark.jars.ivy", str(ivy))
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={derby}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.adaptive.enabled", "false")
    )
    # Keep the driver quiet unless a test is debugging a failure.
    _SESSION = configure_spark_with_delta_pip(builder).getOrCreate()
    _SESSION.sparkContext.setLogLevel(os.environ.get("TELEMETRY_SPARK_LOG_LEVEL", "ERROR"))
    _SESSION.conf.set("spark.sql.session.timeZone", "UTC")
    return _SESSION
