"""One local Spark session with Delta Lake."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession

_SESSION: SparkSession | None = None


def get_spark(warehouse: Path, app_name: str = "security-telemetry-lakehouse", shuffle_partitions: int = 1) -> SparkSession:
    """Return the process-wide session.

    Spark allows one context per process. The warehouse directory is fixed by
    the first caller; table paths are still chosen per pipeline run.
    """
    global _SESSION
    # PySpark's local TimestampType conversion uses the Python process timezone
    # when collecting timestamps. SQL session UTC alone is insufficient on a
    # developer machine whose OS timezone differs from UTC.
    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):
        time.tzset()
    if _SESSION is not None:
        _SESSION.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
        return _SESSION

    # Ivy rejects relative cache roots. The documented CLI uses a relative
    # warehouse, so resolve it before constructing JVM paths.
    warehouse = Path(warehouse).resolve()
    # Use the same interpreter for Python workers as for the driver.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
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
        .config("spark.executorEnv.TZ", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.adaptive.enabled", "false")
    )
    # Optional predownloaded official Delta JARs for environments where the
    # Python/HTTP proxy works but JVM dependency downloads are unavailable.
    # Default installs still resolve the pinned Delta package through Ivy.
    jars = os.environ.get("TELEMETRY_DELTA_JARS", "")
    if jars:
        paths = [Path(value).resolve() for value in jars.split(os.pathsep)]
        if not all(path.is_file() and path.suffix == ".jar" for path in paths):
            raise ValueError("TELEMETRY_DELTA_JARS must contain existing JAR paths separated by the OS path separator")
        # This factory is local[2] only: both driver and executor can read the
        # same files, without opening a file-transfer server for cached JARs.
        classpath = os.pathsep.join(str(path) for path in paths)
        builder = builder.config("spark.driver.extraClassPath", classpath)
        builder = builder.config("spark.executor.extraClassPath", classpath)
    else:
        builder = configure_spark_with_delta_pip(builder)
    # Keep the driver quiet unless a test is debugging a failure.
    _SESSION = builder.getOrCreate()
    _SESSION.sparkContext.setLogLevel(os.environ.get("TELEMETRY_SPARK_LOG_LEVEL", "ERROR"))
    _SESSION.conf.set("spark.sql.session.timeZone", "UTC")
    return _SESSION
