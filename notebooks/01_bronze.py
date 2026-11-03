# Databricks notebook source
# Auto Loader -> bronze. Input and checkpoints are persistent volume paths.
from telemetry.orchestrator import config_from_env

PARAMETER_DEFAULTS = {
    "backend": "databricks",
    "catalog": "",
    "namespace": "",
    "events_path": "",
    "customers_path": "",
    "bronze_checkpoint": "",
    "silver_checkpoint": "",
    "stream_generation": "",
    "job_run_id": "",
    "use_auto_loader": "true",
    "force_gold_assertion_failure": "false",
}
parameters = {}
for name, default in PARAMETER_DEFAULTS.items():
    try:
        parameters[name] = dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)
        parameters[name] = dbutils.widgets.get(name)

config = config_from_env(parameters=parameters)
# The job environment installs telemetry without its Spark extra.
# Use Databricks' provided Spark session; never initialize local[2] here.
spark.conf.set("spark.sql.session.timeZone", "UTC")

from telemetry.transforms import run_bronze

run_bronze(spark, config)
print("BRONZE_COMPLETED", config.job_run_id)
