# Databricks notebook source
# Two separate job tasks share this notebook: gold, then validate/publish.
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

import json

from telemetry.transforms import quality_counts, run_gold, run_validation

try:
    stage = dbutils.widgets.get("stage")
except Exception:
    dbutils.widgets.text("stage", "")
    stage = dbutils.widgets.get("stage")
if stage == "gold":
    run_gold(spark, config)
    print("GOLD_REBUILT", config.job_run_id)
elif stage == "validate":
    run_validation(spark, config)
    print("PIPELINE_SUMMARY " + json.dumps(quality_counts(spark, config), sort_keys=True))
else:
    raise ValueError("stage must be gold or validate; use the corresponding job task")
