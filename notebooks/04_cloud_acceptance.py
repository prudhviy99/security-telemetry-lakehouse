# Databricks notebook source
# On-demand cloud checks in new job-run-scoped demo schemas.
from telemetry.cloud_acceptance import run_cloud_acceptance

defaults = {
    "catalog": "",
    "namespace_prefix": "stl_acceptance",
    "volume_root": "",
    "job_run_id": "",
    "include_failure_injection": "false",
    "include_extended_checks": "false",
    "reference_events_path": "",
    "reference_customers_path": "",
}
parameters = {}
for name, default in defaults.items():
    try:
        parameters[name] = dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)
        parameters[name] = dbutils.widgets.get(name)


def put_text(path, text, overwrite):
    # This helper runs in the notebook driver, never in a streaming callback.
    dbutils.fs.mkdirs(path.rsplit("/", 1)[0])
    if overwrite:
        dbutils.fs.put(path, text, overwrite=True)
        return
    try:
        existing = dbutils.fs.head(path, max(65536, len(text.encode("utf-8")) + 1))
    except Exception:
        dbutils.fs.put(path, text, overwrite=False)
    else:
        if existing != text:
            raise RuntimeError("Refusing to overwrite immutable acceptance input: " + path)


run_cloud_acceptance(spark, parameters, put_text)
