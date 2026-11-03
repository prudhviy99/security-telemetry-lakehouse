"""Local stand-in for a Lakeflow job definition.

This is the local orchestrator outline. The deployable Databricks job is in
resources/telemetry.job.yml, selected by databricks.yml. It is not a
recorded Databricks job, and it has no workspace run id.
"""

from __future__ import annotations

JOB_DEFINITION = {
    "name": "security_telemetry_daily",
    "format": "local-lakeflow-equivalent",
    "max_concurrent_runs": 1,
    "trigger": "availableNow",
    "databricks_job_id": None,
    "note": (
        "Exported local orchestrator. Tasks run bronze, then silver plus the "
        "customer dimension, then gold, then validation. A Databricks workspace "
        "run has not been executed."
    ),
    "parameters": {
        "backend": "local",
        "stream_generation": "v1",
        "catalog": "local",
        "namespace": "security_telemetry_demo",
        "events_path": "data/events.jsonl",
        "customers_path": "data/customers.jsonl",
        "warehouse_path": "warehouse",
        "bronze_checkpoint": "checkpoints/bronze",
        "silver_checkpoint": "checkpoints/silver",
        "use_auto_loader": False,
        "source": "spark structured streaming text files with Trigger.AvailableNow",
    },
    "tasks": [
        {
            "task_key": "bronze",
            "depends_on": [],
            "entry": "telemetry.transforms.run_bronze",
            "description": "Append new immutable JSONL lines to bronze_events.",
        },
        {
            "task_key": "silver_dimension",
            "depends_on": ["bronze"],
            "entry": "telemetry.transforms.run_silver + run_dimension",
            "description": "Validate, MERGE the version ledger and current state, publish dim_customer.",
        },
        {
            "task_key": "gold",
            "depends_on": ["silver_dimension"],
            "entry": "telemetry.transforms.run_gold",
            "parameters": {"stage": "gold"},
            "description": "Full rebuild of the three gold marts from READY silver events.",
        },
        {
            "task_key": "validate",
            "depends_on": ["gold"],
            "entry": "telemetry.transforms.run_validation",
            "parameters": {"stage": "validate"},
            "description": "Reconcile grains and rates. Advance the publication marker only on success.",
        },
    ],
}


def job_definition() -> dict:
    return json_copy(JOB_DEFINITION)


def json_copy(value):
    import json

    return json.loads(json.dumps(value))
