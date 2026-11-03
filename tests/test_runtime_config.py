"""Runtime boundaries: URI preservation, managed tables, and stream identity."""

from __future__ import annotations

from dataclasses import asdict
import json
import pickle
from pathlib import Path

import pytest

from telemetry.config import PipelineConfig, comparable_storage_path, join_storage_path


def cloud_config(**overrides):
    values = dict(
        backend="databricks", catalog="main", namespace="telemetry_test",
        events_path="/Volumes/main/demo/landing/events",
        customers_path="/Volumes/main/demo/landing/customers.jsonl",
        bronze_checkpoint="/Volumes/main/demo/state/v1/bronze",
        silver_checkpoint="/Volumes/main/demo/state/v1/silver",
        use_auto_loader=True, job_run_id="job-123",
    )
    values.update(overrides)
    return PipelineConfig(**values)


def test_cloud_uris_survive_configuration_and_landing_resolution():
    config = cloud_config(events_path="s3://demo-bucket/events/part.jsonl",
                          customers_path="abfss://data@demo.dfs.core.windows.net/customers.jsonl",
                          bronze_checkpoint="s3://demo-bucket/state/bronze",
                          silver_checkpoint="s3://demo-bucket/state/silver")
    assert config.events_path == "s3://demo-bucket/events/part.jsonl"
    assert config.customers_path.startswith("abfss://")
    assert config.landing_dir_and_glob() == ("s3://demo-bucket/events", "part.jsonl")
    assert join_storage_path(config.bronze_checkpoint, "metadata") == "s3://demo-bucket/state/bronze/metadata"
    assert pickle.loads(pickle.dumps(config)) == config
    assert json.loads(json.dumps(asdict(config)))["events_path"] == config.events_path


def test_cloud_tables_have_names_not_user_controlled_storage_paths():
    config = cloud_config()
    assert config.qualified("silver_current") == "main.telemetry_test.silver_current"
    with pytest.raises(ValueError, match="managed tables"):
        config.table_path("silver_current")
    assert config.landing_dir_and_glob() == ("/Volumes/main/demo/landing/events", "*.jsonl")


@pytest.mark.parametrize("overrides", [
    {"catalog": "local"},
    {"job_run_id": ""},
    {"bronze_checkpoint": None},
    {"events_path": "s3:/demo-bucket/events"},
    {"events_path": "relative/events"},
    {"silver_checkpoint": "/Volumes/main/demo/state/v1/bronze/nested"},
    {"silver_checkpoint": "dbfs:/Volumes/main/demo/state/v1/bronze"},
    {"stream_generation": "bad/identity"},
])
def test_invalid_cloud_configuration_fails_before_runtime(overrides):
    with pytest.raises(ValueError):
        cloud_config(**overrides)


def test_local_configuration_keeps_path_operations_and_rejects_cloud_uris(tmp_path):
    config = PipelineConfig(events_path=tmp_path / "events", customers_path=tmp_path / "customers.jsonl",
                            warehouse=tmp_path / "warehouse")
    assert isinstance(config.events_path, Path)
    assert config.table_path("silver_current") == tmp_path / "warehouse" / config.namespace / "silver_current"
    with pytest.raises(ValueError, match="URI"):
        PipelineConfig(events_path="s3://bucket/events", customers_path=tmp_path / "customers.jsonl")


def test_stream_identity_is_stable_per_target_generation_not_job_run():
    original = cloud_config(job_run_id="attempt-1")
    retry = cloud_config(job_run_id="attempt-2")
    rebuilt = cloud_config(namespace="telemetry_rebuilt", stream_generation="v2")
    assert original.stream_identity("bronze") == retry.stream_identity("bronze")
    assert original.stream_identity("bronze") != rebuilt.stream_identity("bronze")
    assert original.stream_identity("bronze") != original.stream_identity("silver")
    assert comparable_storage_path("dbfs:/Volumes/main/demo/state/") == "/Volumes/main/demo/state"
