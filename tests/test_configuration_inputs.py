"""Notebook and local parameter handoff without notebook-global assumptions."""
from pathlib import Path

import pytest

from telemetry.orchestrator import config_from_env, main


def test_explicit_notebook_parameters_override_environment(monkeypatch):
    monkeypatch.setenv("TELEMETRY_CATALOG", "wrong_catalog")
    config = config_from_env({
        "backend": "databricks", "catalog": "workspace", "namespace": "telemetry",
        "events_path": "/Volumes/workspace/telemetry/io/landing/events",
        "customers_path": "/Volumes/workspace/telemetry/io/landing/customers.jsonl",
        "bronze_checkpoint": "/Volumes/workspace/telemetry/io/state/v1/bronze",
        "silver_checkpoint": "/Volumes/workspace/telemetry/io/state/v1/silver",
        "job_run_id": "123456", "stream_generation": "v1",
    })
    assert config.catalog == "workspace"
    assert config.use_auto_loader is True
    assert config.backend == "databricks"
    assert config.job_run_id == "123456"
    assert config.events_path.startswith("/Volumes/")
    assert not isinstance(config.events_path, Path)


def test_environment_inputs_preserve_local_paths_and_stable_stream_identity(monkeypatch):
    monkeypatch.setenv("TELEMETRY_EVENTS", "some/events.jsonl")
    monkeypatch.setenv("TELEMETRY_STREAM_GENERATION", "demo_v2")
    first = config_from_env({"job_run_id": "attempt-1"})
    second = config_from_env({"job_run_id": "attempt-2"})
    assert first.events_path == Path("some/events.jsonl")
    assert first.stream_generation == "demo_v2"
    assert first.stream_identity("bronze") == second.stream_identity("bronze")


def test_bad_boolean_and_missing_cloud_configuration_fail_early():
    with pytest.raises(ValueError, match="true or false"):
        config_from_env({"use_auto_loader": "tru"})
    with pytest.raises(ValueError):
        config_from_env({"backend": "databricks"})


def test_local_cli_does_not_initialize_cloud_runtime():
    with pytest.raises(SystemExit) as exc:
        main(["--use-auto-loader"])
    assert exc.value.code == 2
