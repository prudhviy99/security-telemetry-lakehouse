"""Spark and Delta integration tests for MERGE, checkpoints, recovery, and publication."""

from __future__ import annotations

import json
import shutil
from datetime import date
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from telemetry.config import PipelineConfig
from telemetry.contracts import classify_raw_line, reconcile_lines, validate_customer_snapshot
from telemetry.marts import gold_from_states
from telemetry.transforms import (
    SINK_ORDER,
    DimensionPublicationBlocked,
    GoldAssertionError,
    InjectedSinkFailure,
    execute,
    quality_counts,
    run_dimension,
    run_gold,
)
from tests.sql_expectations import load_queries, values_close
from tests.tiny_fixture import AS_OF, customer_lines, event_lines

pytestmark = pytest.mark.spark
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from telemetry.spark_session import get_spark

    session = get_spark(tmp_path_factory.mktemp("metastore"))
    yield session


def _line(**overrides) -> str:
    payload = {
        "event_id": "event-1",
        "event_version": 1,
        "customer_id": "cust-001",
        "device_id": "device-01",
        "event_ts": "2026-06-01T12:00:00Z",
        "received_ts": "2026-06-01T12:10:00Z",
        "event_type": "scan",
        "product_version": "1.0.0",
        "scan_status": "success",
        "duration_ms": 10,
    }
    payload.update(overrides)
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _customers(*ids: str) -> str:
    rows = []
    for customer_id in ids:
        rows.append(
            json.dumps(
                {
                    "acquisition_channel": "organic",
                    "country_code": "US",
                    "customer_id": customer_id,
                    "plan_tier": "premium",
                    "signup_date": "2026-01-15",
                    "updated_at": "2026-01-15T00:00:00Z",
                },
                sort_keys=True,
            )
        )
    return "\n".join(rows) + "\n"


def _write_inputs(directory: Path, events: list[str], customers: str, file_name: str = "events.jsonl") -> tuple[Path, Path]:
    landing = directory / "landing"
    landing.mkdir(parents=True, exist_ok=True)
    events_path = landing / file_name
    events_path.write_text("\n".join(events) + "\n", encoding="utf-8")
    customers_path = directory / "customers.jsonl"
    customers_path.write_text(customers, encoding="utf-8")
    return events_path, customers_path


def _config(directory: Path, events_path: Path, customers_path: Path, run_id: str, **overrides) -> PipelineConfig:
    return PipelineConfig(
        events_path=events_path,
        customers_path=customers_path,
        warehouse=directory / "warehouse",
        namespace="stl_" + run_id.replace("-", "_"),
        bronze_checkpoint=directory / "checkpoints" / "bronze",
        silver_checkpoint=directory / "checkpoints" / "silver",
        job_run_id=run_id,
        **overrides,
    )


def _run(spark, config: PipelineConfig) -> dict:
    spark.conf.set("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
    return execute(spark, config)


def _rows(spark, config: PipelineConfig, table: str, columns: list[str]) -> list[tuple]:
    frame = spark.table(config.qualified(table)).select(*columns).collect()

    def norm(value):
        if hasattr(value, "isoformat") and not isinstance(value, str):
            text = value.isoformat()
            return text.replace("+00:00", "")
        return value

    return sorted(tuple(norm(row[column]) for column in columns) for row in frame)


def _resume(spark, config: PipelineConfig) -> dict:
    last = None
    for _ in range(len(SINK_ORDER) + 2):
        try:
            return _run(spark, config)
        except InjectedSinkFailure as exc:
            last = exc
    raise AssertionError(f"pipeline did not recover after injected failures; last={last}")


def test_matrix_01_reprocess_and_replay_keep_sinks_stable(spark, tmp_path: Path):
    events_path, customers_path = _write_inputs(
        tmp_path,
        [_line(event_id="event-1"), _line(event_id="event-2", event_type="heartbeat", scan_status=None, duration_ms=None)],
        _customers("cust-001"),
    )
    config = _config(tmp_path, events_path, customers_path, "replay1")
    _run(spark, config)
    before = {
        "bronze": _rows(spark, config, "bronze_events", ["raw_json"]),
        "ledger": _rows(spark, config, "silver_event_versions", ["event_id", "event_version", "payload_hash"]),
        "current": _rows(spark, config, "silver_current", ["event_id", "state", "payload_hash"]),
        "quarantine": _rows(spark, config, "quarantine_events", ["raw_payload_hash", "quality_reason"]),
        "gold": _rows(spark, config, "gold_device_day", ["event_date", "customer_id", "device_id", "accepted_event_count"]),
    }
    config.job_run_id = "replay2"
    _run(spark, config)
    after = {
        "bronze": _rows(spark, config, "bronze_events", ["raw_json"]),
        "ledger": _rows(spark, config, "silver_event_versions", ["event_id", "event_version", "payload_hash"]),
        "current": _rows(spark, config, "silver_current", ["event_id", "state", "payload_hash"]),
        "quarantine": _rows(spark, config, "quarantine_events", ["raw_payload_hash", "quality_reason"]),
        "gold": _rows(spark, config, "gold_device_day", ["event_date", "customer_id", "device_id", "accepted_event_count"]),
    }
    assert before == after
    landing = events_path.parent.resolve()
    assert landing not in config.bronze_checkpoint.resolve().parents
    assert landing not in config.silver_checkpoint.resolve().parents


def test_matrix_02_later_lower_version_does_not_overwrite(spark, tmp_path: Path):
    events_path, customers_path = _write_inputs(
        tmp_path,
        [_line(event_version=2, product_version="2.0.0")],
        _customers("cust-001"),
        "batch-a.jsonl",
    )
    config = _config(tmp_path, events_path.parent, customers_path, "order-v2")
    _run(spark, config)
    (events_path.parent / "batch-b.jsonl").write_text(_line(event_version=1, product_version="1.0.0") + "\n", encoding="utf-8")
    config.job_run_id = "order-v1"
    _run(spark, config)
    current = _rows(spark, config, "silver_current", ["event_id", "event_version", "state", "product_version"])
    assert current == [("event-1", 2, "READY", "2.0.0")]


def test_matrix_03_same_payload_different_receipt_is_one_version(spark, tmp_path: Path):
    events_path, customers_path = _write_inputs(
        tmp_path,
        [
            _line(received_ts="2026-06-01T18:00:00Z"),
            _line(received_ts="2026-06-01T12:10:00Z"),
        ],
        _customers("cust-001"),
    )
    config = _config(tmp_path, events_path, customers_path, "dup-receipt")
    summary = _run(spark, config)
    assert summary["ledger_versions"] == 1
    assert summary["duplicate_delivery_rows"] == 1
    assert summary["ready_events"] == 1
    received = _rows(spark, config, "silver_event_versions", ["received_ts"])
    assert received == [("2026-06-01T12:10:00",)]


def test_matrix_04_conflict_order_is_independent(spark, tmp_path: Path):
    left = _line(scan_status="success", duration_ms=10, received_ts="2026-06-01T12:10:00Z")
    right = _line(scan_status="failed", duration_ms=15, received_ts="2026-06-01T12:20:00Z")
    stable = _line(event_id="stable", event_ts="2026-06-02T00:00:00Z", received_ts="2026-06-02T01:00:00Z")
    snapshots = []
    plans = {
        "together": [[left, right, stable]],
        "left-then-right": [[left, stable], [right]],
        "right-then-left": [[right, stable], [left]],
    }
    for name, batches in plans.items():
        directory = tmp_path / name
        events_path, customers_path = _write_inputs(directory, batches[0], _customers("cust-001"), "part-0.jsonl")
        config = _config(directory, events_path.parent, customers_path, name.replace("-", "_") + "a")
        _run(spark, config)
        for index, batch in enumerate(batches[1:], start=1):
            (events_path.parent / f"part-{index}.jsonl").write_text("\n".join(batch) + "\n", encoding="utf-8")
            config.job_run_id = f"{name}-{index}".replace("-", "_")
            _run(spark, config)
        snapshots.append(
            (
                _rows(spark, config, "silver_current", ["event_id", "state", "product_version", "scan_status"]),
                _rows(spark, config, "gold_device_day", ["event_date", "customer_id", "device_id", "accepted_event_count", "failed_scan_count"]),
            )
        )
    assert snapshots[0] == snapshots[1] == snapshots[2]
    states = snapshots[0][0]
    assert ("event-1", "CONFLICT", None, None) in states
    assert ("stable", "READY", "1.0.0", "success") in states


def test_matrix_05_higher_version_resolves_conflict(spark, tmp_path: Path):
    events_path, customers_path = _write_inputs(
        tmp_path,
        [
            _line(scan_status="success", duration_ms=10),
            _line(scan_status="failed", duration_ms=15, received_ts="2026-06-01T12:30:00Z"),
        ],
        _customers("cust-001"),
        "conflict.jsonl",
    )
    config = _config(tmp_path, events_path.parent, customers_path, "conflict")
    _run(spark, config)
    assert _rows(spark, config, "silver_current", ["state"]) == [("CONFLICT",)]
    (events_path.parent / "fix.jsonl").write_text(
        _line(event_version=4, product_version="3.0.0", scan_status="success", duration_ms=4) + "\n",
        encoding="utf-8",
    )
    config.job_run_id = "resolved"
    _run(spark, config)
    assert _rows(spark, config, "silver_current", ["event_version", "state", "product_version"]) == [(4, "READY", "3.0.0")]
    assert spark.table(config.qualified("silver_events")).count() == 1


def test_matrix_06_failure_after_each_sink_recovers(spark, tmp_path: Path):
    clean_dir = tmp_path / "clean"
    dirty_dir = tmp_path / "dirty"
    events = [
        _line(event_id="good"),
        _line(event_id="bad", event_type="reboot"),
        _line(event_id="clash", scan_status="success", duration_ms=1),
        _line(event_id="clash", scan_status="failed", duration_ms=2, received_ts="2026-06-01T12:40:00Z"),
    ]
    clean_events, clean_customers = _write_inputs(clean_dir, events, _customers("cust-001"))
    dirty_events, dirty_customers = _write_inputs(dirty_dir, events, _customers("cust-001"))
    clean = _config(clean_dir, clean_events, clean_customers, "clean")
    dirty = _config(
        dirty_dir,
        dirty_events,
        dirty_customers,
        "dirty",
        fail_after_sinks=list(SINK_ORDER),
    )
    _run(spark, clean)
    _resume(spark, dirty)
    columns = {
        "silver_event_versions": ["event_id", "event_version", "payload_hash", "scan_status", "duration_ms"],
        "silver_current": ["event_id", "state", "event_version", "payload_hash"],
        "quarantine_events": ["quality_reason", "raw_json"],
        "gold_device_day": ["event_date", "customer_id", "device_id", "accepted_event_count", "scan_count", "failed_scan_count"],
    }
    for table, selected in columns.items():
        assert _rows(spark, clean, table, selected) == _rows(spark, dirty, table, selected)


def test_matrix_07_late_arrival_and_date_correction(spark, tmp_path: Path):
    events_path, customers_path = _write_inputs(
        tmp_path,
        [_line(event_id="movable", event_ts="2026-05-02T01:00:00Z", received_ts="2026-05-02T02:00:00Z")],
        _customers("cust-001"),
        "initial.jsonl",
    )
    config = _config(tmp_path, events_path.parent, customers_path, "late0")
    _run(spark, config)
    assert _rows(spark, config, "gold_device_day", ["event_date", "accepted_event_count"]) == [("2026-05-02", 1)]
    (events_path.parent / "late.jsonl").write_text(
        _line(event_id="older", event_ts="2026-05-01T23:00:00Z", received_ts="2026-05-05T00:00:00Z") + "\n",
        encoding="utf-8",
    )
    config.job_run_id = "late1"
    _run(spark, config)
    assert ("2026-05-01", 1) in _rows(spark, config, "gold_device_day", ["event_date", "accepted_event_count"])
    (events_path.parent / "correct.jsonl").write_text(
        _line(
            event_id="movable",
            event_version=2,
            event_ts="2026-05-03T00:30:00Z",
            received_ts="2026-05-03T01:00:00Z",
            product_version="1.1.0",
        )
        + "\n",
        encoding="utf-8",
    )
    config.job_run_id = "late2"
    _run(spark, config)
    dates = dict(_rows(spark, config, "gold_device_day", ["event_date", "accepted_event_count"]))
    assert "2026-05-02" not in dates
    assert dates["2026-05-03"] == 1
    assert dates["2026-05-01"] == 1


def test_matrix_08_duplicate_customer_blocks_and_unknown_stays(spark, tmp_path: Path):
    blocked_dir = tmp_path / "blocked"
    events_path, customers_path = _write_inputs(blocked_dir, [_line()], _customers("cust-001"))
    customers_path.write_text(_customers("cust-001") + _customers("cust-001"), encoding="utf-8")
    config = _config(blocked_dir, events_path, customers_path, "blocked")
    with pytest.raises(DimensionPublicationBlocked):
        _run(spark, config)
    assert spark.read.format("delta").load(str(config.table_path("successful_publication"))).count() == 0
    assert not (config.table_path("gold_device_day") / "_delta_log").exists()

    unknown_dir = tmp_path / "unknown"
    events_path, customers_path = _write_inputs(unknown_dir, [_line(customer_id="missing")], _customers("cust-001"))
    config = _config(unknown_dir, events_path, customers_path, "unknown")
    _run(spark, config)
    cohort = _rows(spark, config, "gold_signup_cohort_day", ["signup_month", "plan_tier", "accepted_event_count"])
    assert cohort == [("UNKNOWN", "UNKNOWN", 1)]
    assert _rows(spark, config, "silver_events", ["customer_id"]) == [("missing",)]

    customers_path.write_text(
        json.dumps(
            {
                "acquisition_channel": "paid",
                "country_code": "US",
                "customer_id": "missing",
                "plan_tier": "family",
                "signup_date": "2026-04-01",
                "updated_at": "2026-04-02T00:00:00Z",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    run_dimension(spark, config)
    run_gold(spark, config)
    assert _rows(spark, config, "gold_signup_cohort_day", ["signup_month", "plan_tier"]) == [("2026-04", "family")]


def test_matrix_09_and_10_reference_reconciliation(spark, tmp_path: Path):
    data = ROOT / "data"
    config = _config(tmp_path, data / "events.jsonl", data / "customers.jsonl", "reference")
    summary = _run(spark, config)
    assert summary["raw_rows"] == 105
    assert summary["ready_events"] == 100
    assert summary["invalid_rows"] == 3
    assert summary["valid_rows"] + summary["invalid_rows"] == summary["raw_rows"]
    assert summary["duplicate_delivery_rows"] == 1
    assert summary["superseded_versions"] == 1
    assert summary["conflict_events"] == 0
    assert summary["valid_rows"] != summary["ready_events"]
    pure = reconcile_lines((data / "events.jsonl").read_text(encoding="utf-8").splitlines())
    customers, errors = validate_customer_snapshot((data / "customers.jsonl").read_text(encoding="utf-8").splitlines())
    assert errors == []
    expected = gold_from_states(pure["states"], customers)
    for table, key in (
        ("gold_device_day", ("event_date", "customer_id", "device_id")),
        ("gold_product_version_day", ("event_date", "product_version")),
        ("gold_signup_cohort_day", ("event_date", "signup_month", "plan_tier")),
    ):
        actual = spark.table(config.qualified(table)).collect()
        assert len(actual) == len(expected[table.removeprefix("gold_")])
        by_key = {tuple(str(row[column]) for column in key): row for row in actual}
        pure_name = {
            "gold_device_day": "device_day",
            "gold_product_version_day": "product_version_day",
            "gold_signup_cohort_day": "signup_cohort_day",
        }[table]
        for row in expected[pure_name]:
            match = by_key[tuple(str(row[column]) for column in key)]
            assert int(match.accepted_event_count) == row["accepted_event_count"]
            assert int(match.scan_count) == row["scan_count"]
            assert int(match.failed_scan_count) == row["failed_scan_count"]
            if row["scan_failure_rate"] is None:
                assert match.scan_failure_rate is None
                assert match.avg_scan_duration_ms is None
            else:
                assert match.scan_failure_rate == pytest.approx(row["scan_failure_rate"])
                assert match.avg_scan_duration_ms == pytest.approx(row["avg_scan_duration_ms"])
    boundary = spark.sql(
        f"""
        SELECT CAST(event_ts AS DATE) AS event_date
        FROM {config.qualified("silver_events")}
        WHERE event_id = 'event-0010'
        """
    ).collect()[0].event_date
    assert str(boundary) == "2026-06-01"
    config.job_run_id = "reference-rerun"
    rerun = _run(spark, config)
    assert rerun["raw_rows"] == 105
    assert rerun["ready_events"] == 100
    assert quality_counts(spark, config)["ledger_versions"] == 101


def test_matrix_11_optional_attribute_and_incompatible_field(spark, tmp_path: Path):
    good = json.loads(_line(event_id="optional"))
    good["agent_build"] = "endpoint-build-7"
    bad = json.loads(_line(event_id="broken"))
    bad["event_version"] = "2"
    events_path, customers_path = _write_inputs(
        tmp_path,
        [json.dumps(good, sort_keys=True), json.dumps(bad, sort_keys=True)],
        _customers("cust-001"),
    )
    config = _config(tmp_path, events_path, customers_path, "schema")
    _run(spark, config)
    raw_values = [row.raw_json for row in spark.table(config.qualified("bronze_events")).select("raw_json").collect()]
    assert any("endpoint-build-7" in raw for raw in raw_values)
    optional = classify_raw_line(json.dumps(good, sort_keys=True))
    without = dict(good)
    del without["agent_build"]
    assert optional.event.payload_hash == classify_raw_line(json.dumps(without, sort_keys=True)).event.payload_hash
    stored_hash = spark.sql(
        f"SELECT payload_hash FROM {config.qualified('silver_events')} WHERE event_id = 'optional'"
    ).collect()[0].payload_hash
    assert stored_hash == optional.event.payload_hash
    reasons = _rows(spark, config, "quarantine_events", ["quality_reason", "detail"])
    assert len(reasons) == 1
    assert reasons[0][0] == "incorrect_type:event_version"
    assert "event_version" in reasons[0][1]
    assert spark.table(config.qualified("silver_events")).count() == 1


def test_matrix_12_failed_gold_assertion_does_not_advance_marker(spark, tmp_path: Path):
    events_path, customers_path = _write_inputs(tmp_path, [_line()], _customers("cust-001"))
    config = _config(tmp_path, events_path, customers_path, "publish-ok")
    _run(spark, config)
    marker = _rows(spark, config, "successful_publication", ["publication_id", "reconciliation"])
    assert marker == [("publish-ok", "passed")]
    config.job_run_id = "publish-fail"
    config.force_gold_assertion_failure = True
    with pytest.raises(GoldAssertionError):
        _run(spark, config)
    assert _rows(spark, config, "successful_publication", ["publication_id"]) == [("publish-ok",)]


def test_analysis_sql_matches_tiny_expected_tables(spark, tmp_path: Path):
    events_path, customers_path = _write_inputs(tmp_path, event_lines(), "\n".join(customer_lines()) + "\n")
    config = _config(tmp_path, events_path, customers_path, "tiny-sql")
    _run(spark, config)
    spark.catalog.setCurrentDatabase(config.namespace)
    spark.sql(f"CREATE OR REPLACE TEMP VIEW analysis_params AS SELECT DATE '{AS_OF.isoformat()}' AS as_of_date")
    for query in load_queries():
        rows = spark.sql(query["sql"]).collect()
        assert len(rows) == len(query["expected"]), query["name"]
        for actual, expected in zip(rows, query["expected"]):
            for column, expected_value in zip(query["columns"], expected):
                assert values_close(actual[column], expected_value), (query["name"], column, actual[column], expected_value)


def test_checkpoint_inside_input_is_rejected(spark, tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()
    events = landing / "events.jsonl"
    events.write_text(_line() + "\n", encoding="utf-8")
    customers = tmp_path / "customers.jsonl"
    customers.write_text(_customers("cust-001"), encoding="utf-8")
    config = _config(tmp_path, events, customers, "badckpt")
    config.bronze_checkpoint = landing / "checkpoints"
    with pytest.raises(RuntimeError, match="outside the input"):
        _run(spark, config)


def test_storage_boundary_inputs_are_quarantined_before_delta_writes(spark, tmp_path: Path):
    """One invalid source row must not poison the batch or analytical doubles."""
    invalid = {
        "duration-overflow": (
            _line(event_id="duration-overflow").replace('"duration_ms":10', '"duration_ms":1e309'),
            "invalid_duration",
        ),
        "version-overflow": (
            _line(event_id="version-overflow", event_version=2**63),
            "invalid_event_version",
        ),
        "event-utc-underflow": (
            _line(event_id="event-utc-underflow", event_ts="0001-01-01T00:00:00+01:00"),
            "invalid_timestamp:event_ts",
        ),
        "receipt-utc-underflow": (
            _line(event_id="receipt-utc-underflow", received_ts="0001-01-01T00:00:00+01:00"),
            "invalid_timestamp:received_ts",
        ),
        "event-bad-offset": (
            _line(event_id="event-bad-offset", event_ts="2026-06-01T12:00:00+00:99"),
            "invalid_timestamp:event_ts",
        ),
        "receipt-bad-offset": (
            _line(event_id="receipt-bad-offset", received_ts="2026-06-01T12:10:00+00:99"),
            "invalid_timestamp:received_ts",
        ),
    }
    events_path, customers_path = _write_inputs(
        tmp_path,
        [
            _line(event_id="valid-control", duration_ms=10),
            _line(event_id="valid-max-version", event_version=2**63 - 1, duration_ms=0),
            *[line for line, _reason in invalid.values()],
        ],
        _customers("cust-001"),
    )
    config = _config(tmp_path, events_path, customers_path, "storage-boundaries")
    summary = _run(spark, config)
    assert summary["raw_rows"] == 8
    assert summary["valid_rows"] == 2
    assert summary["invalid_rows"] == 6
    assert summary["ready_events"] == 2
    assert summary["ledger_versions"] == 2
    actual_invalid = {
        json.loads(row.raw_json)["event_id"]: row.quality_reason
        for row in spark.table(config.qualified("quarantine_events")).select("raw_json", "quality_reason").collect()
    }
    assert actual_invalid == {event_id: reason for event_id, (_line_text, reason) in invalid.items()}
    assert _rows(spark, config, "silver_events", ["event_id", "event_version"]) == [
        ("valid-control", 1),
        ("valid-max-version", 2**63 - 1),
    ]
    for table in ("gold_device_day", "gold_product_version_day", "gold_signup_cohort_day"):
        assert _rows(spark, config, table, ["accepted_event_count", "scan_count", "avg_scan_duration_ms"]) == [
            (2, 2, 5.0)
        ]


def test_checkpoint_loss_is_rejected_and_isolated_rebuild_preserves_original(spark, tmp_path: Path):
    """Recovery retains progress; a rebuild gets a new target and stream generation."""
    from telemetry.transforms import UnsafeCheckpointReset

    events_path = ROOT / "data" / "events.jsonl"
    customers_path = ROOT / "data" / "customers.jsonl"
    original = _config(
        tmp_path / "original", events_path, customers_path, "rebuild-original", stream_generation="initial"
    )
    original_summary = _run(spark, original)
    columns = {
        "bronze_events": ["raw_json"],
        "silver_event_versions": ["event_id", "event_version", "payload_hash", "duration_ms"],
        "silver_current": ["event_id", "state", "event_version", "payload_hash"],
        "quarantine_events": ["quality_reason", "raw_json"],
        "gold_device_day": ["event_date", "customer_id", "device_id", "accepted_event_count", "failed_scan_count"],
        "gold_product_version_day": ["event_date", "product_version", "accepted_event_count", "active_device_count"],
        "gold_signup_cohort_day": ["event_date", "signup_month", "plan_tier", "accepted_event_count", "active_device_count"],
    }
    original_rows = {table: _rows(spark, original, table, selected) for table, selected in columns.items()}
    # Delete only a test-owned temporary checkpoint. Retained target data must
    # trigger a clear rejection before a new batch can be silently skipped.
    shutil.rmtree(original.bronze_checkpoint)
    original.job_run_id = "unsafe-reset-attempt"
    with pytest.raises(UnsafeCheckpointReset):
        _run(spark, original)

    rebuilt = _config(
        tmp_path / "rebuilt", events_path, customers_path, "rebuild-fresh", stream_generation="rebuild"
    )
    rebuilt_summary = _run(spark, rebuilt)
    expected_counts = {"raw_rows": 105, "valid_rows": 102, "invalid_rows": 3, "ledger_versions": 101, "ready_events": 100}
    for key, expected in expected_counts.items():
        assert original_summary[key] == rebuilt_summary[key] == expected
    assert original.namespace != rebuilt.namespace
    assert original.stream_generation != rebuilt.stream_generation
    assert original.bronze_checkpoint != rebuilt.bronze_checkpoint
    assert original.silver_checkpoint != rebuilt.silver_checkpoint
    for table, selected in columns.items():
        assert _rows(spark, rebuilt, table, selected) == original_rows[table]
        assert _rows(spark, original, table, selected) == original_rows[table]
