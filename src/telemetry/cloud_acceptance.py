"""Small, genuine Databricks acceptance runs; this module never simulates a cloud result.

Call from notebooks/04_cloud_acceptance.py with the workspace Spark session.
All writes use new, job-run-scoped demo schemas and an existing UC volume.
The caller supplies a driver-side file writer; it is never captured by foreachBatch.
"""

from __future__ import annotations

import json
import re
from dataclasses import replace
from datetime import datetime, timezone
from importlib.metadata import version
from typing import Callable

from telemetry.config import PipelineConfig, require_identifier


def _event(**changes) -> str:
    row = {
        "event_id": "movable",
        "event_version": 1,
        "customer_id": "known",
        "device_id": "device-1",
        "event_ts": "2026-06-01T12:00:00Z",
        "received_ts": "2026-06-01T12:10:00Z",
        "event_type": "scan",
        "product_version": "1.0.0",
        "scan_status": "failed",
        "duration_ms": 10,
    }
    row.update(changes)
    return json.dumps(row, sort_keys=True, separators=(",", ":"))


def _customers() -> str:
    return json.dumps({
        "customer_id": "known", "signup_date": "2026-01-01",
        "plan_tier": "premium", "country_code": "US",
        "acquisition_channel": "organic", "updated_at": "2026-01-01T00:00:00Z",
    }, sort_keys=True) + "\n"


def _rows(spark, config: PipelineConfig, table: str, columns: list[str]) -> list[str]:
    return sorted(
        json.dumps(row.asDict(recursive=True), sort_keys=True, default=str)
        for row in spark.table(config.qualified(table)).select(*columns).collect()
    )


def _snapshot(spark, config: PipelineConfig) -> dict:
    columns = {
        "silver_event_versions": ["event_id", "event_version", "payload_hash", "received_ts"],
        "silver_current": ["event_id", "event_version", "state", "payload_hash"],
        "quarantine_events": ["quality_reason", "raw_json"],
        "gold_device_day": ["event_date", "customer_id", "device_id", "accepted_event_count",
                            "scan_count", "failed_scan_count", "scan_failure_rate"],
        "gold_product_version_day": ["event_date", "product_version", "active_device_count",
                                     "accepted_event_count", "scan_count", "failed_scan_count"],
        "gold_signup_cohort_day": ["event_date", "signup_month", "plan_tier",
                                  "accepted_event_count", "active_customer_count", "active_device_count"],
    }
    return {table: _rows(spark, config, table, selected) for table, selected in columns.items()}


def _publication(spark, config: PipelineConfig) -> list[str]:
    return _rows(spark, config, "successful_publication", ["publication_id", "published_at", "ready_event_count"])


def run_cloud_acceptance(spark, parameters: dict[str, str],
                         put_text: Callable[[str, str, bool], None]) -> dict:
    """Run actual cloud checks and write a measured JSON report.

    put_text(path, text, overwrite) must reject changing an existing immutable
    input unless overwrite=True (used only for the dimension snapshot/report).
    Failures are re-raised after a FAILED report is written.
    """
    from telemetry.transforms import (
        SINK_ORDER, DimensionPublicationBlocked, GoldAssertionError, UnsafeCheckpointReset,
        quality_counts, run_bronze, run_dimension, run_gold, run_silver, run_validation,
    )

    catalog = require_identifier(parameters["catalog"], "catalog")
    prefix = require_identifier(parameters.get("namespace_prefix", "stl_acceptance"), "namespace_prefix")
    job_run_id = parameters["job_run_id"].strip()
    if not re.fullmatch(r"[0-9]+", job_run_id):
        raise ValueError("job_run_id must be the real numeric Lakeflow job run ID")
    root = parameters["volume_root"].rstrip("/")
    if not root.startswith("/Volumes/") or len(root.split("/")) < 5 or ".." in root.split("/"):
        raise ValueError("volume_root must be an existing absolute /Volumes/catalog/schema/volume path")
    root = root + "/" + job_run_id
    include_failures = parameters.get("include_failure_injection", "false").lower() == "true"
    include_extended = parameters.get("include_extended_checks", "false").lower() == "true"
    report = {
        "status": "running",
        "scope": "cloud_acceptance" if include_failures and include_extended else "cloud_smoke",
        "include_failure_injection": include_failures,
        "include_extended_checks": include_extended,
        "job_run_id": job_run_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "spark_version": spark.version,
        "package_version": version("security-telemetry-lakehouse"),
        "checks": [],
        "schemas": [],
        "not_run": ["second_principal_permissions", "analysis_sql_tiny_fixture", "cloud_volume_benchmark"],
    }
    if not include_failures:
        report["not_run"].append("failure_after_each_silver_sink")
    if not include_extended:
        report["not_run"].extend([
            "conflict_all_arrival_orders", "lower_after_higher", "storage_type_boundaries",
            "additive_and_wrong_type_schema", "isolated_rebuild",
        ])
    report_path = root + "/result.json"
    execution = parameters.get("task_execution_count", "1")
    if not re.fullmatch(r"[1-9][0-9]*", execution):
        raise ValueError("task_execution_count must be a positive integer")
    report["task_execution_count"] = int(execution)

    def save_report() -> None:
        text = json.dumps(report, indent=2, sort_keys=True, default=str) + "\n"
        # Preserve attempt history even if an operator explicitly repairs a run.
        put_text(root + "/attempt-" + execution + "/result.json", text, True)
        put_text(report_path, text, True)

    def check(name: str, observed) -> None:
        result = {"name": name, "status": "passed", "observed": observed}
        report["checks"].append(result)
        save_report()
        print("CLOUD_CHECK " + json.dumps(result, sort_keys=True, default=str))

    def config_for(case: str, phase: str = "initial") -> PipelineConfig:
        namespace = require_identifier(f"{prefix}_{job_run_id}_{case}", "namespace")
        if namespace not in report["schemas"]:
            report["schemas"].append(namespace)
        return PipelineConfig(
            backend="databricks", catalog=catalog, namespace=namespace,
            events_path=f"{root}/{case}/events",
            customers_path=f"{root}/{case}/customers.jsonl",
            bronze_checkpoint=f"{root}/{case}/checkpoints/v1/bronze",
            silver_checkpoint=f"{root}/{case}/checkpoints/v1/silver",
            stream_generation="v1", use_auto_loader=True,
            # Phase is an acceptance observation within the actual job above,
            # not an invented Databricks job/run ID.
            job_run_id=f"{job_run_id}/{case}/{phase}",
        )

    def inputs(config: PipelineConfig, events: list[str], filename: str = "initial.jsonl") -> None:
        put_text(str(config.events_path) + "/" + filename, "\n".join(events) + "\n", False)
        put_text(str(config.customers_path), _customers(), False)

    def run(config: PipelineConfig) -> dict:
        run_bronze(spark, config)
        run_silver(spark, config)
        run_dimension(spark, config)
        run_gold(spark, config)
        run_validation(spark, config)
        return quality_counts(spark, config)

    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        reference = config_for("reference")
        # These are the exact committed 105-row fixtures uploaded during setup,
        # read through Spark because workspace paths are not local Python files.
        reference_events = [row.value for row in spark.read.text(parameters["reference_events_path"]).collect()]
        reference_customers = [row.value for row in spark.read.text(parameters["reference_customers_path"]).collect()]
        assert len(reference_events) == 105 and len(reference_customers) == 12
        put_text(str(reference.events_path) + "/events.jsonl", "\n".join(reference_events) + "\n", False)
        put_text(str(reference.customers_path), "\n".join(reference_customers) + "\n", False)
        reference_counts = run(reference)
        expected_counts = {
            "raw_rows": 105, "valid_rows": 102, "invalid_rows": 3,
            "duplicate_delivery_rows": 1, "ledger_versions": 101,
            "superseded_versions": 1, "conflict_events": 0, "ready_events": 100,
            "quarantine_rows": 3,
        }
        assert reference_counts == expected_counts, reference_counts
        gold_sums = {
            table: int(spark.sql(
                f"SELECT SUM(accepted_event_count) AS n FROM {reference.qualified(table)}"
            ).first().n)
            for table in ("gold_device_day", "gold_product_version_day", "gold_signup_cohort_day")
        }
        assert set(gold_sums.values()) == {100}, gold_sums
        check("reference_fixture_counts_and_gold", {"counts": reference_counts, "gold_sums": gold_sums})

        config = config_for("sequence")
        if spark.catalog.tableExists(config.qualified("bronze_events")):
            raise RuntimeError("Acceptance namespace already exists; start a new job run instead of overwriting evidence")
        heartbeat = _event(event_id="unknown", customer_id="missing", event_type="heartbeat",
                           scan_status=None, duration_ms=None)
        inputs(config, [_event(), heartbeat, _event(event_id="invalid", event_type="reboot")])
        counts = run(config)
        assert counts["raw_rows"] == 3 and counts["ready_events"] == 2 and counts["invalid_rows"] == 1, counts
        check("auto_loader_baseline_and_quarantine", counts)
        unknown = spark.sql(
            f"SELECT SUM(accepted_event_count) AS n FROM {config.qualified('gold_signup_cohort_day')} "
            "WHERE signup_month = 'UNKNOWN' AND plan_tier = 'UNKNOWN'"
        ).first().n
        assert unknown == 1, unknown
        check("unknown_customer_is_retained", unknown)

        before = _snapshot(spark, config)
        replay = replace(config, job_run_id=f"{job_run_id}/sequence/replay")
        replay_counts = run(replay)
        assert before == _snapshot(spark, replay)
        assert replay_counts["raw_rows"] == 3, replay_counts
        check("same_checkpoint_replay", {"raw_rows": 3, "business_state_unchanged": True})

        earlier_receipt = _event(received_ts="2026-06-01T12:05:00Z")
        put_text(str(config.events_path) + "/receipt-replay.jsonl", earlier_receipt + "\n", False)
        receipt = replace(config, job_run_id=f"{job_run_id}/sequence/receipt")
        counts = run(receipt)
        assert counts["ledger_versions"] == 2 and counts["duplicate_delivery_rows"] == 1, counts
        earliest = spark.sql(
            f"SELECT CAST(received_ts AS STRING) AS received FROM {config.qualified('silver_event_versions')} "
            "WHERE event_id = 'movable'"
        ).first().received
        assert earliest == "2026-06-01 12:05:00", earliest
        check("receipt_time_excluded_from_identity", {"counts": counts, "earliest_receipt_utc": earliest})

        revised = _event(event_version=2, event_ts="2026-06-02T00:10:00Z",
                         received_ts="2026-06-04T00:00:00Z", scan_status="success", duration_ms=20)
        put_text(str(config.events_path) + "/late-correction.jsonl", revised + "\n", False)
        correction = replace(config, job_run_id=f"{job_run_id}/sequence/correction")
        run(correction)
        dates = spark.sql(
            f"SELECT event_date FROM {config.qualified('silver_events')} WHERE event_id = 'movable'"
        ).collect()
        assert len(dates) == 1 and str(dates[0].event_date) == "2026-06-02", dates
        old_scans = spark.sql(
            f"SELECT COALESCE(SUM(scan_count), 0) AS n FROM {config.qualified('gold_device_day')} "
            "WHERE event_date = DATE '2026-06-01'"
        ).first().n
        assert old_scans == 0, old_scans
        check("late_revision_moves_historical_day", {"new_day": "2026-06-02", "old_day_scans": 0})

        contradiction = _event(event_version=2, event_ts="2026-06-02T00:10:00Z",
                               received_ts="2026-06-04T00:00:00Z", scan_status="failed", duration_ms=21)
        put_text(str(config.events_path) + "/contradiction.jsonl", contradiction + "\n", False)
        conflicted = replace(config, job_run_id=f"{job_run_id}/sequence/conflict")
        counts = run(conflicted)
        assert counts["conflict_events"] == 1 and counts["ready_events"] == 1, counts
        check("separate_batch_conflict_is_excluded", counts)

        resolved = _event(event_version=3, event_ts="2026-06-02T00:10:00Z",
                          received_ts="2026-06-04T00:00:00Z", scan_status="success", duration_ms=19)
        put_text(str(config.events_path) + "/resolution.jsonl", resolved + "\n", False)
        fixed = replace(config, job_run_id=f"{job_run_id}/sequence/resolved")
        counts = run(fixed)
        assert counts["conflict_events"] == 0 and counts["ready_events"] == 2, counts
        check("higher_version_resolves_conflict", counts)

        published = _publication(spark, fixed)
        put_text(str(config.customers_path), _customers() + _customers(), True)
        try:
            run_dimension(spark, fixed)
        except DimensionPublicationBlocked:
            pass
        else:
            raise AssertionError("Duplicate customer key did not block dimension publication")
        assert _publication(spark, fixed) == published
        put_text(str(config.customers_path), _customers(), True)
        run_dimension(spark, fixed)
        check("duplicate_dimension_blocks_publication", {"publication_unchanged": True})

        failing_gold = replace(fixed, job_run_id=f"{job_run_id}/sequence/rejected",
                               force_gold_assertion_failure=True)
        run_gold(spark, failing_gold)
        try:
            run_validation(spark, failing_gold)
        except GoldAssertionError:
            pass
        else:
            raise AssertionError("Forced gold validation failure did not fail")
        assert _publication(spark, failing_gold) == published
        check("failed_gold_does_not_publish", {"publication_unchanged": True})

        if include_extended:
            lower = config_for("lower_after_higher")
            inputs(lower, [_event(event_version=2, product_version="2.0.0")])
            run(lower)
            put_text(str(lower.events_path) + "/lower.jsonl", _event(product_version="1.0.0") + "\n", False)
            run(replace(lower, job_run_id=f"{job_run_id}/lower/late"))
            current = spark.table(lower.qualified("silver_current")).select("event_version", "product_version").first()
            assert current.event_version == 2 and current.product_version == "2.0.0", current
            check("lower_after_higher", {"current_version": 2})

            left = _event(scan_status="success", duration_ms=10)
            right = _event(scan_status="failed", duration_ms=15, received_ts="2026-06-01T12:20:00Z")
            stable = _event(event_id="stable")
            orders = {
                "together": [[left, right, stable]],
                "left_then_right": [[left, stable], [right]],
                "right_then_left": [[right, stable], [left]],
            }
            snapshots = []
            for order, batches in orders.items():
                ordered = config_for(order)
                inputs(ordered, batches[0])
                run(ordered)
                for index, batch in enumerate(batches[1:], 1):
                    put_text(str(ordered.events_path) + f"/batch-{index}.jsonl", "\n".join(batch) + "\n", False)
                    run(replace(ordered, job_run_id=f"{job_run_id}/{order}/{index}"))
                counts = quality_counts(spark, ordered)
                assert counts["conflict_events"] == 1 and counts["ready_events"] == 1, counts
                snapshots.append(_snapshot(spark, ordered))
            assert snapshots[0] == snapshots[1] == snapshots[2]
            check("conflict_all_arrival_orders", {"orders": list(orders), "business_state_equal": True})

            boundaries = config_for("boundaries")
            invalid = {
                "duration_overflow": (_event(event_id="duration_overflow").replace('"duration_ms":10', '"duration_ms":1e309'), "invalid_duration"),
                "version_overflow": (_event(event_id="version_overflow", event_version=2**63), "invalid_event_version"),
                "event_underflow": (_event(event_id="event_underflow", event_ts="0001-01-01T00:00:00+01:00"), "invalid_timestamp:event_ts"),
                "receipt_underflow": (_event(event_id="receipt_underflow", received_ts="0001-01-01T00:00:00+01:00"), "invalid_timestamp:received_ts"),
                "event_offset": (_event(event_id="event_offset", event_ts="2026-06-01T12:00:00+00:99"), "invalid_timestamp:event_ts"),
                "receipt_offset": (_event(event_id="receipt_offset", received_ts="2026-06-01T12:10:00+00:99"), "invalid_timestamp:received_ts"),
            }
            inputs(boundaries, [_event(event_id="control"), _event(event_id="max_version", event_version=2**63 - 1)]
                   + [line for line, _reason in invalid.values()])
            counts = run(boundaries)
            assert counts["raw_rows"] == 8 and counts["invalid_rows"] == 6 and counts["ready_events"] == 2, counts
            actual = {
                json.loads(row.raw_json)["event_id"]: row.quality_reason
                for row in spark.table(boundaries.qualified("quarantine_events")).select("raw_json", "quality_reason").collect()
            }
            assert actual == {key: reason for key, (_line, reason) in invalid.items()}, actual
            check("storage_type_boundaries", {"counts": counts, "quarantine_reasons": actual})

            schema = config_for("schema_changes")
            inputs(schema, [_event(event_id="optional", agent_build="endpoint-build-7"),
                            _event(event_id="wrong_type", event_version="2")])
            counts = run(schema)
            assert counts["ready_events"] == 1 and counts["invalid_rows"] == 1, counts
            from telemetry.contracts import classify_raw_line
            expected_hash = classify_raw_line(_event(event_id="optional")).event.payload_hash
            stored = spark.table(schema.qualified("silver_events")).select("payload_hash").first().payload_hash
            assert stored == expected_hash
            assert any("endpoint-build-7" in row.raw_json for row in spark.table(schema.qualified("bronze_events")).select("raw_json").collect())
            reason = spark.table(schema.qualified("quarantine_events")).select("quality_reason").first().quality_reason
            assert reason == "incorrect_type:event_version", reason
            check("additive_and_wrong_type_schema", {"additive_field_preserved_raw": True, "hash_unchanged": True, "wrong_type_reason": reason})

            try:
                run_bronze(spark, replace(reference, bronze_checkpoint=f"{root}/unsafe-reset/bronze"))
            except UnsafeCheckpointReset:
                pass
            else:
                raise AssertionError("Changing checkpoints against populated targets was not blocked")
            rebuilt = replace(
                config_for("reference_rebuild"), stream_generation="v2",
                bronze_checkpoint=f"{root}/reference_rebuild/checkpoints/v2/bronze",
                silver_checkpoint=f"{root}/reference_rebuild/checkpoints/v2/silver",
            )
            put_text(str(rebuilt.events_path) + "/events.jsonl", "\n".join(reference_events) + "\n", False)
            put_text(str(rebuilt.customers_path), "\n".join(reference_customers) + "\n", False)
            rebuilt_counts = run(rebuilt)
            assert rebuilt_counts == reference_counts
            assert _snapshot(spark, rebuilt) == _snapshot(spark, reference)
            check("isolated_rebuild", {"generation": "v2", "counts_match": True, "business_state_equal": True, "unsafe_reset_blocked": True})

        if include_failures:
            recovery_events = [
                _event(event_id="good"), _event(event_id="bad", event_type="reboot"),
                _event(event_id="clash", scan_status="success", duration_ms=1),
                _event(event_id="clash", scan_status="failed", duration_ms=2),
            ]
            clean = config_for("clean")
            inputs(clean, recovery_events)
            run(clean)
            expected = _snapshot(spark, clean)
            for sink in SINK_ORDER:
                dirty = config_for("fail_" + sink)
                inputs(dirty, recovery_events)
                run_bronze(spark, dirty)
                injected = replace(dirty, fail_after_sinks=[sink])
                try:
                    run_silver(spark, injected)
                except Exception as exc:
                    if f"injected failure after sink {sink}" not in str(exc):
                        raise
                else:
                    raise AssertionError(f"Failure injection did not fire at {sink}")
                recovered = replace(dirty, fail_after_sinks=[], job_run_id=f"{job_run_id}/{sink}/recovered")
                run(recovered)
                assert _snapshot(spark, recovered) == expected, sink
                check("recovery_after_" + sink, {"matches_uninterrupted": True})

        report["status"] = "passed"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = {"type": type(exc).__name__, "message": str(exc)}
        raise
    finally:
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        save_report()
        print("CLOUD_ACCEPTANCE_REPORT " + json.dumps(report, sort_keys=True, default=str))
        print("CLOUD_ACCEPTANCE_REPORT_PATH " + report_path)
    return report
