"""Dependency-light contract tests. No Spark session."""

from __future__ import annotations

import json
import math
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from telemetry.contracts import (
    InMemoryLake,
    MAX_EVENT_VERSION,
    canonical_hash_document,
    canonical_payload_hash,
    canonical_timestamp,
    classify_raw_line,
    parse_timestamp,
    reconcile_lines,
    scan_failure_rate,
    validate_customer_snapshot,
)
from telemetry.generate import write_scenario
from telemetry.locking import SingleRunLock
from telemetry.marts import (
    assert_rate_rules,
    build_device_day,
    distinct_active_devices,
    gold_from_states,
    summed_version_active_devices,
)

ROOT = Path(__file__).resolve().parents[1]
UTC = timezone.utc


def _event(**overrides) -> dict:
    payload = {
        "event_id": "event-1",
        "event_version": 1,
        "customer_id": "cust-001",
        "device_id": "device-01",
        "event_ts": "2026-06-01T12:00:00Z",
        "received_ts": "2026-06-01T12:00:00Z",
        "event_type": "scan",
        "product_version": "1.0.0",
        "scan_status": "success",
        "duration_ms": 10,
    }
    payload.update(overrides)
    return payload


def _line(**overrides) -> str:
    return json.dumps(_event(**overrides), sort_keys=True, separators=(",", ":"))


def _customer(**overrides) -> str:
    payload = {
        "customer_id": "cust-001",
        "signup_date": "2026-01-15",
        "plan_tier": "premium",
        "country_code": "US",
        "acquisition_channel": "organic",
        "updated_at": "2026-01-15T00:00:00Z",
    }
    payload.update(overrides)
    return json.dumps(payload)


def test_malformed_json_and_wrong_shape():
    row = classify_raw_line("{")
    assert row.status == "invalid"
    assert row.quality_reason == "invalid_json"
    row = classify_raw_line("[1, 2]")
    assert row.quality_reason == "not_an_object"


def test_missing_blank_and_wrong_types():
    missing = dict(_event())
    del missing["device_id"]
    assert classify_raw_line(json.dumps(missing)).quality_reason == "missing_required_field:device_id"
    assert classify_raw_line(_line(event_id="  ")).quality_reason == "blank_required_field:event_id"
    assert classify_raw_line(_line(event_version="2")).quality_reason == "incorrect_type:event_version"
    assert classify_raw_line(_line(event_version=1.5)).quality_reason == "incorrect_type:event_version"
    assert classify_raw_line(_line(event_version=True)).quality_reason == "incorrect_type:event_version"
    assert classify_raw_line(_line(event_version=0)).quality_reason == "invalid_event_version"
    assert classify_raw_line(_line(event_version=-3)).quality_reason == "invalid_event_version"


def test_timestamps_require_explicit_zone_and_do_not_use_locale():
    assert classify_raw_line(_line(event_ts="2026-06-01T12:00:00")).quality_reason == "missing_timezone:event_ts"
    assert classify_raw_line(_line(received_ts="2026-06-01 12:00:00")).quality_reason == "missing_timezone:received_ts"
    zulu = parse_timestamp("2026-06-01T12:00:00Z")
    offset = parse_timestamp("2026-06-01T07:00:00-05:00")
    assert zulu == offset
    assert zulu.tzinfo == UTC
    ahead_clock = parse_timestamp("2026-06-02T03:30:00+05:00")
    assert ahead_clock == datetime(2026, 6, 1, 22, 30, tzinfo=UTC)


def test_event_version_must_fit_the_delta_long_type():
    maximum = classify_raw_line(_line(event_version=MAX_EVENT_VERSION))
    assert maximum.status == "valid"
    assert maximum.event.event_version == 9223372036854775807
    too_large = classify_raw_line(_line(event_version=MAX_EVENT_VERSION + 1))
    assert too_large.status == "invalid"
    assert too_large.event is None
    assert too_large.quality_reason == "invalid_event_version"


def test_invalid_offsets_and_utc_overflow_become_validation_failures():
    malformed = (
        "2026-06-01T12:00:00+00:99",
        "2026-06-01T12:00:00-0060",
        "2026-06-01T12:00:00+24:00",
        "0001-01-01T00:00:00+01:00",
        "9999-12-31T23:59:59-01:00",
    )
    for timestamp in malformed:
        with pytest.raises(ValueError, match="invalid_timestamp"):
            parse_timestamp(timestamp)
        for field in ("event_ts", "received_ts"):
            row = classify_raw_line(_line(**{field: timestamp}))
            assert row.status == "invalid"
            assert row.quality_reason == f"invalid_timestamp:{field}"
        customers, errors = validate_customer_snapshot([_customer(updated_at=timestamp)])
        assert customers is None
        assert errors == ["line 1: invalid_timestamp:updated_at"]
    # Minutes at the boundary are still accepted and converted to UTC.
    assert parse_timestamp("2026-06-01T12:00:00+00:59") == datetime(2026, 6, 1, 11, 1, tzinfo=UTC)


def test_future_skew_boundary_and_old_events():
    base = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
    exactly = classify_raw_line(
        _line(
            event_ts=canonical_timestamp(base),
            received_ts=canonical_timestamp(base - timedelta(minutes=5)),
        )
    )
    assert exactly.status == "valid"
    too_far = classify_raw_line(
        _line(
            event_ts=canonical_timestamp(base),
            received_ts=canonical_timestamp(base - timedelta(minutes=5, seconds=1)),
        )
    )
    assert too_far.quality_reason == "event_ts_ahead_of_received_ts"
    old = classify_raw_line(
        _line(
            event_ts="2020-01-01T00:00:00Z",
            received_ts="2026-06-01T00:00:00Z",
        )
    )
    assert old.status == "valid"


def test_event_type_and_scan_fields():
    assert classify_raw_line(_line(event_type="reboot")).quality_reason == "unsupported_event_type"
    assert classify_raw_line(_line(scan_status="Failed")).quality_reason == "invalid_scan_status"
    assert classify_raw_line(_line(duration_ms=-1)).quality_reason == "invalid_duration"
    assert classify_raw_line(_line(duration_ms="10")).quality_reason == "invalid_duration"
    heartbeat = classify_raw_line(_line(event_type="heartbeat", scan_status=None, duration_ms=None))
    assert heartbeat.status == "valid"
    leaked = classify_raw_line(_line(event_type="heartbeat", scan_status="success", duration_ms=None))
    assert leaked.quality_reason == "invalid_scan_status"
    threat = classify_raw_line(_line(event_type="threat_detected", scan_status=None, duration_ms=4))
    assert threat.quality_reason == "invalid_duration"


def test_duration_must_remain_finite_in_the_delta_double_type():
    # Use literal JSON numbers so parsing exercises Decimal, not Python's
    # float overflow before serialization. Also cover JSON's nonfinite tokens.
    for literal in ("1e309", "1.7976931348623159e308", "9" * 309, "NaN", "Infinity", "-Infinity"):
        raw = _line().replace('"duration_ms":10', f'"duration_ms":{literal}')
        row = classify_raw_line(raw)
        assert row.status == "invalid", literal
        assert row.event is None
        assert row.quality_reason == "invalid_duration"
    maximum = classify_raw_line(_line(duration_ms=sys.float_info.max))
    assert maximum.status == "valid"
    assert math.isfinite(float(maximum.event.duration_ms))
    assert float(maximum.event.duration_ms) == sys.float_info.max


def test_canonical_hash_excludes_receipt_and_optional_attribute():
    first = classify_raw_line(_line(received_ts="2026-06-01T12:00:00Z", duration_ms=10))
    second = classify_raw_line(_line(received_ts="2026-06-02T01:00:00Z", duration_ms=10.0))
    third = classify_raw_line(_line(received_ts="2026-06-02T01:00:00Z", duration_ms=10.00))
    assert first.event is not None and second.event is not None and third.event is not None
    assert first.event.payload_hash == second.event.payload_hash == third.event.payload_hash
    with_build = dict(_event(duration_ms=10))
    with_build["agent_build"] = "build-9"
    hashed = classify_raw_line(json.dumps(with_build, sort_keys=True))
    assert hashed.status == "valid"
    assert hashed.event.payload_hash == first.event.payload_hash
    assert "build-9" in hashed.raw_json
    assert "received_ts" not in canonical_hash_document(
        {
            "event_id": "event-1",
            "event_version": 1,
            "customer_id": "cust-001",
            "device_id": "device-01",
            "event_ts": first.event.event_ts,
            "event_type": "scan",
            "product_version": "1.0.0",
            "scan_status": "success",
            "duration_ms": 10,
        }
    )
    shifted = classify_raw_line(_line(event_ts="2026-06-01T17:00:00+05:00"))
    same_instant = classify_raw_line(_line(event_ts="2026-06-01T12:00:00Z"))
    assert shifted.event.payload_hash == same_instant.event.payload_hash


def test_duration_canonical_text_and_fractional_difference():
    ten = classify_raw_line(_line(duration_ms=10))
    ten_point_five = classify_raw_line(_line(duration_ms=10.5))
    assert ten.event.payload_hash != ten_point_five.event.payload_hash
    document = canonical_hash_document(
        {
            "customer_id": "cust-001",
            "device_id": "device-01",
            "duration_ms": 10.50,
            "event_id": "event-1",
            "event_ts": parse_timestamp("2026-06-01T12:00:00Z"),
            "event_type": "scan",
            "event_version": 1,
            "product_version": "1.0.0",
            "scan_status": "success",
        }
    )
    assert '"duration_ms":"10.5"' in document


def test_optional_attribute_and_incompatible_schema_change():
    extra = dict(_event())
    extra["agent_build"] = "2026.6"
    row = classify_raw_line(json.dumps(extra))
    assert row.status == "valid"
    bad_optional = dict(_event())
    bad_optional["agent_build"] = 12
    assert classify_raw_line(json.dumps(bad_optional)).quality_reason == "incorrect_type:agent_build"
    renamed = dict(_event())
    renamed["schema_version"] = 2
    assert classify_raw_line(json.dumps(renamed)).quality_reason == "unsupported_schema_version"
    string_version = dict(_event())
    string_version["event_version"] = "2"
    failed = classify_raw_line(json.dumps(string_version))
    assert failed.quality_reason == "incorrect_type:event_version"
    assert "event_version" in (failed.detail or "")


def test_duplicate_customer_blocks_and_invalid_customer_blocks():
    customers, errors = validate_customer_snapshot([_customer(), _customer(plan_tier="free")])
    assert customers is None
    assert any(error.startswith("duplicate_customer_id:") for error in errors)
    blocked, reasons = validate_customer_snapshot([_customer(country_code="usa")])
    assert blocked is None
    assert reasons[0].endswith("invalid_country_code")
    published, ok = validate_customer_snapshot([_customer(), _customer(customer_id="cust-002")])
    assert ok == []
    assert published is not None and len(published) == 2
    assert published[0].signup_month == "2026-01"


def test_same_version_receipts_and_revision_order():
    lake = InMemoryLake()
    lake.apply_batch([_line(received_ts="2026-06-01T15:00:00Z")], "1")
    lake.apply_batch([_line(received_ts="2026-06-01T13:00:00Z")], "2")
    assert len(lake.versions) == 1
    only = next(iter(lake.versions.values()))
    assert only.received_ts == parse_timestamp("2026-06-01T13:00:00Z")
    assert len(lake.audit) == 2

    ordered = InMemoryLake()
    ordered.apply_batch([_line(event_version=2, product_version="9.0.0")], "1")
    ordered.apply_batch([_line(event_version=1, product_version="1.0.0")], "2")
    assert ordered.current["event-1"]["state"] == "READY"
    assert ordered.current["event-1"]["event_version"] == 2
    assert ordered.current["event-1"]["product_version"] == "9.0.0"


def _conflict_pair(order: str) -> list[list[str]]:
    left = _line(scan_status="success", duration_ms=10, received_ts="2026-06-01T12:00:00Z")
    right = _line(scan_status="failed", duration_ms=12, received_ts="2026-06-01T12:05:00Z")
    if order == "together":
        return [[left, right]]
    if order == "left-then-right":
        return [[left], [right]]
    if order == "right-then-left":
        return [[right], [left]]
    raise AssertionError(order)


@pytest.mark.parametrize("order", ["together", "left-then-right", "right-then-left"])
def test_same_version_conflict_is_order_independent(order: str):
    lake = InMemoryLake()
    for index, batch in enumerate(_conflict_pair(order), start=1):
        lake.apply_batch(batch, str(index))
    state = lake.current["event-1"]
    assert state["state"] == "CONFLICT"
    assert state["payload_hash"] is None
    assert state["product_version"] is None
    assert lake.ready() == []
    reasons = sorted(row["quality_reason"] for row in lake.quarantine.values())
    assert reasons == ["conflicting_version_payload", "conflicting_version_payload"]


def test_higher_version_resolves_conflict():
    lake = InMemoryLake()
    lake.apply_batch(_conflict_pair("together")[0], "1")
    lake.apply_batch([_line(event_version=3, product_version="2.0.0", scan_status="success", duration_ms=8)], "2")
    state = lake.current["event-1"]
    assert state["state"] == "READY"
    assert state["event_version"] == 3
    assert state["product_version"] == "2.0.0"
    assert len(lake.ready()) == 1


def test_rate_rules_and_version_device_counts_are_not_additive():
    assert scan_failure_rate(0, 0) is None
    assert scan_failure_rate(1, 4) == 0.25
    left = classify_raw_line(
        _line(event_id="a", customer_id="c", device_id="d", product_version="1.0.0", event_type="heartbeat", scan_status=None, duration_ms=None)
    ).event
    right = classify_raw_line(
        _line(event_id="b", customer_id="c", device_id="d", product_version="1.1.0", event_type="scan", scan_status="failed", duration_ms=5)
    ).event
    from telemetry.contracts import current_states

    states = current_states([left, right])
    gold = gold_from_states(states, [])
    day = date(2026, 6, 1)
    assert distinct_active_devices(gold["device_day"] and [], day) == 0
    ready = [
        {
            "customer_id": state["customer_id"],
            "device_id": state["device_id"],
            "event_ts": state["event_ts"],
            "event_type": state["event_type"],
            "product_version": state["product_version"],
            "scan_status": state["scan_status"],
            "duration_ms": state["duration_ms"],
        }
        for state in states
    ]
    assert distinct_active_devices(ready, day) == 1
    version_rows = __import__("telemetry.marts", fromlist=["build_product_version_day"]).build_product_version_day(ready)
    assert summed_version_active_devices(version_rows, day) == 2
    assert assert_rate_rules(build_device_day(ready)) == []
    heartbeat_day = [row for row in build_device_day(ready) if row["scan_count"] == 0]
    assert heartbeat_day == [] or all(row["scan_failure_rate"] is None for row in build_device_day([ready[0]]))


def test_late_event_and_date_correction_move_gold_days():
    from telemetry.contracts import current_states

    original = classify_raw_line(_line(event_id="late", event_ts="2026-05-02T01:00:00Z", received_ts="2026-05-02T02:00:00Z")).event
    states = current_states([original])
    customers, _ = validate_customer_snapshot([_customer()])
    before = gold_from_states(states, customers)
    assert {row["event_date"] for row in before["device_day"]} == {date(2026, 5, 2)}
    late = classify_raw_line(
        _line(event_id="older", event_ts="2026-05-01T23:30:00Z", received_ts="2026-05-04T00:00:00Z")
    ).event
    with_late = gold_from_states(current_states([original, late]), customers)
    assert date(2026, 5, 1) in {row["event_date"] for row in with_late["device_day"]}
    corrected = classify_raw_line(
        _line(
            event_id="late",
            event_version=2,
            event_ts="2026-05-03T00:30:00Z",
            received_ts="2026-05-03T01:00:00Z",
            product_version="1.1.0",
        )
    ).event
    moved = gold_from_states(current_states([original, late, corrected]), customers)
    dates = {row["event_date"] for row in moved["device_day"]}
    assert date(2026, 5, 2) not in dates
    assert date(2026, 5, 3) in dates
    assert date(2026, 5, 1) in dates


def test_unknown_customer_keeps_event_and_uses_unknown_segments():
    event = classify_raw_line(_line(customer_id="missing")).event
    from telemetry.contracts import current_states

    customers, errors = validate_customer_snapshot([_customer()])
    assert errors == []
    gold = gold_from_states(current_states([event]), customers)
    assert gold["device_day"][0]["customer_id"] == "missing"
    assert gold["signup_cohort_day"][0]["signup_month"] == "UNKNOWN"
    assert gold["signup_cohort_day"][0]["plan_tier"] == "UNKNOWN"
    assert gold["signup_cohort_day"][0]["accepted_event_count"] == 1


def test_reference_fixture_counts_and_byte_stability(tmp_path: Path):
    first = write_scenario("reference", tmp_path / "a")
    second = write_scenario("reference", tmp_path / "b")
    events_a = (tmp_path / "a" / "events.jsonl").read_bytes()
    events_b = (tmp_path / "b" / "events.jsonl").read_bytes()
    assert events_a == events_b
    assert first["event_rows"] == 105
    lines = (tmp_path / "a" / "events.jsonl").read_text(encoding="utf-8").splitlines()
    summary = reconcile_lines(lines)
    assert summary["raw_rows"] == 105
    assert summary["ready_events"] == 100
    assert summary["invalid_rows"] == 3
    assert summary["valid_rows"] + summary["invalid_rows"] == summary["raw_rows"]
    assert summary["duplicate_delivery_rows"] == 1
    assert summary["superseded_versions"] == 1
    assert summary["conflict_events"] == 0
    assert summary["valid_rows"] != summary["ready_events"]
    assert summary["invalid_reasons"] == [
        "event_ts_ahead_of_received_ts",
        "missing_required_field:event_id",
        "unsupported_event_type",
    ]
    committed = (ROOT / "data" / "events.jsonl").read_bytes()
    assert committed == events_a
    assert (ROOT / "data" / "customers.jsonl").read_bytes() == (tmp_path / "a" / "customers.jsonl").read_bytes()


def test_reference_boundary_event_uses_utc_date():
    lines = (ROOT / "data" / "events.jsonl").read_text(encoding="utf-8").splitlines()
    boundary = classify_raw_line(next(line for line in lines if '"event-0010"' in line))
    assert boundary.status == "valid"
    assert boundary.event.event_ts == datetime(2026, 6, 1, 22, 30, tzinfo=UTC)
    assert boundary.event.event_ts.astimezone(UTC).date() == date(2026, 6, 1)


def test_reprocess_in_memory_is_idempotent():
    lines = (ROOT / "data" / "events.jsonl").read_text(encoding="utf-8").splitlines()
    lake = InMemoryLake()
    lake.apply_batch(lines, "7")
    ready_before = sorted((row["event_id"], row["payload_hash"]) for row in lake.ready())
    quarantine_before = set(lake.quarantine)
    lake.apply_batch(lines, "7")
    ready_after = sorted((row["event_id"], row["payload_hash"]) for row in lake.ready())
    assert ready_before == ready_after
    assert quarantine_before == set(lake.quarantine)
    assert len(lake.versions) == 101


def test_single_run_lock_blocks_another_process(tmp_path: Path):
    lock_path = tmp_path / "pipeline.lock"
    code = (
        "import sys\n"
        f"sys.path.insert(0, {str(ROOT / 'src')!r})\n"
        "from telemetry.locking import SingleRunLock\n"
        "from pathlib import Path\n"
        "try:\n"
        f"    with SingleRunLock(Path({str(lock_path)!r})):\n"
        "        pass\n"
        "except RuntimeError:\n"
        "    raise SystemExit(2)\n"
    )
    with SingleRunLock(lock_path):
        completed = subprocess.run([sys.executable, "-c", code], check=False, capture_output=True, text=True)
    assert completed.returncode == 2, completed.stderr


def test_readme_states_the_same_denominator_rules():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "scan_failure_rate = failed_scan_count / scan_count" in readme
    assert "null when scan_count is 0" in readme
    assert "accepted scans only" in readme
    assert "UTC date" in readme
    assert "(customer_id, device_id)" in readme
    assert "do not sum version-level active devices" in readme
    assert canonical_payload_hash(
        {
            "event_id": "e",
            "event_version": 1,
            "customer_id": "c",
            "device_id": "d",
            "event_ts": parse_timestamp("2026-06-01T00:00:00.000000Z"),
            "event_type": "heartbeat",
            "product_version": "1.0.0",
            "scan_status": None,
            "duration_ms": None,
        }
    )
