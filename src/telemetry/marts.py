"""Pure gold aggregations. Spark SQL in transforms.py must match these rules.

scan_failure_rate = failed_scan_count / scan_count, null when scan_count is 0.
Average duration uses accepted scans only.
event_date is the UTC date of event_ts.
An active device is a distinct (customer_id, device_id) pair.
Do not sum version-level active-device counts into a global distinct total.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime

from telemetry.contracts import (
    UNKNOWN_SEGMENT,
    Customer,
    ready_event_dicts,
    scan_failure_rate,
    segment_attributes,
)


def _utc_date(event: dict) -> date:
    if "event_date" in event and isinstance(event["event_date"], date) and not isinstance(event["event_date"], datetime):
        return event["event_date"]
    event_ts = event["event_ts"]
    if not isinstance(event_ts, datetime) or event_ts.tzinfo is None:
        raise ValueError("event_ts must be timezone-aware; timezone is not inferred from the locale")
    from datetime import timezone

    return event_ts.astimezone(timezone.utc).date()


def _metrics(items: list[dict]) -> dict:
    scans = [event for event in items if event["event_type"] == "scan"]
    failed = [event for event in scans if event["scan_status"] == "failed"]
    threats = [event for event in items if event["event_type"] == "threat_detected"]
    durations = [float(event["duration_ms"]) for event in scans]
    scan_count = len(scans)
    failed_count = len(failed)
    return {
        "accepted_event_count": len(items),
        "scan_count": scan_count,
        "failed_scan_count": failed_count,
        "threat_event_count": len(threats),
        "avg_scan_duration_ms": (sum(durations) / len(durations)) if durations else None,
        "scan_failure_rate": scan_failure_rate(failed_count, scan_count),
    }


def build_device_day(events: list[dict]) -> list[dict]:
    grouped: dict[tuple, list[dict]] = defaultdict(list)
    for event in events:
        key = (_utc_date(event), event["customer_id"], event["device_id"])
        grouped[key].append(event)
    rows = []
    for (event_date, customer_id, device_id), items in grouped.items():
        rows.append(
            {
                "event_date": event_date,
                "customer_id": customer_id,
                "device_id": device_id,
                **_metrics(items),
            }
        )
    rows.sort(key=lambda row: (row["event_date"], row["customer_id"], row["device_id"]))
    return rows


def build_product_version_day(events: list[dict]) -> list[dict]:
    grouped: dict[tuple, list[dict]] = defaultdict(list)
    for event in events:
        key = (_utc_date(event), event["product_version"])
        grouped[key].append(event)
    rows = []
    for (event_date, product_version), items in grouped.items():
        devices = {(event["customer_id"], event["device_id"]) for event in items}
        rows.append(
            {
                "event_date": event_date,
                "product_version": product_version,
                "active_device_count": len(devices),
                **_metrics(items),
            }
        )
    rows.sort(key=lambda row: (row["event_date"], row["product_version"]))
    return rows


def build_signup_cohort_day(events: list[dict], customers: list[Customer]) -> list[dict]:
    """Current plan segments. Missing customers stay on the event with UNKNOWN attributes."""
    by_id: dict[str, Customer] = {}
    for customer in customers:
        if customer.customer_id in by_id:
            raise ValueError(f"duplicate_customer_id:{customer.customer_id}")
        by_id[customer.customer_id] = customer
    grouped: dict[tuple, list[dict]] = defaultdict(list)
    for event in events:
        segment = segment_attributes(by_id.get(event["customer_id"]))
        key = (_utc_date(event), segment["signup_month"], segment["plan_tier"])
        grouped[key].append(event)
    rows = []
    for (event_date, signup_month, plan_tier), items in grouped.items():
        rows.append(
            {
                "event_date": event_date,
                "signup_month": signup_month,
                "plan_tier": plan_tier,
                "active_customer_count": len({event["customer_id"] for event in items}),
                "active_device_count": len({(event["customer_id"], event["device_id"]) for event in items}),
                **_metrics(items),
            }
        )
    rows.sort(key=lambda row: (row["event_date"], row["signup_month"], row["plan_tier"]))
    return rows


def distinct_active_devices(events: list[dict], event_date: date) -> int:
    return len(
        {
            (event["customer_id"], event["device_id"])
            for event in events
            if _utc_date(event) == event_date
        }
    )


def summed_version_active_devices(version_rows: list[dict], event_date: date) -> int:
    return sum(row["active_device_count"] for row in version_rows if row["event_date"] == event_date)


def gold_from_states(states: list[dict], customers: list[Customer]) -> dict[str, list[dict]]:
    events = ready_event_dicts(states)
    return {
        "device_day": build_device_day(events),
        "product_version_day": build_product_version_day(events),
        "signup_cohort_day": build_signup_cohort_day(events, customers),
    }


def assert_rate_rules(rows: list[dict]) -> list[str]:
    """Return human-readable failures. Empty means the denominator rules hold."""
    failures = []
    for row in rows:
        rate = row["scan_failure_rate"]
        scans = row["scan_count"]
        failed = row["failed_scan_count"]
        average = row["avg_scan_duration_ms"]
        label = str({key: row[key] for key in row if key in {"event_date", "customer_id", "device_id", "product_version", "signup_month", "plan_tier"}})
        if scans == 0:
            if rate is not None:
                failures.append(f"{label} scan_failure_rate must be null when scan_count is 0")
            if average is not None:
                failures.append(f"{label} avg_scan_duration_ms must be null when there are no accepted scans")
        else:
            expected = failed / scans
            if rate is None or abs(rate - expected) > 1e-9:
                failures.append(f"{label} scan_failure_rate {rate} != {expected}")
            if average is None:
                failures.append(f"{label} avg_scan_duration_ms must be set when scans were accepted")
    return failures


# Imported for a readable UNKNOWN in docs and tests.
__all__ = [
    "UNKNOWN_SEGMENT",
    "assert_rate_rules",
    "build_device_day",
    "build_product_version_day",
    "build_signup_cohort_day",
    "distinct_active_devices",
    "gold_from_states",
    "summed_version_active_devices",
]
