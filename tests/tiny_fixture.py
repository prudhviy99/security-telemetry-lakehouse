"""Hand-sized fixture for the six analytical questions.

Today is 2026-07-08. Product 1.0.0 is observed on July 1, July 2, and July 8,
so the prior observed day is July 2, not the previous calendar day.
"""

from __future__ import annotations

import json
from datetime import date

AS_OF = date(2026, 7, 8)


def _line(payload: dict) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _event(
    event_id: str,
    customer_id: str,
    device_id: str,
    event_ts: str,
    event_type: str,
    product_version: str,
    scan_status: str | None,
    duration_ms: int | float | None,
    received_ts: str | None = None,
    event_version: int = 1,
) -> dict:
    return {
        "customer_id": customer_id,
        "device_id": device_id,
        "duration_ms": duration_ms,
        "event_id": event_id,
        "event_ts": event_ts,
        "event_type": event_type,
        "event_version": event_version,
        "product_version": product_version,
        "received_ts": received_ts or event_ts.replace("T09", "T12").replace("T10", "T18").replace("T11", "T19"),
        "scan_status": scan_status,
    }


def logical_events() -> list[dict]:
    return [
        _event("e01", "c1", "d1", "2026-07-01T10:00:00Z", "scan", "1.0.0", "failed", 10, "2026-07-01T12:00:00Z"),
        _event("e02", "c1", "d1", "2026-07-01T11:00:00Z", "scan", "1.0.0", "success", 30, "2026-07-01T13:00:00Z"),
        _event("e03", "c1", "d2", "2026-07-01T12:00:00Z", "scan", "1.1.0", "failed", 20, "2026-07-01T14:00:00Z"),
        _event("e04", "c2", "d1", "2026-07-01T13:00:00Z", "heartbeat", "1.0.0", None, None, "2026-07-01T15:00:00Z"),
        _event("e05", "c2", "d1", "2026-07-01T14:00:00Z", "threat_detected", "1.1.0", None, None, "2026-07-01T16:00:00Z"),
        _event("e06", "c1", "d1", "2026-07-02T10:00:00Z", "scan", "1.0.0", "failed", 40, "2026-07-02T12:00:00Z"),
        _event("e07", "c1", "d1", "2026-07-02T11:00:00Z", "scan", "1.0.0", "success", 60, "2026-07-02T13:00:00Z"),
        _event("e08", "cust-x", "d9", "2026-07-02T15:00:00Z", "scan", "1.2.0", "success", 5, "2026-07-02T16:00:00Z"),
        _event("e09", "c1", "d1", "2026-07-08T09:00:00Z", "scan", "1.0.0", "success", 10, "2026-07-08T10:00:00Z"),
        _event("e10", "c2", "d1", "2026-07-08T09:30:00Z", "scan", "1.1.0", "failed", 100, "2026-07-08T11:00:00Z"),
        _event("e11", "c2", "d1", "2026-07-08T09:45:00Z", "scan", "1.1.0", "failed", 50, "2026-07-08T11:15:00Z"),
    ]


def customer_lines() -> list[str]:
    rows = [
        {
            "acquisition_channel": "organic",
            "country_code": "US",
            "customer_id": "c1",
            "plan_tier": "premium",
            "signup_date": "2026-01-15",
            "updated_at": "2026-01-15T00:00:00Z",
        },
        {
            "acquisition_channel": "paid",
            "country_code": "CA",
            "customer_id": "c2",
            "plan_tier": "free",
            "signup_date": "2026-03-02",
            "updated_at": "2026-03-02T00:00:00Z",
        },
    ]
    return [_line(row) for row in rows]


def event_lines() -> list[str]:
    """11 logical events, one duplicate delivery, one higher revision, one invalid row."""
    events = logical_events()
    lines = [_line(event) for event in events]
    duplicate = dict(events[1])
    duplicate["received_ts"] = "2026-07-01T18:30:00Z"
    lines.append(_line(duplicate))
    revision = dict(events[0])
    revision["event_version"] = 2
    revision["received_ts"] = "2026-07-01T19:00:00Z"
    lines.append(_line(revision))
    invalid = dict(events[2])
    invalid["event_id"] = "e-bad"
    invalid["event_type"] = "reboot"
    lines.append(_line(invalid))
    return lines
