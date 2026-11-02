"""Deterministic JSONL fixtures. Extend scenarios instead of editing meanings in place."""

from __future__ import annotations

import argparse
import json
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from telemetry.contracts import canonical_timestamp, parse_timestamp

REPO_ROOT = Path(__file__).resolve().parents[2]
PLANS = ("free", "premium", "family")
CHANNELS = ("organic", "referral", "paid")
COUNTRIES = ("US", "CA", "GB", "DE", "IN", "AU", "FR", "JP", "BR", "MX", "SE", "NL")


def dump_line(obj: dict) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def load_scenario(name: str, root: Path | None = None) -> dict:
    path = (root or REPO_ROOT) / "scenarios" / f"{name}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def build_customers(count: int) -> list[dict]:
    customers = []
    for index in range(1, count + 1):
        signup = date(2025, 1, 1) + timedelta(days=index * 17)
        customers.append(
            {
                "acquisition_channel": CHANNELS[(index - 1) % len(CHANNELS)],
                "country_code": COUNTRIES[(index - 1) % len(COUNTRIES)],
                "customer_id": f"cust-{index:03d}",
                "plan_tier": PLANS[(index - 1) % len(PLANS)],
                "signup_date": signup.isoformat(),
                "updated_at": "2026-05-01T00:00:00.000000Z",
            }
        )
    return customers


def _event_shell(number: int, scenario: dict, rng: random.Random, customers: list[dict]) -> dict:
    versions = scenario["product_versions"]
    base = date.fromisoformat(scenario["base_date"])
    day_span = int(scenario["day_span"])
    event_type = rng.choices(["heartbeat", "scan", "threat_detected"], weights=[4, 5, 1], k=1)[0]
    customer = customers[(number - 1) % len(customers)]
    event_ts = datetime(base.year, base.month, base.day, tzinfo=timezone.utc) + timedelta(
        days=(number * 3) % day_span,
        hours=rng.randrange(24),
        minutes=rng.randrange(60),
        seconds=rng.randrange(60),
    )
    received_ts = event_ts + timedelta(hours=rng.randrange(0, 48), minutes=rng.randrange(0, 30))
    event = {
        "customer_id": customer["customer_id"],
        "device_id": f"device-{(number % 3) + 1:02d}",
        "duration_ms": None,
        "event_id": f"event-{number:04d}",
        "event_ts": canonical_timestamp(event_ts),
        "event_type": event_type,
        "event_version": 1,
        "product_version": versions[(number - 1) % len(versions)],
        "received_ts": canonical_timestamp(received_ts),
        "scan_status": None,
    }
    if event_type == "scan":
        event["scan_status"] = "failed" if rng.randrange(4) == 0 else "success"
        event["duration_ms"] = rng.randrange(0, 5000)
    return event


def _shift_received(event: dict, minutes: int) -> dict:
    updated = dict(event)
    received = parse_timestamp(event["received_ts"]) + timedelta(minutes=minutes)
    updated["received_ts"] = canonical_timestamp(received)
    return updated


def apply_reference_overrides(events: list[dict], scenario: dict) -> list[dict]:
    """Documented mutations. They do not change the 100 logical event ids."""
    by_number = {int(event["event_id"].split("-")[1]): event for event in events}
    unknown_number = scenario["unknown_customer_event_number"]
    by_number[unknown_number]["customer_id"] = scenario["unknown_customer_id"]

    boundary_number = scenario["utc_boundary_event_number"]
    by_number[boundary_number]["event_ts"] = scenario["utc_boundary_event_ts"]
    by_number[boundary_number]["received_ts"] = scenario["utc_boundary_received_ts"]

    old_number = scenario["old_event_number"]
    by_number[old_number]["event_ts"] = scenario["old_event_ts"]
    by_number[old_number]["received_ts"] = scenario["old_received_ts"]

    fractional_number = scenario["fractional_duration_event_number"]
    fractional = by_number[fractional_number]
    fractional["event_type"] = "scan"
    fractional["scan_status"] = "success"
    fractional["duration_ms"] = 10.5
    fractional["product_version"] = scenario["product_versions"][0]
    return [by_number[number] for number in sorted(by_number)]


def invalid_lines(scenario: dict) -> list[str]:
    lines = []
    cases = scenario.get("invalid_cases") or []
    if "missing_event_id" in cases:
        lines.append(
            dump_line(
                {
                    "customer_id": "cust-001",
                    "device_id": "device-01",
                    "duration_ms": None,
                    "event_ts": "2026-06-01T00:00:00.000000Z",
                    "event_type": "heartbeat",
                    "event_version": 1,
                    "product_version": "1.0.0",
                    "received_ts": "2026-06-01T00:00:00.000000Z",
                    "scan_status": None,
                }
            )
        )
    if "unsupported_event_type" in cases:
        lines.append(
            dump_line(
                {
                    "customer_id": "cust-001",
                    "device_id": "device-01",
                    "duration_ms": None,
                    "event_id": "event-bad-type",
                    "event_ts": "2026-06-01T00:00:00.000000Z",
                    "event_type": "reboot",
                    "event_version": 1,
                    "product_version": "1.0.0",
                    "received_ts": "2026-06-01T00:00:00.000000Z",
                    "scan_status": None,
                }
            )
        )
    if "event_ts_ahead" in cases:
        lines.append(
            dump_line(
                {
                    "customer_id": "cust-001",
                    "device_id": "device-01",
                    "duration_ms": 15,
                    "event_id": "event-ahead",
                    "event_ts": "2026-06-01T00:10:00.000000Z",
                    "event_type": "scan",
                    "event_version": 1,
                    "product_version": "1.0.0",
                    "received_ts": "2026-06-01T00:00:00.000000Z",
                    "scan_status": "success",
                }
            )
        )
    return lines


def build_event_lines(scenario: dict) -> list[str]:
    rng = random.Random(int(scenario["seed"]))
    customers = build_customers(int(scenario["customer_count"]))
    events = [
        _event_shell(number, scenario, rng, customers)
        for number in range(1, int(scenario["logical_events"]) + 1)
    ]
    if scenario.get("name") == "reference":
        events = apply_reference_overrides(events, scenario)
    lines = [dump_line(event) for event in events]
    if scenario.get("name") == "reference":
        duplicate_source = events[int(scenario["duplicate_event_number"]) - 1]
        lines.append(dump_line(_shift_received(duplicate_source, 30)))
        revised = dict(events[int(scenario["revision_event_number"]) - 1])
        revised["event_version"] = 2
        revised["product_version"] = scenario["revision_product_version"]
        revised = _shift_received(revised, 60)
        lines.append(dump_line(revised))
    lines.extend(invalid_lines(scenario))
    return lines


def write_scenario(name: str, out_dir: Path, root: Path | None = None) -> dict:
    scenario = load_scenario(name, root)
    out_dir.mkdir(parents=True, exist_ok=True)
    customers = build_customers(int(scenario["customer_count"]))
    customer_lines = [dump_line(row) for row in customers]
    event_lines = build_event_lines(scenario)
    customers_path = out_dir / "customers.jsonl"
    customers_path.write_text("\n".join(customer_lines) + "\n", encoding="utf-8")
    if scenario.get("single_file", True):
        events_path = out_dir / "events.jsonl"
        events_path.write_text("\n".join(event_lines) + "\n", encoding="utf-8")
        event_files = [events_path]
    else:
        landing = out_dir / "landing"
        landing.mkdir(parents=True, exist_ok=True)
        file_count = int(scenario.get("output_files") or 1)
        buckets: list[list[str]] = [[] for _ in range(file_count)]
        for index, line in enumerate(event_lines):
            buckets[index % file_count].append(line)
        event_files = []
        for index, bucket in enumerate(buckets):
            path = landing / f"events-{index:02d}.jsonl"
            path.write_text("\n".join(bucket) + ("\n" if bucket else ""), encoding="utf-8")
            event_files.append(path)
        events_path = landing
    return {
        "scenario": name,
        "events_path": str(events_path),
        "customers_path": str(customers_path),
        "event_files": [str(path) for path in event_files],
        "event_rows": len(event_lines),
        "customer_rows": len(customer_lines),
        "event_bytes": sum(path.stat().st_size for path in event_files if path.is_file()),
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Generate a deterministic telemetry fixture")
    parser.add_argument("--scenario", default="reference")
    parser.add_argument("--out-dir", type=Path, default=Path("data"))
    args = parser.parse_args(argv)
    written = write_scenario(args.scenario, args.out_dir)
    print(json.dumps(written, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
