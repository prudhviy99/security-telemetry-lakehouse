"""Source contracts, canonical hashing, and current-state rules.

No Spark imports. The same functions decide validity for unit tests and for
the Delta MERGE batch.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Iterable, Mapping

# Canonical hash document, version 1:
# UTF-8 JSON object, keys sorted, separators (',', ':').
# Fields: customer_id, device_id, duration_ms, event_id, event_ts,
# event_type, event_version, product_version, scan_status.
# event_ts is UTC `YYYY-MM-DDTHH:MM:SS.ffffffZ` (microseconds always 6 digits).
# event_version is a JSON integer.
# duration_ms is JSON null, or a JSON string of a canonical decimal
# (no exponent, trailing zeros stripped, no trailing dot).
# scan_status is JSON null or a JSON string.
# received_ts, source path, load time, and any other attribute are excluded.
CANONICAL_HASH_VERSION = "v1"

EVENT_SCHEMA_VERSION = 1
OPTIONAL_EVENT_ATTRIBUTES = ("agent_build",)
EVENT_TYPES = ("heartbeat", "scan", "threat_detected")
SCAN_STATUSES = ("success", "failed")
PLAN_TIERS = ("free", "premium", "family")
ACQUISITION_CHANNELS = ("organic", "referral", "paid")
UNKNOWN_SEGMENT = "UNKNOWN"
FUTURE_SKEW = timedelta(minutes=5)

REQUIRED_EVENT_FIELDS = (
    "event_id",
    "event_version",
    "customer_id",
    "device_id",
    "event_ts",
    "received_ts",
    "event_type",
    "product_version",
)

_TIMESTAMP_RE = re.compile(
    r"^(?P<ymd>\d{4}-\d{2}-\d{2})T(?P<hms>\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<frac>\d{1,9}))?"
    r"(?P<tz>Z|[+-]\d{2}:\d{2}|[+-]\d{4}|[+-]\d{2})$"
)
_NAIVE_TIMESTAMP_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?$"
)
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class CanonicalEvent:
    event_id: str
    event_version: int
    customer_id: str
    device_id: str
    event_ts: datetime
    received_ts: datetime
    event_type: str
    product_version: str
    scan_status: str | None
    duration_ms: Decimal | None
    payload_hash: str
    raw_json: str
    source_file: str | None = None


@dataclass(frozen=True)
class ClassifiedRow:
    raw_json: str
    raw_payload_hash: str
    source_file: str | None
    status: str
    quality_reason: str | None
    detail: str | None
    event: CanonicalEvent | None


@dataclass(frozen=True)
class Customer:
    customer_id: str
    signup_date: date
    signup_month: str
    plan_tier: str
    country_code: str
    acquisition_channel: str
    updated_at: datetime


@dataclass(frozen=True)
class ValidationFailure:
    reason: str
    detail: str


def raw_payload_hash(raw_json: str) -> str:
    return hashlib.sha256(raw_json.encode("utf-8")).hexdigest()


def canonical_timestamp(value: datetime) -> str:
    utc = value.astimezone(timezone.utc)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond:06d}Z"


def canonical_duration(value: Decimal | int | float | None) -> str | None:
    """Stable decimal text. Null stays null. 10, 10.0, and 10.00 all become '10'."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise TypeError("duration must be numeric")
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("duration must be finite")
        decimal_value = Decimal(str(value))
    elif isinstance(value, Decimal):
        decimal_value = value
    else:
        decimal_value = Decimal(value)
    text = format(decimal_value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    if text in {"", "-0"}:
        text = "0"
    return text


def canonical_hash_document(fields: Mapping[str, object]) -> str:
    event_ts = fields["event_ts"]
    if isinstance(event_ts, str):
        event_ts_text = canonical_timestamp(parse_timestamp(event_ts))
    elif isinstance(event_ts, datetime):
        event_ts_text = canonical_timestamp(event_ts)
    else:
        raise TypeError("event_ts must be a datetime or timestamp string")
    payload = {
        "customer_id": fields["customer_id"],
        "device_id": fields["device_id"],
        "duration_ms": canonical_duration(fields.get("duration_ms")),  # type: ignore[arg-type]
        "event_id": fields["event_id"],
        "event_ts": event_ts_text,
        "event_type": fields["event_type"],
        "event_version": int(fields["event_version"]),  # type: ignore[arg-type]
        "product_version": fields["product_version"],
        "scan_status": fields.get("scan_status"),
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def canonical_payload_hash(fields: Mapping[str, object]) -> str:
    return hashlib.sha256(canonical_hash_document(fields).encode("utf-8")).hexdigest()


def parse_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 instant with Z or a numeric offset. Never guess a locale."""
    if not isinstance(value, str):
        raise TypeError("timestamp must be a string")
    match = _TIMESTAMP_RE.fullmatch(value)
    if match is None:
        if _NAIVE_TIMESTAMP_RE.fullmatch(value):
            raise ValueError("missing_timezone")
        raise ValueError("invalid_timestamp")
    year, month, day = (int(part) for part in match.group("ymd").split("-"))
    hour, minute, second = (int(part) for part in match.group("hms").split(":"))
    fraction = match.group("frac") or "0"
    microsecond = int((fraction + "000000")[:6])
    tz_text = match.group("tz")
    if tz_text == "Z":
        tzinfo = timezone.utc
    else:
        sign = 1 if tz_text[0] == "+" else -1
        rest = tz_text[1:]
        if ":" in rest:
            offset_hour, offset_minute = rest.split(":")
        elif len(rest) == 4:
            offset_hour, offset_minute = rest[:2], rest[2:]
        else:
            offset_hour, offset_minute = rest, "0"
        tzinfo = timezone(sign * timedelta(hours=int(offset_hour), minutes=int(offset_minute)))
    try:
        parsed = datetime(year, month, day, hour, minute, second, microsecond, tzinfo=tzinfo)
    except ValueError as exc:
        raise ValueError("invalid_timestamp") from exc
    return parsed.astimezone(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def scan_failure_rate(failed_scan_count: int, scan_count: int) -> float | None:
    """failed_scan_count / scan_count. Null when scan_count is 0."""
    if scan_count == 0:
        return None
    return failed_scan_count / scan_count


def _fail(reason: str, detail: str) -> ValidationFailure:
    return ValidationFailure(reason=reason, detail=detail)


def _parse_object(raw_json: str) -> tuple[dict | None, ValidationFailure | None]:
    try:
        obj = json.loads(raw_json, parse_float=Decimal)
    except json.JSONDecodeError as exc:
        return None, _fail("invalid_json", f"malformed JSON: {exc.msg}")
    if not isinstance(obj, dict):
        return None, _fail("not_an_object", f"expected a JSON object, got {type(obj).__name__}")
    return obj, None


def validate_event_object(obj: Mapping[str, object], raw_json: str, source_file: str | None = None) -> ClassifiedRow:
    """Validate one object. The first failing rule is the quarantine reason."""
    hashed = raw_payload_hash(raw_json)

    def invalid(failure: ValidationFailure) -> ClassifiedRow:
        return ClassifiedRow(
            raw_json=raw_json,
            raw_payload_hash=hashed,
            source_file=source_file,
            status="invalid",
            quality_reason=failure.reason,
            detail=failure.detail,
            event=None,
        )

    if "schema_version" in obj and obj["schema_version"] is not None:
        version = obj["schema_version"]
        if isinstance(version, bool) or not isinstance(version, int) or version != EVENT_SCHEMA_VERSION:
            return invalid(
                _fail(
                    "unsupported_schema_version",
                    f"parser supports schema_version {EVENT_SCHEMA_VERSION}, got {version!r}",
                )
            )
    if "agent_build" in obj and obj["agent_build"] is not None and not isinstance(obj["agent_build"], str):
        return invalid(
            _fail(
                "incorrect_type:agent_build",
                "optional agent_build must be a string or null",
            )
        )

    for field in REQUIRED_EVENT_FIELDS:
        if field not in obj:
            return invalid(
                _fail(
                    f"missing_required_field:{field}",
                    f"required field {field} is missing",
                )
            )

    for field in ("event_id", "customer_id", "device_id", "product_version", "event_type"):
        if not isinstance(obj[field], str):
            return invalid(
                _fail(
                    f"incorrect_type:{field}",
                    f"{field} must be a string, got {type(obj[field]).__name__}",
                )
            )
    for field in ("event_ts", "received_ts"):
        if not isinstance(obj[field], str):
            return invalid(
                _fail(
                    f"incorrect_type:{field}",
                    f"{field} must be a timestamp string, got {type(obj[field]).__name__}",
                )
            )

    version = obj["event_version"]
    if isinstance(version, bool) or not isinstance(version, int):
        return invalid(
            _fail(
                "incorrect_type:event_version",
                f"event_version must be a JSON integer, got {type(version).__name__}",
            )
        )
    if version < 1:
        return invalid(
            _fail(
                "invalid_event_version",
                "event_version must be a positive integer",
            )
        )

    for field in ("event_id", "customer_id", "device_id", "product_version"):
        if str(obj[field]).strip() == "":
            return invalid(
                _fail(
                    f"blank_required_field:{field}",
                    f"{field} must be nonblank",
                )
            )

    try:
        event_ts = parse_timestamp(str(obj["event_ts"]))
    except ValueError as exc:
        code = str(exc)
        reason = "missing_timezone:event_ts" if code == "missing_timezone" else "invalid_timestamp:event_ts"
        return invalid(_fail(reason, f"event_ts {obj['event_ts']!r} is not an ISO-8601 instant with a timezone"))
    try:
        received_ts = parse_timestamp(str(obj["received_ts"]))
    except ValueError as exc:
        code = str(exc)
        reason = "missing_timezone:received_ts" if code == "missing_timezone" else "invalid_timestamp:received_ts"
        return invalid(
            _fail(reason, f"received_ts {obj['received_ts']!r} is not an ISO-8601 instant with a timezone")
        )

    event_type = str(obj["event_type"])
    if event_type not in EVENT_TYPES:
        return invalid(
            _fail(
                "unsupported_event_type",
                f"event_type must be one of {', '.join(EVENT_TYPES)}, got {event_type!r}",
            )
        )

    scan_status = obj.get("scan_status")
    duration = obj.get("duration_ms")
    if event_type == "scan":
        if scan_status not in SCAN_STATUSES:
            return invalid(
                _fail(
                    "invalid_scan_status",
                    "scans require scan_status success or failed",
                )
            )
        if "duration_ms" not in obj or duration is None:
            return invalid(_fail("invalid_duration", "scans require a finite nonnegative duration_ms"))
        if isinstance(duration, bool) or not isinstance(duration, (int, Decimal)):
            # json parse_float yields Decimal; a plain float is still accepted from callers.
            if isinstance(duration, float) and math.isfinite(duration):
                pass
            else:
                return invalid(
                    _fail(
                        "invalid_duration",
                        "duration_ms must be a finite nonnegative number",
                    )
                )
        if isinstance(duration, float) and not math.isfinite(duration):
            return invalid(_fail("invalid_duration", "duration_ms must be finite"))
        if duration < 0:
            return invalid(_fail("invalid_duration", "duration_ms must be nonnegative"))
        duration_value: Decimal | None = Decimal(str(duration)) if isinstance(duration, float) else Decimal(duration)
    else:
        if scan_status is not None:
            return invalid(
                _fail(
                    "invalid_scan_status",
                    f"{event_type} requires scan_status null",
                )
            )
        if duration is not None:
            return invalid(_fail("invalid_duration", f"{event_type} requires duration_ms null"))
        duration_value = None
        scan_status = None

    if event_ts - received_ts > FUTURE_SKEW:
        return invalid(
            _fail(
                "event_ts_ahead_of_received_ts",
                "event_ts is more than five minutes ahead of received_ts",
            )
        )

    fields = {
        "event_id": obj["event_id"],
        "event_version": version,
        "customer_id": obj["customer_id"],
        "device_id": obj["device_id"],
        "event_ts": event_ts,
        "event_type": event_type,
        "product_version": obj["product_version"],
        "scan_status": scan_status,
        "duration_ms": duration_value,
    }
    event = CanonicalEvent(
        event_id=str(obj["event_id"]),
        event_version=version,
        customer_id=str(obj["customer_id"]),
        device_id=str(obj["device_id"]),
        event_ts=event_ts,
        received_ts=received_ts,
        event_type=event_type,
        product_version=str(obj["product_version"]),
        scan_status=None if scan_status is None else str(scan_status),
        duration_ms=duration_value,
        payload_hash=canonical_payload_hash(fields),
        raw_json=raw_json,
        source_file=source_file,
    )
    return ClassifiedRow(
        raw_json=raw_json,
        raw_payload_hash=hashed,
        source_file=source_file,
        status="valid",
        quality_reason=None,
        detail=None,
        event=event,
    )


def classify_raw_line(raw_json: str, source_file: str | None = None) -> ClassifiedRow:
    text = raw_json.strip("\n").strip("\r")
    obj, failure = _parse_object(text)
    hashed = raw_payload_hash(text)
    if failure is not None:
        return ClassifiedRow(
            raw_json=text,
            raw_payload_hash=hashed,
            source_file=source_file,
            status="invalid",
            quality_reason=failure.reason,
            detail=failure.detail,
            event=None,
        )
    assert obj is not None
    return validate_event_object(obj, text, source_file)


def validate_customer_object(obj: Mapping[str, object]) -> tuple[Customer | None, ValidationFailure | None]:
    required = (
        "customer_id",
        "signup_date",
        "plan_tier",
        "country_code",
        "acquisition_channel",
        "updated_at",
    )
    for field in required:
        if field not in obj:
            return None, _fail(f"missing_required_field:{field}", f"required field {field} is missing")
    if not isinstance(obj["customer_id"], str):
        return None, _fail("incorrect_type:customer_id", "customer_id must be a string")
    if obj["customer_id"].strip() == "":
        return None, _fail("blank_required_field:customer_id", "customer_id must be nonblank")
    signup = obj["signup_date"]
    if not isinstance(signup, str) or _DATE_RE.fullmatch(signup) is None:
        return None, _fail("invalid_signup_date", "signup_date must be YYYY-MM-DD")
    try:
        signup_date = date.fromisoformat(signup)
    except ValueError:
        return None, _fail("invalid_signup_date", f"signup_date {signup!r} is not a real date")
    plan = obj["plan_tier"]
    if plan not in PLAN_TIERS:
        return None, _fail("invalid_plan_tier", f"plan_tier must be one of {', '.join(PLAN_TIERS)}")
    country = obj["country_code"]
    if not isinstance(country, str) or re.fullmatch(r"[A-Z]{2}", country) is None:
        return None, _fail("invalid_country_code", "country_code must be two uppercase letters")
    channel = obj["acquisition_channel"]
    if channel not in ACQUISITION_CHANNELS:
        return None, _fail(
            "invalid_acquisition_channel",
            f"acquisition_channel must be one of {', '.join(ACQUISITION_CHANNELS)}",
        )
    updated = obj["updated_at"]
    if not isinstance(updated, str):
        return None, _fail("incorrect_type:updated_at", "updated_at must be a timestamp string")
    try:
        updated_at = parse_timestamp(updated)
    except ValueError as exc:
        code = str(exc)
        reason = "missing_timezone:updated_at" if code == "missing_timezone" else "invalid_timestamp:updated_at"
        return None, _fail(reason, f"updated_at {updated!r} needs an explicit timezone")
    customer = Customer(
        customer_id=str(obj["customer_id"]),
        signup_date=signup_date,
        signup_month=f"{signup_date.year:04d}-{signup_date.month:02d}",
        plan_tier=str(plan),
        country_code=str(country),
        acquisition_channel=str(channel),
        updated_at=updated_at,
    )
    return customer, None


def validate_customer_snapshot(raw_lines: Iterable[str]) -> tuple[list[Customer] | None, list[str]]:
    """Type 1 snapshot. Any invalid row or duplicate id blocks publication of the snapshot."""
    customers: list[Customer] = []
    errors: list[str] = []
    for index, raw in enumerate(raw_lines, start=1):
        text = raw.strip("\n").strip("\r")
        if text == "":
            continue
        obj, failure = _parse_object(text)
        if failure is not None or obj is None:
            errors.append(f"line {index}: {failure.reason if failure else 'invalid_json'}")
            continue
        customer, customer_failure = validate_customer_object(obj)
        if customer_failure is not None or customer is None:
            errors.append(f"line {index}: {customer_failure.reason if customer_failure else 'invalid'}")
            continue
        customers.append(customer)
    counts: dict[str, int] = defaultdict(int)
    for customer in customers:
        counts[customer.customer_id] += 1
    duplicates = sorted(customer_id for customer_id, count in counts.items() if count > 1)
    if duplicates:
        errors.append("duplicate_customer_id:" + ",".join(duplicates))
    if errors:
        return None, errors
    return customers, []


def dedupe_versions(events: Iterable[CanonicalEvent]) -> list[CanonicalEvent]:
    """One row per (event_id, version, payload hash). Keep the earliest receipt."""
    best: dict[tuple[str, int, str], CanonicalEvent] = {}
    for event in events:
        key = (event.event_id, event.event_version, event.payload_hash)
        current = best.get(key)
        if current is None or event.received_ts < current.received_ts:
            best[key] = event
    return list(best.values())


def merge_version(existing: CanonicalEvent | None, incoming: CanonicalEvent) -> CanonicalEvent:
    if existing is None or incoming.received_ts < existing.received_ts:
        return incoming
    return existing


def current_states(versions: Iterable[CanonicalEvent]) -> list[dict]:
    """Greatest version is READY with one payload, otherwise CONFLICT.

    Recompute from every stored version of an event, not from the latest batch only.
    """
    grouped: dict[str, list[CanonicalEvent]] = defaultdict(list)
    for version in versions:
        grouped[version.event_id].append(version)
    states: list[dict] = []
    for event_id, records in grouped.items():
        max_version = max(record.event_version for record in records)
        top = [record for record in records if record.event_version == max_version]
        by_hash: dict[str, CanonicalEvent] = {}
        for record in top:
            chosen = by_hash.get(record.payload_hash)
            if chosen is None or record.received_ts < chosen.received_ts:
                by_hash[record.payload_hash] = record
        if len(by_hash) == 1:
            record = next(iter(by_hash.values()))
            states.append(
                {
                    "event_id": event_id,
                    "event_version": record.event_version,
                    "state": "READY",
                    "payload_hash": record.payload_hash,
                    "payload_count": 1,
                    "received_ts": record.received_ts,
                    "event_ts": record.event_ts,
                    "customer_id": record.customer_id,
                    "device_id": record.device_id,
                    "event_type": record.event_type,
                    "product_version": record.product_version,
                    "scan_status": record.scan_status,
                    "duration_ms": None if record.duration_ms is None else float(record.duration_ms),
                    "raw_json": record.raw_json,
                    "source_file": record.source_file,
                }
            )
        else:
            states.append(
                {
                    "event_id": event_id,
                    "event_version": max_version,
                    "state": "CONFLICT",
                    "payload_hash": None,
                    "payload_count": len(by_hash),
                    "received_ts": None,
                    "event_ts": None,
                    "customer_id": None,
                    "device_id": None,
                    "event_type": None,
                    "product_version": None,
                    "scan_status": None,
                    "duration_ms": None,
                    "raw_json": None,
                    "source_file": None,
                }
            )
    states.sort(key=lambda row: row["event_id"])
    return states


def conflict_evidence(versions: Iterable[CanonicalEvent]) -> list[CanonicalEvent]:
    """Raw payloads that disagree at the greatest version."""
    grouped: dict[str, list[CanonicalEvent]] = defaultdict(list)
    for version in versions:
        grouped[version.event_id].append(version)
    evidence: list[CanonicalEvent] = []
    for records in grouped.values():
        max_version = max(record.event_version for record in records)
        top = [record for record in records if record.event_version == max_version]
        hashes = {record.payload_hash for record in top}
        if len(hashes) > 1:
            evidence.extend(top)
    return evidence


def segment_attributes(customer: Customer | None) -> dict[str, str]:
    if customer is None:
        return {
            "signup_month": UNKNOWN_SEGMENT,
            "plan_tier": UNKNOWN_SEGMENT,
            "country_code": UNKNOWN_SEGMENT,
            "acquisition_channel": UNKNOWN_SEGMENT,
        }
    return {
        "signup_month": customer.signup_month,
        "plan_tier": customer.plan_tier,
        "country_code": customer.country_code,
        "acquisition_channel": customer.acquisition_channel,
    }


def ready_event_dicts(states: Iterable[dict]) -> list[dict]:
    ready = []
    for state in states:
        if state["state"] != "READY":
            continue
        event_ts = state["event_ts"]
        ready.append(
            {
                "event_id": state["event_id"],
                "event_version": state["event_version"],
                "customer_id": state["customer_id"],
                "device_id": state["device_id"],
                "event_ts": event_ts,
                "event_date": _as_utc(event_ts).date(),
                "event_type": state["event_type"],
                "product_version": state["product_version"],
                "scan_status": state["scan_status"],
                "duration_ms": state["duration_ms"],
                "received_ts": state["received_ts"],
                "payload_hash": state["payload_hash"],
            }
        )
    return ready


def reconcile_lines(raw_lines: Iterable[str], source_file: str | None = None) -> dict:
    """Occurrence-level reconciliation. Valid plus invalid equals raw.

    Duplicates, superseded revisions, and conflicts are separate counts.
    Raw valid occurrences are not expected to equal current READY events.
    """
    classified = [classify_raw_line(line, source_file) for line in raw_lines if line.strip()]
    valid = [row.event for row in classified if row.status == "valid" and row.event is not None]
    invalid = [row for row in classified if row.status == "invalid"]
    ledger = dedupe_versions(valid)
    states = current_states(ledger)
    ready = [state for state in states if state["state"] == "READY"]
    conflicts = [state for state in states if state["state"] == "CONFLICT"]
    max_version: dict[str, int] = {}
    for record in ledger:
        max_version[record.event_id] = max(max_version.get(record.event_id, 0), record.event_version)
    superseded = sum(1 for record in ledger if record.event_version < max_version[record.event_id])
    return {
        "raw_rows": len(classified),
        "valid_rows": len(valid),
        "invalid_rows": len(invalid),
        "invalid_reasons": sorted(row.quality_reason or "" for row in invalid),
        "ledger_versions": len(ledger),
        "duplicate_delivery_rows": len(valid) - len(ledger),
        "superseded_versions": superseded,
        "conflict_events": len(conflicts),
        "ready_events": len(ready),
        "states": states,
        "ledger": ledger,
        "classified": classified,
    }


class InMemoryLake:
    """Batch-at-a-time ledger used to prove order independence without Spark."""

    def __init__(self) -> None:
        self.versions: dict[tuple[str, int, str], CanonicalEvent] = {}
        self.quarantine: dict[tuple[str, str], dict] = {}
        self.current: dict[str, dict] = {}
        self.audit: dict[tuple[str, str], dict] = {}

    def apply_batch(self, raw_lines: Iterable[str], batch_id: str, stream_id: str = "silver_events") -> None:
        classified = [classify_raw_line(line) for line in raw_lines if line.strip()]
        run_id = f"{stream_id}:{batch_id}"
        for row in classified:
            if row.status != "invalid":
                continue
            key = (row.raw_payload_hash, row.quality_reason or "")
            if key not in self.quarantine:
                self.quarantine[key] = {
                    "raw_payload_hash": row.raw_payload_hash,
                    "quality_reason": row.quality_reason,
                    "raw_json": row.raw_json,
                    "first_seen_run": run_id,
                    "detail": row.detail,
                }
        incoming = dedupe_versions(row.event for row in classified if row.event is not None)
        touched: set[str] = set()
        for event in incoming:
            key = (event.event_id, event.event_version, event.payload_hash)
            self.versions[key] = merge_version(self.versions.get(key), event)
            touched.add(event.event_id)
        if touched:
            relevant = [event for event in self.versions.values() if event.event_id in touched]
            for state in current_states(relevant):
                self.current[state["event_id"]] = state
            for event in conflict_evidence(relevant):
                hashed = raw_payload_hash(event.raw_json)
                key = (hashed, "conflicting_version_payload")
                if key not in self.quarantine:
                    self.quarantine[key] = {
                        "raw_payload_hash": hashed,
                        "quality_reason": "conflicting_version_payload",
                        "raw_json": event.raw_json,
                        "first_seen_run": run_id,
                        "detail": f"event_id={event.event_id} version={event.event_version}",
                    }
        audit_key = (stream_id, batch_id)
        if audit_key not in self.audit:
            self.audit[audit_key] = {
                "stream_id": stream_id,
                "batch_id": batch_id,
                "status": "COMPLETED",
                "input_rows": len(classified),
            }

    def ready(self) -> list[dict]:
        return ready_event_dicts(state for state in self.current.values() if state["state"] == "READY")
