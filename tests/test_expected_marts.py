"""Tiny-fixture expectations published in sql/analysis.sql."""

from __future__ import annotations

from datetime import date, timedelta

from telemetry.contracts import ready_event_dicts, reconcile_lines, validate_customer_snapshot
from telemetry.marts import (
    build_product_version_day,
    distinct_active_devices,
    gold_from_states,
    summed_version_active_devices,
)
from tests.sql_expectations import load_queries, values_close
from tests.tiny_fixture import AS_OF, event_lines, customer_lines


def _ready_and_gold():
    summary = reconcile_lines(event_lines())
    customers, errors = validate_customer_snapshot(customer_lines())
    assert errors == []
    gold = gold_from_states(summary["states"], customers)
    return summary, customers, gold, ready_event_dicts(summary["states"])


def _question_rows():
    summary, customers, gold, ready = _ready_and_gold()
    customers_by_id = {customer.customer_id: customer for customer in customers}

    q1 = sorted(
        (
            (
                row["product_version"],
                row["scan_count"],
                row["failed_scan_count"],
                row["scan_failure_rate"],
            )
            for row in gold["product_version_day"]
            if row["event_date"] == AS_OF
        ),
        key=lambda row: (-1 if row[3] is None else -row[3], -row[1], row[0]),
    )

    by_version: dict[str, list[dict]] = {}
    for row in gold["product_version_day"]:
        by_version.setdefault(row["product_version"], []).append(row)
    q2 = []
    for version, rows in sorted(by_version.items()):
        rows = sorted(rows, key=lambda item: item["event_date"])
        for index, row in enumerate(rows):
            if row["event_date"] != AS_OF:
                continue
            prior = rows[index - 1] if index else None
            q2.append(
                (
                    version,
                    row["event_date"],
                    row["scan_count"],
                    row["failed_scan_count"],
                    row["scan_failure_rate"],
                    None if prior is None else prior["event_date"],
                    None if prior is None else prior["scan_failure_rate"],
                    None if prior is None else row["scan_failure_rate"] - prior["scan_failure_rate"],
                )
            )

    window_start = AS_OF - timedelta(days=6)
    totals: dict[str, list[int]] = {}
    for row in gold["product_version_day"]:
        if window_start <= row["event_date"] <= AS_OF:
            bucket = totals.setdefault(row["product_version"], [0, 0])
            bucket[0] += row["scan_count"]
            bucket[1] += row["failed_scan_count"]
    q3 = []
    for version, (scans, failed) in totals.items():
        q3.append((version, scans, failed, None if scans == 0 else failed / scans, 7))
    q3.sort(key=lambda row: (-1 if row[3] is None else -row[3], -row[1], row[0]))

    week_start = AS_OF - timedelta(days=AS_OF.weekday())
    week_end = week_start + timedelta(days=6)
    cohort: dict[tuple[str, str], set] = {}
    for event in ready:
        if not (week_start <= event["event_date"] <= week_end):
            continue
        customer = customers_by_id.get(event["customer_id"])
        signup = "UNKNOWN" if customer is None else customer.signup_month
        plan = "UNKNOWN" if customer is None else customer.plan_tier
        cohort.setdefault((signup, plan), set()).add((event["customer_id"], event["device_id"]))
    q4 = [
        (
            signup,
            plan,
            len({customer_id for customer_id, _device in pairs}),
            len(pairs),
            week_start,
            week_end,
        )
        for (signup, plan), pairs in sorted(cohort.items())
    ]

    failed: dict[tuple[str, str], int] = {}
    for row in gold["device_day"]:
        key = (row["customer_id"], row["device_id"])
        failed[key] = failed.get(key, 0) + row["failed_scan_count"]
    ranked = sorted(failed.items(), key=lambda item: (-item[1], item[0][0], item[0][1]))
    q5 = [(index, customer_id, device_id, count) for index, ((customer_id, device_id), count) in enumerate(ranked[:5], start=1)]

    q6 = [
        (
            summary["raw_rows"],
            summary["invalid_rows"],
            summary["valid_rows"],
            summary["duplicate_delivery_rows"],
            summary["ledger_versions"],
            summary["superseded_versions"],
            summary["conflict_events"],
            summary["ready_events"],
        )
    ]
    return {"q1_version_failures_today": q1, "q2_failure_rate_vs_prior_observed_day": q2, "q3_seven_calendar_day_failure_rate": q3, "q4_signup_cohorts_active_this_week": q4, "q5_devices_with_most_failed_scans": q5, "q6_raw_rows_to_current_events": q6}


def test_sql_file_has_six_questions_and_expected_tables():
    queries = load_queries()
    assert len(queries) == 6
    assert [query["name"] for query in queries] == list(_question_rows())
    for query in queries:
        assert query["question"]
        assert query["sql"]
        assert query["expected"]
        assert query["columns"]


def test_tiny_fixture_matches_published_expected_tables():
    published = {query["name"]: query["expected"] for query in load_queries()}
    actual = _question_rows()
    for name, rows in actual.items():
        assert len(rows) == len(published[name])
        for got, expected in zip(rows, published[name]):
            assert len(got) == len(expected)
            assert all(values_close(left, right) for left, right in zip(got, expected)), (name, got, expected)


def test_seven_day_window_is_not_a_seven_row_window():
    _summary, _customers, gold, ready = _ready_and_gold()
    window_start = AS_OF - timedelta(days=6)
    scans = sum(
        row["scan_count"]
        for row in gold["product_version_day"]
        if row["product_version"] == "1.0.0" and window_start <= row["event_date"] <= AS_OF
    )
    including_july_1 = sum(
        row["scan_count"] for row in gold["product_version_day"] if row["product_version"] == "1.0.0"
    )
    assert scans == 3
    assert including_july_1 == 5
    july_1 = date(2026, 7, 1)
    assert distinct_active_devices(ready, july_1) == 3
    assert summed_version_active_devices(build_product_version_day(ready), july_1) == 4


def test_prior_observed_day_skips_the_calendar_gap():
    rows = dict((row[0], row) for row in _question_rows()["q2_failure_rate_vs_prior_observed_day"])
    assert rows["1.0.0"][5] == date(2026, 7, 2)
    assert rows["1.0.0"][5] != date(2026, 7, 7)
