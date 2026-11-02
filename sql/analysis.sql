-- Analytical questions for the security telemetry marts.
-- Spark SQL. The runner must create temp view analysis_params(as_of_date DATE).
--
-- Expected tables below are for the tiny fixture in tests/tiny_fixture.py
-- with as_of_date = 2026-07-08. They are ratio-of-sums, not averages of rates.
-- scan_failure_rate = failed_scan_count / scan_count, null when scan_count is 0.
-- A seven-row window is not seven calendar days: 1.0.0 has a gap between
-- July 2 and July 8, and July 1 is outside the seven-day window.
-- Signup-cohort activity is not retention; there is no eligibility denominator.
-- Do not sum version-level active_device_count into a global distinct total.

-- QUERY: q1_version_failures_today
-- QUESTION: Which product versions show more scan failures today?
-- Report scan volume beside the rate. Do not average customer-level percentages.
-- EXPECTED_COLUMNS: product_version, scan_count, failed_scan_count, scan_failure_rate
-- EXPECTED: 1.1.0 | 2 | 2 | 1
-- EXPECTED: 1.0.0 | 1 | 0 | 0
SELECT
    product_version,
    SUM(scan_count) AS scan_count,
    SUM(failed_scan_count) AS failed_scan_count,
    CASE
        WHEN SUM(scan_count) = 0 THEN NULL
        ELSE CAST(SUM(failed_scan_count) AS DOUBLE) / SUM(scan_count)
    END AS scan_failure_rate
FROM gold_product_version_day
WHERE event_date = (SELECT as_of_date FROM analysis_params)
GROUP BY product_version
ORDER BY scan_failure_rate DESC, scan_count DESC, product_version ASC;

-- QUERY: q2_failure_rate_vs_prior_observed_day
-- QUESTION: How does today's failure rate compare with the prior observed day?
-- LAG is over observed version-days, not the previous calendar day.
-- EXPECTED_COLUMNS: product_version, event_date, scan_count, failed_scan_count, scan_failure_rate, prior_observed_date, prior_observed_rate, rate_delta
-- EXPECTED: 1.0.0 | 2026-07-08 | 1 | 0 | 0 | 2026-07-02 | 0.5 | -0.5
-- EXPECTED: 1.1.0 | 2026-07-08 | 2 | 2 | 1 | 2026-07-01 | 1 | 0
WITH daily AS (
    SELECT
        event_date,
        product_version,
        SUM(scan_count) AS scan_count,
        SUM(failed_scan_count) AS failed_scan_count,
        CASE
            WHEN SUM(scan_count) = 0 THEN NULL
            ELSE CAST(SUM(failed_scan_count) AS DOUBLE) / SUM(scan_count)
        END AS scan_failure_rate
    FROM gold_product_version_day
    GROUP BY event_date, product_version
),
lagged AS (
    SELECT
        daily.*,
        LAG(event_date) OVER (PARTITION BY product_version ORDER BY event_date) AS prior_observed_date,
        LAG(scan_failure_rate) OVER (PARTITION BY product_version ORDER BY event_date) AS prior_observed_rate
    FROM daily
)
SELECT
    product_version,
    event_date,
    scan_count,
    failed_scan_count,
    scan_failure_rate,
    prior_observed_date,
    prior_observed_rate,
    scan_failure_rate - prior_observed_rate AS rate_delta
FROM lagged
WHERE event_date = (SELECT as_of_date FROM analysis_params)
ORDER BY product_version;

-- QUERY: q3_seven_calendar_day_failure_rate
-- QUESTION: What is the seven-calendar-day scan failure rate?
-- The spine is seven dates ending on as_of_date. Rates use sums over that
-- date predicate, so missing days contribute nothing and are not dropped
-- from the window just because no row exists.
-- EXPECTED_COLUMNS: product_version, scan_count, failed_scan_count, scan_failure_rate, window_day_count
-- EXPECTED: 1.1.0 | 2 | 2 | 1 | 7
-- EXPECTED: 1.0.0 | 3 | 1 | 0.333333333333 | 7
-- EXPECTED: 1.2.0 | 1 | 0 | 0 | 7
WITH params AS (
    SELECT as_of_date, date_add(as_of_date, -6) AS window_start
    FROM analysis_params
),
spine AS (
    SELECT explode(sequence(window_start, as_of_date, interval 1 day)) AS spine_date
    FROM params
),
daily AS (
    SELECT
        event_date,
        product_version,
        SUM(scan_count) AS scan_count,
        SUM(failed_scan_count) AS failed_scan_count
    FROM gold_product_version_day
    GROUP BY event_date, product_version
)
SELECT
    daily.product_version,
    SUM(daily.scan_count) AS scan_count,
    SUM(daily.failed_scan_count) AS failed_scan_count,
    CASE
        WHEN SUM(daily.scan_count) = 0 THEN NULL
        ELSE CAST(SUM(daily.failed_scan_count) AS DOUBLE) / SUM(daily.scan_count)
    END AS scan_failure_rate,
    (SELECT COUNT(*) FROM spine) AS window_day_count
FROM params
JOIN daily
  ON daily.event_date BETWEEN params.window_start AND params.as_of_date
GROUP BY daily.product_version
ORDER BY scan_failure_rate DESC, scan_count DESC, daily.product_version ASC;

-- QUERY: q4_signup_cohorts_active_this_week
-- QUESTION: Which signup cohorts are active this week?
-- One-to-one customer join. Counts are distinct customers and devices.
-- Activity in the ISO week (Monday through Sunday) is not retention.
-- EXPECTED_COLUMNS: signup_month, plan_tier, active_customer_count, active_device_count, week_start, week_end
-- EXPECTED: 2026-01 | premium | 1 | 1 | 2026-07-06 | 2026-07-12
-- EXPECTED: 2026-03 | free | 1 | 1 | 2026-07-06 | 2026-07-12
WITH bounds AS (
    SELECT
        date_sub(as_of_date, pmod(dayofweek(as_of_date) + 5, 7)) AS week_start,
        date_add(date_sub(as_of_date, pmod(dayofweek(as_of_date) + 5, 7)), 6) AS week_end
    FROM analysis_params
)
SELECT
    COALESCE(customers.signup_month, 'UNKNOWN') AS signup_month,
    COALESCE(customers.plan_tier, 'UNKNOWN') AS plan_tier,
    COUNT(DISTINCT events.customer_id) AS active_customer_count,
    COUNT(DISTINCT events.customer_id, events.device_id) AS active_device_count,
    bounds.week_start,
    bounds.week_end
FROM silver_events events
LEFT JOIN dim_customer customers
  ON events.customer_id = customers.customer_id
CROSS JOIN bounds
WHERE events.event_date BETWEEN bounds.week_start AND bounds.week_end
GROUP BY
    COALESCE(customers.signup_month, 'UNKNOWN'),
    COALESCE(customers.plan_tier, 'UNKNOWN'),
    bounds.week_start,
    bounds.week_end
ORDER BY signup_month, plan_tier;

-- QUERY: q5_devices_with_most_failed_scans
-- QUESTION: Which five customer/device pairs have the most failed scans?
-- Ties break by customer_id, then device_id.
-- EXPECTED_COLUMNS: rank, customer_id, device_id, failed_scan_count
-- EXPECTED: 1 | c1 | d1 | 2
-- EXPECTED: 2 | c2 | d1 | 2
-- EXPECTED: 3 | c1 | d2 | 1
-- EXPECTED: 4 | cust-x | d9 | 0
SELECT rank, customer_id, device_id, failed_scan_count
FROM (
    SELECT
        customer_id,
        device_id,
        SUM(failed_scan_count) AS failed_scan_count,
        ROW_NUMBER() OVER (
            ORDER BY SUM(failed_scan_count) DESC, customer_id ASC, device_id ASC
        ) AS rank
    FROM gold_device_day
    GROUP BY customer_id, device_id
) ranked
WHERE rank <= 5
ORDER BY rank;

-- QUERY: q6_raw_rows_to_current_events
-- QUESTION: How many raw rows became current analytical events?
-- Validity, repeated deliveries, superseded revisions, conflicts, and current
-- READY events are separate counts. Do not expect valid raw rows to equal
-- READY events.
-- EXPECTED_COLUMNS: raw_rows, invalid_rows, valid_rows, duplicate_delivery_rows, ledger_versions, superseded_versions, conflict_events, ready_events
-- EXPECTED: 14 | 1 | 13 | 1 | 12 | 1 | 0 | 11
SELECT
    (SELECT COUNT(*) FROM bronze_events) AS raw_rows,
    (SELECT COUNT(*) FROM bronze_row_classification WHERE status = 'invalid') AS invalid_rows,
    (SELECT COUNT(*) FROM bronze_row_classification WHERE status = 'valid') AS valid_rows,
    (
        SELECT COUNT(*) FROM bronze_row_classification WHERE status = 'valid'
    ) - (
        SELECT COUNT(*) FROM (
            SELECT event_id, event_version, payload_hash
            FROM bronze_row_classification
            WHERE status = 'valid'
            GROUP BY event_id, event_version, payload_hash
        )
    ) AS duplicate_delivery_rows,
    (SELECT COUNT(*) FROM silver_event_versions) AS ledger_versions,
    (
        SELECT COUNT(*)
        FROM silver_event_versions versions
        JOIN silver_current current
          ON versions.event_id = current.event_id
        WHERE versions.event_version < current.event_version
    ) AS superseded_versions,
    (SELECT COUNT(*) FROM silver_current WHERE state = 'CONFLICT') AS conflict_events,
    (SELECT COUNT(*) FROM silver_events) AS ready_events;
