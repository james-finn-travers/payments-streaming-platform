"""
Data Quality Checks DAG

Runs every hour against MongoDB to detect:
1. Schema violations (missing/extra fields, wrong types)
2. Volume anomalies (too few or too many transactions vs rolling average)
3. Null/empty field checks on critical columns
4. Status distribution drift (approved rate deviates from expected 90%)
5. Stale data detection (no new records within threshold)
6. Duplicate transaction IDs
7. Amount outliers (negative values, unreasonably large amounts)

Alerts are logged as critical errors. In production, swap the alert task
for a Slack/PagerDuty/email operator.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger("data-quality")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MONGO_URI = "mongodb://mongo:27017"
MONGO_DB = "txn_analytics"
TRANSACTIONS_COLLECTION = "transactions"
ANOMALIES_COLLECTION = "anomalies"

EXPECTED_SCHEMA_FIELDS = {
    "txn_id": str,
    "user_id": str,
    "amount": (int, float),
    "currency": str,
    "timestamp": str,
    "merchant_id": str,
    "merchant_category": str,
    "payment_method": str,
    "status": str,
    "location": dict,
}

REQUIRED_NOT_NULL_FIELDS = [
    "txn_id",
    "user_id",
    "amount",
    "currency",
    "timestamp",
    "merchant_id",
    "status",
]

VALID_CURRENCIES = {"CAD", "USD", "EUR", "GBP"}
VALID_STATUSES = {"approved", "declined", "pending"}
VALID_PAYMENT_METHODS = {
    "credit_card",
    "debit_card",
    "e_transfer",
    "apple_pay",
    "google_pay",
}

# Volume: flag if hourly count is outside this factor of the 24h rolling avg
VOLUME_DEVIATION_FACTOR = 3.0

# Status drift: flag if approved rate falls outside this range
APPROVED_RATE_MIN = 0.80
APPROVED_RATE_MAX = 0.96

# Staleness: flag if no new records in this many minutes
STALENESS_THRESHOLD_MINUTES = 15

# Amount bounds
AMOUNT_MIN = 0.01
AMOUNT_MAX = 50_000.00

# How many sample violations to include in logs
MAX_SAMPLE_VIOLATIONS = 10


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _get_collection(collection_name: str):
    """Return a pymongo collection handle."""
    from pymongo import MongoClient

    client = MongoClient(MONGO_URI)
    return client[MONGO_DB][collection_name]


def _time_range_filter(hours: int = 1) -> dict:
    """MongoDB filter for documents created in the last N hours."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    return {"timestamp": {"$gte": cutoff}}


def _raise_quality_alert(check_name: str, details: str):
    """
    Central alert handler. Logs critical and raises so Airflow marks
    the task as failed. Replace with Slack/PagerDuty in production.
    """
    msg = f"DATA QUALITY ALERT [{check_name}]: {details}"
    logger.critical(msg)
    raise ValueError(msg)


# ---------------------------------------------------------------------------
# Check 1 — Schema violations
# ---------------------------------------------------------------------------
def check_schema_violations(**kwargs):
    """
    Sample recent transactions and verify every document has the expected
    fields with the correct types. Flags missing fields, extra fields,
    and type mismatches.
    """
    col = _get_collection(TRANSACTIONS_COLLECTION)
    recent = list(col.find(_time_range_filter(1)).limit(1000))

    if not recent:
        logger.warning("Schema check: no recent documents found; skipping.")
        return

    violations = []
    for doc in recent:
        txn_id = doc.get("txn_id", doc.get("_id"))

        # Missing fields
        for field in EXPECTED_SCHEMA_FIELDS:
            if field not in doc:
                violations.append(f"{txn_id}: missing field '{field}'")

        # Extra fields (excluding Mongo _id and enrichment fields)
        allowed_extras = {"_id", "enriched_at", "mean_amount", "txn_count", "z_score"}
        extra = set(doc.keys()) - set(EXPECTED_SCHEMA_FIELDS.keys()) - allowed_extras
        for field in extra:
            violations.append(f"{txn_id}: unexpected field '{field}'")

        # Type checks
        for field, expected_type in EXPECTED_SCHEMA_FIELDS.items():
            if field in doc and not isinstance(doc[field], expected_type):
                violations.append(
                    f"{txn_id}: field '{field}' has type "
                    f"{type(doc[field]).__name__}, expected {expected_type}"
                )

    if violations:
        sample = violations[:MAX_SAMPLE_VIOLATIONS]
        _raise_quality_alert(
            "SCHEMA_VIOLATION",
            f"{len(violations)} violations in {len(recent)} docs. "
            f"Samples: {sample}",
        )

    logger.info("Schema check PASSED — %d documents OK.", len(recent))


# ---------------------------------------------------------------------------
# Check 2 — Volume anomalies
# ---------------------------------------------------------------------------
def check_volume_anomalies(**kwargs):
    """
    Compare the last hour's transaction count against the 24h rolling
    hourly average. Flag if it deviates by more than VOLUME_DEVIATION_FACTOR.
    """
    col = _get_collection(TRANSACTIONS_COLLECTION)

    last_hour_count = col.count_documents(_time_range_filter(1))
    last_24h_count = col.count_documents(_time_range_filter(24))
    rolling_avg = last_24h_count / 24.0 if last_24h_count > 0 else 0

    logger.info(
        "Volume check: last_hour=%d, 24h_total=%d, hourly_avg=%.1f",
        last_hour_count,
        last_24h_count,
        rolling_avg,
    )

    if rolling_avg == 0:
        if last_hour_count == 0:
            logger.warning("Volume check: no data in the last 24 hours.")
            return
        # First data appearing — skip deviation check
        logger.info("Volume check: first data detected, skipping deviation.")
        return

    ratio = last_hour_count / rolling_avg

    if ratio > VOLUME_DEVIATION_FACTOR:
        _raise_quality_alert(
            "VOLUME_SPIKE",
            f"Last hour count ({last_hour_count}) is {ratio:.1f}x the "
            f"rolling average ({rolling_avg:.0f}). Possible duplicate ingestion.",
        )

    if ratio < (1.0 / VOLUME_DEVIATION_FACTOR):
        _raise_quality_alert(
            "VOLUME_DROP",
            f"Last hour count ({last_hour_count}) is only {ratio:.2f}x the "
            f"rolling average ({rolling_avg:.0f}). Possible pipeline outage.",
        )

    logger.info("Volume check PASSED — ratio %.2f within bounds.", ratio)


# ---------------------------------------------------------------------------
# Check 3 — Null / empty field checks
# ---------------------------------------------------------------------------
def check_null_fields(**kwargs):
    """
    Count documents where any required field is null, empty string, or missing.
    """
    col = _get_collection(TRANSACTIONS_COLLECTION)

    null_conditions = [
        {"$or": [{field: None}, {field: ""}, {field: {"$exists": False}}]}
        for field in REQUIRED_NOT_NULL_FIELDS
    ]

    total_nulls = 0
    field_counts = {}

    for field, condition in zip(REQUIRED_NOT_NULL_FIELDS, null_conditions):
        # Only check recent data to keep fast
        combined = {"$and": [_time_range_filter(1), condition]}
        count = col.count_documents(combined)
        if count > 0:
            field_counts[field] = count
            total_nulls += count

    if total_nulls > 0:
        _raise_quality_alert(
            "NULL_FIELDS",
            f"{total_nulls} null/empty values in required fields "
            f"(last hour): {field_counts}",
        )

    logger.info("Null field check PASSED — all required fields populated.")


# ---------------------------------------------------------------------------
# Check 4 — Status distribution drift
# ---------------------------------------------------------------------------
def check_status_distribution(**kwargs):
    """
    Verify the approved/declined/pending ratio hasn't drifted from expected
    distribution. Catches upstream bugs or fraud spikes.
    """
    col = _get_collection(TRANSACTIONS_COLLECTION)

    pipeline = [
        {"$match": _time_range_filter(6)},
        {"$group": {"_id": "$status", "count": {"$sum": 1}}},
    ]
    results = {doc["_id"]: doc["count"] for doc in col.aggregate(pipeline)}
    total = sum(results.values())

    if total == 0:
        logger.warning("Status distribution check: no data in last 6 hours.")
        return

    # Check for invalid statuses
    invalid = set(results.keys()) - VALID_STATUSES
    if invalid:
        _raise_quality_alert(
            "INVALID_STATUS",
            f"Found unexpected status values: {invalid} "
            f"(counts: {results})",
        )

    approved_count = results.get("approved", 0)
    approved_rate = approved_count / total

    logger.info(
        "Status distribution (6h): %s — approved rate: %.2f%%",
        results,
        approved_rate * 100,
    )

    if approved_rate < APPROVED_RATE_MIN or approved_rate > APPROVED_RATE_MAX:
        _raise_quality_alert(
            "STATUS_DRIFT",
            f"Approved rate {approved_rate:.2%} outside expected range "
            f"[{APPROVED_RATE_MIN:.0%}, {APPROVED_RATE_MAX:.0%}]. "
            f"Distribution: {results}",
        )

    logger.info("Status distribution check PASSED.")


# ---------------------------------------------------------------------------
# Check 5 — Stale data detection
# ---------------------------------------------------------------------------
def check_staleness(**kwargs):
    """
    Verify we received at least one transaction within the staleness window.
    Detects silent pipeline failures.
    """
    col = _get_collection(TRANSACTIONS_COLLECTION)

    cutoff = (
        datetime.now(timezone.utc)
        - timedelta(minutes=STALENESS_THRESHOLD_MINUTES)
    ).isoformat()

    recent_count = col.count_documents({"timestamp": {"$gte": cutoff}})

    if recent_count == 0:
        _raise_quality_alert(
            "STALE_DATA",
            f"No new transactions in the last {STALENESS_THRESHOLD_MINUTES} "
            f"minutes. Pipeline may be down.",
        )

    logger.info(
        "Staleness check PASSED — %d records in last %d min.",
        recent_count,
        STALENESS_THRESHOLD_MINUTES,
    )


# ---------------------------------------------------------------------------
# Check 6 — Duplicate transaction IDs
# ---------------------------------------------------------------------------
def check_duplicates(**kwargs):
    """
    Look for duplicate txn_id values in the last hour.
    Idempotent IDs should be unique.
    """
    col = _get_collection(TRANSACTIONS_COLLECTION)

    pipeline = [
        {"$match": _time_range_filter(1)},
        {"$group": {"_id": "$txn_id", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": MAX_SAMPLE_VIOLATIONS},
    ]
    dupes = list(col.aggregate(pipeline))

    if dupes:
        total_dupes = sum(d["count"] for d in dupes) - len(dupes)
        sample = [{"txn_id": d["_id"], "count": d["count"]} for d in dupes]
        _raise_quality_alert(
            "DUPLICATE_TXN_IDS",
            f"{total_dupes} duplicate records found. "
            f"Top offenders: {sample}",
        )

    logger.info("Duplicate check PASSED — no duplicate txn_ids in last hour.")


# ---------------------------------------------------------------------------
# Check 7 — Amount outliers
# ---------------------------------------------------------------------------
def check_amount_outliers(**kwargs):
    """
    Flag transactions with negative amounts or amounts exceeding the
    configured maximum. Also checks for exact-zero amounts.
    """
    col = _get_collection(TRANSACTIONS_COLLECTION)
    time_filter = _time_range_filter(1)

    # Negative or zero amounts
    bad_amounts = col.count_documents(
        {"$and": [time_filter, {"amount": {"$lte": 0}}]}
    )

    # Exceeds maximum
    over_max = col.count_documents(
        {"$and": [time_filter, {"amount": {"$gt": AMOUNT_MAX}}]}
    )

    # Below minimum (but positive)
    under_min = col.count_documents(
        {"$and": [
            time_filter,
            {"amount": {"$gt": 0}},
            {"amount": {"$lt": AMOUNT_MIN}},
        ]}
    )

    issues = []
    if bad_amounts > 0:
        issues.append(f"{bad_amounts} transactions with amount <= 0")
    if over_max > 0:
        issues.append(f"{over_max} transactions with amount > {AMOUNT_MAX}")
    if under_min > 0:
        issues.append(f"{under_min} transactions with amount < {AMOUNT_MIN}")

    if issues:
        _raise_quality_alert("AMOUNT_OUTLIER", "; ".join(issues))

    logger.info("Amount outlier check PASSED.")


# ---------------------------------------------------------------------------
# Check 8 — Accepted values validation
# ---------------------------------------------------------------------------
def check_accepted_values(**kwargs):
    """
    Verify currency, payment_method, and status fields only contain
    known values. Catches schema evolution or producer bugs.
    """
    col = _get_collection(TRANSACTIONS_COLLECTION)
    time_filter = _time_range_filter(1)

    checks = {
        "currency": VALID_CURRENCIES,
        "payment_method": VALID_PAYMENT_METHODS,
        "status": VALID_STATUSES,
    }

    violations = []
    for field, valid_set in checks.items():
        pipeline = [
            {"$match": time_filter},
            {"$group": {"_id": f"${field}"}},
        ]
        found = {doc["_id"] for doc in col.aggregate(pipeline)}
        invalid = found - valid_set
        if invalid:
            violations.append(f"{field}: unexpected values {invalid}")

    if violations:
        _raise_quality_alert("INVALID_VALUES", "; ".join(violations))

    logger.info("Accepted values check PASSED.")


# ---------------------------------------------------------------------------
# Summary task
# ---------------------------------------------------------------------------
def log_quality_summary(**kwargs):
    """Log that all checks completed (only reached if all upstream pass)."""
    logger.info(
        "All data quality checks PASSED at %s",
        datetime.now(timezone.utc).isoformat(),
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_quality_checks",
    default_args=default_args,
    description="Hourly data quality checks on MongoDB transaction data",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["data-quality", "monitoring"],
) as dag:

    schema_check = PythonOperator(
        task_id="check_schema_violations",
        python_callable=check_schema_violations,
    )

    volume_check = PythonOperator(
        task_id="check_volume_anomalies",
        python_callable=check_volume_anomalies,
    )

    null_check = PythonOperator(
        task_id="check_null_fields",
        python_callable=check_null_fields,
    )

    status_check = PythonOperator(
        task_id="check_status_distribution",
        python_callable=check_status_distribution,
    )

    staleness_check = PythonOperator(
        task_id="check_staleness",
        python_callable=check_staleness,
    )

    duplicate_check = PythonOperator(
        task_id="check_duplicates",
        python_callable=check_duplicates,
    )

    amount_check = PythonOperator(
        task_id="check_amount_outliers",
        python_callable=check_amount_outliers,
    )

    accepted_values_check = PythonOperator(
        task_id="check_accepted_values",
        python_callable=check_accepted_values,
    )

    summary = PythonOperator(
        task_id="log_quality_summary",
        python_callable=log_quality_summary,
        trigger_rule="all_success",
    )

    # All checks run in parallel, then converge on the summary
    [
        schema_check,
        volume_check,
        null_check,
        status_check,
        staleness_check,
        duplicate_check,
        amount_check,
        accepted_values_check,
    ] >> summary