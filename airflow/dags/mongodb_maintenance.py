"""
MongoDB Maintenance DAG

Runs daily at 03:00 UTC to perform routine database upkeep:

Phase 1 — TTL Cleanup
  1. Purge transactions older than the retention window (default 90 days)
  2. Purge anomalies older than the retention window
  3. Remove orphan anomalies whose txn_id has no matching transaction

Phase 2 — Index Health
  4. Collect and log collStats for each collection
  5. Verify expected indexes exist; rebuild any that are missing
  6. Run the compact command on each collection (reclaims disk space)

Phase 3 — Performance Checks
  7. Detect slow queries from the system.profile collection
  8. Check connection pool utilisation via serverStatus

Phase 4 — Summary
  9. Log an overall maintenance summary

Alerts are logged as warnings/errors. Swap for Slack/PagerDuty in prod.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger("mongodb-maintenance")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MONGO_URI = "mongodb://mongo:27017"
MONGO_DB = "txn_analytics"

COLLECTIONS = {
    "transactions": {
        "retention_days": 90,
        "expected_indexes": [
            [("txn_id", 1)],
            [("merchant_id", 1)],
            [("merchant_category", 1)],
        ],
    },
    "anomalies": {
        "retention_days": 90,
        "expected_indexes": [
            [("txn_id", 1)],
            [("z_score", 1)],
        ],
    },
}

# Slow-query threshold in milliseconds
SLOW_QUERY_THRESHOLD_MS = 200

# Connection pool warning threshold (fraction of max connections)
CONNECTION_POOL_WARN_RATIO = 0.80

# Max orphan anomalies to delete per run (safety cap)
ORPHAN_DELETE_LIMIT = 10_000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _get_client():
    """Return a fresh MongoClient."""
    from pymongo import MongoClient

    return MongoClient(MONGO_URI)


def _get_db():
    client = _get_client()
    return client, client[MONGO_DB]


# ═══════════════════════════════════════════════════════════════════════════
# Phase 1 — TTL Cleanup
# ═══════════════════════════════════════════════════════════════════════════
def purge_old_transactions(**kwargs):
    """Delete transactions older than the retention window."""
    cfg = COLLECTIONS["transactions"]
    cutoff = datetime.now(timezone.utc) - timedelta(days=cfg["retention_days"])

    client, db = _get_db()
    try:
        col = db["transactions"]
        result = col.delete_many({"timestamp": {"$lt": cutoff.isoformat()}})
        logger.info(
            "Purged %d transactions older than %s (retention=%d days).",
            result.deleted_count,
            cutoff.date(),
            cfg["retention_days"],
        )
    finally:
        client.close()


def purge_old_anomalies(**kwargs):
    """Delete anomalies older than the retention window."""
    cfg = COLLECTIONS["anomalies"]
    cutoff = datetime.now(timezone.utc) - timedelta(days=cfg["retention_days"])

    client, db = _get_db()
    try:
        col = db["anomalies"]
        result = col.delete_many({"timestamp": {"$lt": cutoff.isoformat()}})
        logger.info(
            "Purged %d anomalies older than %s (retention=%d days).",
            result.deleted_count,
            cutoff.date(),
            cfg["retention_days"],
        )
    finally:
        client.close()


def remove_orphan_anomalies(**kwargs):
    """
    Delete anomalies whose txn_id doesn't exist in the transactions collection.
    Uses a lookup pipeline to find orphans efficiently.
    """
    client, db = _get_db()
    try:
        anomalies_col = db["anomalies"]
        pipeline = [
            {
                "$lookup": {
                    "from": "transactions",
                    "localField": "txn_id",
                    "foreignField": "txn_id",
                    "as": "_matched",
                }
            },
            {"$match": {"_matched": {"$size": 0}}},
            {"$limit": ORPHAN_DELETE_LIMIT},
            {"$project": {"_id": 1, "txn_id": 1}},
        ]
        orphans = list(anomalies_col.aggregate(pipeline))

        if not orphans:
            logger.info("No orphan anomalies found.")
            return

        orphan_ids = [doc["_id"] for doc in orphans]
        result = anomalies_col.delete_many({"_id": {"$in": orphan_ids}})
        logger.warning(
            "Removed %d orphan anomalies (txn_ids no longer in transactions).",
            result.deleted_count,
        )
    finally:
        client.close()


# ═══════════════════════════════════════════════════════════════════════════
# Phase 2 — Index Health
# ═══════════════════════════════════════════════════════════════════════════
def collect_collection_stats(**kwargs):
    """Log collStats (size, count, avgObjSize, storageSize) for each collection."""
    client, db = _get_db()
    try:
        for col_name in COLLECTIONS:
            try:
                stats = db.command("collStats", col_name)
                logger.info(
                    "collStats [%s]: count=%s  size=%s bytes  "
                    "avgObjSize=%s  storageSize=%s  nindexes=%s",
                    col_name,
                    stats.get("count", "N/A"),
                    stats.get("size", "N/A"),
                    stats.get("avgObjSize", "N/A"),
                    stats.get("storageSize", "N/A"),
                    stats.get("nindexes", "N/A"),
                )
            except Exception as exc:
                logger.error("Failed to get collStats for %s: %s", col_name, exc)
    finally:
        client.close()


def verify_indexes(**kwargs):
    """
    For each collection, ensure the expected indexes exist.
    Recreate any that are missing.
    """
    client, db = _get_db()
    try:
        for col_name, cfg in COLLECTIONS.items():
            col = db[col_name]
            existing_indexes = col.index_information()

            # Build a set of existing index key tuples for comparison
            existing_keys = set()
            for idx_info in existing_indexes.values():
                key_tuple = tuple(tuple(k) for k in idx_info["key"])
                existing_keys.add(key_tuple)

            for expected in cfg["expected_indexes"]:
                expected_tuple = tuple(tuple(k) for k in expected)
                if expected_tuple not in existing_keys:
                    logger.warning(
                        "Missing index on %s: %s — recreating …",
                        col_name,
                        expected,
                    )
                    col.create_index(expected)
                    logger.info("Recreated index on %s: %s", col_name, expected)

            logger.info(
                "Index verification for %s complete — %d indexes present.",
                col_name,
                len(existing_indexes),
            )
    finally:
        client.close()


def compact_collections(**kwargs):
    """
    Run the ``compact`` command on each collection to reclaim disk space
    from deleted documents. This is a blocking operation.
    """
    client, db = _get_db()
    try:
        for col_name in COLLECTIONS:
            try:
                result = db.command({"compact": col_name})
                logger.info(
                    "Compact [%s]: ok=%s  bytesFreed=%s",
                    col_name,
                    result.get("ok"),
                    result.get("bytesFreed", "N/A"),
                )
            except Exception as exc:
                # compact may not be supported on time-series or sharded
                logger.warning(
                    "Compact failed for %s (may be expected for "
                    "time-series collections): %s",
                    col_name,
                    exc,
                )
    finally:
        client.close()


# ═══════════════════════════════════════════════════════════════════════════
# Phase 3 — Performance Checks
# ═══════════════════════════════════════════════════════════════════════════
def detect_slow_queries(**kwargs):
    """
    Query system.profile for operations exceeding the threshold.
    Requires profiling level >= 1 to be enabled on the database.
    """
    client, db = _get_db()
    try:
        profile_col = db["system.profile"]
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

        slow = list(
            profile_col.find(
                {
                    "millis": {"$gte": SLOW_QUERY_THRESHOLD_MS},
                    "ts": {"$gte": cutoff},
                }
            )
            .sort("millis", -1)
            .limit(20)
        )

        if slow:
            logger.warning(
                "Found %d slow queries (>%d ms) in the last 24h:",
                len(slow),
                SLOW_QUERY_THRESHOLD_MS,
            )
            for q in slow[:5]:
                logger.warning(
                    "  %s ms | ns=%s | op=%s | query=%s",
                    q.get("millis"),
                    q.get("ns"),
                    q.get("op"),
                    str(q.get("command", q.get("query", "")))[:200],
                )
        else:
            logger.info(
                "No slow queries (>%d ms) in the last 24h.",
                SLOW_QUERY_THRESHOLD_MS,
            )
    except Exception as exc:
        # system.profile may not exist if profiling is disabled
        logger.info("Could not read system.profile (profiling may be off): %s", exc)
    finally:
        client.close()


def check_connection_health(**kwargs):
    """
    Check current vs available connections via serverStatus.
    Warn if utilisation exceeds the threshold.
    """
    client, db = _get_db()
    try:
        status = db.command("serverStatus")
        connections = status.get("connections", {})
        current = connections.get("current", 0)
        available = connections.get("available", 1)
        total_created = connections.get("totalCreated", 0)

        total_capacity = current + available
        utilisation = current / total_capacity if total_capacity else 0

        logger.info(
            "Connections: current=%d  available=%d  totalCreated=%d  "
            "utilisation=%.1f%%",
            current,
            available,
            total_created,
            utilisation * 100,
        )

        if utilisation >= CONNECTION_POOL_WARN_RATIO:
            logger.warning(
                "Connection pool utilisation %.1f%% exceeds threshold %.0f%%. "
                "Consider increasing maxPoolSize or reducing open connections.",
                utilisation * 100,
                CONNECTION_POOL_WARN_RATIO * 100,
            )
    finally:
        client.close()


# ═══════════════════════════════════════════════════════════════════════════
# Phase 4 — Summary
# ═══════════════════════════════════════════════════════════════════════════
def log_maintenance_summary(**kwargs):
    """Log that all maintenance tasks completed."""
    logger.info(
        "MongoDB maintenance completed at %s.",
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
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="mongodb_maintenance",
    default_args=default_args,
    description="Daily MongoDB maintenance: TTL cleanup, index health, performance checks",
    schedule_interval="0 3 * * *",  # 03:00 UTC daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["maintenance", "mongodb"],
) as dag:

    # Phase 1 — TTL Cleanup (parallel)
    t_purge_txns = PythonOperator(
        task_id="purge_old_transactions",
        python_callable=purge_old_transactions,
    )
    t_purge_anomalies = PythonOperator(
        task_id="purge_old_anomalies",
        python_callable=purge_old_anomalies,
    )
    t_orphans = PythonOperator(
        task_id="remove_orphan_anomalies",
        python_callable=remove_orphan_anomalies,
    )

    # Phase 2 — Index Health (sequential: stats → verify → compact)
    t_stats = PythonOperator(
        task_id="collect_collection_stats",
        python_callable=collect_collection_stats,
    )
    t_indexes = PythonOperator(
        task_id="verify_indexes",
        python_callable=verify_indexes,
    )
    t_compact = PythonOperator(
        task_id="compact_collections",
        python_callable=compact_collections,
    )

    # Phase 3 — Performance Checks (parallel)
    t_slow = PythonOperator(
        task_id="detect_slow_queries",
        python_callable=detect_slow_queries,
    )
    t_connections = PythonOperator(
        task_id="check_connection_health",
        python_callable=check_connection_health,
    )

    # Phase 4 — Summary
    t_summary = PythonOperator(
        task_id="log_maintenance_summary",
        python_callable=log_maintenance_summary,
        trigger_rule="all_done",
    )

    # ── Dependencies ──────────────────────────────────────────────────
    # Phase 1: purge txns + anomalies in parallel, then orphan cleanup
    [t_purge_txns, t_purge_anomalies] >> t_orphans

    # Phase 2: after orphans done → stats → verify → compact
    t_orphans >> t_stats >> t_indexes >> t_compact

    # Phase 3: after compact → slow queries + connection check in parallel
    t_compact >> [t_slow, t_connections]

    # Phase 4: all converge on summary
    [t_slow, t_connections] >> t_summary
