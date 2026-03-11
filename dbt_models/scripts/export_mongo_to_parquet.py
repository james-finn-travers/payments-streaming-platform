"""
Export MongoDB collections to Parquet for dbt-duckdb.

Reads ``transactions`` and ``anomalies`` time-series collections, converts
them to Parquet files that DuckDB (and therefore dbt) can query directly.

Usage
-----
    python -m dbt_models.scripts.export_mongo_to_parquet

Environment variables
---------------------
    MONGO_URI          — MongoDB connection string (default: mongodb://localhost:27017)
    MONGO_DB           — Database name             (default: txn_analytics)
    EXPORT_DIR         — Output directory           (default: dbt_models/seeds/raw)
    EXPORT_LOOKBACK    — Days of data to export     (default: 90)
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pymongo import MongoClient

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "txn_analytics")
EXPORT_DIR = Path(os.getenv("EXPORT_DIR", "dbt_models/seeds/raw"))
LOOKBACK_DAYS = int(os.getenv("EXPORT_LOOKBACK", "90"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("mongo-export")


# ---------------------------------------------------------------------------
# Schema definitions for Arrow conversion
# ---------------------------------------------------------------------------
TXN_SCHEMA = pa.schema([
    ("txn_id", pa.string()),
    ("user_id", pa.string()),
    ("amount", pa.float64()),
    ("currency", pa.string()),
    ("timestamp", pa.timestamp("us", tz="UTC")),
    ("merchant_id", pa.string()),
    ("merchant_category", pa.string()),
    ("payment_method", pa.string()),
    ("status", pa.string()),
    ("location_lat", pa.float64()),
    ("location_lon", pa.float64()),
    ("user_txn_count", pa.int64()),
    ("user_running_mean", pa.float64()),
    ("z_score", pa.float64()),
    ("is_anomaly", pa.bool_()),
])

ANOMALY_SCHEMA = pa.schema([
    ("txn_id", pa.string()),
    ("user_id", pa.string()),
    ("amount", pa.float64()),
    ("currency", pa.string()),
    ("timestamp", pa.timestamp("us", tz="UTC")),
    ("merchant_id", pa.string()),
    ("merchant_category", pa.string()),
    ("payment_method", pa.string()),
    ("z_score", pa.float64()),
    ("user_running_mean", pa.float64()),
    ("user_txn_count", pa.int64()),
])


def _flatten_location(doc: dict) -> dict:
    """Flatten nested location object into top-level lat/lon columns."""
    loc = doc.pop("location", None) or {}
    doc["location_lat"] = loc.get("lat")
    doc["location_lon"] = loc.get("lon")
    return doc


def _clean_doc(doc: dict) -> dict:
    """Remove Mongo _id and flatten nested fields."""
    doc.pop("_id", None)
    return _flatten_location(doc)


def export_collection(
    db,
    collection_name: str,
    schema: pa.Schema,
    output_path: Path,
    cutoff: datetime,
) -> int:
    """Read docs from MongoDB, convert to Parquet, return row count."""
    col = db[collection_name]
    query = {"timestamp": {"$gte": cutoff}}
    cursor = col.find(query).sort("timestamp", 1)

    docs = [_clean_doc(doc) for doc in cursor]
    if not docs:
        logger.warning("No documents found in %s (cutoff=%s)", collection_name, cutoff)
        return 0

    # Build Arrow table — use schema to coerce types, fill missing cols
    columns = {}
    for field in schema:
        values = [doc.get(field.name) for doc in docs]
        columns[field.name] = values

    table = pa.table(columns, schema=schema)
    pq.write_table(table, str(output_path), compression="snappy")

    logger.info(
        "Exported %d rows from %s → %s (%.1f MB)",
        len(docs),
        collection_name,
        output_path,
        output_path.stat().st_size / 1_048_576,
    )
    return len(docs)


def main():
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    logger.info(
        "Exporting from %s (lookback=%d days, cutoff=%s)",
        MONGO_DB,
        LOOKBACK_DAYS,
        cutoff.isoformat(),
    )

    txn_rows = export_collection(
        db, "transactions", TXN_SCHEMA, EXPORT_DIR / "transactions.parquet", cutoff
    )
    anom_rows = export_collection(
        db, "anomalies", ANOMALY_SCHEMA, EXPORT_DIR / "anomalies.parquet", cutoff
    )

    client.close()
    logger.info("Export complete — %d transactions, %d anomalies", txn_rows, anom_rows)


if __name__ == "__main__":
    main()
