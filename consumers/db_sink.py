"""
Real-Time Customer Transaction Analytics Pipeline — MongoDB Sink

Kafka consumer that reads enriched transactions from ``transactions_enriched``
and anomalies from ``anomalies`` topics, then upserts them into MongoDB Atlas
time-series collections.
"""

import os
import sys
import json
import signal
import logging
from datetime import datetime, timezone

from kafka import KafkaConsumer
from pymongo import MongoClient, UpdateOne, ASCENDING
from pymongo.errors import BulkWriteError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ENRICHED_TOPIC = os.getenv("KAFKA_ENRICHED_TOPIC", "transactions_enriched")
ANOMALY_TOPIC = os.getenv("KAFKA_ANOMALY_TOPIC", "anomalies")
CONSUMER_GROUP = os.getenv("MONGO_CONSUMER_GROUP", "mongo-sink")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "txn_analytics")
TXN_COLLECTION = os.getenv("MONGO_TXN_COLLECTION", "transactions")
ANOMALY_COLLECTION = os.getenv("MONGO_ANOMALY_COLLECTION", "anomalies")

BATCH_SIZE = int(os.getenv("SINK_BATCH_SIZE", "500"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("mongo-sink")


# ---------------------------------------------------------------------------
# MongoDB helpers
# ---------------------------------------------------------------------------
def get_mongo_client() -> MongoClient:
    client = MongoClient(MONGO_URI)
    return client


def ensure_collections(db):
    """
    Create time-series collections and indexes if they don't exist.
    MongoDB 5.0+ time-series collections are optimised for temporal queries.
    """
    existing = db.list_collection_names()

    # Transactions time-series collection
    if TXN_COLLECTION not in existing:
        db.create_collection(
            TXN_COLLECTION,
            timeseries={
                "timeField": "timestamp",
                "metaField": "user_id",
                "granularity": "seconds",
            },
        )
        logger.info("Created time-series collection: %s", TXN_COLLECTION)

    # Anomalies time-series collection
    if ANOMALY_COLLECTION not in existing:
        db.create_collection(
            ANOMALY_COLLECTION,
            timeseries={
                "timeField": "timestamp",
                "metaField": "user_id",
                "granularity": "seconds",
            },
        )
        logger.info("Created time-series collection: %s", ANOMALY_COLLECTION)

    # Secondary indexes for fast queries
    db[TXN_COLLECTION].create_index([("txn_id", ASCENDING)], unique=True, sparse=True)
    db[TXN_COLLECTION].create_index([("merchant_id", ASCENDING)])
    db[TXN_COLLECTION].create_index([("merchant_category", ASCENDING)])
    db[ANOMALY_COLLECTION].create_index([("txn_id", ASCENDING)], unique=True, sparse=True)
    db[ANOMALY_COLLECTION].create_index([("z_score", ASCENDING)])


def _parse_record(raw_value: bytes) -> dict | None:
    try:
        record = json.loads(raw_value.decode("utf-8"))
        # Convert ISO timestamp string to datetime for MongoDB time-series
        if isinstance(record.get("timestamp"), str):
            record["timestamp"] = datetime.fromisoformat(record["timestamp"])
        return record
    except (json.JSONDecodeError, ValueError) as exc:
        logger.warning("Skipping malformed record: %s", exc)
        return None


def upsert_batch(collection, batch: list[dict]):
    """Idempotent upsert batch using txn_id as the natural key."""
    if not batch:
        return
    ops = [
        UpdateOne({"txn_id": doc["txn_id"]}, {"$set": doc}, upsert=True)
        for doc in batch
        if "txn_id" in doc
    ]
    try:
        result = collection.bulk_write(ops, ordered=False)
        logger.debug(
            "Upserted %d (matched=%d, modified=%d, inserted=%d)",
            len(ops),
            result.matched_count,
            result.modified_count,
            result.upserted_count,
        )
    except BulkWriteError as bwe:
        logger.error("Bulk write error: %s", bwe.details)


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------
_running = True


def _shutdown(signum, _frame):
    global _running
    logger.info("Signal %s received — shutting down …", signum)
    _running = False


def run():
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    client = get_mongo_client()
    db = client[MONGO_DB]
    ensure_collections(db)

    consumer = KafkaConsumer(
        ENRICHED_TOPIC,
        ANOMALY_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=None,  # raw bytes
    )
    logger.info(
        "Mongo sink started — topics=[%s, %s]  bootstrap=%s",
        ENRICHED_TOPIC,
        ANOMALY_TOPIC,
        KAFKA_BOOTSTRAP,
    )

    txn_batch: list[dict] = []
    anomaly_batch: list[dict] = []

    try:
        while _running:
            records = consumer.poll(timeout_ms=1000, max_records=BATCH_SIZE)
            for tp, messages in records.items():
                for msg in messages:
                    parsed = _parse_record(msg.value)
                    if parsed is None:
                        continue

                    if tp.topic == ANOMALY_TOPIC:
                        anomaly_batch.append(parsed)
                    else:
                        txn_batch.append(parsed)

            # Flush when batch full or poll returned data
            if len(txn_batch) >= BATCH_SIZE:
                upsert_batch(db[TXN_COLLECTION], txn_batch)
                txn_batch.clear()

            if len(anomaly_batch) >= BATCH_SIZE:
                upsert_batch(db[ANOMALY_COLLECTION], anomaly_batch)
                anomaly_batch.clear()

            # Flush smaller batches periodically (every poll cycle)
            if txn_batch:
                upsert_batch(db[TXN_COLLECTION], txn_batch)
                txn_batch.clear()
            if anomaly_batch:
                upsert_batch(db[ANOMALY_COLLECTION], anomaly_batch)
                anomaly_batch.clear()

            consumer.commit()

    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
    finally:
        # Final flush
        upsert_batch(db[TXN_COLLECTION], txn_batch)
        upsert_batch(db[ANOMALY_COLLECTION], anomaly_batch)
        consumer.close()
        client.close()
        logger.info("Mongo sink stopped.")


if __name__ == "__main__":
    run()
