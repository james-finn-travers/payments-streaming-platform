"""
Real-Time Customer Transaction Analytics Pipeline — Daily Spend Sink

Kafka consumer that reads windowed daily-spend aggregates from the
``daily_spend`` topic (produced by the Flink WindowAggregate) and
upserts them into a MongoDB collection keyed on (user_id, window_start).

Each record has the shape:
    {
        "user_id":      str,
        "window_start": int (epoch ms),
        "window_end":   int (epoch ms),
        "txn_count":    int,
        "total_spend":  float,
        "avg_spend":    float,
    }
"""

import os
import json
import signal
import logging
from datetime import datetime, timezone

from kafka import KafkaConsumer
from pymongo import MongoClient, UpdateOne, ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DAILY_SPEND_TOPIC = os.getenv("KAFKA_DAILY_SPEND_TOPIC", "daily_spend")
CONSUMER_GROUP = os.getenv("DAILY_SPEND_CONSUMER_GROUP", "daily-spend-sink")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "txn_analytics")
DAILY_SPEND_COLLECTION = os.getenv("MONGO_DAILY_SPEND_COLLECTION", "daily_spend")

BATCH_SIZE = int(os.getenv("SINK_BATCH_SIZE", "500"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("daily-spend-sink")


# ---------------------------------------------------------------------------
# MongoDB helpers
# ---------------------------------------------------------------------------
def get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI)


def ensure_collection(db):
    """
    Create the daily_spend collection and indexes if they don't exist.
    Uses a compound unique index on (user_id, window_start) so that
    late-arriving duplicates are merged via upsert.
    """
    existing = db.list_collection_names()

    if DAILY_SPEND_COLLECTION not in existing:
        db.create_collection(DAILY_SPEND_COLLECTION)
        logger.info("Created collection: %s", DAILY_SPEND_COLLECTION)

    col = db[DAILY_SPEND_COLLECTION]
    col.create_index(
        [("user_id", ASCENDING), ("window_start", ASCENDING)],
        unique=True,
        name="ux_user_window",
    )
    col.create_index([("window_start", DESCENDING)], name="ix_window_start")
    col.create_index([("user_id", ASCENDING)], name="ix_user_id")


def _parse_record(raw_value: bytes) -> dict | None:
    """Parse a raw Kafka value into a dict, converting epoch-ms timestamps."""
    try:
        record = json.loads(raw_value.decode("utf-8"))

        # Convert epoch-ms window boundaries to datetime for easier querying
        for field in ("window_start", "window_end"):
            val = record.get(field)
            if isinstance(val, (int, float)):
                record[field] = datetime.fromtimestamp(
                    val / 1000.0, tz=timezone.utc
                )

        return record
    except (json.JSONDecodeError, ValueError, OverflowError) as exc:
        logger.warning("Skipping malformed daily_spend record: %s", exc)
        return None


def upsert_batch(collection, batch: list[dict]):
    """
    Idempotent upsert using (user_id, window_start) as the natural key.
    Late-arriving windows overwrite with the latest aggregates.
    """
    if not batch:
        return

    ops = []
    for doc in batch:
        uid = doc.get("user_id")
        ws = doc.get("window_start")
        if uid is None or ws is None:
            continue
        ops.append(
            UpdateOne(
                {"user_id": uid, "window_start": ws},
                {"$set": doc},
                upsert=True,
            )
        )

    if not ops:
        return

    try:
        result = collection.bulk_write(ops, ordered=False)
        logger.debug(
            "Upserted %d daily_spend docs (matched=%d, modified=%d, inserted=%d)",
            len(ops),
            result.matched_count,
            result.modified_count,
            result.upserted_count,
        )
    except BulkWriteError as bwe:
        logger.error("Bulk write error (daily_spend): %s", bwe.details)


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
    ensure_collection(db)

    consumer = KafkaConsumer(
        DAILY_SPEND_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=None,
    )
    logger.info(
        "Daily-spend sink started — topic=%s  bootstrap=%s",
        DAILY_SPEND_TOPIC,
        KAFKA_BOOTSTRAP,
    )

    batch: list[dict] = []

    try:
        while _running:
            records = consumer.poll(timeout_ms=1000, max_records=BATCH_SIZE)
            for tp, messages in records.items():
                for msg in messages:
                    parsed = _parse_record(msg.value)
                    if parsed is not None:
                        batch.append(parsed)

            if batch:
                upsert_batch(db[DAILY_SPEND_COLLECTION], batch)
                batch.clear()

            consumer.commit()

    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
    finally:
        upsert_batch(db[DAILY_SPEND_COLLECTION], batch)
        consumer.close()
        client.close()
        logger.info("Daily-spend sink stopped.")


if __name__ == "__main__":
    run()
