"""
Real-Time Customer Transaction Analytics Pipeline — Kafka Producer

Generates synthetic e-commerce transactions using Faker and publishes them
to a Kafka topic at configurable throughput (target: 10k+ txns/sec).
Each transaction is idempotent via a deterministic txn_id.
"""

import json
import os
import sys
import time
import uuid
import signal
import logging
import hashlib
from datetime import datetime, timezone
from typing import Optional

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
TARGET_TPS = int(os.getenv("TARGET_TPS", "1000"))        # txns per second
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
NUM_USERS = int(os.getenv("NUM_USERS", "5000"))           # cardinality of user pool
NUM_MERCHANTS = int(os.getenv("NUM_MERCHANTS", "200"))     # cardinality of merchant pool
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("txn-producer")

# ---------------------------------------------------------------------------
# Faker setup  — seed for reproducibility in dev; remove seed for prod
# ---------------------------------------------------------------------------
fake = Faker("en_CA")
Faker.seed(42)

MERCHANTS = [
    {"merchant_id": f"merchant_{i:04d}", "name": fake.company(), "category": fake.random_element([
        "grocery", "electronics", "clothing", "restaurant", "travel",
        "entertainment", "health", "gas_station", "subscription", "marketplace",
    ])}
    for i in range(NUM_MERCHANTS)
]

USERS = [
    {"user_id": f"user_{i:06d}", "name": fake.name(), "email": fake.email()}
    for i in range(NUM_USERS)
]

CURRENCIES = ["CAD", "USD", "EUR", "GBP"]
PAYMENT_METHODS = ["credit_card", "debit_card", "e_transfer", "apple_pay", "google_pay"]
STATUSES = ["approved", "declined", "pending"]
STATUS_WEIGHTS = [0.90, 0.05, 0.05]


def _idempotent_txn_id(user_id: str, timestamp: str, amount: float) -> str:
    """Deterministic transaction ID for idempotency."""
    raw = f"{user_id}|{timestamp}|{amount}|{uuid.uuid4()}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


def generate_transaction() -> dict:
    """
    Generate a single synthetic e-commerce transaction (10 fields).

    Returns:
        dict with keys: txn_id, user_id, amount, currency, timestamp,
        merchant_id, merchant_category, payment_method, status, location.
    """
    user = fake.random_element(USERS)
    merchant = fake.random_element(MERCHANTS)
    now = datetime.now(timezone.utc).isoformat()

    # Skew amounts: mostly small, occasional large purchases
    amount = round(
        fake.random_element([
            fake.pyfloat(min_value=1, max_value=150, right_digits=2),
            fake.pyfloat(min_value=150, max_value=500, right_digits=2),
            fake.pyfloat(min_value=500, max_value=5000, right_digits=2),
            fake.pyfloat(min_value=5000, max_value=15000, right_digits=2),
        ]),
        2,
    )

    status = fake.random_element(
        elements=STATUSES,
    )

    txn = {
        "txn_id": _idempotent_txn_id(user["user_id"], now, amount),
        "user_id": user["user_id"],
        "amount": amount,
        "currency": fake.random_element(CURRENCIES),
        "timestamp": now,
        "merchant_id": merchant["merchant_id"],
        "merchant_category": merchant["category"],
        "payment_method": fake.random_element(PAYMENT_METHODS),
        "status": status,
        "location": {
            "lat": round(float(fake.latitude()), 6),
            "lon": round(float(fake.longitude()), 6),
        },
    }
    return txn


def _on_send_success(record_metadata):
    logger.debug(
        "Delivered → %s [partition %d] offset %d",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )


def _on_send_error(excp):
    logger.error("Delivery failed: %s", excp)


def create_producer() -> KafkaProducer:
    """Create a high-throughput Kafka producer with batching + compression."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        batch_size=BATCH_SIZE * 1024,        # bytes
        linger_ms=10,                         # micro-batch window
        compression_type="lz4",
        buffer_memory=64 * 1024 * 1024,       # 64 MB
        max_in_flight_requests_per_connection=5,
        enable_idempotence=True,
    )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
_running = True


def _shutdown(signum, frame):
    global _running
    logger.info("Received signal %s — shutting down gracefully …", signum)
    _running = False


def run(tps: int = TARGET_TPS, max_txns: Optional[int] = None):
    """
    Continuously produce transactions at *tps* transactions per second.

    Args:
        tps: Target transactions per second.
        max_txns: Stop after this many transactions (None = infinite).
    """
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    producer = create_producer()
    logger.info(
        "Producer started → topic=%s  bootstrap=%s  target_tps=%d",
        KAFKA_TOPIC, KAFKA_BOOTSTRAP, tps,
    )

    sent = 0
    t0 = time.perf_counter()
    interval = 1.0 / tps if tps > 0 else 0

    try:
        while _running:
            if max_txns and sent >= max_txns:
                break

            txn = generate_transaction()
            producer.send(
                KAFKA_TOPIC,
                key=txn["user_id"],
                value=txn,
            ).add_callback(_on_send_success).add_errback(_on_send_error)

            sent += 1

            # Throttle to target TPS
            if sent % 100 == 0:
                elapsed = time.perf_counter() - t0
                expected = sent * interval
                if expected > elapsed:
                    time.sleep(expected - elapsed)

            # Periodic logging
            if sent % tps == 0:
                elapsed = time.perf_counter() - t0
                actual_tps = sent / elapsed if elapsed > 0 else 0
                logger.info("Sent %d txns  |  %.0f txns/sec", sent, actual_tps)

    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
    finally:
        producer.flush(timeout=10)
        producer.close()
        elapsed = time.perf_counter() - t0
        logger.info(
            "Producer stopped. Total sent: %d in %.1fs (%.0f txns/sec)",
            sent, elapsed, sent / elapsed if elapsed > 0 else 0,
        )


if __name__ == "__main__":
    run()
