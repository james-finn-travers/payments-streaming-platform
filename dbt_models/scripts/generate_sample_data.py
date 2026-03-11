"""
Generate sample Parquet files for dbt development/testing.

Creates realistic synthetic data matching the schema that would
normally come from the MongoDB export.
"""

import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

SEED_DIR = Path("dbt_models/seeds/raw")
SEED_DIR.mkdir(parents=True, exist_ok=True)

random.seed(42)

NUM_TXNS = 5000
NUM_ANOMALIES = 120
NUM_USERS = 200
NUM_MERCHANTS = 40
DAYS_BACK = 60

CATEGORIES = [
    "grocery", "electronics", "clothing", "restaurant", "travel",
    "entertainment", "health", "gas_station", "subscription", "marketplace",
]
CURRENCIES = ["CAD", "USD", "EUR", "GBP"]
METHODS = ["credit_card", "debit_card", "e_transfer", "apple_pay", "google_pay"]
STATUSES = ["approved", "declined", "pending"]
STATUS_WEIGHTS = [0.90, 0.05, 0.05]

now = datetime.now(timezone.utc)
merchants = [
    {"id": f"merchant_{i:04d}", "cat": random.choice(CATEGORIES)}
    for i in range(NUM_MERCHANTS)
]


def rand_ts():
    return now - timedelta(
        days=random.uniform(0, DAYS_BACK),
        hours=random.uniform(0, 24),
    )


def make_txn(i, force_anomaly=False):
    m = random.choice(merchants)
    amount = round(random.uniform(1, 500), 2)
    z = round(random.gauss(0, 1), 4)
    is_anomaly = False

    if force_anomaly:
        amount = round(random.uniform(3000, 15000), 2)
        z = round(random.uniform(3.5, 8.0), 4)
        is_anomaly = True

    return {
        "txn_id": f"txn_{i:08d}",
        "user_id": f"user_{random.randint(0, NUM_USERS - 1):06d}",
        "amount": amount,
        "currency": random.choice(CURRENCIES),
        "timestamp": rand_ts(),
        "merchant_id": m["id"],
        "merchant_category": m["cat"],
        "payment_method": random.choice(METHODS),
        "status": random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0],
        "location_lat": round(random.uniform(42, 56), 6),
        "location_lon": round(random.uniform(-130, -60), 6),
        "user_txn_count": random.randint(1, 500),
        "user_running_mean": round(random.uniform(50, 300), 2),
        "z_score": z,
        "is_anomaly": is_anomaly,
    }


# --- Transactions ---
txns = [make_txn(i) for i in range(NUM_TXNS)]

txn_table = pa.table(
    {col: [t[col] for t in txns] for col in [
        "txn_id", "user_id", "amount", "currency", "timestamp",
        "merchant_id", "merchant_category", "payment_method", "status",
        "location_lat", "location_lon", "user_txn_count",
        "user_running_mean", "z_score", "is_anomaly",
    ]},
    schema=pa.schema([
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
    ]),
)
pq.write_table(txn_table, str(SEED_DIR / "transactions.parquet"), compression="snappy")
print(f"Wrote {len(txns)} transactions → {SEED_DIR / 'transactions.parquet'}")

# --- Anomalies (subset with high z-scores) ---
anoms = [make_txn(NUM_TXNS + i, force_anomaly=True) for i in range(NUM_ANOMALIES)]

anom_table = pa.table(
    {col: [a[col] for a in anoms] for col in [
        "txn_id", "user_id", "amount", "currency", "timestamp",
        "merchant_id", "merchant_category", "payment_method",
        "z_score", "user_running_mean", "user_txn_count",
    ]},
    schema=pa.schema([
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
    ]),
)
pq.write_table(anom_table, str(SEED_DIR / "anomalies.parquet"), compression="snappy")
print(f"Wrote {len(anoms)} anomalies  → {SEED_DIR / 'anomalies.parquet'}")
