"""
Real-Time Customer Transaction Analytics Pipeline — FastAPI

REST endpoints for querying enriched transactions and anomalies
stored in MongoDB time-series collections.

Endpoints
---------
- GET  /health                   — Liveness probe
- GET  /users/{user_id}/trends   — Per-user spending trends
- GET  /anomalies                — Recent anomaly alerts
- GET  /merchants/top            — Top merchants by revenue
"""

import os
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field
from pymongo import MongoClient, DESCENDING

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "txn_analytics")
TXN_COLLECTION = os.getenv("MONGO_TXN_COLLECTION", "transactions")
ANOMALY_COLLECTION = os.getenv("MONGO_ANOMALY_COLLECTION", "anomalies")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("api")


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------
class HealthResponse(BaseModel):
    status: str = "ok"
    mongo: str = "connected"


class LocationOut(BaseModel):
    lat: float
    lon: float


class TransactionOut(BaseModel):
    txn_id: str
    user_id: str
    amount: float
    currency: str
    timestamp: datetime
    merchant_id: str
    merchant_category: str
    payment_method: str
    status: str
    location: Optional[LocationOut] = None
    user_txn_count: Optional[int] = None
    user_running_mean: Optional[float] = None
    z_score: Optional[float] = None
    is_anomaly: Optional[bool] = None


class UserTrendsResponse(BaseModel):
    user_id: str
    period_days: int
    total_transactions: int
    total_spend: float
    avg_transaction: float
    currencies: list[str]
    top_categories: list[dict]
    recent_transactions: list[TransactionOut]


class AnomalyOut(BaseModel):
    txn_id: str
    user_id: str
    amount: float
    currency: str
    timestamp: datetime
    merchant_id: str
    merchant_category: str
    z_score: Optional[float] = None
    user_running_mean: Optional[float] = None


class AnomaliesResponse(BaseModel):
    count: int
    anomalies: list[AnomalyOut]


class MerchantRollup(BaseModel):
    merchant_id: str
    merchant_category: str
    total_revenue: float
    transaction_count: int
    avg_transaction: float


class TopMerchantsResponse(BaseModel):
    period_days: int
    limit: int
    merchants: list[MerchantRollup]


# ---------------------------------------------------------------------------
# App lifecycle — MongoDB client
# ---------------------------------------------------------------------------
mongo_client: Optional[MongoClient] = None


def get_db():
    return mongo_client[MONGO_DB]


@asynccontextmanager
async def lifespan(app: FastAPI):
    global mongo_client
    mongo_client = MongoClient(MONGO_URI)
    # Quick connectivity check
    mongo_client.admin.command("ping")
    logger.info("Connected to MongoDB at %s", MONGO_URI)
    yield
    mongo_client.close()
    logger.info("MongoDB connection closed.")


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Transaction Analytics API",
    description="REST API for the Real-Time Customer Transaction Analytics Pipeline",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _serialize_doc(doc: dict) -> dict:
    """Remove MongoDB _id and convert ObjectId to str for JSON serialisation."""
    doc.pop("_id", None)
    return doc


def _cutoff(days: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/", include_in_schema=False)
async def root():
    """Redirect root to interactive API docs."""
    return RedirectResponse(url="/docs")


@app.get("/health", response_model=HealthResponse)
async def health():
    """Liveness / readiness probe."""
    try:
        mongo_client.admin.command("ping")
        return HealthResponse()
    except Exception:
        raise HTTPException(status_code=503, detail="MongoDB unreachable")


@app.get("/users/{user_id}/trends", response_model=UserTrendsResponse)
async def user_trends(
    user_id: str,
    days: int = Query(default=30, ge=1, le=365, description="Lookback window in days"),
    limit: int = Query(default=20, ge=1, le=100, description="Max recent transactions"),
):
    """
    Per-user spending trends over the specified lookback period.

    Returns aggregate stats (total spend, avg spend, top categories)
    and the most recent transactions.
    """
    db = get_db()
    txn_col = db[TXN_COLLECTION]
    cutoff = _cutoff(days)

    query = {"user_id": user_id, "timestamp": {"$gte": cutoff}}

    # Aggregate: total spend, count, currencies, top categories
    pipeline = [
        {"$match": query},
        {
            "$facet": {
                "stats": [
                    {
                        "$group": {
                            "_id": None,
                            "total_spend": {"$sum": "$amount"},
                            "count": {"$sum": 1},
                            "currencies": {"$addToSet": "$currency"},
                        }
                    }
                ],
                "by_category": [
                    {
                        "$group": {
                            "_id": "$merchant_category",
                            "spend": {"$sum": "$amount"},
                            "count": {"$sum": 1},
                        }
                    },
                    {"$sort": {"spend": -1}},
                    {"$limit": 5},
                ],
            }
        },
    ]

    agg_result = list(txn_col.aggregate(pipeline))
    facets = agg_result[0] if agg_result else {"stats": [], "by_category": []}

    stats = facets["stats"][0] if facets["stats"] else {
        "total_spend": 0,
        "count": 0,
        "currencies": [],
    }

    total_spend = round(stats["total_spend"], 2)
    total_count = stats["count"]
    avg_txn = round(total_spend / total_count, 2) if total_count else 0.0
    currencies = stats["currencies"]

    top_categories = [
        {"category": c["_id"], "spend": round(c["spend"], 2), "count": c["count"]}
        for c in facets["by_category"]
    ]

    # Recent transactions
    recent_docs = list(
        txn_col.find(query)
        .sort("timestamp", DESCENDING)
        .limit(limit)
    )
    recent = [_serialize_doc(d) for d in recent_docs]

    return UserTrendsResponse(
        user_id=user_id,
        period_days=days,
        total_transactions=total_count,
        total_spend=total_spend,
        avg_transaction=avg_txn,
        currencies=currencies,
        top_categories=top_categories,
        recent_transactions=recent,
    )


@app.get("/anomalies", response_model=AnomaliesResponse)
async def anomalies(
    days: int = Query(default=7, ge=1, le=90, description="Lookback window in days"),
    limit: int = Query(default=50, ge=1, le=500, description="Max anomalies to return"),
    user_id: Optional[str] = Query(default=None, description="Filter by user ID"),
    min_z: Optional[float] = Query(default=None, description="Minimum absolute z-score"),
):
    """
    Recent anomaly alerts, optionally filtered by user or z-score threshold.
    """
    db = get_db()
    anom_col = db[ANOMALY_COLLECTION]
    cutoff = _cutoff(days)

    query: dict = {"timestamp": {"$gte": cutoff}}
    if user_id:
        query["user_id"] = user_id
    if min_z is not None:
        query["z_score"] = {"$gte": min_z}

    docs = list(
        anom_col.find(query)
        .sort("timestamp", DESCENDING)
        .limit(limit)
    )
    results = [_serialize_doc(d) for d in docs]

    return AnomaliesResponse(count=len(results), anomalies=results)


@app.get("/merchants/top", response_model=TopMerchantsResponse)
async def top_merchants(
    days: int = Query(default=30, ge=1, le=365, description="Lookback window in days"),
    limit: int = Query(default=10, ge=1, le=50, description="Number of merchants"),
):
    """
    Top merchants ranked by total revenue in the lookback period.
    """
    db = get_db()
    txn_col = db[TXN_COLLECTION]
    cutoff = _cutoff(days)

    pipeline = [
        {"$match": {"timestamp": {"$gte": cutoff}}},
        {
            "$group": {
                "_id": {
                    "merchant_id": "$merchant_id",
                    "merchant_category": "$merchant_category",
                },
                "total_revenue": {"$sum": "$amount"},
                "transaction_count": {"$sum": 1},
            }
        },
        {"$sort": {"total_revenue": -1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "merchant_id": "$_id.merchant_id",
                "merchant_category": "$_id.merchant_category",
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "transaction_count": 1,
                "avg_transaction": {
                    "$round": [
                        {"$divide": ["$total_revenue", "$transaction_count"]},
                        2,
                    ]
                },
            }
        },
    ]

    results = list(txn_col.aggregate(pipeline))

    return TopMerchantsResponse(
        period_days=days,
        limit=limit,
        merchants=results,
    )


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level=LOG_LEVEL.lower(),
    )
