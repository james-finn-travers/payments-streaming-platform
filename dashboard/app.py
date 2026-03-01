"""
Real-Time Customer Transaction Analytics — Streamlit Dashboard

Live monitoring dashboard that queries MongoDB directly for:
  • KPI summary cards (total txns, revenue, anomaly count, avg amount)
  • Transaction volume over time (hourly bucketed)
  • Top merchants by revenue (bar chart)
  • Recent anomaly alerts (table)
  • Spend breakdown by category (donut chart)
  • Transaction status distribution

Auto-refreshes every 30 seconds.
"""

import os
import time
from datetime import datetime, timedelta, timezone

import altair as alt
import pandas as pd
import streamlit as st
from pymongo import MongoClient, DESCENDING

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "txn_analytics")
TXN_COLLECTION = os.getenv("MONGO_TXN_COLLECTION", "transactions")
ANOMALY_COLLECTION = os.getenv("MONGO_ANOMALY_COLLECTION", "anomalies")
REFRESH_INTERVAL = int(os.getenv("DASHBOARD_REFRESH_SECS", "30"))

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Transaction Analytics",
    page_icon="📊",
    layout="wide",
)


# ---------------------------------------------------------------------------
# MongoDB connection (cached)
# ---------------------------------------------------------------------------
@st.cache_resource
def get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI)


def get_db():
    return get_mongo_client()[MONGO_DB]


# ---------------------------------------------------------------------------
# Data-fetching helpers
# ---------------------------------------------------------------------------
def _cutoff(days: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days)


@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch_kpis(days: int) -> dict:
    """Aggregate headline KPIs."""
    db = get_db()
    cutoff = _cutoff(days)
    txn_col = db[TXN_COLLECTION]
    anom_col = db[ANOMALY_COLLECTION]

    pipeline = [
        {"$match": {"timestamp": {"$gte": cutoff}}},
        {
            "$group": {
                "_id": None,
                "total_txns": {"$sum": 1},
                "total_revenue": {"$sum": "$amount"},
                "avg_amount": {"$avg": "$amount"},
            }
        },
    ]

    result = list(txn_col.aggregate(pipeline))
    stats = result[0] if result else {"total_txns": 0, "total_revenue": 0, "avg_amount": 0}

    anomaly_count = anom_col.count_documents({"timestamp": {"$gte": cutoff}})

    return {
        "total_txns": stats["total_txns"],
        "total_revenue": round(stats["total_revenue"], 2),
        "avg_amount": round(stats["avg_amount"], 2) if stats["avg_amount"] else 0,
        "anomaly_count": anomaly_count,
    }


@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch_volume_over_time(days: int) -> pd.DataFrame:
    """Hourly transaction volume and revenue."""
    db = get_db()
    cutoff = _cutoff(days)

    pipeline = [
        {"$match": {"timestamp": {"$gte": cutoff}}},
        {
            "$group": {
                "_id": {
                    "$dateTrunc": {
                        "date": "$timestamp",
                        "unit": "hour",
                    }
                },
                "count": {"$sum": 1},
                "revenue": {"$sum": "$amount"},
            }
        },
        {"$sort": {"_id": 1}},
    ]

    docs = list(db[TXN_COLLECTION].aggregate(pipeline))
    if not docs:
        return pd.DataFrame(columns=["timestamp", "transactions", "revenue"])

    df = pd.DataFrame(docs)
    df.rename(columns={"_id": "timestamp", "count": "transactions"}, inplace=True)
    df["revenue"] = df["revenue"].round(2)
    return df


@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch_top_merchants(days: int, limit: int = 10) -> pd.DataFrame:
    """Top merchants by revenue."""
    db = get_db()
    cutoff = _cutoff(days)

    pipeline = [
        {"$match": {"timestamp": {"$gte": cutoff}}},
        {
            "$group": {
                "_id": {
                    "merchant_id": "$merchant_id",
                    "category": "$merchant_category",
                },
                "revenue": {"$sum": "$amount"},
                "txn_count": {"$sum": 1},
            }
        },
        {"$sort": {"revenue": -1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "merchant_id": "$_id.merchant_id",
                "category": "$_id.category",
                "revenue": {"$round": ["$revenue", 2]},
                "txn_count": 1,
            }
        },
    ]

    docs = list(db[TXN_COLLECTION].aggregate(pipeline))
    if not docs:
        return pd.DataFrame(columns=["merchant_id", "category", "revenue", "txn_count"])
    return pd.DataFrame(docs)


@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch_category_breakdown(days: int) -> pd.DataFrame:
    """Spend by merchant category."""
    db = get_db()
    cutoff = _cutoff(days)

    pipeline = [
        {"$match": {"timestamp": {"$gte": cutoff}}},
        {
            "$group": {
                "_id": "$merchant_category",
                "spend": {"$sum": "$amount"},
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"spend": -1}},
    ]

    docs = list(db[TXN_COLLECTION].aggregate(pipeline))
    if not docs:
        return pd.DataFrame(columns=["category", "spend", "count"])

    df = pd.DataFrame(docs)
    df.rename(columns={"_id": "category"}, inplace=True)
    df["spend"] = df["spend"].round(2)
    return df


@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch_status_distribution(days: int) -> pd.DataFrame:
    """Transaction status breakdown."""
    db = get_db()
    cutoff = _cutoff(days)

    pipeline = [
        {"$match": {"timestamp": {"$gte": cutoff}}},
        {"$group": {"_id": "$status", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]

    docs = list(db[TXN_COLLECTION].aggregate(pipeline))
    if not docs:
        return pd.DataFrame(columns=["status", "count"])

    df = pd.DataFrame(docs)
    df.rename(columns={"_id": "status"}, inplace=True)
    return df


@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch_recent_anomalies(days: int, limit: int = 20) -> pd.DataFrame:
    """Most recent anomaly alerts."""
    db = get_db()
    cutoff = _cutoff(days)

    docs = list(
        db[ANOMALY_COLLECTION]
        .find(
            {"timestamp": {"$gte": cutoff}},
            {
                "_id": 0,
                "txn_id": 1,
                "user_id": 1,
                "amount": 1,
                "currency": 1,
                "timestamp": 1,
                "merchant_category": 1,
                "z_score": 1,
            },
        )
        .sort("timestamp", DESCENDING)
        .limit(limit)
    )
    if not docs:
        return pd.DataFrame(
            columns=["timestamp", "txn_id", "user_id", "amount", "currency", "merchant_category", "z_score"]
        )
    df = pd.DataFrame(docs)
    if "z_score" in df.columns:
        df["z_score"] = df["z_score"].round(2)
    return df


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------
def _fmt_number(n: int | float) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return f"{n:,.0f}"


def _fmt_currency(n: float) -> str:
    if n >= 1_000_000:
        return f"${n / 1_000_000:.2f}M"
    if n >= 1_000:
        return f"${n / 1_000:.1f}K"
    return f"${n:,.2f}"


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------
st.title("📊 Transaction Analytics Dashboard")
st.caption("Real-time monitoring · auto-refreshes every 30 s")

# Sidebar controls
with st.sidebar:
    st.header("Controls")
    lookback = st.selectbox("Lookback period", [1, 7, 14, 30, 90], index=1, format_func=lambda d: f"{d} day{'s' if d != 1 else ''}")
    merchant_limit = st.slider("Top merchants to show", 5, 20, 10)
    auto_refresh = st.toggle("Auto-refresh", value=True)

# ---------------------------------------------------------------------------
# KPI cards
# ---------------------------------------------------------------------------
kpis = fetch_kpis(lookback)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Transactions", _fmt_number(kpis["total_txns"]))
col2.metric("Total Revenue", _fmt_currency(kpis["total_revenue"]))
col3.metric("Avg Transaction", _fmt_currency(kpis["avg_amount"]))
col4.metric("Anomalies Detected", _fmt_number(kpis["anomaly_count"]))

st.divider()

# ---------------------------------------------------------------------------
# Row 1: Volume over time + Status breakdown
# ---------------------------------------------------------------------------
r1_left, r1_right = st.columns([3, 1])

with r1_left:
    st.subheader("Transaction Volume (Hourly)")
    vol_df = fetch_volume_over_time(lookback)
    if vol_df.empty:
        st.info("No transaction data for this period.")
    else:
        volume_chart = (
            alt.Chart(vol_df)
            .mark_area(opacity=0.5, color="#4C78A8")
            .encode(
                x=alt.X("timestamp:T", title="Time"),
                y=alt.Y("transactions:Q", title="Transactions"),
                tooltip=[
                    alt.Tooltip("timestamp:T", title="Hour"),
                    alt.Tooltip("transactions:Q", title="Txns", format=","),
                    alt.Tooltip("revenue:Q", title="Revenue ($)", format=",.2f"),
                ],
            )
            .properties(height=320)
            .interactive()
        )
        st.altair_chart(volume_chart, use_container_width=True)

with r1_right:
    st.subheader("Status Breakdown")
    status_df = fetch_status_distribution(lookback)
    if status_df.empty:
        st.info("No data.")
    else:
        STATUS_COLORS = {"approved": "#4CAF50", "declined": "#F44336", "pending": "#FF9800"}
        color_scale = alt.Scale(
            domain=list(STATUS_COLORS.keys()),
            range=list(STATUS_COLORS.values()),
        )
        status_chart = (
            alt.Chart(status_df)
            .mark_arc(innerRadius=50)
            .encode(
                theta=alt.Theta("count:Q"),
                color=alt.Color("status:N", scale=color_scale, title="Status"),
                tooltip=[
                    alt.Tooltip("status:N", title="Status"),
                    alt.Tooltip("count:Q", title="Count", format=","),
                ],
            )
            .properties(height=280)
        )
        st.altair_chart(status_chart, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Row 2: Top merchants + Category breakdown
# ---------------------------------------------------------------------------
r2_left, r2_right = st.columns(2)

with r2_left:
    st.subheader(f"Top {merchant_limit} Merchants by Revenue")
    merch_df = fetch_top_merchants(lookback, merchant_limit)
    if merch_df.empty:
        st.info("No merchant data for this period.")
    else:
        merchant_chart = (
            alt.Chart(merch_df)
            .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
            .encode(
                x=alt.X("revenue:Q", title="Revenue ($)"),
                y=alt.Y("merchant_id:N", sort="-x", title="Merchant"),
                color=alt.Color("category:N", title="Category"),
                tooltip=[
                    alt.Tooltip("merchant_id:N", title="Merchant"),
                    alt.Tooltip("category:N", title="Category"),
                    alt.Tooltip("revenue:Q", title="Revenue ($)", format=",.2f"),
                    alt.Tooltip("txn_count:Q", title="Transactions", format=","),
                ],
            )
            .properties(height=400)
        )
        st.altair_chart(merchant_chart, use_container_width=True)

with r2_right:
    st.subheader("Spend by Category")
    cat_df = fetch_category_breakdown(lookback)
    if cat_df.empty:
        st.info("No category data for this period.")
    else:
        cat_chart = (
            alt.Chart(cat_df)
            .mark_arc(innerRadius=60)
            .encode(
                theta=alt.Theta("spend:Q"),
                color=alt.Color("category:N", title="Category"),
                tooltip=[
                    alt.Tooltip("category:N", title="Category"),
                    alt.Tooltip("spend:Q", title="Spend ($)", format=",.2f"),
                    alt.Tooltip("count:Q", title="Transactions", format=","),
                ],
            )
            .properties(height=400)
        )
        st.altair_chart(cat_chart, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Row 3: Anomaly alerts table
# ---------------------------------------------------------------------------
st.subheader("🚨 Recent Anomaly Alerts")
anom_df = fetch_recent_anomalies(lookback, limit=30)
if anom_df.empty:
    st.info("No anomalies detected in this period.")
else:
    st.dataframe(
        anom_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "timestamp": st.column_config.DatetimeColumn("Time", format="YYYY-MM-DD HH:mm:ss"),
            "txn_id": st.column_config.TextColumn("Transaction ID"),
            "user_id": st.column_config.TextColumn("User"),
            "amount": st.column_config.NumberColumn("Amount ($)", format="$%.2f"),
            "currency": st.column_config.TextColumn("Currency"),
            "merchant_category": st.column_config.TextColumn("Category"),
            "z_score": st.column_config.NumberColumn("Z-Score", format="%.2f"),
        },
    )

# ---------------------------------------------------------------------------
# Auto-refresh
# ---------------------------------------------------------------------------
if auto_refresh:
    time.sleep(REFRESH_INTERVAL)
    st.rerun()
