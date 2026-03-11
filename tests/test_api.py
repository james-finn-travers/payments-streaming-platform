"""
Unit tests for the FastAPI REST API.

Tests all endpoints with a mocked MongoDB backend via monkeypatching
the module-level mongo_client used by api.main.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from bson import ObjectId

from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def mock_mongo_client():
    """Build a MagicMock that behaves like a MongoClient."""
    client = MagicMock()
    # ping succeeds by default
    client.admin.command.return_value = {"ok": 1}
    return client


@pytest.fixture
def client(mock_mongo_client):
    """
    Yield a FastAPI TestClient with MongoClient patched so the lifespan
    context manager creates our mock instead of a real connection.
    """
    import api.main as api_mod

    with patch.object(api_mod, "MongoClient", return_value=mock_mongo_client):
        with TestClient(api_mod.app, raise_server_exceptions=False) as tc:
            yield tc


def _mock_db(mock_mongo_client):
    """Shortcut to the mocked DB object."""
    return mock_mongo_client["txn_analytics"]


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------
class TestHealth:
    def test_health_ok(self, client, mock_mongo_client):
        resp = client.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert body["mongo"] == "connected"

    def test_health_mongo_down(self, client, mock_mongo_client):
        mock_mongo_client.admin.command.side_effect = Exception("down")
        resp = client.get("/health")
        assert resp.status_code == 503


# ---------------------------------------------------------------------------
# GET / (redirect)
# ---------------------------------------------------------------------------
class TestRoot:
    def test_root_redirects_to_docs(self, client):
        resp = client.get("/", follow_redirects=False)
        assert resp.status_code == 307
        assert "/docs" in resp.headers["location"]


# ---------------------------------------------------------------------------
# GET /users/{user_id}/trends
# ---------------------------------------------------------------------------
class TestUserTrends:
    def _setup_aggregate(self, mock_mongo_client, stats=None, by_category=None):
        """Wire up the mock so txn_col.aggregate returns a faceted result."""
        if stats is None:
            stats = [{"_id": None, "total_spend": 250.0, "count": 5, "currencies": ["CAD"]}]
        if by_category is None:
            by_category = [{"_id": "grocery", "spend": 150.0, "count": 3}]

        # aggregate() returns an iterator
        db = mock_mongo_client.__getitem__.return_value  # db
        col = db.__getitem__.return_value                # collection
        col.aggregate.return_value = iter([
            {"stats": stats, "by_category": by_category}
        ])

        # find().sort().limit() for recent txns
        recent_doc = {
            "_id": ObjectId(),
            "txn_id": "aabbccdd11223344aabbccdd",
            "user_id": "user_000001",
            "amount": 50.0,
            "currency": "CAD",
            "timestamp": datetime.now(timezone.utc),
            "merchant_id": "merchant_0001",
            "merchant_category": "grocery",
            "payment_method": "credit_card",
            "status": "approved",
            "location": {"lat": 43.65, "lon": -79.38},
        }
        col.find.return_value.sort.return_value.limit.return_value = [recent_doc]

    def test_user_trends_200(self, client, mock_mongo_client):
        self._setup_aggregate(mock_mongo_client)
        resp = client.get("/users/user_000001/trends")
        assert resp.status_code == 200
        body = resp.json()
        assert body["user_id"] == "user_000001"
        assert body["total_spend"] == 250.0
        assert body["total_transactions"] == 5

    def test_user_trends_empty(self, client, mock_mongo_client):
        self._setup_aggregate(mock_mongo_client, stats=[], by_category=[])
        db = mock_mongo_client.__getitem__.return_value
        col = db.__getitem__.return_value
        col.find.return_value.sort.return_value.limit.return_value = []
        resp = client.get("/users/user_000001/trends?days=7")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total_transactions"] == 0
        assert body["total_spend"] == 0

    def test_user_trends_custom_days(self, client, mock_mongo_client):
        self._setup_aggregate(mock_mongo_client)
        resp = client.get("/users/user_000001/trends?days=90&limit=5")
        assert resp.status_code == 200
        body = resp.json()
        assert body["period_days"] == 90

    def test_user_trends_invalid_days(self, client, mock_mongo_client):
        resp = client.get("/users/user_000001/trends?days=0")
        assert resp.status_code == 422

    def test_user_trends_days_too_large(self, client, mock_mongo_client):
        resp = client.get("/users/user_000001/trends?days=999")
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /anomalies
# ---------------------------------------------------------------------------
class TestAnomalies:
    def _setup_anomalies(self, mock_mongo_client, docs=None):
        db = mock_mongo_client.__getitem__.return_value
        col = db.__getitem__.return_value
        if docs is None:
            docs = [
                {
                    "_id": ObjectId(),
                    "txn_id": "aabbccdd11223344aabbccdd",
                    "user_id": "user_000001",
                    "amount": 9999.0,
                    "currency": "CAD",
                    "timestamp": datetime.now(timezone.utc),
                    "merchant_id": "merchant_0010",
                    "merchant_category": "electronics",
                    "z_score": 5.2,
                    "user_running_mean": 80.0,
                },
            ]
        col.find.return_value.sort.return_value.limit.return_value = docs

    def test_anomalies_200(self, client, mock_mongo_client):
        self._setup_anomalies(mock_mongo_client)
        resp = client.get("/anomalies")
        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 1
        assert len(body["anomalies"]) == 1

    def test_anomalies_empty(self, client, mock_mongo_client):
        self._setup_anomalies(mock_mongo_client, docs=[])
        resp = client.get("/anomalies")
        assert resp.status_code == 200
        assert resp.json()["count"] == 0

    def test_anomalies_filter_user(self, client, mock_mongo_client):
        self._setup_anomalies(mock_mongo_client)
        resp = client.get("/anomalies?user_id=user_000001")
        assert resp.status_code == 200

    def test_anomalies_filter_min_z(self, client, mock_mongo_client):
        self._setup_anomalies(mock_mongo_client)
        resp = client.get("/anomalies?min_z=4.0")
        assert resp.status_code == 200

    def test_anomalies_invalid_days(self, client, mock_mongo_client):
        resp = client.get("/anomalies?days=0")
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /merchants/top
# ---------------------------------------------------------------------------
class TestTopMerchants:
    def _setup_merchants(self, mock_mongo_client, docs=None):
        db = mock_mongo_client.__getitem__.return_value
        col = db.__getitem__.return_value
        if docs is None:
            docs = [
                {
                    "merchant_id": "merchant_0001",
                    "merchant_category": "grocery",
                    "total_revenue": 12500.0,
                    "transaction_count": 250,
                    "avg_transaction": 50.0,
                },
            ]
        col.aggregate.return_value = iter(docs)

    def test_top_merchants_200(self, client, mock_mongo_client):
        self._setup_merchants(mock_mongo_client)
        resp = client.get("/merchants/top")
        assert resp.status_code == 200
        body = resp.json()
        assert body["period_days"] == 30
        assert len(body["merchants"]) == 1
        assert body["merchants"][0]["merchant_id"] == "merchant_0001"

    def test_top_merchants_empty(self, client, mock_mongo_client):
        self._setup_merchants(mock_mongo_client, docs=[])
        resp = client.get("/merchants/top")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["merchants"]) == 0

    def test_top_merchants_custom_limit(self, client, mock_mongo_client):
        self._setup_merchants(mock_mongo_client)
        resp = client.get("/merchants/top?limit=5&days=60")
        assert resp.status_code == 200
        body = resp.json()
        assert body["period_days"] == 60
        assert body["limit"] == 5

    def test_top_merchants_invalid_limit(self, client, mock_mongo_client):
        resp = client.get("/merchants/top?limit=0")
        assert resp.status_code == 422
