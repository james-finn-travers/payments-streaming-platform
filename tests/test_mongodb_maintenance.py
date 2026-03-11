"""
Unit tests for the MongoDB maintenance DAG.

Each maintenance function is tested by mocking _get_db / _get_client
to return controlled pymongo-like objects.
"""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta, timezone

from airflow.dags.mongodb_maintenance import (
    COLLECTIONS,
    SLOW_QUERY_THRESHOLD_MS,
    CONNECTION_POOL_WARN_RATIO,
    ORPHAN_DELETE_LIMIT,
    purge_old_transactions,
    purge_old_anomalies,
    remove_orphan_anomalies,
    collect_collection_stats,
    verify_indexes,
    compact_collections,
    detect_slow_queries,
    check_connection_health,
    log_maintenance_summary,
)

PATCH_GET_DB = "airflow.dags.mongodb_maintenance._get_db"


@pytest.fixture
def mock_db():
    """Return (mock_client, mock_db) pair with close() wired up."""
    client = MagicMock()
    db = MagicMock()
    return client, db


# ---------------------------------------------------------------------------
# Config sanity
# ---------------------------------------------------------------------------
class TestConfig:
    def test_collections_defined(self):
        assert "transactions" in COLLECTIONS
        assert "anomalies" in COLLECTIONS

    def test_retention_days_positive(self):
        for cfg in COLLECTIONS.values():
            assert cfg["retention_days"] > 0

    def test_expected_indexes_non_empty(self):
        for cfg in COLLECTIONS.values():
            assert len(cfg["expected_indexes"]) > 0


# ---------------------------------------------------------------------------
# Phase 1 — TTL Cleanup
# ---------------------------------------------------------------------------
class TestPurgeOldTransactions:
    @patch(PATCH_GET_DB)
    def test_deletes_old_docs(self, mock_get, mock_db):
        client, db = mock_db
        mock_get.return_value = (client, db)
        col = db.__getitem__.return_value
        col.delete_many.return_value = MagicMock(deleted_count=42)

        purge_old_transactions()

        col.delete_many.assert_called_once()
        filter_arg = col.delete_many.call_args[0][0]
        assert "timestamp" in filter_arg
        assert "$lt" in filter_arg["timestamp"]
        client.close.assert_called_once()

    @patch(PATCH_GET_DB)
    def test_zero_deletions(self, mock_get, mock_db):
        client, db = mock_db
        mock_get.return_value = (client, db)
        col = db.__getitem__.return_value
        col.delete_many.return_value = MagicMock(deleted_count=0)

        purge_old_transactions()
        client.close.assert_called_once()


class TestPurgeOldAnomalies:
    @patch(PATCH_GET_DB)
    def test_deletes_old_anomalies(self, mock_get, mock_db):
        client, db = mock_db
        mock_get.return_value = (client, db)
        col = db.__getitem__.return_value
        col.delete_many.return_value = MagicMock(deleted_count=15)

        purge_old_anomalies()

        col.delete_many.assert_called_once()
        client.close.assert_called_once()


class TestRemoveOrphanAnomalies:
    @patch(PATCH_GET_DB)
    def test_no_orphans(self, mock_get, mock_db):
        client, db = mock_db
        mock_get.return_value = (client, db)
        col = db.__getitem__.return_value
        col.aggregate.return_value = []

        remove_orphan_anomalies()

        col.delete_many.assert_not_called()
        client.close.assert_called_once()

    @patch(PATCH_GET_DB)
    def test_removes_orphans(self, mock_get, mock_db):
        client, db = mock_db
        mock_get.return_value = (client, db)
        col = db.__getitem__.return_value
        col.aggregate.return_value = [
            {"_id": "id1", "txn_id": "orphan_1"},
            {"_id": "id2", "txn_id": "orphan_2"},
        ]
        col.delete_many.return_value = MagicMock(deleted_count=2)

        remove_orphan_anomalies()

        col.delete_many.assert_called_once()
        filter_arg = col.delete_many.call_args[0][0]
        assert "$in" in filter_arg["_id"]
        assert len(filter_arg["_id"]["$in"]) == 2
        client.close.assert_called_once()


# ---------------------------------------------------------------------------
# Phase 2 — Index Health
# ---------------------------------------------------------------------------
class TestCollectCollectionStats:
    @patch(PATCH_GET_DB)
    def test_logs_stats_for_each_collection(self, mock_get, mock_db):
        client, db = mock_db
        mock_get.return_value = (client, db)
        db.command.return_value = {
            "count": 1000,
            "size": 512000,
            "avgObjSize": 512,
            "storageSize": 600000,
            "nindexes": 3,
        }

        collect_collection_stats()

        assert db.command.call_count == len(COLLECTIONS)
        client.close.assert_called_once()

    @patch(PATCH_GET_DB)
    def test_handles_command_error(self, mock_get, mock_db):
        client, db = mock_db
        mock_get.return_value = (client, db)
        db.command.side_effect = Exception("collection not found")

        collect_collection_stats()  # should not raise
        client.close.assert_called_once()


class TestVerifyIndexes:
    @patch(PATCH_GET_DB)
    def test_all_indexes_present(self, mock_get, mock_db):
        client, db = mock_db
        mock_get.return_value = (client, db)
        col = db.__getitem__.return_value

        # Simulate existing indexes matching expected ones
        col.index_information.return_value = {
            "_id_": {"key": [("_id", 1)]},
            "txn_id_1": {"key": [("txn_id", 1)]},
            "merchant_id_1": {"key": [("merchant_id", 1)]},
            "merchant_category_1": {"key": [("merchant_category", 1)]},
            "z_score_1": {"key": [("z_score", 1)]},
        }

        verify_indexes()

        col.create_index.assert_not_called()
        client.close.assert_called_once()

    @patch(PATCH_GET_DB)
    def test_recreates_missing_index(self, mock_get, mock_db):
        client, db = mock_db
        mock_get.return_value = (client, db)
        col = db.__getitem__.return_value

        # Only _id exists — all expected indexes are missing
        col.index_information.return_value = {
            "_id_": {"key": [("_id", 1)]},
        }

        verify_indexes()

        assert col.create_index.call_count > 0
        client.close.assert_called_once()


class TestCompactCollections:
    @patch(PATCH_GET_DB)
    def test_compact_succeeds(self, mock_get, mock_db):
        client, db = mock_db
        mock_get.return_value = (client, db)
        db.command.return_value = {"ok": 1, "bytesFreed": 1024}

        compact_collections()

        assert db.command.call_count == len(COLLECTIONS)
        client.close.assert_called_once()

    @patch(PATCH_GET_DB)
    def test_compact_failure_non_fatal(self, mock_get, mock_db):
        client, db = mock_db
        mock_get.return_value = (client, db)
        db.command.side_effect = Exception("not supported for time-series")

        compact_collections()  # should not raise
        client.close.assert_called_once()


# ---------------------------------------------------------------------------
# Phase 3 — Performance Checks
# ---------------------------------------------------------------------------
class TestDetectSlowQueries:
    @patch(PATCH_GET_DB)
    def test_no_slow_queries(self, mock_get, mock_db):
        client, db = mock_db
        mock_get.return_value = (client, db)
        col = db.__getitem__.return_value
        col.find.return_value.sort.return_value.limit.return_value = []

        detect_slow_queries()
        client.close.assert_called_once()

    @patch(PATCH_GET_DB)
    def test_slow_queries_found(self, mock_get, mock_db):
        client, db = mock_db
        mock_get.return_value = (client, db)
        col = db.__getitem__.return_value
        col.find.return_value.sort.return_value.limit.return_value = [
            {"millis": 500, "ns": "payments.transactions", "op": "query", "command": {}},
        ]

        detect_slow_queries()
        client.close.assert_called_once()

    @patch(PATCH_GET_DB)
    def test_profiling_disabled(self, mock_get, mock_db):
        client, db = mock_db
        mock_get.return_value = (client, db)
        db.__getitem__.side_effect = Exception("profiling not enabled")

        detect_slow_queries()  # should not raise
        client.close.assert_called_once()


class TestCheckConnectionHealth:
    @patch(PATCH_GET_DB)
    def test_healthy_connections(self, mock_get, mock_db):
        client, db = mock_db
        mock_get.return_value = (client, db)
        db.command.return_value = {
            "connections": {
                "current": 10,
                "available": 90,
                "totalCreated": 500,
            }
        }

        check_connection_health()
        client.close.assert_called_once()

    @patch(PATCH_GET_DB)
    def test_high_utilisation_warns(self, mock_get, mock_db):
        client, db = mock_db
        mock_get.return_value = (client, db)
        db.command.return_value = {
            "connections": {
                "current": 90,
                "available": 10,
                "totalCreated": 1000,
            }
        }

        check_connection_health()  # logs warning but doesn't raise
        client.close.assert_called_once()


# ---------------------------------------------------------------------------
# Phase 4 — Summary
# ---------------------------------------------------------------------------
class TestSummary:
    def test_summary_does_not_raise(self):
        log_maintenance_summary()
