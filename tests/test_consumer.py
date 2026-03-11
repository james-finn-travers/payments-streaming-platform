"""
Unit tests for the MongoDB sink consumer.

Tests record parsing, batch upsert logic, and collection setup
without requiring a live MongoDB or Kafka instance.
"""

import json
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

from consumers.db_sink import (
    _parse_record,
    upsert_batch,
    ensure_collections,
    ENRICHED_TOPIC,
    ANOMALY_TOPIC,
    TXN_COLLECTION,
    ANOMALY_COLLECTION,
)


# ---------------------------------------------------------------------------
# _parse_record
# ---------------------------------------------------------------------------
class TestParseRecord:
    """Tests for raw Kafka value → dict parsing."""

    def test_valid_json(self):
        raw = json.dumps({
            "txn_id": "abc123",
            "user_id": "user_000001",
            "amount": 49.99,
            "timestamp": "2026-03-01T12:00:00+00:00",
        }).encode("utf-8")
        result = _parse_record(raw)
        assert result is not None
        assert result["txn_id"] == "abc123"
        assert result["amount"] == 49.99

    def test_converts_iso_timestamp_to_datetime(self):
        raw = json.dumps({
            "txn_id": "abc123",
            "timestamp": "2026-03-01T12:00:00+00:00",
        }).encode("utf-8")
        result = _parse_record(raw)
        assert isinstance(result["timestamp"], datetime)

    def test_non_string_timestamp_unchanged(self):
        raw = json.dumps({
            "txn_id": "abc123",
            "timestamp": 1709316000,
        }).encode("utf-8")
        result = _parse_record(raw)
        assert result["timestamp"] == 1709316000

    def test_invalid_json_returns_none(self):
        raw = b"not valid json"
        result = _parse_record(raw)
        assert result is None

    def test_empty_bytes_returns_none(self):
        raw = b""
        result = _parse_record(raw)
        assert result is None

    def test_malformed_timestamp_returns_none(self):
        raw = json.dumps({
            "txn_id": "abc123",
            "timestamp": "not-a-date",
        }).encode("utf-8")
        result = _parse_record(raw)
        assert result is None

    def test_nested_json(self):
        raw = json.dumps({
            "txn_id": "abc123",
            "timestamp": "2026-03-01T00:00:00+00:00",
            "location": {"lat": 43.65, "lon": -79.38},
        }).encode("utf-8")
        result = _parse_record(raw)
        assert result["location"]["lat"] == 43.65


# ---------------------------------------------------------------------------
# upsert_batch
# ---------------------------------------------------------------------------
class TestUpsertBatch:
    """Tests for idempotent batch upserts."""

    def test_empty_batch_does_nothing(self):
        mock_col = MagicMock()
        upsert_batch(mock_col, [])
        mock_col.bulk_write.assert_not_called()

    def test_single_doc_upsert(self):
        mock_col = MagicMock()
        batch = [{"txn_id": "abc123", "amount": 49.99}]
        upsert_batch(mock_col, batch)
        mock_col.bulk_write.assert_called_once()
        ops = mock_col.bulk_write.call_args[0][0]
        assert len(ops) == 1

    def test_multiple_docs_upsert(self):
        mock_col = MagicMock()
        batch = [
            {"txn_id": f"txn_{i:04d}", "amount": i * 10.0}
            for i in range(10)
        ]
        upsert_batch(mock_col, batch)
        mock_col.bulk_write.assert_called_once()
        ops = mock_col.bulk_write.call_args[0][0]
        assert len(ops) == 10

    def test_skips_docs_without_txn_id(self):
        mock_col = MagicMock()
        batch = [
            {"txn_id": "good_doc", "amount": 50.0},
            {"no_txn_id": True, "amount": 25.0},
        ]
        upsert_batch(mock_col, batch)
        ops = mock_col.bulk_write.call_args[0][0]
        assert len(ops) == 1

    def test_unordered_writes(self):
        mock_col = MagicMock()
        batch = [{"txn_id": "abc", "amount": 1.0}]
        upsert_batch(mock_col, batch)
        _, kwargs = mock_col.bulk_write.call_args
        assert kwargs.get("ordered") is False

    def test_handles_bulk_write_error(self):
        from pymongo.errors import BulkWriteError

        mock_col = MagicMock()
        mock_col.bulk_write.side_effect = BulkWriteError(
            {"writeErrors": [{"errmsg": "duplicate"}]}
        )
        batch = [{"txn_id": "dup", "amount": 1.0}]
        # Should not raise — just logs
        upsert_batch(mock_col, batch)


# ---------------------------------------------------------------------------
# ensure_collections
# ---------------------------------------------------------------------------
class TestEnsureCollections:
    """Tests for time-series collection and index creation."""

    def test_creates_txn_collection_if_missing(self):
        mock_db = MagicMock()
        mock_db.list_collection_names.return_value = []
        ensure_collections(mock_db)
        calls = [c[0][0] for c in mock_db.create_collection.call_args_list]
        assert TXN_COLLECTION in calls

    def test_creates_anomaly_collection_if_missing(self):
        mock_db = MagicMock()
        mock_db.list_collection_names.return_value = []
        ensure_collections(mock_db)
        calls = [c[0][0] for c in mock_db.create_collection.call_args_list]
        assert ANOMALY_COLLECTION in calls

    def test_skips_existing_collections(self):
        mock_db = MagicMock()
        mock_db.list_collection_names.return_value = [
            TXN_COLLECTION,
            ANOMALY_COLLECTION,
        ]
        ensure_collections(mock_db)
        mock_db.create_collection.assert_not_called()

    def test_creates_indexes(self):
        mock_db = MagicMock()
        mock_db.list_collection_names.return_value = [
            TXN_COLLECTION,
            ANOMALY_COLLECTION,
        ]
        ensure_collections(mock_db)
        # Should create indexes on existing collections
        txn_col = mock_db[TXN_COLLECTION]
        anom_col = mock_db[ANOMALY_COLLECTION]
        assert txn_col.create_index.call_count >= 3
        assert anom_col.create_index.call_count >= 2

    def test_timeseries_config(self):
        mock_db = MagicMock()
        mock_db.list_collection_names.return_value = []
        ensure_collections(mock_db)
        # Verify time-series settings
        for call_item in mock_db.create_collection.call_args_list:
            _, kwargs = call_item
            ts = kwargs.get("timeseries", {})
            assert ts["timeField"] == "timestamp"
            assert ts["metaField"] == "user_id"
            assert ts["granularity"] == "seconds"
