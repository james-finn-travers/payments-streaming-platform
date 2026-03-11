"""
Unit tests for the daily-spend MongoDB sink consumer.

Tests record parsing, epoch-ms timestamp conversion, batch upsert logic,
and collection/index setup without requiring a live MongoDB or Kafka instance.
"""

import json
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from consumers.daily_spend_sink import (
    _parse_record,
    upsert_batch,
    ensure_collection,
    DAILY_SPEND_COLLECTION,
)


# ---------------------------------------------------------------------------
# _parse_record
# ---------------------------------------------------------------------------
class TestParseRecord:
    def test_valid_record(self):
        raw = json.dumps({
            "user_id": "user_000001",
            "window_start": 1709251200000,  # 2024-03-01T00:00:00Z
            "window_end": 1709337600000,
            "txn_count": 5,
            "total_spend": 250.50,
            "avg_spend": 50.10,
        }).encode()
        result = _parse_record(raw)
        assert result is not None
        assert result["user_id"] == "user_000001"
        assert result["txn_count"] == 5
        assert result["total_spend"] == 250.50

    def test_converts_epoch_ms_to_datetime(self):
        raw = json.dumps({
            "user_id": "user_000001",
            "window_start": 1709251200000,
            "window_end": 1709337600000,
        }).encode()
        result = _parse_record(raw)
        assert isinstance(result["window_start"], datetime)
        assert isinstance(result["window_end"], datetime)
        assert result["window_start"].tzinfo == timezone.utc

    def test_non_numeric_window_unchanged(self):
        raw = json.dumps({
            "user_id": "user_000001",
            "window_start": "2024-03-01T00:00:00Z",
            "window_end": "2024-03-02T00:00:00Z",
        }).encode()
        result = _parse_record(raw)
        assert result["window_start"] == "2024-03-01T00:00:00Z"

    def test_invalid_json_returns_none(self):
        assert _parse_record(b"not json") is None

    def test_empty_bytes_returns_none(self):
        assert _parse_record(b"") is None

    def test_preserves_extra_fields(self):
        raw = json.dumps({
            "user_id": "user_000001",
            "window_start": 1000000,
            "custom_field": "hello",
        }).encode()
        result = _parse_record(raw)
        assert result["custom_field"] == "hello"


# ---------------------------------------------------------------------------
# upsert_batch
# ---------------------------------------------------------------------------
class TestUpsertBatch:
    def test_empty_batch_does_nothing(self):
        col = MagicMock()
        upsert_batch(col, [])
        col.bulk_write.assert_not_called()

    def test_single_doc_upsert(self):
        col = MagicMock()
        batch = [{
            "user_id": "user_000001",
            "window_start": datetime(2024, 3, 1, tzinfo=timezone.utc),
            "total_spend": 100.0,
        }]
        upsert_batch(col, batch)
        col.bulk_write.assert_called_once()
        ops = col.bulk_write.call_args[0][0]
        assert len(ops) == 1

    def test_multiple_docs(self):
        col = MagicMock()
        batch = [
            {"user_id": f"user_{i:06d}", "window_start": datetime(2024, 3, 1, tzinfo=timezone.utc), "total_spend": i * 10.0}
            for i in range(5)
        ]
        upsert_batch(col, batch)
        ops = col.bulk_write.call_args[0][0]
        assert len(ops) == 5

    def test_skips_docs_missing_user_id(self):
        col = MagicMock()
        batch = [
            {"window_start": datetime(2024, 3, 1, tzinfo=timezone.utc), "total_spend": 50.0},
        ]
        upsert_batch(col, batch)
        col.bulk_write.assert_not_called()

    def test_skips_docs_missing_window_start(self):
        col = MagicMock()
        batch = [
            {"user_id": "user_000001", "total_spend": 50.0},
        ]
        upsert_batch(col, batch)
        col.bulk_write.assert_not_called()

    def test_unordered_writes(self):
        col = MagicMock()
        batch = [{"user_id": "u", "window_start": datetime(2024, 1, 1, tzinfo=timezone.utc)}]
        upsert_batch(col, batch)
        _, kwargs = col.bulk_write.call_args
        assert kwargs.get("ordered") is False

    def test_handles_bulk_write_error(self):
        from pymongo.errors import BulkWriteError

        col = MagicMock()
        col.bulk_write.side_effect = BulkWriteError(
            {"writeErrors": [{"errmsg": "dup key"}]}
        )
        batch = [{"user_id": "u", "window_start": datetime(2024, 1, 1, tzinfo=timezone.utc)}]
        upsert_batch(col, batch)  # should not raise


# ---------------------------------------------------------------------------
# ensure_collection
# ---------------------------------------------------------------------------
class TestEnsureCollection:
    def test_creates_collection_if_missing(self):
        db = MagicMock()
        db.list_collection_names.return_value = []
        ensure_collection(db)
        db.create_collection.assert_called_once_with(DAILY_SPEND_COLLECTION)

    def test_skips_existing_collection(self):
        db = MagicMock()
        db.list_collection_names.return_value = [DAILY_SPEND_COLLECTION]
        ensure_collection(db)
        db.create_collection.assert_not_called()

    def test_creates_indexes(self):
        db = MagicMock()
        db.list_collection_names.return_value = [DAILY_SPEND_COLLECTION]
        col = db.__getitem__.return_value
        ensure_collection(db)
        assert col.create_index.call_count == 3  # compound unique + window_start + user_id

    def test_unique_compound_index(self):
        db = MagicMock()
        db.list_collection_names.return_value = []
        col = db.__getitem__.return_value
        ensure_collection(db)
        # First create_index call should be the compound unique index
        first_call = col.create_index.call_args_list[0]
        keys, kwargs = first_call[0][0], first_call[1]
        assert ("user_id", 1) in keys
        assert ("window_start", 1) in keys
        assert kwargs["unique"] is True
