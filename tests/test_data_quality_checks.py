"""
Unit tests for the Airflow data quality checks DAG.

Each check function is tested in isolation by mocking _get_collection
to return controlled MongoDB-like responses.

Airflow stubs are registered in conftest.py.
"""

import pytest
from unittest.mock import patch, MagicMock

from airflow.dags.data_quality_checks import (
    EXPECTED_SCHEMA_FIELDS,
    REQUIRED_NOT_NULL_FIELDS,
    VALID_CURRENCIES,
    VALID_STATUSES,
    VALID_PAYMENT_METHODS,
    VOLUME_DEVIATION_FACTOR,
    APPROVED_RATE_MIN,
    APPROVED_RATE_MAX,
    AMOUNT_MIN,
    AMOUNT_MAX,
    check_schema_violations,
    check_volume_anomalies,
    check_null_fields,
    check_status_distribution,
    check_staleness,
    check_duplicates,
    check_amount_outliers,
    check_accepted_values,
    log_quality_summary,
)

PATCH_TARGET = "airflow.dags.data_quality_checks._get_collection"


def _good_doc():
    return {
        "txn_id": "aabbccdd11223344aabbccdd",
        "user_id": "user_000001",
        "amount": 49.99,
        "currency": "CAD",
        "timestamp": "2026-03-01T12:00:00+00:00",
        "merchant_id": "merchant_0001",
        "merchant_category": "grocery",
        "payment_method": "credit_card",
        "status": "approved",
        "location": {"lat": 43.65, "lon": -79.38},
    }


# ---------------------------------------------------------------------------
# Config sanity checks
# ---------------------------------------------------------------------------
class TestConfigSanity:
    def test_expected_fields_count(self):
        assert len(EXPECTED_SCHEMA_FIELDS) == 10

    def test_required_not_null_subset(self):
        for f in REQUIRED_NOT_NULL_FIELDS:
            assert f in EXPECTED_SCHEMA_FIELDS

    def test_volume_factor_gt_1(self):
        assert VOLUME_DEVIATION_FACTOR > 1.0

    def test_approved_rate_range(self):
        assert 0 < APPROVED_RATE_MIN < APPROVED_RATE_MAX < 1.0

    def test_amount_bounds(self):
        assert 0 < AMOUNT_MIN < AMOUNT_MAX


# ---------------------------------------------------------------------------
# Check 1 — Schema violations
# ---------------------------------------------------------------------------
class TestSchemaViolations:
    @patch(PATCH_TARGET)
    def test_passes_good_docs(self, mock_get):
        col = MagicMock()
        col.find.return_value.limit.return_value = [_good_doc() for _ in range(5)]
        mock_get.return_value = col
        check_schema_violations()  # should not raise

    @patch(PATCH_TARGET)
    def test_fails_missing_field(self, mock_get):
        doc = _good_doc()
        del doc["txn_id"]
        col = MagicMock()
        col.find.return_value.limit.return_value = [doc]
        mock_get.return_value = col
        with pytest.raises(ValueError, match="SCHEMA_VIOLATION"):
            check_schema_violations()

    @patch(PATCH_TARGET)
    def test_fails_wrong_type(self, mock_get):
        doc = _good_doc()
        doc["amount"] = "not_a_number"
        col = MagicMock()
        col.find.return_value.limit.return_value = [doc]
        mock_get.return_value = col
        with pytest.raises(ValueError, match="SCHEMA_VIOLATION"):
            check_schema_violations()

    @patch(PATCH_TARGET)
    def test_fails_extra_field(self, mock_get):
        doc = _good_doc()
        doc["rogue_field"] = "unexpected"
        col = MagicMock()
        col.find.return_value.limit.return_value = [doc]
        mock_get.return_value = col
        with pytest.raises(ValueError, match="SCHEMA_VIOLATION"):
            check_schema_violations()

    @patch(PATCH_TARGET)
    def test_skips_when_no_data(self, mock_get):
        col = MagicMock()
        col.find.return_value.limit.return_value = []
        mock_get.return_value = col
        check_schema_violations()  # should not raise

    @patch(PATCH_TARGET)
    def test_allows_enrichment_extras(self, mock_get):
        doc = _good_doc()
        doc["_id"] = "mongo_id"
        doc["z_score"] = 1.5
        doc["mean_amount"] = 45.0
        col = MagicMock()
        col.find.return_value.limit.return_value = [doc]
        mock_get.return_value = col
        check_schema_violations()  # z_score, mean_amount are in allowed_extras


# ---------------------------------------------------------------------------
# Check 2 — Volume anomalies
# ---------------------------------------------------------------------------
class TestVolumeAnomalies:
    @patch(PATCH_TARGET)
    def test_passes_normal_volume(self, mock_get):
        col = MagicMock()
        col.count_documents.side_effect = [1000, 24000]  # ratio = 1.0
        mock_get.return_value = col
        check_volume_anomalies()

    @patch(PATCH_TARGET)
    def test_fails_spike(self, mock_get):
        col = MagicMock()
        col.count_documents.side_effect = [10000, 6000]  # avg=250, ratio=40
        mock_get.return_value = col
        with pytest.raises(ValueError, match="VOLUME_SPIKE"):
            check_volume_anomalies()

    @patch(PATCH_TARGET)
    def test_fails_drop(self, mock_get):
        col = MagicMock()
        col.count_documents.side_effect = [1, 24000]  # avg=1000, ratio=0.001
        mock_get.return_value = col
        with pytest.raises(ValueError, match="VOLUME_DROP"):
            check_volume_anomalies()

    @patch(PATCH_TARGET)
    def test_no_data_at_all(self, mock_get):
        col = MagicMock()
        col.count_documents.side_effect = [0, 0]
        mock_get.return_value = col
        check_volume_anomalies()  # should not raise

    @patch(PATCH_TARGET)
    def test_first_data_appearing(self, mock_get):
        col = MagicMock()
        col.count_documents.side_effect = [100, 100]  # all in last hour
        mock_get.return_value = col
        # ratio = 100 / (100/24) ≈ 24 > 3 → VOLUME_SPIKE
        with pytest.raises(ValueError, match="VOLUME_SPIKE"):
            check_volume_anomalies()


# ---------------------------------------------------------------------------
# Check 3 — Null fields
# ---------------------------------------------------------------------------
class TestNullFields:
    @patch(PATCH_TARGET)
    def test_passes_no_nulls(self, mock_get):
        col = MagicMock()
        col.count_documents.return_value = 0
        mock_get.return_value = col
        check_null_fields()

    @patch(PATCH_TARGET)
    def test_fails_with_nulls(self, mock_get):
        col = MagicMock()
        # First call returns nulls, rest return 0
        col.count_documents.side_effect = [5] + [0] * (len(REQUIRED_NOT_NULL_FIELDS) - 1)
        mock_get.return_value = col
        with pytest.raises(ValueError, match="NULL_FIELDS"):
            check_null_fields()


# ---------------------------------------------------------------------------
# Check 4 — Status distribution
# ---------------------------------------------------------------------------
class TestStatusDistribution:
    @patch(PATCH_TARGET)
    def test_passes_normal(self, mock_get):
        col = MagicMock()
        col.aggregate.return_value = [
            {"_id": "approved", "count": 900},
            {"_id": "declined", "count": 50},
            {"_id": "pending", "count": 50},
        ]
        mock_get.return_value = col
        check_status_distribution()

    @patch(PATCH_TARGET)
    def test_fails_low_approval(self, mock_get):
        col = MagicMock()
        col.aggregate.return_value = [
            {"_id": "approved", "count": 500},
            {"_id": "declined", "count": 500},
        ]
        mock_get.return_value = col
        with pytest.raises(ValueError, match="STATUS_DRIFT"):
            check_status_distribution()

    @patch(PATCH_TARGET)
    def test_fails_invalid_status(self, mock_get):
        col = MagicMock()
        col.aggregate.return_value = [
            {"_id": "approved", "count": 900},
            {"_id": "refunded", "count": 100},
        ]
        mock_get.return_value = col
        with pytest.raises(ValueError, match="INVALID_STATUS"):
            check_status_distribution()

    @patch(PATCH_TARGET)
    def test_skips_no_data(self, mock_get):
        col = MagicMock()
        col.aggregate.return_value = []
        mock_get.return_value = col
        check_status_distribution()


# ---------------------------------------------------------------------------
# Check 5 — Staleness
# ---------------------------------------------------------------------------
class TestStaleness:
    @patch(PATCH_TARGET)
    def test_passes_fresh(self, mock_get):
        col = MagicMock()
        col.count_documents.return_value = 50
        mock_get.return_value = col
        check_staleness()

    @patch(PATCH_TARGET)
    def test_fails_stale(self, mock_get):
        col = MagicMock()
        col.count_documents.return_value = 0
        mock_get.return_value = col
        with pytest.raises(ValueError, match="STALE_DATA"):
            check_staleness()


# ---------------------------------------------------------------------------
# Check 6 — Duplicates
# ---------------------------------------------------------------------------
class TestDuplicates:
    @patch(PATCH_TARGET)
    def test_passes_no_dupes(self, mock_get):
        col = MagicMock()
        col.aggregate.return_value = []
        mock_get.return_value = col
        check_duplicates()

    @patch(PATCH_TARGET)
    def test_fails_with_dupes(self, mock_get):
        col = MagicMock()
        col.aggregate.return_value = [{"_id": "dup_txn", "count": 3}]
        mock_get.return_value = col
        with pytest.raises(ValueError, match="DUPLICATE_TXN_IDS"):
            check_duplicates()


# ---------------------------------------------------------------------------
# Check 7 — Amount outliers
# ---------------------------------------------------------------------------
class TestAmountOutliers:
    @patch(PATCH_TARGET)
    def test_passes_normal(self, mock_get):
        col = MagicMock()
        col.count_documents.return_value = 0
        mock_get.return_value = col
        check_amount_outliers()

    @patch(PATCH_TARGET)
    def test_fails_negative(self, mock_get):
        col = MagicMock()
        col.count_documents.side_effect = [5, 0, 0]
        mock_get.return_value = col
        with pytest.raises(ValueError, match="AMOUNT_OUTLIER"):
            check_amount_outliers()

    @patch(PATCH_TARGET)
    def test_fails_over_max(self, mock_get):
        col = MagicMock()
        col.count_documents.side_effect = [0, 3, 0]
        mock_get.return_value = col
        with pytest.raises(ValueError, match="AMOUNT_OUTLIER"):
            check_amount_outliers()

    @patch(PATCH_TARGET)
    def test_fails_under_min(self, mock_get):
        col = MagicMock()
        col.count_documents.side_effect = [0, 0, 2]
        mock_get.return_value = col
        with pytest.raises(ValueError, match="AMOUNT_OUTLIER"):
            check_amount_outliers()


# ---------------------------------------------------------------------------
# Check 8 — Accepted values
# ---------------------------------------------------------------------------
class TestAcceptedValues:
    @patch(PATCH_TARGET)
    def test_passes_valid(self, mock_get):
        col = MagicMock()
        col.aggregate.side_effect = [
            [{"_id": "CAD"}, {"_id": "USD"}],
            [{"_id": "credit_card"}, {"_id": "debit_card"}],
            [{"_id": "approved"}, {"_id": "declined"}],
        ]
        mock_get.return_value = col
        check_accepted_values()

    @patch(PATCH_TARGET)
    def test_fails_invalid_currency(self, mock_get):
        col = MagicMock()
        col.aggregate.side_effect = [
            [{"_id": "CAD"}, {"_id": "BTC"}],
            [{"_id": "credit_card"}],
            [{"_id": "approved"}],
        ]
        mock_get.return_value = col
        with pytest.raises(ValueError, match="INVALID_VALUES"):
            check_accepted_values()

    @patch(PATCH_TARGET)
    def test_fails_invalid_payment_method(self, mock_get):
        col = MagicMock()
        col.aggregate.side_effect = [
            [{"_id": "CAD"}],
            [{"_id": "bitcoin"}],
            [{"_id": "approved"}],
        ]
        mock_get.return_value = col
        with pytest.raises(ValueError, match="INVALID_VALUES"):
            check_accepted_values()


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
class TestSummary:
    def test_summary_does_not_raise(self):
        log_quality_summary()
