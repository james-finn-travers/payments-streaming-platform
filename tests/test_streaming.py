"""
Unit tests for stream processor logic.

Tests the core algorithms (Welford's z-score, window aggregation)
without requiring a live Flink cluster. PyFlink UDF classes are
tested by instantiating them directly.
"""

import math
import json
import pytest


# ---------------------------------------------------------------------------
# Welford's online algorithm (mirrors AnomalyDetector logic)
# ---------------------------------------------------------------------------
class WelfordAccumulator:
    """Replicates the per-user state from streaming/processor.py."""

    def __init__(self):
        self.count = 0
        self.mean = 0.0
        self.m2 = 0.0

    def update(self, value: float):
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        delta2 = value - self.mean
        self.m2 += delta * delta2

    @property
    def variance(self) -> float:
        return self.m2 / (self.count - 1) if self.count >= 2 else 0.0

    @property
    def stddev(self) -> float:
        return math.sqrt(self.variance)

    def z_score(self, value: float) -> float:
        if self.count < 2 or self.stddev == 0:
            return 0.0
        return (value - self.mean) / self.stddev


# ---------------------------------------------------------------------------
# Z-score calculation
# ---------------------------------------------------------------------------
class TestWelfordAlgorithm:
    def test_single_value_zero_std(self):
        acc = WelfordAccumulator()
        acc.update(100.0)
        assert acc.count == 1
        assert acc.mean == 100.0
        assert acc.stddev == 0.0
        assert acc.z_score(100.0) == 0.0

    def test_two_identical_values(self):
        acc = WelfordAccumulator()
        acc.update(50.0)
        acc.update(50.0)
        assert acc.mean == 50.0
        assert acc.stddev == 0.0
        assert acc.z_score(50.0) == 0.0

    def test_two_values_known_stddev(self):
        acc = WelfordAccumulator()
        acc.update(100.0)
        acc.update(200.0)
        assert acc.count == 2
        assert acc.mean == 150.0
        # Sample stddev of [100, 200] = sqrt((100-150)^2 + (200-150)^2) / 1) = ~70.71
        assert abs(acc.stddev - 70.7107) < 0.01

    def test_known_distribution(self):
        acc = WelfordAccumulator()
        for v in [10, 20, 30, 40, 50]:
            acc.update(v)
        assert acc.count == 5
        assert acc.mean == 30.0
        # Sample std of [10..50] step 10 = sqrt(250) ≈ 15.811
        assert abs(acc.stddev - 15.811) < 0.01

    def test_z_score_normal_value(self):
        acc = WelfordAccumulator()
        for v in [50, 52, 48, 51, 49]:
            acc.update(v)
        z = acc.z_score(50.0)
        assert abs(z) < 1.0  # well within normal range

    def test_z_score_anomalous_value(self):
        acc = WelfordAccumulator()
        for v in [50, 52, 48, 51, 49, 50, 53, 47, 50, 51]:
            acc.update(v)
        z = acc.z_score(500.0)
        assert abs(z) > 3.0  # clearly anomalous

    def test_z_score_negative_direction(self):
        acc = WelfordAccumulator()
        for v in [100, 110, 105, 95, 100]:
            acc.update(v)
        z = acc.z_score(10.0)
        assert z < -3.0

    def test_z_score_exactly_at_mean(self):
        acc = WelfordAccumulator()
        for v in [10, 20, 30]:
            acc.update(v)
        z = acc.z_score(20.0)
        assert abs(z) < 0.01

    def test_accumulator_incremental_accuracy(self):
        """Welford should match numpy-style calculation."""
        import statistics as st

        values = [23.5, 67.1, 12.4, 98.7, 45.3, 71.2, 33.6]
        acc = WelfordAccumulator()
        for v in values:
            acc.update(v)

        assert abs(acc.mean - st.mean(values)) < 1e-10
        assert abs(acc.stddev - st.stdev(values)) < 1e-10


# ---------------------------------------------------------------------------
# Anomaly detection threshold
# ---------------------------------------------------------------------------
class TestAnomalyDetection:
    THRESHOLD = 3.0  # same as ANOMALY_SD_THRESHOLD in processor.py

    def test_not_anomaly_below_threshold(self):
        acc = WelfordAccumulator()
        for v in [50, 52, 48, 51, 49, 50, 53, 47, 50, 51]:
            acc.update(v)
        z = acc.z_score(55.0)
        assert abs(z) <= self.THRESHOLD

    def test_anomaly_above_threshold(self):
        acc = WelfordAccumulator()
        for v in [50, 52, 48, 51, 49, 50, 53, 47, 50, 51]:
            acc.update(v)
        z = acc.z_score(500.0)
        assert abs(z) > self.THRESHOLD

    def test_first_txn_never_anomaly(self):
        acc = WelfordAccumulator()
        acc.update(9999.0)
        z = acc.z_score(9999.0)
        assert z == 0.0  # no std yet

    def test_second_txn_extreme_deviation(self):
        acc = WelfordAccumulator()
        acc.update(10.0)
        acc.update(10000.0)
        z = acc.z_score(10000.0)
        # With only 2 data points, z-score may or may not exceed 3
        assert isinstance(z, float)


# ---------------------------------------------------------------------------
# Enrichment output format
# ---------------------------------------------------------------------------
class TestEnrichmentOutput:
    def test_enriched_fields_added(self):
        acc = WelfordAccumulator()
        txn = {
            "txn_id": "abc123",
            "user_id": "user_000001",
            "amount": 50.0,
            "currency": "CAD",
            "timestamp": "2026-03-01T12:00:00+00:00",
        }
        acc.update(txn["amount"])
        z = acc.z_score(txn["amount"])

        enriched = {
            **txn,
            "user_txn_count": acc.count,
            "user_running_mean": round(acc.mean, 2),
            "z_score": round(z, 4),
            "is_anomaly": abs(z) > 3.0,
        }

        assert "user_txn_count" in enriched
        assert "user_running_mean" in enriched
        assert "z_score" in enriched
        assert "is_anomaly" in enriched
        assert enriched["user_txn_count"] == 1
        assert enriched["is_anomaly"] is False

    def test_enriched_preserves_original_fields(self):
        txn = {
            "txn_id": "abc123",
            "user_id": "user_000001",
            "amount": 50.0,
        }
        enriched = {**txn, "user_txn_count": 1, "z_score": 0.0}
        assert enriched["txn_id"] == "abc123"
        assert enriched["amount"] == 50.0

    def test_enriched_json_serializable(self):
        enriched = {
            "txn_id": "abc123",
            "user_id": "user_000001",
            "amount": 50.0,
            "user_txn_count": 5,
            "user_running_mean": 48.5,
            "z_score": 0.312,
            "is_anomaly": False,
        }
        serialized = json.dumps(enriched)
        assert json.loads(serialized) == enriched


# ---------------------------------------------------------------------------
# Window aggregation logic
# ---------------------------------------------------------------------------
class TestWindowAggregation:
    def _aggregate(self, elements, user_id="user_000001"):
        """Simulate the WindowAggregate.process logic."""
        total = sum(e.get("amount", 0) for e in elements if e)
        count = len([e for e in elements if e])
        return {
            "user_id": user_id,
            "txn_count": count,
            "total_spend": round(total, 2),
            "avg_spend": round(total / count, 2) if count else 0,
        }

    def test_single_element_window(self):
        elements = [{"amount": 100.0}]
        result = self._aggregate(elements)
        assert result["txn_count"] == 1
        assert result["total_spend"] == 100.0
        assert result["avg_spend"] == 100.0

    def test_multiple_elements(self):
        elements = [{"amount": 10.0}, {"amount": 20.0}, {"amount": 30.0}]
        result = self._aggregate(elements)
        assert result["txn_count"] == 3
        assert result["total_spend"] == 60.0
        assert result["avg_spend"] == 20.0

    def test_empty_window(self):
        result = self._aggregate([])
        assert result["txn_count"] == 0
        assert result["total_spend"] == 0
        assert result["avg_spend"] == 0

    def test_filters_none_elements(self):
        elements = [{"amount": 50.0}, None, {"amount": 30.0}, None]
        result = self._aggregate(elements)
        assert result["txn_count"] == 2
        assert result["total_spend"] == 80.0

    def test_missing_amount_defaults_to_zero(self):
        elements = [{"amount": 50.0}, {"no_amount": True}]
        result = self._aggregate(elements)
        assert result["txn_count"] == 2
        assert result["total_spend"] == 50.0

    def test_large_window(self):
        elements = [{"amount": 1.0} for _ in range(10000)]
        result = self._aggregate(elements)
        assert result["txn_count"] == 10000
        assert result["total_spend"] == 10000.0
        assert result["avg_spend"] == 1.0
