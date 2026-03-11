"""
Unit tests for JSON Schema validation of transactions.

Uses the official transaction schema at schemas/transaction.schema.json
to validate that generated transactions conform, and that invalid data
is correctly rejected.
"""

import json
import os
import pytest
from jsonschema import validate, ValidationError

from producer.producer import generate_transaction

# ---------------------------------------------------------------------------
# Load schema
# ---------------------------------------------------------------------------
SCHEMA_PATH = os.path.join(
    os.path.dirname(__file__), "..", "schemas", "transaction.schema.json"
)


@pytest.fixture(scope="module")
def schema():
    with open(SCHEMA_PATH) as f:
        return json.load(f)


@pytest.fixture
def valid_txn():
    """A known-good transaction matching the schema."""
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
# Generated transactions should pass
# ---------------------------------------------------------------------------
class TestGeneratedTransactions:
    def test_single_generated_txn_valid(self, schema):
        txn = generate_transaction()
        validate(instance=txn, schema=schema)

    def test_100_generated_txns_valid(self, schema):
        for _ in range(100):
            txn = generate_transaction()
            validate(instance=txn, schema=schema)


# ---------------------------------------------------------------------------
# Valid transactions
# ---------------------------------------------------------------------------
class TestValidTransactions:
    def test_minimal_valid(self, schema, valid_txn):
        validate(instance=valid_txn, schema=schema)

    def test_all_currencies(self, schema, valid_txn):
        for cur in ["CAD", "USD", "EUR", "GBP"]:
            valid_txn["currency"] = cur
            validate(instance=valid_txn, schema=schema)

    def test_all_statuses(self, schema, valid_txn):
        for st in ["approved", "declined", "pending"]:
            valid_txn["status"] = st
            validate(instance=valid_txn, schema=schema)

    def test_all_payment_methods(self, schema, valid_txn):
        for pm in ["credit_card", "debit_card", "e_transfer", "apple_pay", "google_pay"]:
            valid_txn["payment_method"] = pm
            validate(instance=valid_txn, schema=schema)

    def test_all_categories(self, schema, valid_txn):
        cats = [
            "grocery", "electronics", "clothing", "restaurant", "travel",
            "entertainment", "health", "gas_station", "subscription", "marketplace",
        ]
        for cat in cats:
            valid_txn["merchant_category"] = cat
            validate(instance=valid_txn, schema=schema)

    def test_boundary_amount_min(self, schema, valid_txn):
        valid_txn["amount"] = 0.01
        validate(instance=valid_txn, schema=schema)

    def test_boundary_amount_max(self, schema, valid_txn):
        valid_txn["amount"] = 15000.0
        validate(instance=valid_txn, schema=schema)

    def test_integer_amount(self, schema, valid_txn):
        valid_txn["amount"] = 100
        validate(instance=valid_txn, schema=schema)

    def test_location_extremes(self, schema, valid_txn):
        valid_txn["location"] = {"lat": -90.0, "lon": -180.0}
        validate(instance=valid_txn, schema=schema)
        valid_txn["location"] = {"lat": 90.0, "lon": 180.0}
        validate(instance=valid_txn, schema=schema)


# ---------------------------------------------------------------------------
# Missing required fields
# ---------------------------------------------------------------------------
class TestMissingFields:
    @pytest.mark.parametrize("field", [
        "txn_id", "user_id", "amount", "currency", "timestamp",
        "merchant_id", "merchant_category", "payment_method",
        "status", "location",
    ])
    def test_missing_required_field(self, schema, valid_txn, field):
        del valid_txn[field]
        with pytest.raises(ValidationError):
            validate(instance=valid_txn, schema=schema)


# ---------------------------------------------------------------------------
# Invalid values
# ---------------------------------------------------------------------------
class TestInvalidValues:
    def test_negative_amount(self, schema, valid_txn):
        valid_txn["amount"] = -10.0
        with pytest.raises(ValidationError):
            validate(instance=valid_txn, schema=schema)

    def test_zero_amount(self, schema, valid_txn):
        valid_txn["amount"] = 0
        with pytest.raises(ValidationError):
            validate(instance=valid_txn, schema=schema)

    def test_amount_exceeds_max(self, schema, valid_txn):
        valid_txn["amount"] = 15000.01
        with pytest.raises(ValidationError):
            validate(instance=valid_txn, schema=schema)

    def test_amount_as_string(self, schema, valid_txn):
        valid_txn["amount"] = "49.99"
        with pytest.raises(ValidationError):
            validate(instance=valid_txn, schema=schema)

    def test_invalid_currency(self, schema, valid_txn):
        valid_txn["currency"] = "BTC"
        with pytest.raises(ValidationError):
            validate(instance=valid_txn, schema=schema)

    def test_invalid_status(self, schema, valid_txn):
        valid_txn["status"] = "refunded"
        with pytest.raises(ValidationError):
            validate(instance=valid_txn, schema=schema)

    def test_invalid_payment_method(self, schema, valid_txn):
        valid_txn["payment_method"] = "bitcoin"
        with pytest.raises(ValidationError):
            validate(instance=valid_txn, schema=schema)

    def test_invalid_category(self, schema, valid_txn):
        valid_txn["merchant_category"] = "weapons"
        with pytest.raises(ValidationError):
            validate(instance=valid_txn, schema=schema)

    def test_empty_txn_id(self, schema, valid_txn):
        valid_txn["txn_id"] = ""
        with pytest.raises(ValidationError):
            validate(instance=valid_txn, schema=schema)

    def test_bad_txn_id_pattern(self, schema, valid_txn):
        valid_txn["txn_id"] = "NOT-HEX-24-CHARS!!!!!!!!"
        with pytest.raises(ValidationError):
            validate(instance=valid_txn, schema=schema)

    def test_bad_user_id_pattern(self, schema, valid_txn):
        valid_txn["user_id"] = "admin"
        with pytest.raises(ValidationError):
            validate(instance=valid_txn, schema=schema)

    def test_bad_merchant_id_pattern(self, schema, valid_txn):
        valid_txn["merchant_id"] = "shop_99"
        with pytest.raises(ValidationError):
            validate(instance=valid_txn, schema=schema)

    def test_null_user_id(self, schema, valid_txn):
        valid_txn["user_id"] = None
        with pytest.raises(ValidationError):
            validate(instance=valid_txn, schema=schema)

    def test_location_lat_out_of_range(self, schema, valid_txn):
        valid_txn["location"] = {"lat": 91.0, "lon": 0.0}
        with pytest.raises(ValidationError):
            validate(instance=valid_txn, schema=schema)

    def test_location_lon_out_of_range(self, schema, valid_txn):
        valid_txn["location"] = {"lat": 0.0, "lon": 181.0}
        with pytest.raises(ValidationError):
            validate(instance=valid_txn, schema=schema)

    def test_location_missing_lat(self, schema, valid_txn):
        valid_txn["location"] = {"lon": -79.0}
        with pytest.raises(ValidationError):
            validate(instance=valid_txn, schema=schema)

    def test_additional_property_rejected(self, schema, valid_txn):
        valid_txn["extra_field"] = "surprise"
        with pytest.raises(ValidationError):
            validate(instance=valid_txn, schema=schema)

    def test_empty_object(self, schema):
        with pytest.raises(ValidationError):
            validate(instance={}, schema=schema)
