"""Shared test fixtures for the transaction analytics pipeline."""

import pytest
from producer.producer import generate_transaction, MERCHANTS, USERS


@pytest.fixture
def sample_transaction():
    """Generate a single sample transaction for testing."""
    return generate_transaction()


@pytest.fixture
def sample_transactions():
    """Generate a batch of 100 sample transactions."""
    return [generate_transaction() for _ in range(100)]


@pytest.fixture
def merchant_ids():
    """List of all valid merchant IDs."""
    return [m["merchant_id"] for m in MERCHANTS]


@pytest.fixture
def user_ids():
    """List of all valid user IDs."""
    return [u["user_id"] for u in USERS]
