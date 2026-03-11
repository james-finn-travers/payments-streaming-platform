"""Shared test fixtures for the transaction analytics pipeline."""

import os
import sys
import types
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Airflow stubs — Apache Airflow is not installed in the local dev venv.
# The project's airflow/ directory contains DAGs that import from airflow.
# We register lightweight stubs so pytest can collect and run those tests.
# ---------------------------------------------------------------------------
_PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
_AIRFLOW_DIR = os.path.join(_PROJECT_ROOT, "airflow")
_DAGS_DIR = os.path.join(_AIRFLOW_DIR, "dags")

if "airflow" not in sys.modules:
    _af = types.ModuleType("airflow")
    _af.__path__ = [_AIRFLOW_DIR]
    _af.DAG = MagicMock()
    sys.modules["airflow"] = _af

if "airflow.dags" not in sys.modules:
    _dags = types.ModuleType("airflow.dags")
    _dags.__path__ = [_DAGS_DIR]
    sys.modules["airflow.dags"] = _dags
    sys.modules["airflow"].dags = _dags

if "airflow.operators" not in sys.modules:
    _ops = types.ModuleType("airflow.operators")
    _ops.__path__ = []
    sys.modules["airflow.operators"] = _ops
    sys.modules["airflow"].operators = _ops

if "airflow.operators.python" not in sys.modules:
    _py = types.ModuleType("airflow.operators.python")
    _py.PythonOperator = MagicMock()
    sys.modules["airflow.operators.python"] = _py
    sys.modules["airflow.operators"].python = _py

if "airflow.operators.bash" not in sys.modules:
    _bash = types.ModuleType("airflow.operators.bash")
    _bash.BashOperator = MagicMock()
    sys.modules["airflow.operators.bash"] = _bash
    sys.modules["airflow.operators"].bash = _bash

# ---------------------------------------------------------------------------
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
