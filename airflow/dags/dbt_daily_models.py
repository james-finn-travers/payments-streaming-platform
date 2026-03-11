"""
Airflow DAG — dbt daily models

Runs the MongoDB → Parquet export, then executes dbt to build
staging views and mart tables (DuckDB backend).

Schedule: daily at 02:00 UTC (after a full day of streaming data).
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

# ---------------------------------------------------------------------------
# Paths — mapped via docker-compose volume
# ---------------------------------------------------------------------------
DBT_PROJECT_DIR = "/opt/dbt_models"
EXPORT_SCRIPT = f"python {DBT_PROJECT_DIR}/scripts/export_mongo_to_parquet.py"

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt_daily_models",
    default_args=default_args,
    description="Export MongoDB snapshots to Parquet, then run dbt models",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dbt", "batch", "analytics"],
) as dag:

    export_mongo = BashOperator(
        task_id="export_mongo_to_parquet",
        bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            {EXPORT_SCRIPT}
        """,
        env={
            "MONGO_URI": "mongodb://mongo:27017",
            "MONGO_DB": "txn_analytics",
            "EXPORT_DIR": f"{DBT_PROJECT_DIR}/seeds/raw",
            "EXPORT_LOOKBACK": "90",
        },
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir .",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir . --full-refresh",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir .",
    )

    export_mongo >> dbt_deps >> dbt_run >> dbt_test
