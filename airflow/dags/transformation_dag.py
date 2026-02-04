"""
DAG 3: Transformation (Topic 8)

Schedule: Daily at 03:00 AM. Depends on batch_processing_dag success.
Tasks: dbt_run, dbt_test, generate_docs.
Retry: Don't retry on test failure (indicates real issue).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 0,  # Don't retry on test failure
    "tags": ["dbt", "transformation"],
}

with DAG(
    dag_id="transformation_dag",
    default_args=default_args,
    schedule_interval="0 3 * * *",  # Daily at 03:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt 2>/dev/null || cd dbt; dbt run",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt 2>/dev/null || cd dbt; dbt test",
    )
    generate_docs = BashOperator(
        task_id="generate_docs",
        bash_command="cd /opt/airflow/dbt 2>/dev/null || cd dbt; dbt docs generate 2>/dev/null || true",
    )
    dbt_run >> dbt_test >> generate_docs
