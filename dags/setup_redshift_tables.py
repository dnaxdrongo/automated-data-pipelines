from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# The above operator is used to execute SQL commands into the database.
with DAG(
    dag_id="setup_redshift_tables",
    description="One-time setup: create all final-project tables in Redshift",
    start_date=datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
    default_args={
        "owner": "udacity",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["udacity", "setup"],
) as dag:

    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id="redshift",
        sql="sql/create_tables.sql",
        split_statements=True,
        autocommit=True,
)

