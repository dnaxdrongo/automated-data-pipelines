"""
Lesson 3 helper DAG: lists keys in s3://{{ var.value.s3_bucket }}/{{ var.value.s3_prefix }}
using the Airflow AWS connection 'aws_credentials'.

If you haven't set the Variables yet, run:
  - Admin -> Variables (UI), or
  - ./set_connections.sh  (bash) or .\set_connections.ps1 (PowerShell)
"""

from __future__ import annotations

from datetime import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Airflow 2.x+ S3 hook lives in the amazon provider, but keep a fallback for older paths.
try:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
except Exception:  # pragma: no cover
    from airflow.hooks.S3_hook import S3Hook  # type: ignore


def _list_keys(**context):
    bucket = Variable.get("s3_bucket")
    prefix = Variable.get("s3_prefix", default_var="")
    hook = S3Hook(aws_conn_id="aws_credentials")

    logging.info("Listing keys from s3://%s/%s", bucket, prefix)
    keys = hook.list_keys(bucket_name=bucket, prefix=prefix) or []
    for k in keys[:50]:
        logging.info("- s3://%s/%s", bucket, k)
    logging.info("Total keys found (may be truncated in logs): %d", len(keys))


with DAG(
    dag_id="l3_list_s3_keys",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["udacity", "lesson-3", "aws", "s3"],
) as dag:
    list_keys = PythonOperator(
        task_id="list_s3_keys",
        python_callable=_list_keys,
    )
