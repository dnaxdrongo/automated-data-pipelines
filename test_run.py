
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # modern path

def test_list_s3():
    hook = S3Hook(aws_conn_id="aws_credentials")
    # Replace with a real bucket you have access to:
    keys = hook.list_keys(bucket_name="YOUR_BUCKET_NAME", prefix="")
    print(keys[:20] if keys else "No keys found (or no access).")

with DAG(
    dag_id="test_aws_credentials_conn",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
) as dag:
    PythonOperator(task_id="list_s3_keys", python_callable=test_list_s3)
