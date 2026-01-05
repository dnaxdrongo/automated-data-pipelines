"""
Udacity Data Pipelines with Airflow - Final Project DAG (Airflow 2.6+).
- Loads and transforms data in Redshift with Airflow
- Stages data from S3 to Redshift
- Runs data quality checks
- Airflow 2.x compatible

"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries

default_args = {
    "owner": "udacity",
    "depends_on_past": False,
    "start_date": datetime(2018, 11, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
}

with DAG(
    dag_id="final_project",
    description="Load and transform data in Redshift with Airflow",
    default_args=default_args,
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
) as dag:

    start_operator = EmptyOperator(task_id="Begin_execution")
    end_operator = EmptyOperator(task_id="Stop_execution")

    # Build S3 keys using variables:
    s3_bucket = "{{ var.value.s3_bucket }}"

    events_key = "{{ var.value.log_data_prefix }}"
    songs_key = "{{ var.value.song_data_prefix }}"
    events_jsonpath_key = "{{ var.value.log_json_path_key }}"

    # Stage data from S3 to Redshift


    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        table="staging_events",
        s3_bucket="{{ var.value.s3_bucket }}",
        s3_key=events_key,
        json_path=events_jsonpath_key,
        iam_role="{{ var.value.redshift_iam_role }}",
        region="{{ var.value.s3_region }}",
        redshift_conn_id="redshift",
        aws_conn_id="aws_credentials",
        truncate_before_load=True,
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        table="staging_songs",
        s3_bucket="{{ var.value.s3_bucket }}",
        s3_key=songs_key,
        json_path="auto",
        iam_role="{{ var.value.redshift_iam_role }}",
        region="{{ var.value.s3_region }}",
        redshift_conn_id="redshift",
        aws_conn_id="aws_credentials",
        truncate_before_load=True,
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        table="songplays",
        sql=SqlQueries.songplay_table_insert,
        redshift_conn_id="redshift",
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        table="users",
        sql=SqlQueries.user_table_insert,
        truncate_insert=True,
        redshift_conn_id="redshift",
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        table="songs",
        sql=SqlQueries.song_table_insert,
        truncate_insert=True,
        redshift_conn_id="redshift",
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        table="artists",
        sql=SqlQueries.artist_table_insert,
        truncate_insert=True,
        redshift_conn_id="redshift",
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        table='"time"',
        sql=SqlQueries.time_table_insert,
        truncate_insert=True,
        redshift_conn_id="redshift",
    )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        redshift_conn_id="redshift",
        checks=[
            {"description": "songplays has rows", "check_sql": "SELECT COUNT(*) FROM songplays;", "expected_result": 0, "comparison": ">"},
            {"description": "users has rows", "check_sql": "SELECT COUNT(*) FROM users;", "expected_result": 0, "comparison": ">"},
            {"description": "songs has rows", "check_sql": "SELECT COUNT(*) FROM songs;", "expected_result": 0, "comparison": ">"},
            {"description": "artists has rows", "check_sql": "SELECT COUNT(*) FROM artists;", "expected_result": 0, "comparison": ">"},
            {"description": "time has rows", "check_sql": 'SELECT COUNT(*) FROM "time";', "expected_result": 0, "comparison": ">"},
            {"description": "songplays.songplay_id has no NULLs", "check_sql": "SELECT COUNT(*) FROM songplays WHERE songplay_id IS NULL;", "expected_result": 0, "comparison": "=="},
            {"description": "users.user_id has no NULLs", "check_sql": "SELECT COUNT(*) FROM users WHERE user_id IS NULL;", "expected_result": 0, "comparison": "=="},
        ],
    )

    # Dependencies (target graph)
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
    ]
    [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
    ] >> run_quality_checks
    run_quality_checks >> end_operator
