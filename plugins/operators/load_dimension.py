"""
Load a dimension table in Redshift.

- Supports truncate-insert (idempotent) or append mode
- Accepts a transformation SQL (usually a SELECT ... from helpers.SqlQueries)
- If sql does not start with INSERT, wraps it as: INSERT INTO <table> <sql>
- Airflow 2.x compatible
"""

from __future__ import annotations

from typing import Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook



class LoadDimensionOperator(BaseOperator):
    ui_color = "#80BD9E"

    def __init__(
        self,
        *,
        table: str,
        sql: str,
        truncate_insert: bool = True,
        redshift_conn_id: str = "redshift",
        pre_sql: Optional[str] = None,
        post_sql: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        if not table:
            raise AirflowException("LoadDimensionOperator requires `table`.")
        if not sql or not sql.strip():
            raise AirflowException("LoadDimensionOperator requires non-empty `sql`.")

        self.table = table
        self.sql = sql
        self.truncate_insert = truncate_insert
        self.redshift_conn_id = redshift_conn_id
        self.pre_sql = pre_sql
        self.post_sql = post_sql

    def execute(self, context):  # type: ignore[override]
        hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Loading dimension table: %s", self.table)
        self.log.info("Mode: %s", "TRUNCATE+INSERT" if self.truncate_insert else "APPEND")

        if self.pre_sql:
            self.log.info("Running pre_sql for dimension table: %s", self.table)
            hook.run(self.pre_sql, autocommit=True)

        if self.truncate_insert:
            self.log.info("Truncating dimension table: %s", self.table)
            hook.run(f"TRUNCATE TABLE {self.table};", autocommit=True)

        stmt = self.sql.strip()
        if not stmt.lower().startswith("insert"):
            stmt = f"INSERT INTO {self.table}\n{stmt}"

        self.log.info("Executing dimension insert into %s", self.table)
        hook.run(stmt, autocommit=True)

        if self.post_sql:
            self.log.info("Running post_sql for dimension table: %s", self.table)
            hook.run(self.post_sql, autocommit=True)

        self.log.info("Dimension load complete: %s", self.table)
