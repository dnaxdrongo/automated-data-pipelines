"""
Load a fact table in Redshift.

- Accepts a transformation SQL (usually a SELECT ... from helpers.SqlQueries)
- If sql does not start with INSERT, wraps it as: INSERT INTO <table> <sql>
- Airflow 2.x compatible
"""

from __future__ import annotations

from typing import Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook



class LoadFactOperator(BaseOperator):
    ui_color = "#F98866"

    def __init__(
        self,
        *,
        table: str,
        sql: str,
        redshift_conn_id: str = "redshift",
        pre_sql: Optional[str] = None,
        post_sql: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        if not table:
            raise AirflowException("LoadFactOperator requires `table`.")
        if not sql or not sql.strip():
            raise AirflowException("LoadFactOperator requires non-empty `sql`.")

        self.table = table
        self.sql = sql
        self.redshift_conn_id = redshift_conn_id
        self.pre_sql = pre_sql
        self.post_sql = post_sql

    def execute(self, context):  # type: ignore[override]
        hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Loading fact table: %s", self.table)

        if self.pre_sql:
            self.log.info("Running pre_sql for fact table: %s", self.table)
            hook.run(self.pre_sql, autocommit=True)

        stmt = self.sql.strip()
        if not stmt.lower().startswith("insert"):
            stmt = f"INSERT INTO {self.table}\n{stmt}"

        self.log.info("Executing fact insert into %s", self.table)
        hook.run(stmt, autocommit=True)

        if self.post_sql:
            self.log.info("Running post_sql for fact table: %s", self.table)
            hook.run(self.post_sql, autocommit=True)

        self.log.info("Fact load complete: %s", self.table)
