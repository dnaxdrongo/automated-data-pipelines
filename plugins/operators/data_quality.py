"""
Run data quality checks in Redshift.

Checks are parameterized (passed from DAG) and each check must return a single scalar
(first row, first column).

Check dict format:
{
  "description": "users has rows",
  "check_sql": "SELECT COUNT(*) FROM users;",
  "expected_result": 0,
  "comparison": ">"   # one of: ==, !=, >, >=, <, <=   (default ==)
}
"""

from __future__ import annotations

from typing import Any, Dict, List, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook



class DataQualityOperator(BaseOperator):
    ui_color = "#89DA59"

    def __init__(
        self,
        *,
        checks: Sequence[Dict[str, Any]],
        redshift_conn_id: str = "redshift",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.checks = list(checks)
        self.redshift_conn_id = redshift_conn_id

        if not self.checks:
            raise AirflowException("DataQualityOperator requires at least one check.")

    def execute(self, context):  # type: ignore[override]
        hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Running %d data quality checks...", len(self.checks))

        failures: List[str] = []

        for i, check in enumerate(self.checks, start=1):
            sql = check.get("check_sql") or check.get("sql")
            expected = check.get("expected_result")
            op = (check.get("comparison") or "==").strip()
            desc = check.get("description") or f"check_{i}"

            if not sql:
                raise AirflowException(f"Check #{i} missing check_sql/sql.")
            if expected is None:
                raise AirflowException(f"Check '{desc}' missing expected_result.")

            self.log.info("[%d/%d] %s", i, len(self.checks), desc)
            records = hook.get_records(sql)

            if not records or not records[0] or len(records[0]) < 1:
                raise AirflowException(f"Check '{desc}' returned no results. SQL: {sql}")

            actual = records[0][0]
            passed = self._compare(actual, expected, op)

            if passed:
                self.log.info("PASS: actual=%r %s expected=%r", actual, op, expected)
            else:
                msg = f"FAIL: {desc}: actual={actual!r} {op} expected={expected!r} | SQL: {sql}"
                self.log.error(msg)
                failures.append(msg)

        if failures:
            raise AirflowException("Data quality checks failed:\n" + "\n".join(failures))

        self.log.info("All data quality checks passed.")

    @staticmethod
    def _compare(actual: Any, expected: Any, op: str) -> bool:
        if op == "==":
            return actual == expected
        if op == "!=":
            return actual != expected
        if op == ">":
            return actual > expected
        if op == ">=":
            return actual >= expected
        if op == "<":
            return actual < expected
        if op == "<=":
            return actual <= expected

        raise AirflowException(f"Unsupported comparison operator '{op}'. Use one of ==, !=, >, >=, <, <=")
