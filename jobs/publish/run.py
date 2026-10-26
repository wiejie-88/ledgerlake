"""Publish gold marts and the customer profile to the Postgres serving database, with roles and grants.

Usage (inside the Airflow container): ``python -m jobs.publish.run --date 2026-10-04``

1. Apply serving/*.sql (tables, masked view, roles, grants) and set role passwords from .env.
2. Read the marts and build customer_profile with Spark (all small).
3. In ONE transaction: TRUNCATE and re-insert every serving table. Readers never see a partial publish,
   grants stay in place, and re-running a date gives identical tables.
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import date
from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from jobs.common.iceberg import qualified
from jobs.common.pipeline_runs import record_run, run_context
from jobs.common.spark import get_spark
from jobs.publish.contract import MART_COLUMNS, PROFILE_COLUMNS, SERVING_SCHEMA, serving_columns
from jobs.publish.transforms import customer_profile, with_as_of

SQL_DIR = Path(__file__).resolve().parents[2] / "serving"
ROLE_PASSWORD_ENV = {"analyst": "ANALYST_PASSWORD", "engineer": "ENGINEER_PASSWORD"}

log = logging.getLogger("publish")


def apply_serving_sql(conn: psycopg2.extensions.connection) -> None:
    """Run serving/*.sql in name order, then set the role passwords from the environment."""
    passwords = {role: os.environ.get(var, "") for role, var in ROLE_PASSWORD_ENV.items()}
    missing = [ROLE_PASSWORD_ENV[r] for r, p in passwords.items() if not p]
    if missing:
        raise SystemExit(f"Missing {', '.join(missing)}; add them to .env (see .env.example)")
    with conn, conn.cursor() as cur:
        for path in sorted(SQL_DIR.glob("*.sql")):
            cur.execute(path.read_text())
            log.info("serving: applied %s", path.name)
        for role, password in passwords.items():
            cur.execute(sql.SQL("ALTER ROLE {} PASSWORD {}").format(sql.Identifier(role), sql.Literal(password)))


def publish(conn: psycopg2.extensions.connection, tables: dict[str, list[tuple]]) -> None:
    """Replace the contents of every serving table in a single transaction."""
    columns = serving_columns()
    with conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("TRUNCATE {}").format(sql.SQL(", ").join(sql.Identifier(SERVING_SCHEMA, name) for name in tables))
        )
        for name, rows in tables.items():
            query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                sql.Identifier(SERVING_SCHEMA, name), sql.SQL(", ").join(map(sql.Identifier, columns[name]))
            )
            execute_values(cur, query, rows, page_size=5_000)


def main(argv: list[str] | None = None) -> None:
    """Entry point."""
    parser = argparse.ArgumentParser(prog="jobs.publish.run", description="Publish gold to the serving database")
    parser.add_argument("--date", required=True, type=date.fromisoformat, help="logical date, YYYY-MM-DD")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    started = time.monotonic()
    conn = psycopg2.connect(os.environ["SERVING_PG_DSN"])
    try:
        apply_serving_sql(conn)
        spark = get_spark("publish")
        frames = {name: with_as_of(spark.table(qualified(f"gold.{name}")), cols, args.date)
                  for name, cols in MART_COLUMNS.items()}  # fmt: skip
        profile = customer_profile(
            spark.table(qualified("silver.customers_pii")), spark.table(qualified("gold.dim_customer"))
        )
        frames["customer_profile"] = with_as_of(profile, PROFILE_COLUMNS, args.date)
        # Marts and profiles are small (thousands of rows), so collecting to the driver is fine.
        tables = {name: [tuple(row) for row in df.collect()] for name, df in frames.items()}
        publish(conn, tables)
        for name, rows in tables.items():
            log.info("table=%s.%s logical_date=%s rows_published=%d", SERVING_SCHEMA, name, args.date, len(rows))
        rows_out = sum(len(rows) for rows in tables.values())
        record_run(spark, run_context("publish"), args.date, rows_out, rows_out, round(time.monotonic() - started, 1))
        spark.stop()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
