"""Run log in lake.ops.pipeline_runs: one row per task run (success rows from jobs, failure rows from callbacks).

Spark jobs call :func:`record_run` with their own session at the end of a successful run. Tasks without Spark,
and Airflow failure/retry callbacks, use the CLI:

    python -m jobs.common.pipeline_runs --status failed --date 2026-10-03 --task-id silver

Airflow exports AIRFLOW_CTX_DAG_ID / AIRFLOW_CTX_TASK_ID / AIRFLOW_CTX_DAG_RUN_ID to BashOperator commands;
outside Airflow the ids default to ``manual``.
"""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from jobs.common.spark import CATALOG, get_spark

PIPELINE_RUNS = f"{CATALOG}.ops.pipeline_runs"
SCHEMA = (
    "run_id STRING, dag_id STRING, task_id STRING, logical_date DATE, rows_in BIGINT, rows_out BIGINT, "
    "duration_s DOUBLE, status STRING"
)

log = logging.getLogger("pipeline_runs")


@dataclass(frozen=True)
class RunContext:
    run_id: str
    dag_id: str
    task_id: str


def run_context(default_task_id: str) -> RunContext:
    """Airflow ids from the environment, or ``manual`` ids when run by hand."""
    return RunContext(
        run_id=os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "manual"),
        dag_id=os.environ.get("AIRFLOW_CTX_DAG_ID", "manual"),
        task_id=os.environ.get("AIRFLOW_CTX_TASK_ID", default_task_id),
    )


def record_run(
    spark: SparkSession,
    ctx: RunContext,
    logical_date: date,
    rows_in: int | None,
    rows_out: int | None,
    duration_s: float | None,
    status: str = "success",
) -> None:
    """Append one row to ops.pipeline_runs (creating the table on first use)."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.ops")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {PIPELINE_RUNS} ({SCHEMA}, recorded_at TIMESTAMP) USING iceberg")
    row = (ctx.run_id, ctx.dag_id, ctx.task_id, logical_date, rows_in, rows_out, duration_s, status)
    spark.createDataFrame([row], SCHEMA).withColumn("recorded_at", F.current_timestamp()).writeTo(
        PIPELINE_RUNS
    ).append()
    log.info(
        "pipeline_runs: run_id=%s task_id=%s logical_date=%s rows_in=%s rows_out=%s duration_s=%s status=%s",
        ctx.run_id, ctx.task_id, logical_date, rows_in, rows_out, duration_s, status,
    )  # fmt: skip


def main(argv: list[str] | None = None) -> None:
    """CLI for tasks without a Spark session and for Airflow callbacks."""
    parser = argparse.ArgumentParser(prog="jobs.common.pipeline_runs", description="Record a task run")
    parser.add_argument("--status", required=True, choices=["success", "failed", "up_for_retry"])
    parser.add_argument("--date", required=True, type=date.fromisoformat, help="logical (business) date")
    parser.add_argument("--task-id", default="manual")
    parser.add_argument("--dag-id")
    parser.add_argument("--run-id")
    parser.add_argument("--rows-in", type=int)
    parser.add_argument("--rows-out", type=int)
    parser.add_argument("--duration-s", type=float)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    env = run_context(args.task_id)
    ctx = RunContext(args.run_id or env.run_id, args.dag_id or env.dag_id, args.task_id or env.task_id)
    spark = get_spark("pipeline_runs")
    record_run(spark, ctx, args.date, args.rows_in, args.rows_out, args.duration_s, args.status)
    spark.stop()


if __name__ == "__main__":
    main()
