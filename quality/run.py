"""Run a DQ suite, store the results in ops.dq_results, and fail (exit 1) on any error-severity failure.

Usage (inside the Airflow container):
    python -m quality.run --layer silver --date 2026-10-01 [--run-id RUN_ID] [--simulate-defect null_amount]

``--simulate-defect`` adds one bad row to the in-memory copy of silver.transactions that the checks read.
Stored data is never changed. It exists to demonstrate that a failing error check stops downstream tasks.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.common.iceberg import qualified
from jobs.common.spark import CATALOG, get_spark
from quality.checks import Loader, evaluate
from quality.gate import CheckResult, DataQualityError, blocking_failures, enforce, warnings
from quality.suites import SUITES

log = logging.getLogger("quality")

DQ_RESULTS = f"{CATALOG}.ops.dq_results"
SIMULATED_DEFECTS = ("null_amount", "duplicate_key", "orphan_account", "unknown_category")


def simulate_defect(df: DataFrame, defect: str) -> DataFrame:
    """Return ``df`` (silver.transactions) plus one row carrying ``defect``."""
    row = df.limit(1)
    if defect == "null_amount":
        bad = row.withColumn("txn_id", F.lit("SIMULATED-NULL")).withColumn("amount", F.lit(None).cast("decimal(12,2)"))
    elif defect == "duplicate_key":
        bad = row
    elif defect == "orphan_account":
        bad = row.withColumn("txn_id", F.lit("SIMULATED-ORPHAN")).withColumn("account_id", F.lit("A9999999"))
    elif defect == "unknown_category":
        bad = row.withColumn("txn_id", F.lit("SIMULATED-CATEGORY")).withColumn("category", F.lit("crypto_exchange"))
    else:
        raise ValueError(f"unknown simulated defect {defect!r}")
    return df.unionByName(bad)


def make_loader(spark: SparkSession, defect: str | None) -> Loader:
    """Load lake tables (cached per run); optionally inject a simulated defect into silver.transactions."""
    cache: dict[str, DataFrame] = {}

    def load(table: str) -> DataFrame:
        if table not in cache:
            df = spark.table(qualified(table))
            if defect and table == "silver.transactions":
                df = simulate_defect(df, defect)
                log.warning("table=%s SIMULATED DEFECT %s added in memory (stored data unchanged)", table, defect)
            cache[table] = df.cache()
        return cache[table]

    return load


def store_results(spark: SparkSession, run_id: str, logical_date: date, results: list[CheckResult]) -> None:
    """Replace this run_id's rows in ops.dq_results with ``results``."""
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {DQ_RESULTS} (run_id STRING, logical_date DATE, `table` STRING, "
        "check_name STRING, severity STRING, passed BOOLEAN, failed_rows BIGINT, details STRING, "
        "checked_at TIMESTAMP) USING iceberg"
    )
    spark.sql(f"DELETE FROM {DQ_RESULTS} WHERE run_id = '{run_id.replace(chr(39), '')}'")
    rows = [
        (run_id, logical_date, r.table, r.check_name, str(r.severity), r.passed, r.failed_rows, r.details)
        for r in results
    ]
    schema = (
        "run_id STRING, logical_date DATE, `table` STRING, check_name STRING, severity STRING, "
        "passed BOOLEAN, failed_rows BIGINT, details STRING"
    )
    spark.createDataFrame(rows, schema).withColumn("checked_at", F.current_timestamp()).writeTo(DQ_RESULTS).append()


def run_suite(
    spark: SparkSession, layer: str, logical_date: date, run_id: str, defect: str | None
) -> list[CheckResult]:
    """Evaluate every check in the layer's suite, store the results and return them."""
    load = make_loader(spark, defect)
    results = []
    for check in SUITES[layer]:
        result = evaluate(check, load, logical_date)
        level = logging.INFO if result.passed else (logging.ERROR if result.severity == "error" else logging.WARNING)
        log.log(
            level,
            "table=%s check=%s severity=%s passed=%s failed_rows=%d%s",
            result.table,
            result.check_name,
            result.severity,
            result.passed,
            result.failed_rows,
            f" {result.details}" if result.details else "",
        )
        results.append(result)
    store_results(spark, run_id, logical_date, results)
    return results


def main(argv: list[str] | None = None) -> None:
    """Entry point. Exits 1 when an error-severity check fails."""
    parser = argparse.ArgumentParser(prog="quality.run", description="Run a data-quality suite")
    parser.add_argument("--layer", required=True, choices=sorted(SUITES))
    parser.add_argument("--date", required=True, type=date.fromisoformat, help="logical date, YYYY-MM-DD")
    parser.add_argument("--run-id", help="pipeline run id (default: manual__<layer>__<date>)")
    parser.add_argument("--simulate-defect", choices=SIMULATED_DEFECTS, help="demo: inject one bad row in memory")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    run_id = args.run_id or f"manual__{args.layer}__{args.date}"
    started = time.monotonic()
    spark = get_spark(f"dq_{args.layer}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.ops")
    results = run_suite(spark, args.layer, args.date, run_id, args.simulate_defect)
    spark.stop()

    log.info(
        "dq summary: layer=%s logical_date=%s run_id=%s checks=%d passed=%d errors=%d warnings=%d duration_s=%.1f",
        args.layer,
        args.date,
        run_id,
        len(results),
        sum(r.passed for r in results),
        len(blocking_failures(results)),
        len(warnings(results)),
        time.monotonic() - started,
    )
    try:
        enforce(results)
    except DataQualityError as exc:
        log.error("DQ GATE FAILED: %s", exc)
        sys.exit(1)
    log.info("DQ GATE PASSED")


if __name__ == "__main__":
    main()
