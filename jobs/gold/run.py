"""Build gold from current silver: dim_date, dim_customer (MERGE), fact_transactions (MERGE) and three marts.

Usage (inside the Airflow container): ``python -m jobs.gold.run --date 2026-10-01``

Dimensions and the fact are upserted with Iceberg MERGE INTO; marts are rebuilt in full. Re-running a date
changes nothing (the dimension MERGE only updates rows whose attribute hash changed).
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.common.iceberg import create_if_missing, merge_latest, merge_preview, qualified, table_exists
from jobs.common.spark import CATALOG, get_spark
from jobs.gold.transforms import (
    build_dim_customer,
    build_dim_date,
    build_fact_transactions,
    mart_advance_repayment,
    mart_customer_features,
    mart_daily_spend,
)

log = logging.getLogger("gold")


def _meta(df: DataFrame, logical_date: date) -> DataFrame:
    return df.withColumn("_logical_date", F.lit(logical_date).cast("date")).withColumn(
        "_built_at", F.current_timestamp()
    )


def merge_changed(df: DataFrame, table: str, key: str) -> tuple[int, int]:
    """SCD type 1 upsert: insert new keys, update rows whose ``_row_hash`` changed. Returns (inserted, updated)."""
    spark = df.sparkSession
    if table_exists(spark, table):
        current = spark.table(qualified(table)).select(key, F.col("_row_hash").alias("_current_hash"))
        joined = df.select(key, "_row_hash").join(current, key, "left")
        inserted = joined.where(F.col("_current_hash").isNull()).count()
        updated = joined.where(F.col("_current_hash") != F.col("_row_hash")).count()
    else:
        inserted, updated = df.count(), 0
    create_if_missing(df, table)
    df.createOrReplaceTempView("_dim_source")
    spark.sql(
        f"""
        MERGE INTO {qualified(table)} t
        USING _dim_source s
        ON t.{key} = s.{key}
        WHEN MATCHED AND t._row_hash <> s._row_hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    return inserted, updated


def _log(table: str, logical_date: date, started: float, **metrics: object) -> None:
    rows = metrics.pop("rows")
    details = " ".join(f"{k}={v}" for k, v in metrics.items())
    log.info(
        "table=%s logical_date=%s rows=%s %s duration_s=%.1f", table, logical_date, rows, details,
        time.monotonic() - started,
    )  # fmt: skip


def build_gold(spark: SparkSession, logical_date: date) -> None:
    """Build every gold table for ``logical_date``."""
    silver = {
        t: spark.table(qualified(f"silver.{t}")) for t in ("customers", "accounts", "transactions", "cash_advances")
    }

    started = time.monotonic()
    created = create_if_missing(build_dim_date(spark), "gold.dim_date")
    if created:
        build_dim_date(spark).writeTo(qualified("gold.dim_date")).append()
    _log("gold.dim_date", logical_date, started, rows=spark.table(qualified("gold.dim_date")).count(), created=created)

    started = time.monotonic()
    dim_customer = _meta(build_dim_customer(silver["customers"], silver["accounts"]), logical_date)
    inserted, updated = merge_changed(dim_customer, "gold.dim_customer", "customer_id")
    rows = spark.table(qualified("gold.dim_customer")).count()
    _log("gold.dim_customer", logical_date, started, rows=rows, inserted=inserted, updated=updated)

    started = time.monotonic()
    fact = _meta(build_fact_transactions(silver["transactions"], silver["accounts"]), logical_date).cache()
    inserted, updated = merge_preview(fact, "gold.fact_transactions", "txn_id")
    merge_latest(fact, "gold.fact_transactions", "txn_id", partition_by=[F.days("txn_ts")])
    fact.unpersist()
    rows = spark.table(qualified("gold.fact_transactions")).count()
    _log("gold.fact_transactions", logical_date, started, rows=rows, inserted=inserted, updated=updated)

    fact_table = spark.table(qualified("gold.fact_transactions"))
    marts = {
        "gold.mart_daily_spend": mart_daily_spend(fact_table),
        "gold.mart_advance_repayment": mart_advance_repayment(silver["cash_advances"], logical_date),
        "gold.mart_customer_features": mart_customer_features(fact_table, silver["cash_advances"]),
    }
    for table, mart in marts.items():
        started = time.monotonic()
        _meta(mart, logical_date).writeTo(qualified(table)).tableProperty("format-version", "2").createOrReplace()
        _log(table, logical_date, started, rows=spark.table(qualified(table)).count(), mode="rebuilt")


def main(argv: list[str] | None = None) -> None:
    """Entry point."""
    parser = argparse.ArgumentParser(prog="jobs.gold.run", description="Build gold tables")
    parser.add_argument("--date", required=True, type=date.fromisoformat, help="logical date, YYYY-MM-DD")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = get_spark("gold")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.gold")
    log.info("gold: logical_date=%s", args.date)
    build_gold(spark, args.date)
    spark.stop()


if __name__ == "__main__":
    main()
