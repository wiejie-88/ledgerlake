"""Small Iceberg write helpers shared by the bronze, silver and quarantine jobs."""

from __future__ import annotations

from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.common.spark import CATALOG


def qualified(table: str) -> str:
    """``bronze.x`` -> ``lake.bronze.x``."""
    return table if table.startswith(f"{CATALOG}.") else f"{CATALOG}.{table}"


def table_exists(spark: SparkSession, table: str) -> bool:
    """True if the Iceberg table exists in the lake catalog."""
    return spark.catalog.tableExists(qualified(table))


def create_if_missing(df: DataFrame, table: str, partition_by: list[Column] | None = None) -> bool:
    """Create ``table`` with ``df``'s schema (no rows) if it doesn't exist. Returns True if created."""
    if table_exists(df.sparkSession, table):
        return False
    writer = df.limit(0).writeTo(qualified(table)).tableProperty("format-version", "2")
    if partition_by:
        writer = writer.partitionedBy(*partition_by)
    writer.create()
    return True


def overwrite_partition(df: DataFrame, table: str, partition_col: str, value: Any) -> None:
    """Replace the rows of ``table`` where ``partition_col == value`` with ``df`` (identity partitioning).

    Unlike overwritePartitions(), this also clears the partition when ``df`` is empty.
    """
    create_if_missing(df, table, [F.col(partition_col)])
    df.writeTo(qualified(table)).overwrite(F.col(partition_col) == F.lit(value))


def changed_rows(df: DataFrame, current: DataFrame | None, key: str) -> DataFrame:
    """Rows of ``df`` that are new or newer than the current version; unchanged and stale rows are dropped.

    ``current`` holds ``key`` and ``updated_at`` of the target table (None if it doesn't exist yet). Adds
    ``_is_new``: True for keys not in the target, False for updates.
    """
    if current is None:
        return df.withColumn("_is_new", F.lit(True))
    cur = current.select(F.col(key).alias("_cur_key"), F.col("updated_at").alias("_cur_updated_at"))
    return (
        df.join(cur, df[key] == cur["_cur_key"], "left")
        .where(F.col("_cur_key").isNull() | (F.col("updated_at") > F.col("_cur_updated_at")))
        .withColumn("_is_new", F.col("_cur_key").isNull())
        .drop("_cur_key", "_cur_updated_at")
    )


def merge_latest(df: DataFrame, table: str, key: str, partition_by: list[Column] | None = None) -> tuple[int, int]:
    """Upsert ``df`` into ``table`` on ``key``; an existing row is replaced only by a newer ``updated_at``.

    Only new and changed rows reach the MERGE, so copy-on-write rewrites just the files holding updated keys,
    and nothing at all (no snapshot) when nothing changed. ``df`` must hold at most one row per key.
    Returns (inserted, updated).
    """
    spark = df.sparkSession
    current = spark.table(qualified(table)).select(key, "updated_at") if table_exists(spark, table) else None
    changes = changed_rows(df, current, key).cache()
    inserted = changes.where(F.col("_is_new")).count()
    updated = changes.where(~F.col("_is_new")).count()
    create_if_missing(df, table, partition_by)
    if inserted or updated:
        changes.drop("_is_new").createOrReplaceTempView("_merge_source")
        spark.sql(
            f"""
            MERGE INTO {qualified(table)} t
            USING _merge_source s
            ON t.{key} = s.{key}
            WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
    changes.unpersist()
    return inserted, updated


def insert_new(df: DataFrame, table: str, key: str) -> int:
    """Append rows of ``df`` whose ``key`` isn't in ``table`` yet (immutable records such as events).

    A plain append after an anti-join: no existing file is rewritten. Returns the number of rows appended.
    """
    existing = df.sparkSession.table(qualified(table)).select(F.col(key).alias("_existing_key"))
    new = df.join(existing, df[key] == existing["_existing_key"], "left_anti").cache()
    appended = new.count()
    if appended:
        new.writeTo(qualified(table)).append()
    new.unpersist()
    return appended
