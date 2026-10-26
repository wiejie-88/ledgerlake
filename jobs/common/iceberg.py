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


def merge_latest(df: DataFrame, table: str, key: str, partition_by: list[Column] | None = None) -> None:
    """Upsert ``df`` into ``table`` on ``key``; an existing row is replaced only by a newer ``updated_at``.

    Re-running with the same input changes nothing. ``df`` must hold at most one row per key.
    """
    create_if_missing(df, table, partition_by)
    df.createOrReplaceTempView("_merge_source")
    df.sparkSession.sql(
        f"""
        MERGE INTO {qualified(table)} t
        USING _merge_source s
        ON t.{key} = s.{key}
        WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def merge_insert_only(df: DataFrame, table: str, key: str) -> None:
    """Insert rows of ``df`` whose ``key`` is not yet in ``table`` (for immutable records such as events)."""
    df.createOrReplaceTempView("_merge_source")
    df.sparkSession.sql(
        f"""
        MERGE INTO {qualified(table)} t
        USING _merge_source s
        ON t.{key} = s.{key}
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def merge_preview(df: DataFrame, table: str, key: str) -> tuple[int, int]:
    """Return (rows merge_latest would insert, rows it would update) without writing anything."""
    spark = df.sparkSession
    if not table_exists(spark, table):
        return df.count(), 0
    current = spark.table(qualified(table)).select(key, F.col("updated_at").alias("_current_updated_at"))
    joined = df.select(key, "updated_at").join(current, key, "left")
    inserts = joined.where(F.col("_current_updated_at").isNull()).count()
    updates = joined.where(F.col("updated_at") > F.col("_current_updated_at")).count()
    return inserts, updates
