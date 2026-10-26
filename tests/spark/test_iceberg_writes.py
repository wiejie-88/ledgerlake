"""Unit tests for the write-efficiency helpers: shuffle partitions and change detection before MERGE."""

from __future__ import annotations

from datetime import datetime

from jobs.common.iceberg import changed_rows
from jobs.common.spark import build_spark_conf

ENV = {
    "LAKE_CATALOG_URI": "jdbc:postgresql://postgres:5432/iceberg_catalog",
    "LAKE_CATALOG_USER": "u",
    "LAKE_CATALOG_PASSWORD": "p",
    "LAKE_WAREHOUSE": "s3://warehouse",
    "S3_ENDPOINT": "http://minio:9000",
    "S3_ACCESS_KEY": "a",
    "S3_SECRET_KEY": "s",
}


def test_shuffle_partitions_default_and_override() -> None:
    assert build_spark_conf(ENV)["spark.sql.shuffle.partitions"] == "8"
    assert build_spark_conf({**ENV, "SPARK_SHUFFLE_PARTITIONS": "4"})["spark.sql.shuffle.partitions"] == "4"


def test_changed_rows_keeps_new_and_newer_only(spark) -> None:
    old, new = datetime(2026, 10, 1), datetime(2026, 10, 2)
    current = spark.createDataFrame([("K1", old), ("K2", old), ("K3", new)], "k STRING, updated_at TIMESTAMP")
    incoming = spark.createDataFrame(
        [("K1", old, "same"), ("K2", new, "updated"), ("K3", old, "stale"), ("K4", new, "new")],
        "k STRING, updated_at TIMESTAMP, note STRING",
    )
    rows = {r["k"]: r for r in changed_rows(incoming, current, "k").collect()}
    assert set(rows) == {"K2", "K4"}
    assert (rows["K2"]["_is_new"], rows["K4"]["_is_new"]) == (False, True)
    assert rows["K2"]["note"] == "updated"


def test_changed_rows_without_a_table_is_everything_new(spark) -> None:
    incoming = spark.createDataFrame([("K1", datetime(2026, 10, 1))], "k STRING, updated_at TIMESTAMP")
    rows = changed_rows(incoming, None, "k").collect()
    assert [(r["k"], r["_is_new"]) for r in rows] == [("K1", True)]
