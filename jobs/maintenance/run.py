"""Iceberg table maintenance: compact data files, rewrite manifests, expire old snapshots.

Usage (inside the Airflow container):
    python -m jobs.maintenance.run [--table silver.transactions] [--retain-last 5] [--older-than-hours 0]

For each table, file counts are measured before and after and stored in lake.ops.maintenance_log:
snapshots, data files in the current snapshot, data files referenced by any snapshot, and objects physically
present in MinIO under the table's data/ and metadata/ prefixes.
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from jobs.common.pipeline_runs import record_run, run_context
from jobs.common.spark import CATALOG, get_spark

NAMESPACES = ("bronze", "silver", "quarantine", "gold", "ops")
MAINTENANCE_LOG = f"{CATALOG}.ops.maintenance_log"
SKIP = {"ops.maintenance_log"}  # written by this job after the other tables are done

log = logging.getLogger("maintenance")


@dataclass(frozen=True)
class TableStats:
    snapshots: int
    current_data_files: int
    current_bytes: int
    referenced_data_files: int
    physical_data_files: int
    physical_metadata_files: int


def list_tables(spark: SparkSession) -> list[str]:
    """Every table in the lake's namespaces, as ``namespace.table``."""
    existing = {r[0] for r in spark.sql(f"SHOW NAMESPACES IN {CATALOG}").collect()}
    tables = []
    for namespace in NAMESPACES:
        if namespace in existing:
            tables += [
                f"{namespace}.{r['tableName']}" for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{namespace}").collect()
            ]
    return sorted(t for t in tables if t not in SKIP)


def _s3() -> Any:
    """S3 client for MinIO (boto3 client classes are generated at runtime, hence Any)."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )


def count_objects(s3: Any, location: str, folder: str) -> int:
    """Objects under ``<location>/<folder>/`` in object storage."""
    parts = urlsplit(location)
    prefix = f"{parts.path.strip('/')}/{folder}/"
    paginator = s3.get_paginator("list_objects_v2")
    return sum(page.get("KeyCount", 0) for page in paginator.paginate(Bucket=parts.netloc, Prefix=prefix))


def table_stats(spark: SparkSession, s3: Any, table: str) -> TableStats:
    """Snapshot and file counts for one table (metadata tables plus a physical listing)."""
    name = f"{CATALOG}.{table}"
    location = spark.sql(f"DESCRIBE TABLE EXTENDED {name}").where("col_name = 'Location'").first()["data_type"]
    current = spark.table(f"{name}.files").agg(F.count("*").alias("n"), F.sum("file_size_in_bytes").alias("b")).first()
    return TableStats(
        snapshots=spark.table(f"{name}.snapshots").count(),
        current_data_files=current["n"],
        current_bytes=current["b"] or 0,
        referenced_data_files=spark.table(f"{name}.all_data_files").select("file_path").distinct().count(),
        physical_data_files=count_objects(s3, location, "data"),
        physical_metadata_files=count_objects(s3, location, "metadata"),
    )


def maintain(spark: SparkSession, table: str, retain_last: int, older_than: datetime) -> dict[str, int]:
    """Compact, rewrite manifests and expire snapshots for one table; return the procedures' counters."""
    compact = spark.sql(
        f"CALL {CATALOG}.system.rewrite_data_files(table => '{table}', options => map('min-input-files', '2'))"
    ).first()
    manifests = spark.sql(f"CALL {CATALOG}.system.rewrite_manifests(table => '{table}')").first()
    expire = spark.sql(
        f"CALL {CATALOG}.system.expire_snapshots(table => '{table}', "
        f"older_than => TIMESTAMP '{older_than:%Y-%m-%d %H:%M:%S}', retain_last => {retain_last})"
    ).first()
    return {
        "rewritten_data_files": compact["rewritten_data_files_count"],
        "added_data_files": compact["added_data_files_count"],
        "rewritten_manifests": manifests["rewritten_manifests_count"],
        "expired_data_files_deleted": expire["deleted_data_files_count"],
        "expired_manifest_lists_deleted": expire["deleted_manifest_lists_count"],
    }


def main(argv: list[str] | None = None) -> None:
    """Entry point."""
    parser = argparse.ArgumentParser(prog="jobs.maintenance.run", description="Compact files and expire snapshots")
    parser.add_argument("--table", action="append", help="namespace.table (default: every lake table)")
    parser.add_argument("--retain-last", type=int, default=5, help="snapshots to keep per table (default 5)")
    parser.add_argument("--older-than-hours", type=float, default=0, help="only expire snapshots older than this")
    parser.add_argument("--date", type=date.fromisoformat, default=date.today(), help="logical date for the run log")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    started = time.monotonic()
    spark = get_spark("maintenance")
    s3 = _s3()
    older_than = datetime.now(UTC) - timedelta(hours=args.older_than_hours)
    run_id = run_context("maintenance").run_id
    log_rows = []
    totals = {"before": 0, "after": 0}
    for table in args.table or list_tables(spark):
        t0 = time.monotonic()
        before = table_stats(spark, s3, table)
        counters = maintain(spark, table, args.retain_last, older_than)
        after = table_stats(spark, s3, table)
        totals["before"] += before.physical_data_files
        totals["after"] += after.physical_data_files
        log.info(
            "table=%s snapshots %d->%d | current data files %d->%d | referenced data files %d->%d | "
            "physical data files %d->%d | physical metadata files %d->%d | compaction rewrote %d into %d | "
            "duration_s=%.1f",
            table, before.snapshots, after.snapshots, before.current_data_files, after.current_data_files,
            before.referenced_data_files, after.referenced_data_files, before.physical_data_files,
            after.physical_data_files, before.physical_metadata_files, after.physical_metadata_files,
            counters["rewritten_data_files"], counters["added_data_files"], time.monotonic() - t0,
        )  # fmt: skip
        row = {"run_id": run_id, "table": table, "retain_last": args.retain_last}
        row.update({f"{k}_before": v for k, v in asdict(before).items()})
        row.update({f"{k}_after": v for k, v in asdict(after).items()})
        row.update(counters)
        row["duration_s"] = round(time.monotonic() - t0, 1)
        log_rows.append(row)

    log_df = spark.createDataFrame(log_rows).withColumn("maintained_at", F.current_timestamp())
    if spark.catalog.tableExists(MAINTENANCE_LOG):
        log_df.writeTo(MAINTENANCE_LOG).append()
    else:
        log_df.writeTo(MAINTENANCE_LOG).tableProperty("format-version", "2").create()
    log.info(
        "maintenance: tables=%d physical data files %d->%d (retain_last=%d)",
        len(log_rows), totals["before"], totals["after"], args.retain_last,
    )  # fmt: skip
    record_run(spark, run_context("maintenance"), args.date, totals["before"], totals["after"],
               round(time.monotonic() - started, 1))  # fmt: skip
    spark.stop()


if __name__ == "__main__":
    main()
