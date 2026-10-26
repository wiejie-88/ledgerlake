"""Extract sources incrementally and land them in Iceberg bronze for one logical date.

Usage (inside the Airflow container): ``python -m jobs.bronze.run --date 2026-09-29 [--entity transactions]``
"""

from __future__ import annotations

import argparse
import logging
import os
import time
import uuid
from datetime import UTC, date, datetime

from pyspark.sql import SparkSession

from jobs.bronze.load import (
    add_ingestion_metadata,
    events_dataframe,
    partition_counts,
    previous_high_watermark,
    read_postgres,
    update_watermark,
    write_partition,
)
from jobs.common.pipeline_runs import record_run, run_context
from jobs.common.spark import CATALOG, get_spark
from jobs.common.time_windows import local_day_end, utc_day_end
from jobs.extract.sources import ENTITIES, Entity, fetch_events, incremental_sql, jdbc_options

log = logging.getLogger("bronze")


def load_entity(spark: SparkSession, entity: Entity, logical_date: date, batch_id: str) -> dict[str, object]:
    """Extract one entity for ``logical_date`` and replace its bronze partition. Returns run metrics."""
    started = time.monotonic()
    is_events = entity.source == "mongodb"
    high = utc_day_end(logical_date) if is_events else local_day_end(logical_date)
    low = previous_high_watermark(spark, entity.bronze_table, entity.watermark_column, logical_date, utc=is_events)

    if is_events:
        df = events_dataframe(spark, fetch_events(os.environ["MONGO_URI"], low, high))
    else:
        options = jdbc_options(os.environ["SOURCE_PG_DSN"])
        df = read_postgres(spark, options, incremental_sql(entity.name, entity.watermark_column, low, high))

    df = add_ingestion_metadata(
        df,
        source=entity.source_ref,
        batch_id=batch_id,
        logical_date=logical_date,
        ingested_at=datetime.now(UTC),
    ).cache()
    rows_in = df.count()
    write_partition(df, entity.bronze_table, logical_date)
    df.unpersist()
    partition_rows, table_rows = partition_counts(spark, entity.bronze_table, logical_date)
    watermark = update_watermark(spark, entity.source, entity.name, entity.bronze_table, entity.watermark_column)

    metrics = {
        "table": entity.bronze_table,
        "logical_date": logical_date.isoformat(),
        "window_low": low.isoformat() if low else "none",
        "window_high": high.isoformat(),
        "rows_extracted": rows_in,
        "partition_rows": partition_rows,
        "table_rows": table_rows,
        "watermark": watermark,
        "duration_s": round(time.monotonic() - started, 1),
    }
    log.info(" ".join(f"{k}={v}" for k, v in metrics.items()))
    return metrics


def main(argv: list[str] | None = None) -> None:
    """Entry point."""
    parser = argparse.ArgumentParser(prog="jobs.bronze.run", description="Extract sources into Iceberg bronze")
    parser.add_argument("--date", required=True, type=date.fromisoformat, help="logical date, YYYY-MM-DD")
    parser.add_argument("--entity", action="append", choices=sorted(ENTITIES), help="limit to one or more entities")
    parser.add_argument("--batch-id", help="run identifier stored in _batch_id (default: manual__<uuid>)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    ctx = run_context("extract_bronze")
    batch_id = args.batch_id or (ctx.run_id if ctx.run_id != "manual" else f"manual__{uuid.uuid4()}")
    started = time.monotonic()
    spark = get_spark("bronze")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.bronze")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.ops")
    log.info("bronze: logical_date=%s batch_id=%s", args.date, batch_id)
    metrics = [load_entity(spark, ENTITIES[name], args.date, batch_id) for name in args.entity or ENTITIES]
    rows_in = sum(m["rows_extracted"] for m in metrics)
    rows_out = sum(m["partition_rows"] for m in metrics)
    record_run(spark, ctx, args.date, rows_in, rows_out, round(time.monotonic() - started, 1))
    spark.stop()


if __name__ == "__main__":
    main()
