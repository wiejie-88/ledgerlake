"""Schema-evolution demo on silver.app_events.

Usage (inside the Airflow container): ``python -m jobs.demo.schema_evolution``

Shows the table's schema id changing in Iceberg's metadata log when ``referral_source`` was added
(ALTER TABLE ADD COLUMN, no data rewrite), the current columns, and how old and new rows read: events before
the drift date return NULL for the new column without having been rewritten.
"""

from __future__ import annotations

import logging

from pyspark.sql import functions as F

from jobs.common.spark import get_spark

TABLE = "lake.silver.app_events"

log = logging.getLogger("schema_evolution")


def main() -> None:
    """Entry point."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    spark = get_spark("schema_evolution_demo")

    log.info("== Schema evolution on %s", TABLE)
    entries = spark.sql(
        f"SELECT timestamp, latest_schema_id, latest_snapshot_id FROM {TABLE}.metadata_log_entries ORDER BY timestamp"
    ).collect()
    previous = None
    for e in entries:
        if e["latest_schema_id"] != previous:
            log.info("metadata at %s: schema_id=%s (snapshot %s)", e["timestamp"], e["latest_schema_id"],
                     e["latest_snapshot_id"])  # fmt: skip
            previous = e["latest_schema_id"]
    log.info("current columns: %s", ", ".join(spark.table(TABLE).columns))

    rows = (
        spark.table(TABLE)
        .groupBy(F.col("referral_source").isNotNull().alias("has_referral_source"))
        .agg(F.count("*").alias("rows"), F.min("event_date").alias("first"), F.max("event_date").alias("last"))
        .orderBy("has_referral_source")
        .collect()
    )
    for r in rows:
        log.info("has_referral_source=%s rows=%d event_dates=%s..%s", r["has_referral_source"], r["rows"], r["first"],
                 r["last"])  # fmt: skip
    for r in (
        spark.table(TABLE)
        .where("referral_source IS NOT NULL")
        .groupBy("referral_source")
        .count()
        .orderBy("referral_source")
        .collect()
    ):
        log.info("  referral_source=%s: %d", r["referral_source"], r["count"])
    spark.stop()


if __name__ == "__main__":
    main()
