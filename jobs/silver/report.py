"""Silver verification report: quarantine reasons, silver row counts, PII layout and schema evolution evidence.

Usage: ``python -m jobs.silver.report`` (or ``make silver-report``). Exits non-zero if an injected defect type
is missing from quarantine.
"""

from __future__ import annotations

import logging
import sys

from pyspark.sql import functions as F

from jobs.common.iceberg import qualified, table_exists
from jobs.common.spark import get_spark

log = logging.getLogger("silver.report")

# Defect types the generator injects, and where they must show up in quarantine.
EXPECTED_REASONS = {
    "transactions": (
        "duplicate_row",
        "null_amount",
        "negative_amount",
        "unknown_category",
        "orphan_account",
        "superseded_by_late_update",
    ),
    "app_events": ("duplicate_row",),
}
TABLES = ("customers", "accounts", "transactions", "cash_advances", "app_events")
KEYS = {
    "customers": "customer_id",
    "accounts": "account_id",
    "transactions": "txn_id",
    "cash_advances": "advance_id",
    "app_events": "event_id",
}


def main() -> None:
    """Print the report and exit 1 if any expected quarantine reason is missing."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = get_spark("silver_report")
    missing: list[str] = []

    for table in TABLES:
        found: set[str] = set()
        if table_exists(spark, f"quarantine.{table}"):
            # A row may fail several rules ("a; b"), so split before counting.
            rows = (
                spark.table(qualified(f"quarantine.{table}"))
                .withColumn("reason", F.explode(F.split("failure_reason", "; ")))
                .groupBy("reason")
                .agg(F.count("*").alias("rows"), F.countDistinct(KEYS[table]).alias("keys"))
                .orderBy("reason")
                .collect()
            )
            for r in rows:
                found.add(r["reason"])
                log.info(
                    "table=quarantine.%s reason=%s rows=%d distinct_keys=%d", table, r["reason"], r["rows"], r["keys"]
                )
        for reason in EXPECTED_REASONS.get(table, ()):
            if reason not in found:
                missing.append(f"quarantine.{table}:{reason}")
        if table_exists(spark, f"silver.{table}"):
            df = spark.table(qualified(f"silver.{table}"))
            log.info(
                "table=silver.%s rows=%d distinct_keys=%d", table, df.count(), df.select(KEYS[table]).distinct().count()
            )

    customer_columns = spark.table(qualified("silver.customers")).columns
    pii_columns = spark.table(qualified("silver.customers_pii")).columns
    log.info("table=silver.customers columns=%s", ",".join(customer_columns))
    log.info("table=silver.customers_pii columns=%s", ",".join(pii_columns))
    leaked = {"full_name", "email", "phone", "date_of_birth"} & set(customer_columns)
    if leaked:
        missing.append(f"silver.customers exposes PII columns {sorted(leaked)}")

    events = qualified("silver.app_events")
    log.info("table=silver.app_events columns=%s", ",".join(spark.table(events).columns))
    for row in spark.sql(
        f"SELECT snapshot_id, operation, summary['added-data-files'] AS added_files, "
        f"summary['added-records'] AS added_records FROM {events}.snapshots ORDER BY committed_at"
    ).collect():
        log.info(
            "table=silver.app_events snapshot=%s operation=%s added_files=%s added_records=%s",
            row["snapshot_id"],
            row["operation"],
            row["added_files"],
            row["added_records"],
        )
    if "referral_source" in spark.table(events).columns:
        split = (
            spark.table(events)
            .groupBy(F.col("referral_source").isNotNull().alias("has_referral_source"))
            .agg(F.count("*").alias("rows"), F.min("event_date").alias("first"), F.max("event_date").alias("last"))
            .orderBy("has_referral_source")
            .collect()
        )
        for r in split:
            log.info(
                "table=silver.app_events has_referral_source=%s rows=%d event_dates=%s..%s",
                r["has_referral_source"],
                r["rows"],
                r["first"],
                r["last"],
            )

    spark.stop()
    if missing:
        log.error("CHECK FAILED: missing %s", "; ".join(missing))
        sys.exit(1)
    log.info("CHECK PASSED: every injected defect type is in quarantine with a reason")


if __name__ == "__main__":
    main()
