"""Time-travel audit demo on gold.fact_transactions.

Usage (inside the Airflow container): ``python -m jobs.demo.time_travel [--as-of-date 2026-10-04]``

Audit question: "What did gold say about business date D when it was built, and what changed since?"
1. Find the snapshot that was current right after the gold run for D (the newest snapshot whose
   max(_logical_date) is D).
2. Read the table VERSION AS OF that snapshot and TIMESTAMP AS OF its commit time; both must agree.
3. Compare D's figures then and now, and list transactions whose status changed (late settlements).
"""

from __future__ import annotations

import argparse
import logging
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from jobs.common.spark import get_spark
from jobs.demo.audit import day_summary, status_changes

TABLE = "lake.gold.fact_transactions"

log = logging.getLogger("time_travel")


def snapshot_for(spark: SparkSession, business_date: date) -> tuple[int, str]:
    """(snapshot_id, committed_at) of the newest snapshot whose max(_logical_date) equals ``business_date``."""
    snapshots = spark.sql(
        f"SELECT snapshot_id, CAST(committed_at AS STRING) AS committed_at FROM {TABLE}.snapshots "
        "ORDER BY committed_at DESC"
    ).collect()
    for snap in snapshots:
        latest = spark.sql(f"SELECT max(_logical_date) FROM {TABLE} VERSION AS OF {snap['snapshot_id']}").first()[0]
        if latest == business_date:
            return snap["snapshot_id"], snap["committed_at"]
    raise SystemExit(f"No retained snapshot of {TABLE} was built for {business_date} (expired by maintenance?)")


def main(argv: list[str] | None = None) -> None:
    """Entry point."""
    parser = argparse.ArgumentParser(prog="jobs.demo.time_travel", description="Iceberg time-travel audit demo")
    parser.add_argument("--as-of-date", type=date.fromisoformat, help="business date to audit (default: previous)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    spark = get_spark("time_travel_demo")

    dates = [r[0] for r in spark.sql(f"SELECT DISTINCT _logical_date FROM {TABLE} ORDER BY 1 DESC").collect()]
    business_date = args.as_of_date or (dates[1] if len(dates) > 1 else dates[0])
    snapshot_id, committed_at = snapshot_for(spark, business_date)
    current_id = spark.sql(f"SELECT snapshot_id FROM {TABLE}.snapshots ORDER BY committed_at DESC LIMIT 1").first()[0]

    then = spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF {snapshot_id}").cache()
    then_by_time = spark.sql(f"SELECT count(*) FROM {TABLE} TIMESTAMP AS OF '{committed_at}'").first()[0]
    now = spark.table(TABLE).cache()

    log.info("== Time travel on %s: audit of business date %s", TABLE, business_date)
    log.info("snapshot built for %s: id=%s committed_at=%s | current snapshot: id=%s", business_date, snapshot_id,
             committed_at, current_id)  # fmt: skip
    log.info("rows VERSION AS OF %s = %d | rows TIMESTAMP AS OF '%s' = %d | rows now = %d",
             snapshot_id, then.count(), committed_at, then_by_time, now.count())  # fmt: skip

    before, after = day_summary(then, business_date), day_summary(now, business_date)
    log.info("business date %s as reported then: rows=%d completed_spend=%s pending=%d",
             business_date, before.rows, before.completed_spend, before.pending)  # fmt: skip
    log.info("business date %s as it stands now: rows=%d completed_spend=%s pending=%d",
             business_date, after.rows, after.completed_spend, after.pending)  # fmt: skip
    log.info("restatement of completed spend since the original report: %s MYR",
             after.completed_spend - before.completed_spend)  # fmt: skip

    changes = status_changes(then, now).cache()
    log.info(
        "transactions whose status changed since snapshot %s: %d (all business dates)", snapshot_id, changes.count()
    )
    for row in changes.groupBy("status_then", "status_now").count().orderBy(F.desc("count")).collect():
        log.info("  %s -> %s: %d", row["status_then"], row["status_now"], row["count"])
    for row in changes.orderBy("txn_id").limit(3).collect():
        log.info("  example %s (txn_date %s): %s then, %s now (updated_at %s UTC)", row["txn_id"], row["txn_date"],
                 row["status_then"], row["status_now"], row["updated_at_now"])  # fmt: skip
    spark.stop()


if __name__ == "__main__":
    main()
