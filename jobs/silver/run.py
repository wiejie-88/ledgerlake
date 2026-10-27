"""Build silver and quarantine from one bronze partition.

Usage (inside the Airflow container): ``python -m jobs.silver.run --date 2026-09-29 [--entity transactions]``

Per entity: validate -> deduplicate -> (transactions) audit late-arriving updates -> write the quarantine
partition for the logical date -> conform -> MERGE into silver. Re-running a date changes nothing.
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.common.iceberg import (
    MERGE_ON_READ,
    create_if_missing,
    insert_new,
    merge_latest,
    overwrite_partition,
    qualified,
    table_exists,
)
from jobs.common.pipeline_runs import record_run, run_context
from jobs.common.spark import CATALOG, get_spark
from jobs.silver import rules
from jobs.silver.transforms import (
    FAILURE_REASON,
    conform_accounts,
    conform_cash_advances,
    conform_customers,
    conform_events,
    conform_transactions,
    evolvable_keys,
    flag_failures,
    late_superseded,
    property_keys,
    split_duplicates,
    with_parent_flag,
)

log = logging.getLogger("silver")


@dataclass(frozen=True)
class SilverEntity:
    name: str
    key: str
    rules: Callable[[], list[rules.Rule]]
    parent: tuple[str, str, str] | None = None  # (foreign key column, parent silver table, parent key)
    late_update_ts: str | None = None  # event-time column used to detect late-arriving updates
    version_col: str = "updated_at"  # newest value wins when deduplicating
    table_properties: dict[str, str] | None = None  # e.g. MERGE_ON_READ for frequently updated tables


ENTITIES = (
    SilverEntity("customers", "customer_id", rules.customer_rules),
    SilverEntity("accounts", "account_id", rules.account_rules, ("customer_id", "silver.customers", "customer_id")),
    SilverEntity(
        "transactions",
        "txn_id",
        rules.transaction_rules,
        ("account_id", "silver.accounts", "account_id"),
        late_update_ts="txn_ts",
        # Late settlements update rows spread across the whole table: merge-on-read writes small
        # delete files instead of rewriting most data files every day.
        table_properties=MERGE_ON_READ,
    ),
    SilverEntity(
        "cash_advances", "advance_id", rules.cash_advance_rules, ("customer_id", "silver.customers", "customer_id")
    ),
    SilverEntity(
        "app_events",
        "event_id",
        rules.event_rules,
        ("customer_id", "silver.customers", "customer_id"),
        version_col="event_ts",  # events are immutable and have no updated_at
    ),
)


def _parent_keys(spark: SparkSession, table: str, key: str) -> DataFrame:
    if table_exists(spark, table):
        return spark.table(qualified(table)).select(key)
    return spark.createDataFrame([], f"{key} STRING")


def _write_events(kept: DataFrame, logical_date: date) -> list[str]:
    """Insert new events into silver.app_events, evolving the schema when a new property key appears.

    Events are loaded in event-time order: rows before the first appearance of a new key are inserted under
    the current schema, then ALTER TABLE ADD COLUMN runs, then the remaining rows are inserted. Data files
    written earlier are never rewritten. Returns the columns added.
    """
    spark, table = kept.sparkSession, "silver.app_events"
    create_if_missing(conform_events(kept, logical_date, []), table, [F.days("event_ts")])
    columns = spark.table(qualified(table)).columns
    base_columns = conform_events(kept.limit(0), logical_date, []).columns
    evolved = [c for c in columns if c not in base_columns]
    added = evolvable_keys(property_keys(kept), columns)
    if not added:
        insert_new(conform_events(kept, logical_date, evolved), table, "event_id")
        return []

    has_new_key = F.lit(False)
    for key in added:
        has_new_key = has_new_key | F.col("properties").getItem(key).isNotNull()
    cutoff = kept.where(has_new_key).agg(F.min("event_ts")).first()[0]
    insert_new(conform_events(kept.where(F.col("event_ts") < cutoff), logical_date, evolved), table, "event_id")
    for key in added:
        spark.sql(f"ALTER TABLE {qualified(table)} ADD COLUMN {key} STRING")
        log.info("table=%s schema evolution: added column %s (first seen at event_ts=%s)", table, key, cutoff)
    rest = kept.where(F.col("event_ts") >= cutoff)
    insert_new(conform_events(rest, logical_date, evolved + added), table, "event_id")
    return added


def process_entity(spark: SparkSession, entity: SilverEntity, logical_date: date, salt: str) -> dict[str, object]:
    """Validate, quarantine and merge one entity's bronze partition. Returns run metrics."""
    started = time.monotonic()
    bronze_table = f"bronze.{entity.name}"
    if not table_exists(spark, bronze_table):
        raise RuntimeError(f"{bronze_table} does not exist; run `make bronze date={logical_date}` first")
    bronze = spark.table(qualified(bronze_table))
    batch = bronze.where(F.col("_logical_date") == F.lit(logical_date).cast("date"))
    if entity.parent:
        fk, parent_table, parent_key = entity.parent
        batch = with_parent_flag(batch, fk, _parent_keys(spark, parent_table, parent_key), parent_key)

    flagged = flag_failures(batch, entity.rules()).cache()
    valid = flagged.where(F.col(FAILURE_REASON).isNull()).drop(FAILURE_REASON)
    kept, dropped = split_duplicates(valid, entity.key, entity.version_col)
    quarantine = flagged.where(F.col(FAILURE_REASON).isNotNull()).unionByName(dropped)
    if entity.late_update_ts:
        earlier = bronze.where(F.col("_logical_date") < F.lit(logical_date).cast("date"))
        superseded = late_superseded(kept, earlier, entity.key, entity.late_update_ts)
        quarantine = quarantine.unionByName(superseded, allowMissingColumns=True)
    quarantine = (
        quarantine.drop(rules.HAS_PARENT)
        .withColumn("_quarantine_date", F.lit(logical_date).cast("date"))
        .withColumn("_quarantined_at", F.current_timestamp())
        .cache()
    )
    overwrite_partition(quarantine, f"quarantine.{entity.name}", "_quarantine_date", logical_date)
    reasons = {r["failure_reason"]: r["count"] for r in quarantine.groupBy(FAILURE_REASON).count().collect()}

    kept = kept.drop(rules.HAS_PARENT).cache()
    silver_table = f"silver.{entity.name}"
    added_columns: list[str] = []
    if entity.name == "app_events":
        before = spark.table(qualified(silver_table)).count() if table_exists(spark, silver_table) else 0
        added_columns = _write_events(kept, logical_date)
        inserted, updated = spark.table(qualified(silver_table)).count() - before, 0
    else:
        if entity.name == "customers":
            silver_df, pii_df = conform_customers(kept, salt, logical_date)
            merge_latest(pii_df, "silver.customers_pii", entity.key)
        else:
            conform = {
                "accounts": conform_accounts,
                "transactions": conform_transactions,
                "cash_advances": conform_cash_advances,
            }[entity.name]
            silver_df = conform(kept, logical_date)
        inserted, updated = merge_latest(silver_df, silver_table, entity.key, properties=entity.table_properties)

    metrics = {
        "table": silver_table,
        "logical_date": logical_date.isoformat(),
        "rows_in": flagged.count(),
        "rows_valid": kept.count(),
        "rows_quarantined": sum(reasons.values()),
        "quarantine_reasons": ",".join(f"{k}:{v}" for k, v in sorted(reasons.items())) or "none",
        "inserted": inserted,
        "updated": updated,
        "table_rows": spark.table(qualified(silver_table)).count(),
        "duration_s": round(time.monotonic() - started, 1),
    }
    if added_columns:
        metrics["schema_evolution_added"] = ",".join(added_columns)
    log.info(" ".join(f"{k}={v}" for k, v in metrics.items()))
    for df in (flagged, quarantine, kept):
        df.unpersist()
    return metrics


def main(argv: list[str] | None = None) -> None:
    """Entry point."""
    parser = argparse.ArgumentParser(prog="jobs.silver.run", description="Build silver and quarantine")
    parser.add_argument("--date", required=True, type=date.fromisoformat, help="logical date, YYYY-MM-DD")
    parser.add_argument("--entity", action="append", choices=[e.name for e in ENTITIES], help="limit to entities")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    salt = os.environ.get("PII_SALT", "")
    if not salt:
        raise SystemExit("PII_SALT is not set; add it to .env (see .env.example)")
    started = time.monotonic()
    spark = get_spark("silver")
    for namespace in ("silver", "quarantine"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{namespace}")
    log.info("silver: logical_date=%s", args.date)
    metrics = [
        process_entity(spark, entity, args.date, salt)
        for entity in ENTITIES
        if not args.entity or entity.name in args.entity
    ]
    rows_in = sum(m["rows_in"] for m in metrics)
    rows_out = sum(m["rows_valid"] for m in metrics)
    record_run(spark, run_context("silver"), args.date, rows_in, rows_out, round(time.monotonic() - started, 1))
    spark.stop()


if __name__ == "__main__":
    main()
