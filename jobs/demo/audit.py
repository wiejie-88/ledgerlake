"""Pure comparisons used by the time-travel audit demo (DataFrame in, values out)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from jobs.gold.transforms import is_spend


@dataclass(frozen=True)
class DaySummary:
    rows: int
    completed_spend: Decimal
    pending: int


def day_summary(fact: DataFrame, business_date: date) -> DaySummary:
    """Transactions of one business date: rows, completed spend (MYR) and pending count."""
    day = fact.where(F.col("txn_date") == F.lit(business_date).cast("date"))
    row = day.agg(
        F.count("*").alias("rows"),
        F.coalesce(F.sum(F.when(is_spend(), F.col("amount"))), F.lit(0)).alias("spend"),
        F.sum((F.col("status") == "pending").cast("int")).alias("pending"),
    ).first()
    return DaySummary(row["rows"], Decimal(row["spend"]), row["pending"] or 0)


def status_changes(then: DataFrame, now: DataFrame) -> DataFrame:
    """Transactions present in both versions whose status differs, with both statuses side by side."""
    before = then.select("txn_id", "txn_date", F.col("status").alias("status_then"))
    after = now.select("txn_id", F.col("status").alias("status_now"), F.col("updated_at").alias("updated_at_now"))
    return before.join(after, "txn_id").where(F.col("status_then") != F.col("status_now"))
