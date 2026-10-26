"""Reconcile gold marts against silver: recompute each mart's totals straight from silver and compare.

Each reconciler takes the DQ loader and returns the number of mismatched groups (0 means reconciled).
A group mismatches when it is missing on either side or any compared measure differs. Sums are exact
decimals, so the comparison is exact equality.
"""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from jobs.gold.transforms import is_spend, local_date

Loader = Callable[[str], DataFrame]


def mismatched_groups(actual: DataFrame, expected: DataFrame, keys: list[str], measures: list[str]) -> int:
    """Count key groups present on only one side or whose measures differ (NULL-safe)."""
    a = actual.select(*keys, *[F.col(m).alias(f"a_{m}") for m in measures])
    e = expected.select(*keys, *[F.col(m).alias(f"e_{m}") for m in measures])
    joined = a.join(e, keys, "full_outer")
    differs = F.lit(False)
    for m in measures:
        differs = differs | ~F.col(f"a_{m}").eqNullSafe(F.col(f"e_{m}"))
    return joined.where(differs).count()


def daily_spend(load: Loader) -> int:
    """gold.mart_daily_spend vs silver.transactions: total_spend and txn_count per day x category."""
    expected = (
        load("silver.transactions")
        .where(is_spend())
        .groupBy("txn_date", "category")
        .agg(F.sum("amount").alias("total_spend"), F.count("*").alias("txn_count"))
    )
    return mismatched_groups(
        load("gold.mart_daily_spend"), expected, ["txn_date", "category"], ["total_spend", "txn_count"]
    )


def advance_repayment(load: Loader) -> int:
    """gold.mart_advance_repayment vs silver.cash_advances: count, amount and repaid count per cohort month."""
    expected = (
        load("silver.cash_advances")
        .groupBy(F.trunc(local_date("requested_at"), "month").alias("cohort_month"))
        .agg(
            F.count("*").alias("advance_count"),
            F.sum("amount").alias("amount_total"),
            F.sum((F.col("status") == "repaid").cast("int")).alias("repaid_count"),
        )
    )
    return mismatched_groups(
        load("gold.mart_advance_repayment"),
        expected,
        ["cohort_month"],
        ["advance_count", "amount_total", "repaid_count"],
    )


def customer_features(load: Loader) -> int:
    """gold.mart_customer_features vs silver: monthly spend total/count and advance count/amount."""
    mart = (
        load("gold.mart_customer_features")
        .groupBy("month")
        .agg(
            F.sum("spend_total").cast("decimal(32,2)").alias("spend_total"),
            F.sum("spend_txn_count").alias("spend_txn_count"),
            F.sum("advance_count").alias("advance_count"),
            F.sum("advance_amount").cast("decimal(32,2)").alias("advance_amount"),
        )
    )
    spend = (
        load("silver.transactions")
        .where(is_spend())
        .groupBy(F.trunc("txn_date", "month").alias("month"))
        .agg(F.sum("amount").alias("spend_total"), F.count("*").alias("spend_txn_count"))
    )
    advances = (
        load("silver.cash_advances")
        .groupBy(F.trunc(local_date("requested_at"), "month").alias("month"))
        .agg(F.count("*").alias("advance_count"), F.sum("amount").alias("advance_amount"))
    )
    zero = F.lit(0)
    expected = spend.join(advances, "month", "full_outer").select(
        "month",
        F.coalesce("spend_total", zero).cast("decimal(32,2)").alias("spend_total"),
        F.coalesce("spend_txn_count", zero).cast("bigint").alias("spend_txn_count"),
        F.coalesce("advance_count", zero).cast("bigint").alias("advance_count"),
        F.coalesce("advance_amount", zero).cast("decimal(32,2)").alias("advance_amount"),
    )
    measures = ["spend_total", "spend_txn_count", "advance_count", "advance_amount"]
    # A month where customers only received salary has all-zero measures in the mart and no silver group.
    mart = mart.where(" OR ".join(f"{m} <> 0" for m in measures))
    return mismatched_groups(mart, expected, ["month"], measures)


RECONCILERS: dict[str, Callable[[Loader], int]] = {
    "mart_daily_spend": daily_spend,
    "mart_advance_repayment": advance_repayment,
    "mart_customer_features": customer_features,
}
