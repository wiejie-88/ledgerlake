"""Pure gold transformations: fact, dimensions and marts from silver DataFrames. No catalog access, no writes.

Business definitions (shared with quality/reconcile.py):
- spend = completed debit transactions;
- salary = completed credit transactions in category ``salary``;
- months and days are Malaysia business dates (``txn_date``; requested_at converted from UTC).
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

SOURCE_TZ = "Asia/Kuala_Lumpur"
CALENDAR_START = date(2024, 1, 1)
CALENDAR_END = date(2027, 12, 31)


def date_key(column: Column) -> Column:
    """yyyymmdd integer key for a date column."""
    return F.date_format(column, "yyyyMMdd").cast("int")


def local_date(utc_ts: str) -> Column:
    """Malaysia business date of a UTC timestamp column."""
    return F.to_date(F.from_utc_timestamp(F.col(utc_ts), SOURCE_TZ))


def is_spend() -> Column:
    """Completed debit transactions."""
    return (F.col("direction") == "debit") & (F.col("status") == "completed")


def is_salary() -> Column:
    """Completed salary credits."""
    return (F.col("direction") == "credit") & (F.col("status") == "completed") & (F.col("category") == "salary")


def row_hash(*columns: str) -> Column:
    """SHA-256 over the given columns, used to detect changed dimension rows."""
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in columns]), 256)


def build_dim_date(spark: SparkSession, start: date = CALENDAR_START, end: date = CALENDAR_END) -> DataFrame:
    """One row per calendar day between ``start`` and ``end`` inclusive."""
    days = spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 DAY)) AS date")
    return days.select(
        date_key(F.col("date")).alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day_of_month"),
        ((F.dayofweek("date") + 5) % 7 + 1).alias("day_of_week"),  # ISO: Monday=1 .. Sunday=7
        F.date_format("date", "EEEE").alias("day_name"),
        F.weekofyear("date").alias("iso_week"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
        F.trunc("date", "month").alias("month_start"),
    )


DIM_CUSTOMER_ATTRIBUTES = (
    "age_band",
    "state",
    "income_band",
    "customer_since",
    "account_count",
    "has_savings_account",
    "active_account_count",
)


def build_dim_customer(customers: DataFrame, accounts: DataFrame) -> DataFrame:
    """One row per customer (no PII): silver attributes plus account roll-ups and a change hash."""
    account_stats = accounts.groupBy("customer_id").agg(
        F.count("*").alias("account_count"),
        F.max(F.when(F.col("account_type") == "savings", True).otherwise(False)).alias("has_savings_account"),
        F.sum(F.when(F.col("status") == "active", 1).otherwise(0)).alias("active_account_count"),
    )
    dim = customers.join(account_stats, "customer_id", "left").select(
        "customer_id",
        "age_band",
        "state",
        "income_band",
        local_date("created_at").alias("customer_since"),
        F.coalesce("account_count", F.lit(0)).cast("int").alias("account_count"),
        F.coalesce("has_savings_account", F.lit(False)).alias("has_savings_account"),
        F.coalesce("active_account_count", F.lit(0)).cast("int").alias("active_account_count"),
    )
    return dim.withColumn("_row_hash", row_hash(*DIM_CUSTOMER_ATTRIBUTES))


def build_fact_transactions(transactions: DataFrame, accounts: DataFrame) -> DataFrame:
    """One row per transaction with customer_id, account_type and date_key added."""
    account_cols = accounts.select("account_id", "customer_id", "account_type")
    return transactions.join(account_cols, "account_id", "inner").select(
        "txn_id",
        "account_id",
        "customer_id",
        "account_type",
        "txn_ts",
        "txn_date",
        date_key(F.col("txn_date")).alias("date_key"),
        "amount",
        "direction",
        "merchant_name",
        "category",
        "channel",
        "status",
        "updated_at",
    )


def mart_daily_spend(fact: DataFrame) -> DataFrame:
    """Day x category: total and average spend, transaction count, distinct customers."""
    return (
        fact.where(is_spend())
        .groupBy("txn_date", "category")
        .agg(
            F.sum("amount").alias("total_spend"),
            F.round(F.avg("amount"), 2).cast("decimal(12,2)").alias("avg_spend"),
            F.count("*").alias("txn_count"),
            F.countDistinct("customer_id").alias("customer_count"),
        )
        .withColumn("date_key", date_key(F.col("txn_date")))
        .select("date_key", "txn_date", "category", "total_spend", "avg_spend", "txn_count", "customer_count")
    )


def mart_advance_repayment(advances: DataFrame, as_of: date) -> DataFrame:
    """Monthly request cohort: volumes, repayment rates and average days late.

    ``repayment_rate_due`` only counts advances whose due date is on or before ``as_of``, so young cohorts
    aren't penalised for advances that aren't due yet. ``avg_days_late`` averages max(0, repaid - due) over
    repaid advances.
    """
    is_repaid = F.col("status") == "repaid"
    is_due = F.col("due_date") <= F.lit(as_of).cast("date")
    days_late = F.greatest(F.datediff(local_date("repaid_at"), F.col("due_date")), F.lit(0))
    cohort = advances.withColumn("cohort_month", F.trunc(local_date("requested_at"), "month"))
    return (
        cohort.groupBy("cohort_month")
        .agg(
            F.count("*").alias("advance_count"),
            F.sum("amount").alias("amount_total"),
            F.sum(is_repaid.cast("int")).alias("repaid_count"),
            F.sum((F.col("status") == "overdue").cast("int")).alias("overdue_count"),
            F.sum((F.col("status") == "active").cast("int")).alias("active_count"),
            F.sum(is_due.cast("int")).alias("due_count"),
            F.sum((is_due & is_repaid).cast("int")).alias("due_repaid_count"),
            F.sum((is_repaid & (days_late > 0)).cast("int")).alias("late_repaid_count"),
            F.avg(F.when(is_repaid, days_late)).alias("avg_days_late"),
        )
        .withColumn("repayment_rate", F.round(F.col("repaid_count") / F.col("advance_count"), 4))
        .withColumn(
            "repayment_rate_due",
            F.round(F.when(F.col("due_count") > 0, F.col("due_repaid_count") / F.col("due_count")), 4),
        )
        .withColumn("avg_days_late", F.round("avg_days_late", 2))
        .withColumn("as_of_date", F.lit(as_of).cast("date"))
        .orderBy("cohort_month")
    )


def mart_customer_features(fact: DataFrame, advances: DataFrame) -> DataFrame:
    """Customer x month features for modelling: spend level and volatility, income regularity, advance usage.

    - daily_spend_stddev / daily_spend_cv: variation of daily spend across the customer's spending days.
    - income_regularity_3m: share of the last 3 calendar months (including this one) with a salary credit.
    - advance_count / advance_amount: cash advances requested in the month.
    """
    month = F.trunc("txn_date", "month")
    daily = (
        fact.where(is_spend())
        .groupBy("customer_id", "txn_date")
        .agg(F.sum("amount").alias("day_spend"), F.count("*").alias("day_txns"))
    )
    spend = daily.groupBy("customer_id", month.alias("month")).agg(
        F.sum("day_spend").alias("spend_total"),
        F.sum("day_txns").alias("spend_txn_count"),
        F.count("*").alias("spend_days"),
        F.round(F.stddev_samp("day_spend"), 2).alias("daily_spend_stddev"),
        F.avg("day_spend").alias("_daily_mean"),
    )
    salary = (
        fact.where(is_salary())
        .groupBy("customer_id", month.alias("month"))
        .agg(F.count("*").alias("salary_credit_count"), F.sum("amount").alias("salary_total"))
    )
    adv = advances.groupBy("customer_id", F.trunc(local_date("requested_at"), "month").alias("month")).agg(
        F.count("*").alias("advance_count"), F.sum("amount").alias("advance_amount")
    )
    keys = (
        spend.select("customer_id", "month")
        .unionByName(salary.select("customer_id", "month"))
        .unionByName(adv.select("customer_id", "month"))
        .distinct()
    )
    features = (
        keys.join(spend, ["customer_id", "month"], "left")
        .join(salary, ["customer_id", "month"], "left")
        .join(adv, ["customer_id", "month"], "left")
    )
    # Income regularity over calendar months: look back 2 months by date, not by row position.
    month_index = F.year("month") * 12 + F.month("month")
    lookback = Window.partitionBy("customer_id").orderBy(month_index).rangeBetween(-2, 0)
    zero = F.lit(0).cast("decimal(22,2)")
    return features.select(
        "customer_id",
        "month",
        F.coalesce("spend_total", zero).alias("spend_total"),
        F.coalesce("spend_txn_count", F.lit(0)).alias("spend_txn_count"),
        F.coalesce("spend_days", F.lit(0)).alias("spend_days"),
        "daily_spend_stddev",
        F.round(F.col("daily_spend_stddev") / F.col("_daily_mean"), 4).alias("daily_spend_cv"),
        F.coalesce("salary_credit_count", F.lit(0)).alias("salary_credit_count"),
        F.coalesce("salary_total", zero).alias("salary_total"),
        F.round(F.sum(F.when(F.col("salary_credit_count") > 0, 1).otherwise(0)).over(lookback) / F.lit(3.0), 4).alias(
            "income_regularity_3m"
        ),
        F.coalesce("advance_count", F.lit(0)).alias("advance_count"),
        F.coalesce("advance_amount", zero).alias("advance_amount"),
    )
