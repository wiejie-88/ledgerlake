"""Pure silver transformations: DataFrame in, DataFrame out. No catalog access, no writes."""

from __future__ import annotations

import re
from datetime import date

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from jobs.silver.rules import HAS_PARENT, Rule

SOURCE_TZ = "Asia/Kuala_Lumpur"
FAILURE_REASON = "failure_reason"
DUPLICATE_ROW = "duplicate_row"
SUPERSEDED_IN_BATCH = "superseded_in_batch"
SUPERSEDED_BY_LATE_UPDATE = "superseded_by_late_update"

# Property keys that become silver.app_events columns; anything else is added by schema evolution.
BASE_EVENT_PROPERTIES = ("offer_id",)
_COLUMN_NAME = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


def local_to_utc(column: str) -> Column:
    """Convert a naive Malaysia-time timestamp (TIMESTAMP_NTZ) into a UTC timestamp."""
    return F.to_utc_timestamp(F.col(column).cast("timestamp"), SOURCE_TZ)


def with_parent_flag(df: DataFrame, fk: str, parent_keys: DataFrame, parent_key: str) -> DataFrame:
    """Add HAS_PARENT: True when ``df[fk]`` exists in ``parent_keys[parent_key]``."""
    keys = parent_keys.select(F.col(parent_key).alias("_parent_key")).distinct()
    return (
        df.join(keys, df[fk] == keys["_parent_key"], "left")
        .withColumn(HAS_PARENT, F.col("_parent_key").isNotNull())
        .drop("_parent_key")
    )


def flag_failures(df: DataFrame, rules: list[Rule]) -> DataFrame:
    """Add FAILURE_REASON: every failed rule name joined by '; ', or NULL when the row passes."""
    reasons = F.concat_ws("; ", *[F.when(condition, F.lit(name)) for name, condition in rules])
    return df.withColumn(FAILURE_REASON, F.when(reasons != "", reasons))


def split_duplicates(df: DataFrame, key: str, order_col: str = "updated_at") -> tuple[DataFrame, DataFrame]:
    """Keep the latest row per ``key``; return (kept, dropped).

    Dropped rows get FAILURE_REASON ``duplicate_row`` when they tie with the kept row's ``order_col``
    (an exact re-delivery), or ``superseded_in_batch`` when an older version arrived in the same batch.
    """
    by_key = Window.partitionBy(key)
    ranked = df.withColumn(
        "_rn", F.row_number().over(by_key.orderBy(F.col(order_col).desc_nulls_last(), F.col("_ingested_at").desc()))
    ).withColumn("_latest", F.max(order_col).over(by_key))
    kept = ranked.where(F.col("_rn") == 1).drop("_rn", "_latest")
    dropped = (
        ranked.where(F.col("_rn") > 1)
        .withColumn(
            FAILURE_REASON,
            F.when(F.col(order_col) == F.col("_latest"), F.lit(DUPLICATE_ROW)).otherwise(F.lit(SUPERSEDED_IN_BATCH)),
        )
        .drop("_rn", "_latest")
    )
    return kept, dropped


def late_superseded(new: DataFrame, earlier: DataFrame, key: str, event_ts_col: str) -> DataFrame:
    """Earlier versions replaced by a late-arriving update, tagged ``superseded_by_late_update``.

    For each key in ``new``, take the latest distinct version in ``earlier`` (bronze partitions before this
    batch). It is superseded by a late-arriving update when the new version's updated_at is later than the
    old one's AND falls on a later day than ``event_ts_col`` (for example, a transaction settling days later).
    Both inputs are raw bronze rows, so dates are Malaysia business dates.
    """
    latest_earlier = (
        earlier.dropDuplicates([key, "updated_at"])
        .withColumn("_rn", F.row_number().over(Window.partitionBy(key).orderBy(F.col("updated_at").desc())))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )
    incoming = new.select(F.col(key).alias("_new_key"), F.col("updated_at").alias("_new_updated_at"))
    return (
        latest_earlier.join(incoming, latest_earlier[key] == incoming["_new_key"])
        .where(
            (F.col("updated_at") < F.col("_new_updated_at")) & (F.to_date("_new_updated_at") > F.to_date(event_ts_col))
        )
        .drop("_new_key", "_new_updated_at")
        .withColumn(FAILURE_REASON, F.lit(SUPERSEDED_BY_LATE_UPDATE))
    )


def hash_pii(column: Column, salt: str) -> Column:
    """SHA-256 of salt + value (hex), or NULL when the value is NULL."""
    return F.sha2(F.concat(F.lit(salt), column), 256)


def age_band(dob_col: str, as_of: date) -> Column:
    """Age band on ``as_of`` from a date of birth: under_18, 18_24, 25_34, ... 65_plus, or unknown."""
    age = F.floor(F.months_between(F.lit(as_of).cast("date"), F.col(dob_col)) / 12)
    return (
        F.when(F.col(dob_col).isNull(), "unknown")
        .when(age < 18, "under_18")
        .when(age < 25, "18_24")
        .when(age < 35, "25_34")
        .when(age < 45, "35_44")
        .when(age < 55, "45_54")
        .when(age < 65, "55_64")
        .otherwise("65_plus")
    )


def _silver_meta(df: DataFrame, logical_date: date) -> DataFrame:
    return df.withColumn("_logical_date", F.lit(logical_date).cast("date")).withColumn(
        "_processed_at", F.current_timestamp()
    )


def conform_customers(df: DataFrame, salt: str, logical_date: date) -> tuple[DataFrame, DataFrame]:
    """Return (silver.customers, silver.customers_pii). Only the PII table keeps name, email, phone, DOB."""
    customers = df.select(
        "customer_id",
        hash_pii(F.lower(F.trim("email")), salt).alias("email_hash"),
        hash_pii(F.regexp_replace("phone", r"[^0-9]", ""), salt).alias("phone_hash"),
        age_band("date_of_birth", logical_date).alias("age_band"),
        "state",
        "income_band",
        local_to_utc("created_at").alias("created_at"),
        local_to_utc("updated_at").alias("updated_at"),
    )
    pii = df.select(
        "customer_id",
        "full_name",
        "email",
        "phone",
        "date_of_birth",
        local_to_utc("updated_at").alias("updated_at"),
    )
    return _silver_meta(customers, logical_date), _silver_meta(pii, logical_date)


def conform_accounts(df: DataFrame, logical_date: date) -> DataFrame:
    """Typed, UTC silver.accounts rows."""
    return _silver_meta(
        df.select(
            "account_id",
            "customer_id",
            "account_type",
            "status",
            local_to_utc("opened_at").alias("opened_at"),
            local_to_utc("updated_at").alias("updated_at"),
        ),
        logical_date,
    )


def conform_transactions(df: DataFrame, logical_date: date) -> DataFrame:
    """Typed, UTC silver.transactions rows; txn_date is the Malaysia business date of the transaction."""
    return _silver_meta(
        df.select(
            "txn_id",
            "account_id",
            local_to_utc("txn_ts").alias("txn_ts"),
            F.to_date("txn_ts").alias("txn_date"),
            F.col("amount").cast("decimal(12,2)").alias("amount"),
            "direction",
            "merchant_name",
            "category",
            "channel",
            "status",
            local_to_utc("updated_at").alias("updated_at"),
        ),
        logical_date,
    )


def conform_cash_advances(df: DataFrame, logical_date: date) -> DataFrame:
    """Typed, UTC silver.cash_advances rows."""
    return _silver_meta(
        df.select(
            "advance_id",
            "customer_id",
            F.col("amount").cast("decimal(12,2)").alias("amount"),
            local_to_utc("requested_at").alias("requested_at"),
            "due_date",
            local_to_utc("repaid_at").alias("repaid_at"),
            "status",
            local_to_utc("updated_at").alias("updated_at"),
        ),
        logical_date,
    )


def property_keys(df: DataFrame) -> list[str]:
    """Distinct keys of the ``properties`` map in a bronze events DataFrame, sorted."""
    rows = df.select(F.explode(F.map_keys("properties")).alias("k")).distinct().collect()
    return sorted(r["k"] for r in rows)


def evolvable_keys(keys: list[str], existing_columns: list[str]) -> list[str]:
    """Property keys that need a new silver column: not already a column and a safe column name."""
    return [k for k in keys if k not in existing_columns and _COLUMN_NAME.match(k)]


def conform_events(df: DataFrame, logical_date: date, extra_properties: list[str]) -> DataFrame:
    """Flatten bronze app events: device -> os, app_version; properties -> one column per known key."""
    properties = [*BASE_EVENT_PROPERTIES, *extra_properties]
    return _silver_meta(
        df.select(
            "event_id",
            "customer_id",
            "event_type",
            "event_ts",
            F.to_date(F.from_utc_timestamp("event_ts", SOURCE_TZ)).alias("event_date"),
            F.col("device.os").alias("os"),
            F.col("device.app_version").alias("app_version"),
            *[F.col("properties").getItem(k).alias(k) for k in properties],
        ),
        logical_date,
    )
