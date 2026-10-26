"""Pure DataFrame preparation for publishing."""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from jobs.publish.contract import PROFILE_COLUMNS


def customer_profile(pii: DataFrame, dim_customer: DataFrame) -> DataFrame:
    """One row per customer: personal data from silver.customers_pii plus gold.dim_customer attributes."""
    dim = dim_customer.select("customer_id", "age_band", "state", "income_band", "customer_since")
    return pii.join(dim, "customer_id", "inner").select(*PROFILE_COLUMNS)


def with_as_of(df: DataFrame, columns: tuple[str, ...], logical_date: date) -> DataFrame:
    """Select ``columns`` in order and append as_of_date."""
    return df.select(*columns).withColumn("as_of_date", F.lit(logical_date).cast("date"))
