"""Unit tests for jobs/publish/transforms.py."""

from __future__ import annotations

from datetime import date

from jobs.publish.contract import PROFILE_COLUMNS, serving_columns
from jobs.publish.transforms import customer_profile, with_as_of


def test_customer_profile_joins_pii_and_dimension(spark) -> None:
    pii = spark.createDataFrame(
        [("C1", "Siti binti Ismail", "s@example.com", "+60 12-345 6789", date(1990, 5, 1)),
         ("C9", "No Dimension Row", None, None, None)],
        "customer_id STRING, full_name STRING, email STRING, phone STRING, date_of_birth DATE",
    )  # fmt: skip
    dim = spark.createDataFrame(
        [("C1", "35_44", "Selangor", "2k_4k", date(2026, 8, 1), 2)],
        "customer_id STRING, age_band STRING, state STRING, income_band STRING, customer_since DATE, account_count INT",
    )
    rows = customer_profile(pii, dim).collect()
    assert [tuple(r) for r in rows] == [
        ("C1", "Siti binti Ismail", "s@example.com", "+60 12-345 6789", date(1990, 5, 1), "35_44", "Selangor",
         "2k_4k", date(2026, 8, 1)),
    ]  # fmt: skip


def test_with_as_of_orders_columns_like_the_serving_table(spark) -> None:
    df = spark.createDataFrame([("x",) * len(PROFILE_COLUMNS)], ", ".join(f"{c} STRING" for c in PROFILE_COLUMNS))
    out = with_as_of(df.select(*reversed(PROFILE_COLUMNS)), PROFILE_COLUMNS, date(2026, 10, 4))
    assert tuple(out.columns) == serving_columns()["customer_profile"]
    assert out.first()["as_of_date"] == date(2026, 10, 4)
