"""Unit tests for jobs/gold/transforms.py and the mart reconciliation in quality/reconcile.py."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from jobs.gold.transforms import (
    build_dim_customer,
    build_dim_date,
    build_fact_transactions,
    mart_advance_repayment,
    mart_customer_features,
    mart_daily_spend,
)
from quality.reconcile import RECONCILERS

TXN_SCHEMA = (
    "txn_id STRING, account_id STRING, txn_ts TIMESTAMP, txn_date DATE, amount DECIMAL(12,2), direction STRING, "
    "merchant_name STRING, category STRING, channel STRING, status STRING, updated_at TIMESTAMP"
)
ADV_SCHEMA = (
    "advance_id STRING, customer_id STRING, amount DECIMAL(12,2), requested_at TIMESTAMP, due_date DATE, "
    "repaid_at TIMESTAMP, status STRING, updated_at TIMESTAMP"
)


def _txn(
    txn_id: str,
    account: str,
    day: date,
    amount: str,
    direction: str = "debit",
    category: str = "groceries",
    status: str = "completed",
) -> tuple:
    ts = datetime(day.year, day.month, day.day, 4, 0)  # 12:00 in Malaysia
    return (txn_id, account, ts, day, Decimal(amount), direction, "Mydin", category, "card", status, ts)


@pytest.fixture
def silver(spark):
    customers = spark.createDataFrame(
        [
            (
                "C1",
                "h1",
                "p1",
                "25_34",
                "Selangor",
                "2k_4k",
                datetime(2026, 7, 31, 17, 0),
                datetime(2026, 7, 31, 17, 0),
            ),
            ("C2", "h2", "p2", "35_44", "Johor", "4k_6k", datetime(2025, 1, 1), datetime(2025, 1, 1)),
        ],
        "customer_id STRING, email_hash STRING, phone_hash STRING, age_band STRING, state STRING, "
        "income_band STRING, created_at TIMESTAMP, updated_at TIMESTAMP",
    )
    accounts = spark.createDataFrame(
        [("A1", "C1", "spending", "active"), ("A2", "C1", "savings", "dormant"), ("A3", "C2", "spending", "active")],
        "account_id STRING, customer_id STRING, account_type STRING, status STRING",
    )
    transactions = spark.createDataFrame(
        [
            _txn("T1", "A1", date(2026, 8, 3), "10.00"),
            _txn("T2", "A1", date(2026, 8, 3), "30.00"),
            _txn("T3", "A1", date(2026, 8, 5), "20.00"),
            _txn("T4", "A1", date(2026, 8, 5), "99.00", status="pending"),  # not spend
            _txn("T5", "A1", date(2026, 8, 25), "3000.00", direction="credit", category="salary"),
            _txn("T6", "A3", date(2026, 8, 3), "5.50", category="transport"),
            _txn("T7", "A1", date(2026, 10, 25), "3000.00", direction="credit", category="salary"),
            _txn("T8", "A9", date(2026, 8, 3), "1.00"),  # orphan: dropped from the fact
        ],
        TXN_SCHEMA,
    )
    advances = spark.createDataFrame(
        [
            # requested 2026-08-31 20:00 UTC = 2026-09-01 in Malaysia -> September cohort
            ("ADV1", "C1", Decimal("100.00"), datetime(2026, 8, 31, 20, 0), date(2026, 10, 1),
             datetime(2026, 10, 4, 2, 0), "repaid", datetime(2026, 10, 4, 2, 0)),
            ("ADV2", "C2", Decimal("50.00"), datetime(2026, 9, 10, 2, 0), date(2026, 10, 10), None, "overdue",
             datetime(2026, 10, 11, 0, 0)),
            ("ADV3", "C2", Decimal("200.00"), datetime(2026, 8, 5, 2, 0), date(2026, 9, 4),
             datetime(2026, 9, 1, 2, 0), "repaid", datetime(2026, 9, 1, 2, 0)),
        ],
        ADV_SCHEMA,
    )  # fmt: skip
    return {"customers": customers, "accounts": accounts, "transactions": transactions, "cash_advances": advances}


def test_dim_date(spark) -> None:
    dim = build_dim_date(spark, date(2026, 9, 28), date(2026, 10, 4))
    rows = {r["date_key"]: r for r in dim.collect()}
    assert len(rows) == 7
    monday, sunday = rows[20260928], rows[20261004]
    assert (monday["day_of_week"], monday["day_name"], monday["is_weekend"]) == (1, "Monday", False)
    assert (sunday["day_of_week"], sunday["is_weekend"], sunday["month_start"]) == (7, True, date(2026, 10, 1))


def test_dim_customer_rollups_and_hash(silver) -> None:
    dim = {r["customer_id"]: r for r in build_dim_customer(silver["customers"], silver["accounts"]).collect()}
    c1 = dim["C1"]
    assert (c1["account_count"], c1["has_savings_account"], c1["active_account_count"]) == (2, True, 1)
    assert c1["customer_since"] == date(2026, 8, 1)  # 2026-07-31 17:00 UTC is 2026-08-01 in Malaysia
    moved = silver["customers"].withColumn("state", F.lit("Perak"))
    moved_hash = {r["customer_id"]: r["_row_hash"] for r in build_dim_customer(moved, silver["accounts"]).collect()}
    assert moved_hash["C1"] != c1["_row_hash"]


def test_fact_adds_customer_and_drops_orphans(silver) -> None:
    fact = {r["txn_id"]: r for r in build_fact_transactions(silver["transactions"], silver["accounts"]).collect()}
    assert "T8" not in fact
    assert (fact["T1"]["customer_id"], fact["T1"]["account_type"], fact["T1"]["date_key"]) == (
        "C1",
        "spending",
        20260803,
    )


def test_daily_spend_counts_only_completed_debits(silver) -> None:
    fact = build_fact_transactions(silver["transactions"], silver["accounts"])
    mart = {(r["txn_date"], r["category"]): r for r in mart_daily_spend(fact).collect()}
    groceries = mart[(date(2026, 8, 3), "groceries")]
    assert (groceries["total_spend"], groceries["avg_spend"], groceries["txn_count"]) == (
        Decimal("40.00"),
        Decimal("20.00"),
        2,
    )
    assert mart[(date(2026, 8, 5), "groceries")]["txn_count"] == 1  # pending T4 excluded
    assert not any(category == "salary" for _, category in mart)


def test_advance_repayment_cohorts(silver) -> None:
    mart = {r["cohort_month"]: r for r in mart_advance_repayment(silver["cash_advances"], date(2026, 10, 12)).collect()}
    september = mart[date(2026, 9, 1)]
    assert (september["advance_count"], september["repaid_count"], september["overdue_count"]) == (2, 1, 1)
    assert (september["repayment_rate"], september["repayment_rate_due"]) == (0.5, 0.5)
    assert september["avg_days_late"] == 3.0  # due 2026-10-01, repaid 2026-10-04
    august = mart[date(2026, 8, 1)]
    assert (august["late_repaid_count"], august["avg_days_late"]) == (0, 0.0)


def test_customer_features(silver) -> None:
    fact = build_fact_transactions(silver["transactions"], silver["accounts"])
    rows = {(r["customer_id"], r["month"]): r for r in mart_customer_features(fact, silver["cash_advances"]).collect()}
    aug = rows[("C1", date(2026, 8, 1))]
    assert (aug["spend_total"], aug["spend_txn_count"], aug["spend_days"]) == (Decimal("60.00"), 3, 2)
    assert aug["daily_spend_stddev"] == pytest.approx(14.14, abs=0.01)  # daily spend 40 and 20
    assert aug["income_regularity_3m"] == pytest.approx(1 / 3, abs=1e-4)
    # September has only an advance; October's window (Aug..Oct) sees 2 salary months, using calendar months.
    assert rows[("C1", date(2026, 9, 1))]["advance_count"] == 1
    assert rows[("C1", date(2026, 10, 1))]["income_regularity_3m"] == pytest.approx(2 / 3, abs=1e-4)


def test_marts_reconcile_with_silver_and_tampering_is_caught(spark, silver) -> None:
    clean_silver = dict(silver, transactions=silver["transactions"].where(F.col("txn_id") != "T8"))
    fact = build_fact_transactions(clean_silver["transactions"], clean_silver["accounts"])
    tables = {
        "silver.transactions": clean_silver["transactions"],
        "silver.cash_advances": clean_silver["cash_advances"],
        "gold.mart_daily_spend": mart_daily_spend(fact),
        "gold.mart_advance_repayment": mart_advance_repayment(clean_silver["cash_advances"], date(2026, 10, 12)),
        "gold.mart_customer_features": mart_customer_features(fact, clean_silver["cash_advances"]),
    }
    assert {name: fn(tables.__getitem__) for name, fn in RECONCILERS.items()} == {
        "mart_daily_spend": 0,
        "mart_advance_repayment": 0,
        "mart_customer_features": 0,
    }
    tampered = dict(tables)
    tampered["gold.mart_daily_spend"] = tables["gold.mart_daily_spend"].withColumn(
        "total_spend",
        F.when(F.col("category") == "transport", F.col("total_spend") + 1).otherwise(F.col("total_spend")),
    )
    assert RECONCILERS["mart_daily_spend"](tampered.__getitem__) == 1
    # The orphan T8 is in silver but not in the fact, so reconciling against the unfiltered silver fails.
    assert (
        RECONCILERS["mart_daily_spend"](dict(tables, **{"silver.transactions": silver["transactions"]}).__getitem__)
        == 1
    )
