"""Unit tests for jobs/demo/audit.py (the time-travel audit comparisons)."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from jobs.demo.audit import DaySummary, day_summary, status_changes

SCHEMA = "txn_id STRING, txn_date DATE, amount DECIMAL(12,2), direction STRING, status STRING, updated_at TIMESTAMP"
D = date(2026, 10, 4)


def test_day_summary_counts_only_that_date(spark) -> None:
    df = spark.createDataFrame(
        [
            ("T1", D, Decimal("10.00"), "debit", "completed", datetime(2026, 10, 4)),
            ("T2", D, Decimal("5.00"), "debit", "pending", datetime(2026, 10, 4)),
            ("T3", D, Decimal("99.00"), "credit", "completed", datetime(2026, 10, 4)),
            ("T4", date(2026, 10, 5), Decimal("7.00"), "debit", "completed", datetime(2026, 10, 5)),
        ],
        SCHEMA,
    )
    assert day_summary(df, D) == DaySummary(rows=3, completed_spend=Decimal("10.00"), pending=1)
    assert day_summary(df, date(2026, 1, 1)) == DaySummary(rows=0, completed_spend=Decimal("0"), pending=0)


def test_status_changes_lists_only_changed_transactions(spark) -> None:
    then = spark.createDataFrame(
        [("T1", D, Decimal("5.00"), "debit", "pending", datetime(2026, 10, 4)),
         ("T2", D, Decimal("5.00"), "debit", "completed", datetime(2026, 10, 4))],
        SCHEMA,
    )  # fmt: skip
    now = spark.createDataFrame(
        [("T1", D, Decimal("5.00"), "debit", "completed", datetime(2026, 10, 6)),
         ("T2", D, Decimal("5.00"), "debit", "completed", datetime(2026, 10, 4)),
         ("T3", D, Decimal("5.00"), "debit", "pending", datetime(2026, 10, 6))],
        SCHEMA,
    )  # fmt: skip
    rows = [tuple(r) for r in status_changes(then, now).select("txn_id", "status_then", "status_now").collect()]
    assert rows == [("T1", "pending", "completed")]
