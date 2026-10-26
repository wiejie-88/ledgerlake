"""Unit tests for quality/checks.py and the simulated-defect helper."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from generator import settings as gen
from quality.checks import Check, evaluate
from quality.gate import Severity
from quality.run import SIMULATED_DEFECTS, simulate_defect
from quality.suites import SILVER

D = date(2026, 10, 1)
TXN_SCHEMA = "txn_id STRING, account_id STRING, amount DECIMAL(12,2), category STRING, txn_date DATE"


@pytest.fixture
def tables(spark):
    txns = spark.createDataFrame(
        [
            ("T1", "A1", Decimal("10.00"), "groceries", date(2026, 10, 1)),
            ("T2", "A1", None, "groceries", date(2026, 9, 30)),
            ("T2", "A2", Decimal("-1.00"), "MISC", date(2026, 9, 30)),
            ("T3", "A9", Decimal("0.00"), None, date(2026, 9, 29)),
        ],
        TXN_SCHEMA,
    )
    accounts = spark.createDataFrame([("A1",), ("A2",)], "account_id STRING")
    quarantine = spark.createDataFrame(
        [("null_amount", D), ("superseded_by_late_update", D), ("duplicate_row", date(2026, 9, 30))],
        "failure_reason STRING, _quarantine_date DATE",
    )
    bronze = spark.createDataFrame([(D,)] * 10 + [(date(2026, 9, 30),)] * 5, "_logical_date DATE")
    data = {"t": txns, "a": accounts, "q": quarantine, "b": bronze}
    return data.__getitem__


@pytest.mark.parametrize(
    ("check", "failed"),
    [
        (Check("t", "not_null", "amount"), 1),
        (Check("t", "unique", "txn_id"), 1),
        (Check("t", "accepted_values", "category", values=("groceries",)), 2),  # MISC and NULL
        (Check("t", "positive", "amount"), 2),  # -1.00 and 0.00
        (Check("t", "referential", "account_id", ref_table="a", ref_column="account_id"), 1),
    ],
)
def test_row_level_checks(tables, check: Check, failed: int) -> None:
    result = evaluate(check, tables, D)
    assert (result.failed_rows, result.passed) == (failed, False)


def test_freshness(tables) -> None:
    assert evaluate(Check("t", "freshness", "txn_date", Severity.WARN, max_lag_days=1), tables, D).passed
    stale = evaluate(Check("t", "freshness", "txn_date", Severity.WARN, max_lag_days=0), tables, date(2026, 10, 2))
    assert (stale.passed, stale.failed_rows, stale.details) == (False, 1, "latest=2026-10-01 required>=2026-10-02")


def test_quarantine_rate_ignores_audit_rows_and_other_dates(tables) -> None:
    check = Check("q", "quarantine_rate", "failure_reason", Severity.WARN, ref_table="b", threshold=0.05)
    result = evaluate(check, tables, D)
    assert (result.failed_rows, result.passed, result.details) == (1, False, "rejected=1 bronze_rows=10 rate=0.1000")
    assert evaluate(Check("q", "quarantine_rate", "failure_reason", ref_table="b", threshold=0.1), tables, D).passed


@pytest.mark.parametrize("defect", SIMULATED_DEFECTS)
def test_each_simulated_defect_fails_an_error_check(spark, defect: str) -> None:
    txns = spark.createDataFrame(
        [("T1", "A1", Decimal("10.00"), "groceries", date(2026, 10, 1), "completed", "debit")],
        TXN_SCHEMA + ", status STRING, direction STRING",
    )
    accounts = spark.createDataFrame([("A1", "C1")], "account_id STRING, customer_id STRING")
    data = {"silver.transactions": simulate_defect(txns, defect), "silver.accounts": accounts}
    checks = [c for c in SILVER if c.table == "silver.transactions" and c.severity is Severity.ERROR]
    failed = [c.name for c in checks if not evaluate(c, data.__getitem__, D).passed]
    expected = {
        "null_amount": "not_null:amount",
        "duplicate_key": "unique:txn_id",
        "orphan_account": "referential:account_id->silver.accounts.account_id",
        "unknown_category": "accepted_values:category",
    }[defect]
    assert failed == [expected]


def test_quarantine_reason_counts_one_defect_type_for_the_day(spark) -> None:
    quarantine = spark.createDataFrame(
        [
            ("null_amount", D),
            ("negative_amount; unknown_category", D),
            ("unknown_category", D),
            ("unknown_category", date(2026, 9, 30)),  # other day: ignored
        ],
        "failure_reason STRING, _quarantine_date DATE",
    )
    bronze = spark.createDataFrame([(D,)] * 50, "_logical_date DATE")
    tables = {"q": quarantine, "b": bronze}.__getitem__
    check = Check("q", "quarantine_reason", "unknown_category", Severity.WARN, ref_table="b", threshold=0.02)
    result = evaluate(check, tables, D)
    assert result.check_name == "quarantine_reason:unknown_category<=2%"
    assert (result.failed_rows, result.passed, result.details) == (2, False, "quarantined=2 bronze_rows=50 rate=0.0400")
    none = evaluate(Check("q", "quarantine_reason", "orphan_account", ref_table="b", threshold=0.02), tables, D)
    assert (none.failed_rows, none.passed) == (0, True)


def test_every_injected_defect_type_is_recorded_by_the_silver_suite() -> None:
    recorded = {(c.table, c.column) for c in SILVER if c.kind == "quarantine_reason"}
    injected = {("quarantine.transactions", reason) for reason in gen.TXN_DEFECT_RATES}
    injected |= {("quarantine.transactions", "superseded_by_late_update"), ("quarantine.app_events", "duplicate_row")}
    assert recorded == injected
