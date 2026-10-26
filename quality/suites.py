"""Check suites per layer. Accepted values come from the silver data contract (jobs/silver/rules.py)."""

from __future__ import annotations

from jobs.silver import rules
from quality.checks import Check
from quality.gate import Severity

WARN = Severity.WARN


def _keys(table: str, key: str) -> list[Check]:
    return [Check(table, "not_null", key), Check(table, "unique", key)]


SILVER: list[Check] = [
    *_keys("silver.customers", "customer_id"),
    Check("silver.customers", "accepted_values", "income_band", values=rules.INCOME_BANDS),
    *_keys("silver.customers_pii", "customer_id"),
    *_keys("silver.accounts", "account_id"),
    Check("silver.accounts", "not_null", "customer_id"),
    Check("silver.accounts", "referential", "customer_id", ref_table="silver.customers", ref_column="customer_id"),
    Check("silver.accounts", "accepted_values", "account_type", values=rules.ACCOUNT_TYPES),
    Check("silver.accounts", "accepted_values", "status", values=rules.ACCOUNT_STATUSES),
    *_keys("silver.transactions", "txn_id"),
    Check("silver.transactions", "not_null", "account_id"),
    Check("silver.transactions", "not_null", "amount"),
    Check("silver.transactions", "positive", "amount"),
    Check("silver.transactions", "referential", "account_id", ref_table="silver.accounts", ref_column="account_id"),
    Check("silver.transactions", "accepted_values", "category", values=rules.CATEGORIES),
    Check("silver.transactions", "accepted_values", "status", values=rules.TXN_STATUSES),
    Check("silver.transactions", "accepted_values", "direction", values=rules.DIRECTIONS),
    Check("silver.transactions", "freshness", "txn_date", severity=WARN, max_lag_days=1),
    Check(
        "quarantine.transactions",
        "quarantine_rate",
        "failure_reason",
        severity=WARN,
        ref_table="bronze.transactions",
        threshold=0.05,
    ),
    *_keys("silver.cash_advances", "advance_id"),
    Check("silver.cash_advances", "not_null", "amount"),
    Check("silver.cash_advances", "positive", "amount"),
    Check("silver.cash_advances", "referential", "customer_id", ref_table="silver.customers", ref_column="customer_id"),
    Check("silver.cash_advances", "accepted_values", "status", values=rules.ADVANCE_STATUSES),
    *_keys("silver.app_events", "event_id"),
    Check("silver.app_events", "referential", "customer_id", ref_table="silver.customers", ref_column="customer_id"),
    Check("silver.app_events", "accepted_values", "event_type", values=rules.EVENT_TYPES),
]

SUITES: dict[str, list[Check]] = {"silver": SILVER}
