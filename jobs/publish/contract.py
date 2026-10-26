"""What gets published to the serving database: source table and column order per serving table.

The column lists must match serving/01_tables.sql (tests/unit/test_serving_contract.py checks this).
``as_of_date`` is added by the publish job from the logical date.
"""

from __future__ import annotations

SERVING_SCHEMA = "serving"

MART_COLUMNS: dict[str, tuple[str, ...]] = {
    "mart_daily_spend": (
        "date_key", "txn_date", "category", "total_spend", "avg_spend", "txn_count", "customer_count",
    ),
    "mart_advance_repayment": (
        "cohort_month", "advance_count", "amount_total", "repaid_count", "overdue_count", "active_count",
        "due_count", "due_repaid_count", "late_repaid_count", "avg_days_late", "repayment_rate",
        "repayment_rate_due",
    ),
    "mart_customer_features": (
        "customer_id", "month", "spend_total", "spend_txn_count", "spend_days", "daily_spend_stddev",
        "daily_spend_cv", "salary_credit_count", "salary_total", "income_regularity_3m", "advance_count",
        "advance_amount",
    ),
}  # fmt: skip

PROFILE_COLUMNS: tuple[str, ...] = (
    "customer_id", "full_name", "email", "phone", "date_of_birth", "age_band", "state", "income_band",
    "customer_since",
)  # fmt: skip


def serving_columns() -> dict[str, tuple[str, ...]]:
    """Every published table with its full column order (including as_of_date)."""
    tables = {**MART_COLUMNS, "customer_profile": PROFILE_COLUMNS}
    return {name: (*columns, "as_of_date") for name, columns in tables.items()}
