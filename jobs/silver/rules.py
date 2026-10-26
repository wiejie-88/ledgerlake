"""Silver data contract: accepted values and the validation rules that send rows to quarantine.

Each rule is (failure_reason, condition). The condition is True when the row FAILS the rule.
Conditions are written so that NULL inputs fail rather than slip through.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

CATEGORIES = (
    "groceries", "food_and_beverage", "transport", "utilities", "telco", "shopping",
    "entertainment", "health", "education", "transfer", "salary",
)  # fmt: skip
TXN_STATUSES = ("completed", "pending", "failed", "reversed")
DIRECTIONS = ("debit", "credit")
ACCOUNT_TYPES = ("spending", "savings")
ACCOUNT_STATUSES = ("active", "dormant", "closed")
INCOME_BANDS = ("below_2k", "2k_4k", "4k_6k", "6k_10k", "above_10k")
ADVANCE_STATUSES = ("active", "repaid", "overdue")
EVENT_TYPES = ("login", "view_offer", "click_offer", "apply_offer")

# Column added by run.py: True when the row's parent key exists in the parent silver table.
HAS_PARENT = "_has_parent"

Rule = tuple[str, Column]


def _missing(column: str) -> Column:
    return F.col(column).isNull() | (F.trim(F.col(column)) == "")


def _not_in(column: str, accepted: tuple[str, ...]) -> Column:
    return ~F.coalesce(F.col(column).isin(*accepted), F.lit(False))


def _amount_rules() -> list[Rule]:
    return [
        ("null_amount", F.col("amount").isNull()),
        ("negative_amount", F.col("amount") < 0),
        ("zero_amount", F.col("amount") == 0),
    ]


def customer_rules() -> list[Rule]:
    """Rules for bronze.customers."""
    return [("missing_key", _missing("customer_id")), ("invalid_income_band", _not_in("income_band", INCOME_BANDS))]


def account_rules() -> list[Rule]:
    """Rules for bronze.accounts (parent: customers)."""
    return [
        ("missing_key", _missing("account_id")),
        ("invalid_account_type", _not_in("account_type", ACCOUNT_TYPES)),
        ("invalid_status", _not_in("status", ACCOUNT_STATUSES)),
        ("orphan_customer", ~F.col(HAS_PARENT)),
    ]


def transaction_rules() -> list[Rule]:
    """Rules for bronze.transactions (parent: accounts)."""
    return [
        ("missing_key", _missing("txn_id")),
        *_amount_rules(),
        ("unknown_category", _not_in("category", CATEGORIES)),
        ("invalid_direction", _not_in("direction", DIRECTIONS)),
        ("invalid_status", _not_in("status", TXN_STATUSES)),
        ("orphan_account", ~F.col(HAS_PARENT)),
    ]


def cash_advance_rules() -> list[Rule]:
    """Rules for bronze.cash_advances (parent: customers)."""
    return [
        ("missing_key", _missing("advance_id")),
        *_amount_rules(),
        ("invalid_status", _not_in("status", ADVANCE_STATUSES)),
        ("orphan_customer", ~F.col(HAS_PARENT)),
    ]


def event_rules() -> list[Rule]:
    """Rules for bronze.app_events (parent: customers)."""
    return [
        ("missing_key", _missing("event_id")),
        ("missing_event_ts", F.col("event_ts").isNull()),
        ("unknown_event_type", _not_in("event_type", EVENT_TYPES)),
        ("orphan_customer", ~F.col(HAS_PARENT)),
    ]
