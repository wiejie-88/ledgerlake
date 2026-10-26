"""Data types shared by the generator's planning (pure) and storage (IO) layers."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

# Postgres source tables and their column order. The first column is the business key.
COLUMNS: dict[str, tuple[str, ...]] = {
    "customers": (
        "customer_id", "full_name", "email", "phone", "date_of_birth",
        "state", "income_band", "created_at", "updated_at",
    ),
    "accounts": ("account_id", "customer_id", "account_type", "status", "opened_at", "updated_at"),
    "transactions": (
        "txn_id", "account_id", "txn_ts", "amount", "direction",
        "merchant_name", "category", "channel", "status", "updated_at",
    ),
    "cash_advances": (
        "advance_id", "customer_id", "amount", "requested_at", "due_date", "repaid_at", "status", "updated_at",
    ),
}  # fmt: skip
EVENTS = "app_events"

Row = dict[str, Any]


@dataclass(frozen=True)
class CustomerRef:
    customer_id: str
    income_band: str
    state: str
    created_at: datetime


@dataclass(frozen=True)
class AccountRef:
    account_id: str
    customer_id: str
    account_type: str
    status: str
    opened_at: datetime


@dataclass(frozen=True)
class AdvanceRef:
    advance_id: str
    customer_id: str
    requested_at: datetime
    due_date: date
    status: str


@dataclass(frozen=True)
class PendingTxn:
    txn_id: str
    txn_ts: datetime


@dataclass(frozen=True)
class SourceState:
    """What the planner needs to know about the source before generating a day.

    Lists are sorted by business key so planning is deterministic.
    """

    customers: list[CustomerRef]
    accounts: list[AccountRef]
    open_advances: list[AdvanceRef]
    pending_txns: list[PendingTxn]
    next_customer_seq: int
    next_account_seq: int
    next_advance_seq: int


@dataclass(frozen=True)
class Update:
    """Set ``values`` on every row of ``table`` whose business key equals ``key``."""

    table: str
    key: str
    values: dict[str, Any]


@dataclass
class DayBatch:
    """Everything generated for one day: inserts per table, updates, and injected-defect counts."""

    day: date
    inserts: dict[str, list[Row]] = field(default_factory=dict)
    updates: list[Update] = field(default_factory=list)
    defects: Counter[str] = field(default_factory=Counter)

    def inserted_counts(self) -> dict[str, int]:
        """Rows inserted per table."""
        return {table: len(rows) for table, rows in self.inserts.items()}

    def updated_counts(self) -> dict[str, int]:
        """Update statements per table."""
        return dict(Counter(u.table for u in self.updates))
