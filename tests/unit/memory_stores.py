"""In-memory stand-ins for the generator's Postgres and MongoDB stores (same filtering and ordering)."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

from generator.model import (
    COLUMNS,
    AccountRef,
    AdvanceRef,
    CustomerRef,
    DayBatch,
    PendingTxn,
    Row,
    SourceState,
)
from generator.synth import to_utc


class MemorySource:
    def __init__(self) -> None:
        self.tables: dict[str, list[Row]] = {}
        self.log: dict[date, DayBatch] = {}

    def reset(self) -> None:
        self.tables = {t: [] for t in COLUMNS}
        self.log = {}

    def is_seeded(self) -> bool:
        return bool(self.tables)

    def generated_days(self) -> set[date]:
        return set(self.log)

    def load_state(self, day: date) -> SourceState:
        start = datetime.combine(day, time())
        t = self.tables
        customers = sorted(
            (CustomerRef(r["customer_id"], r["income_band"], r["state"], r["created_at"]) for r in t["customers"]
             if r["created_at"] < start),
            key=lambda c: c.customer_id,
        )  # fmt: skip
        accounts = sorted(
            (AccountRef(r["account_id"], r["customer_id"], r["account_type"], r["status"], r["opened_at"])
             for r in t["accounts"] if r["opened_at"] < start),
            key=lambda a: a.account_id,
        )  # fmt: skip
        advances = sorted(
            (AdvanceRef(r["advance_id"], r["customer_id"], r["requested_at"], r["due_date"], r["status"])
             for r in t["cash_advances"] if r["status"] in ("active", "overdue") and r["requested_at"] < start),
            key=lambda a: a.advance_id,
        )  # fmt: skip
        pending = sorted(
            {PendingTxn(r["txn_id"], r["txn_ts"]) for r in t["transactions"]
             if r["status"] == "pending" and r["txn_ts"] < start},
            key=lambda p: p.txn_id,
        )  # fmt: skip

        def max_seq(table: str, prefix_len: int) -> int:
            return max((int(r[COLUMNS[table][0]][prefix_len:]) for r in t[table]), default=0)

        return SourceState(
            customers=customers,
            accounts=accounts,
            open_advances=advances,
            pending_txns=pending,
            next_customer_seq=max_seq("customers", 1) + 1,
            next_account_seq=max_seq("accounts", 1) + 1,
            next_advance_seq=max_seq("cash_advances", 3) + 1,
        )

    def apply(self, batch: DayBatch, kind: str) -> None:
        for table in COLUMNS:
            self.tables[table] += [dict(r) for r in batch.inserts.get(table, [])]
        for update in batch.updates:
            key_col = COLUMNS[update.table][0]
            for row in self.tables[update.table]:
                if row[key_col] == update.key:
                    row.update(update.values)
        self.log[batch.day] = batch


class MemoryEvents:
    def __init__(self) -> None:
        self.docs: list[Row] = []

    def reset(self) -> None:
        self.docs = []

    def replace_day(self, day: date, docs: list[Row]) -> None:
        start = to_utc(datetime.combine(day, time()))
        end = start + timedelta(days=1)
        self.docs = [d for d in self.docs if not start <= d["event_ts"] < end] + [dict(d) for d in docs]
