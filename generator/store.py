"""Postgres and MongoDB storage for the generator (the only module that does IO)."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from typing import Any

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch, execute_values
from pymongo import ASCENDING, MongoClient

from generator import reference as ref
from generator.model import (
    COLUMNS,
    EVENTS,
    AccountRef,
    AdvanceRef,
    CustomerRef,
    DayBatch,
    PendingTxn,
    Row,
    SourceState,
)
from generator.synth import to_utc

# Timestamps are local Malaysia time without a time zone (silver converts them to UTC).
# transactions has no primary key or foreign key on purpose: the source carries injected
# duplicate rows and orphan account references.
SOURCE_DDL = """
DROP TABLE IF EXISTS transactions, cash_advances, accounts, customers, generator_log;

CREATE TABLE customers (
    customer_id   text PRIMARY KEY,
    full_name     text NOT NULL,
    email         text,
    phone         text,
    date_of_birth date,
    state         text,
    income_band   text,
    created_at    timestamp NOT NULL,
    updated_at    timestamp NOT NULL
);
CREATE TABLE accounts (
    account_id   text PRIMARY KEY,
    customer_id  text NOT NULL REFERENCES customers (customer_id),
    account_type text NOT NULL,
    status       text NOT NULL,
    opened_at    timestamp NOT NULL,
    updated_at   timestamp NOT NULL
);
CREATE TABLE transactions (
    txn_id        text NOT NULL,
    account_id    text NOT NULL,
    txn_ts        timestamp NOT NULL,
    amount        numeric(12, 2),
    direction     text,
    merchant_name text,
    category      text,
    channel       text,
    status        text,
    updated_at    timestamp NOT NULL
);
CREATE TABLE cash_advances (
    advance_id   text PRIMARY KEY,
    customer_id  text NOT NULL REFERENCES customers (customer_id),
    amount       numeric(12, 2),
    requested_at timestamp NOT NULL,
    due_date     date NOT NULL,
    repaid_at    timestamp,
    status       text NOT NULL,
    updated_at   timestamp NOT NULL
);
CREATE INDEX customers_updated_at_idx ON customers (updated_at);
CREATE INDEX accounts_updated_at_idx ON accounts (updated_at);
CREATE INDEX transactions_updated_at_idx ON transactions (updated_at);
CREATE INDEX transactions_txn_id_idx ON transactions (txn_id);
CREATE INDEX transactions_pending_idx ON transactions (txn_id) WHERE status = 'pending';
CREATE INDEX cash_advances_updated_at_idx ON cash_advances (updated_at);

-- One row per generated day; makes `tick` idempotent and records what was injected.
CREATE TABLE generator_log (
    run_date     date PRIMARY KEY,
    kind         text NOT NULL,
    inserted     jsonb NOT NULL,
    updated      jsonb NOT NULL,
    defects      jsonb NOT NULL,
    generated_at timestamptz NOT NULL DEFAULT now()
);
"""


class PostgresSource:
    """The `source` Postgres database."""

    def __init__(self, dsn: str) -> None:
        self.conn = psycopg2.connect(dsn)

    def close(self) -> None:
        """Close the connection."""
        self.conn.close()

    def reset(self) -> None:
        """Drop and recreate all source tables."""
        with self.conn, self.conn.cursor() as cur:
            cur.execute(SOURCE_DDL)

    def is_seeded(self) -> bool:
        """True if the source tables exist."""
        with self.conn, self.conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.generator_log') IS NOT NULL")
            return bool(cur.fetchone()[0])

    def generated_days(self) -> set[date]:
        """Days already present in generator_log."""
        with self.conn, self.conn.cursor() as cur:
            cur.execute("SELECT run_date FROM generator_log")
            return {row[0] for row in cur.fetchall()}

    def load_state(self, day: date) -> SourceState:
        """Read what the planner needs, as of the start of ``day``, in a deterministic order."""
        start = datetime.combine(day, time())
        with self.conn, self.conn.cursor() as cur:
            cur.execute(
                "SELECT customer_id, income_band, state, created_at FROM customers"
                " WHERE created_at < %s ORDER BY customer_id",
                (start,),
            )
            customers = [CustomerRef(*r) for r in cur.fetchall()]
            cur.execute(
                "SELECT account_id, customer_id, account_type, status, opened_at FROM accounts"
                " WHERE opened_at < %s ORDER BY account_id",
                (start,),
            )
            accounts = [AccountRef(*r) for r in cur.fetchall()]
            cur.execute(
                "SELECT advance_id, customer_id, requested_at, due_date, status FROM cash_advances"
                " WHERE status IN ('active', 'overdue') AND requested_at < %s ORDER BY advance_id",
                (start,),
            )
            advances = [AdvanceRef(*r) for r in cur.fetchall()]
            cur.execute(
                "SELECT DISTINCT txn_id, txn_ts FROM transactions"
                " WHERE status = 'pending' AND txn_ts < %s ORDER BY txn_id",
                (start,),
            )
            pending = [PendingTxn(*r) for r in cur.fetchall()]
            cur.execute(
                "SELECT (SELECT coalesce(max(substr(customer_id, 2)::int), 0) FROM customers),"
                " (SELECT coalesce(max(substr(account_id, 2)::int), 0) FROM accounts),"
                " (SELECT coalesce(max(substr(advance_id, 4)::int), 0) FROM cash_advances)"
            )
            max_customer, max_account, max_advance = cur.fetchone()
        return SourceState(
            customers=customers,
            accounts=accounts,
            open_advances=advances,
            pending_txns=pending,
            next_customer_seq=max_customer + 1,
            next_account_seq=max_account + 1,
            next_advance_seq=max_advance + 1,
        )

    def apply(self, batch: DayBatch, kind: str) -> None:
        """Write a day's inserts, updates and generator_log row in one transaction."""
        with self.conn, self.conn.cursor() as cur:
            for table, columns in COLUMNS.items():
                rows = batch.inserts.get(table, [])
                if rows:
                    query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                        sql.Identifier(table), sql.SQL(", ").join(map(sql.Identifier, columns))
                    )
                    execute_values(cur, query, [tuple(r[c] for c in columns) for r in rows], page_size=5_000)

            grouped: dict[tuple[str, tuple[str, ...]], list[tuple[Any, ...]]] = defaultdict(list)
            for update in batch.updates:
                cols = tuple(update.values)
                grouped[(update.table, cols)].append((*update.values.values(), update.key))
            for (table, cols), params in grouped.items():
                query = sql.SQL("UPDATE {} SET {} WHERE {} = %s").format(
                    sql.Identifier(table),
                    sql.SQL(", ").join(sql.SQL("{} = %s").format(sql.Identifier(c)) for c in cols),
                    sql.Identifier(COLUMNS[table][0]),
                )
                execute_batch(cur, query, params, page_size=1_000)

            cur.execute(
                "INSERT INTO generator_log (run_date, kind, inserted, updated, defects) VALUES (%s, %s, %s, %s, %s)",
                (
                    batch.day,
                    kind,
                    json.dumps(batch.inserted_counts()),
                    json.dumps(batch.updated_counts()),
                    json.dumps(dict(batch.defects)),
                ),
            )

    def measure(self) -> dict[str, Any]:
        """Count rows and defects directly in the database, independently of what the generator reports."""
        with self.conn, self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  (SELECT count(*) FROM customers),
                  (SELECT count(*) FROM accounts),
                  (SELECT count(*) FROM transactions),
                  (SELECT count(*) FROM cash_advances),
                  (SELECT count(*) - count(DISTINCT txn_id) FROM transactions),
                  (SELECT count(*) FROM transactions WHERE amount IS NULL),
                  (SELECT count(*) FROM transactions WHERE amount < 0),
                  (SELECT count(*) FROM transactions WHERE category <> ALL (%s)),
                  (SELECT count(*) FROM transactions t
                     WHERE NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = t.account_id)),
                  (SELECT count(*) FROM transactions WHERE updated_at::date > txn_ts::date),
                  (SELECT md5(string_agg(t::text, '|' ORDER BY t::text)) FROM transactions t)
                """,
                (list(ref.CATEGORIES),),
            )
            r = cur.fetchone()
        return {
            "table=customers rows": r[0],
            "table=accounts rows": r[1],
            "table=transactions rows": r[2],
            "table=cash_advances rows": r[3],
            "transactions.duplicate_row": r[4],
            "transactions.null_amount": r[5],
            "transactions.negative_amount": r[6],
            "transactions.unknown_category": r[7],
            "transactions.orphan_account": r[8],
            "transactions.rows_updated_after_txn_day": r[9],
            "transactions.fingerprint_md5": r[10],
        }


class MongoEvents:
    """The `app.app_events` MongoDB collection."""

    def __init__(self, uri: str) -> None:
        self.client: MongoClient = MongoClient(uri, tz_aware=True)
        self.coll = self.client["app"][EVENTS]

    def close(self) -> None:
        """Close the client."""
        self.client.close()

    def reset(self) -> None:
        """Drop and recreate the collection with its indexes."""
        self.coll.drop()
        self.coll.database.create_collection(EVENTS)
        self.coll.create_index([("event_ts", ASCENDING)])
        self.coll.create_index([("event_id", ASCENDING)])

    def replace_day(self, day: date, docs: list[Row]) -> None:
        """Replace all events of local day ``day`` with ``docs`` (idempotent per day)."""
        start = to_utc(datetime.combine(day, time()))
        self.coll.delete_many({"event_ts": {"$gte": start, "$lt": start + timedelta(days=1)}})
        if docs:
            # insert_many adds _id to each document, so insert copies.
            self.coll.insert_many([dict(d) for d in docs], ordered=False)

    def measure(self) -> dict[str, Any]:
        """Count events, duplicate event_ids and schema-drifted documents directly in MongoDB."""
        duplicates = next(
            self.coll.aggregate(
                [
                    {"$group": {"_id": "$event_id", "n": {"$sum": 1}}},
                    {"$match": {"n": {"$gt": 1}}},
                    {"$group": {"_id": None, "extra": {"$sum": {"$subtract": ["$n", 1]}}}},
                ]
            ),
            {"extra": 0},
        )["extra"]
        drift_filter = {"properties.referral_source": {"$exists": True}}
        first_drift = self.coll.find_one(drift_filter, sort=[("event_ts", ASCENDING)])
        return {
            "collection=app_events docs": self.coll.count_documents({}),
            "app_events.duplicate_row": duplicates,
            "app_events.with_referral_source": self.coll.count_documents(drift_filter),
            "app_events.first_referral_source_ts": first_drift["event_ts"].isoformat() if first_drift else None,
        }
