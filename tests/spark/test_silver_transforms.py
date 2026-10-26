"""Unit tests for jobs/silver/transforms.py and jobs/silver/rules.py."""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from decimal import Decimal

from pyspark.sql import functions as F

from jobs.silver import rules
from jobs.silver.transforms import (
    FAILURE_REASON,
    age_band,
    conform_customers,
    conform_events,
    conform_transactions,
    evolvable_keys,
    flag_failures,
    late_superseded,
    local_to_utc,
    property_keys,
    split_duplicates,
    with_parent_flag,
)

TXN_SCHEMA = (
    "txn_id STRING, account_id STRING, txn_ts TIMESTAMP_NTZ, amount DECIMAL(12,2), direction STRING, "
    "merchant_name STRING, category STRING, channel STRING, status STRING, updated_at TIMESTAMP_NTZ, "
    "_ingested_at TIMESTAMP, _logical_date DATE"
)
TS = datetime(2026, 9, 29, 10, 0)
INGESTED = datetime(2026, 9, 30, 1, 0, tzinfo=UTC)


def _txn(txn_id: str, **overrides: object) -> tuple:
    row = {
        "txn_id": txn_id,
        "account_id": "A0000001",
        "txn_ts": TS,
        "amount": Decimal("25.00"),
        "direction": "debit",
        "merchant_name": "Mydin",
        "category": "groceries",
        "channel": "card",
        "status": "completed",
        "updated_at": TS,
        "_ingested_at": INGESTED,
        "_logical_date": date(2026, 9, 29),
    }
    row.update(overrides)
    return tuple(row.values())


def test_local_to_utc_subtracts_eight_hours(spark) -> None:
    df = spark.createDataFrame([(datetime(2026, 9, 29, 7, 30),)], "ts TIMESTAMP_NTZ")
    assert df.select(local_to_utc("ts").alias("u")).first()["u"] == datetime(2026, 9, 28, 23, 30)


def test_transaction_rules_flag_each_defect(spark) -> None:
    rows = [
        _txn("ok"),
        _txn("null", amount=None),
        _txn("neg", amount=Decimal("-5.00")),
        _txn("cat", category="crypto_exchange"),
        _txn("cat_null", category=None),
        _txn("orphan", account_id="A9123456"),
        _txn("two", amount=Decimal("-1.00"), category="MISC"),
    ]
    df = spark.createDataFrame(rows, TXN_SCHEMA)
    accounts = spark.createDataFrame([("A0000001",)], "account_id STRING")
    flagged = flag_failures(with_parent_flag(df, "account_id", accounts, "account_id"), rules.transaction_rules())
    reasons = {r["txn_id"]: r[FAILURE_REASON] for r in flagged.collect()}
    assert reasons == {
        "ok": None,
        "null": "null_amount",
        "neg": "negative_amount",
        "cat": "unknown_category",
        "cat_null": "unknown_category",
        "orphan": "orphan_account",
        "two": "negative_amount; unknown_category",
    }


def test_split_duplicates_keeps_latest_and_labels_dropped(spark) -> None:
    later = datetime(2026, 9, 29, 12, 0)
    df = spark.createDataFrame(
        [_txn("T1"), _txn("T1"), _txn("T2", status="pending"), _txn("T2", updated_at=later), _txn("T3")], TXN_SCHEMA
    )
    kept, dropped = split_duplicates(df, "txn_id")
    assert sorted((r["txn_id"], r["updated_at"]) for r in kept.collect()) == [("T1", TS), ("T2", later), ("T3", TS)]
    assert sorted((r["txn_id"], r[FAILURE_REASON]) for r in dropped.collect()) == [
        ("T1", "duplicate_row"),
        ("T2", "superseded_in_batch"),
    ]


def test_late_superseded_only_flags_updates_on_a_later_day(spark) -> None:
    earlier = spark.createDataFrame(
        [_txn("LATE", status="pending"), _txn("LATE", status="pending"), _txn("SAMEDAY", status="pending")],
        TXN_SCHEMA,
    )
    new = spark.createDataFrame(
        [
            _txn("LATE", updated_at=datetime(2026, 10, 1, 9, 0)),  # settled two days later
            _txn("SAMEDAY", updated_at=datetime(2026, 9, 29, 18, 0)),  # updated the same day
            _txn("NEW"),
        ],
        TXN_SCHEMA,
    )
    out = late_superseded(new, earlier, "txn_id", "txn_ts").collect()
    assert [(r["txn_id"], r["status"], r[FAILURE_REASON]) for r in out] == [
        ("LATE", "pending", "superseded_by_late_update")
    ]


def test_age_band(spark) -> None:
    as_of = date(2026, 9, 29)
    dobs = [(date(2008, 9, 30),), (date(2008, 9, 29),), (date(1990, 1, 1),), (date(1950, 6, 1),), (None,)]
    df = spark.createDataFrame(dobs, "dob DATE")
    assert [r["b"] for r in df.select(age_band("dob", as_of).alias("b")).collect()] == [
        "under_18",
        "18_24",
        "35_44",
        "65_plus",
        "unknown",
    ]


def test_conform_customers_hashes_pii_and_splits_names(spark) -> None:
    df = spark.createDataFrame(
        [("C000001", "Siti binti Ismail", " Siti.Ismail12@Example.com ", "+60 12-345 6789", date(1990, 5, 1),
          "Selangor", "2k_4k", TS, TS)],
        "customer_id STRING, full_name STRING, email STRING, phone STRING, date_of_birth DATE, state STRING, "
        "income_band STRING, created_at TIMESTAMP_NTZ, updated_at TIMESTAMP_NTZ",
    )  # fmt: skip
    customers, pii = conform_customers(df, "salt", date(2026, 9, 29))
    row = customers.first()
    assert row["email_hash"] == hashlib.sha256(b"saltsiti.ismail12@example.com").hexdigest()
    assert row["phone_hash"] == hashlib.sha256(b"salt60123456789").hexdigest()
    assert row["age_band"] == "35_44"
    assert row["updated_at"] == datetime(2026, 9, 29, 2, 0)
    assert not {"full_name", "email", "phone", "date_of_birth"} & set(customers.columns)
    assert pii.first()["full_name"] == "Siti binti Ismail"


def test_conform_transactions_types_and_business_date(spark) -> None:
    late_evening = datetime(2026, 9, 29, 23, 30)
    df = spark.createDataFrame([_txn("T1", txn_ts=late_evening, updated_at=late_evening)], TXN_SCHEMA)
    row = conform_transactions(df, date(2026, 9, 29)).first()
    assert row["txn_ts"] == datetime(2026, 9, 29, 15, 30)  # UTC
    assert row["txn_date"] == date(2026, 9, 29)  # Malaysia business date
    assert row["amount"] == Decimal("25.00")


def test_event_properties_and_evolution(spark) -> None:
    schema = (
        "event_id STRING, customer_id STRING, event_type STRING, event_ts TIMESTAMP, "
        "device STRUCT<os: STRING, app_version: STRING>, properties MAP<STRING, STRING>"
    )
    ts = datetime(2026, 9, 13, 17, 0, tzinfo=UTC)  # 2026-09-14 01:00 in Malaysia
    df = spark.createDataFrame(
        [
            ("e1", "C1", "login", ts, ("ios", "4.5.0"), {}),
            ("e2", "C1", "view_offer", ts, ("android", "4.5.1"), {"offer_id": "O1", "referral_source": "organic"}),
            ("e3", "C1", "view_offer", ts, ("android", "4.5.1"), {"Bad-Key": "x"}),
        ],
        schema,
    )
    keys = property_keys(df)
    assert keys == ["Bad-Key", "offer_id", "referral_source"]
    base = conform_events(df.limit(0), date(2026, 9, 14), []).columns
    assert evolvable_keys(keys, base) == ["referral_source"]
    rows = {r["event_id"]: r for r in conform_events(df, date(2026, 9, 14), ["referral_source"]).collect()}
    assert rows["e2"]["offer_id"] == "O1" and rows["e2"]["referral_source"] == "organic"
    assert rows["e1"]["referral_source"] is None
    assert rows["e1"]["event_date"] == date(2026, 9, 14)
    assert rows["e1"]["os"] == "ios"


def test_parent_flag_handles_missing_parent_table(spark) -> None:
    df = spark.createDataFrame([("A1",)], "account_id STRING")
    empty = spark.createDataFrame([], "account_id STRING")
    assert with_parent_flag(df, "account_id", empty, "account_id").select(F.col(rules.HAS_PARENT)).first()[0] is False
