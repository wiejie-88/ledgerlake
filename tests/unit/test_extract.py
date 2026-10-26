"""Unit tests for the Spark-free extract helpers and time windows."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from jobs.common.time_windows import format_ts, local_day_end, parse_ts, utc_day_end
from jobs.extract.sources import ENTITIES, event_row, incremental_sql, jdbc_options


def test_day_bounds_are_malaysia_days() -> None:
    assert local_day_end(date(2026, 9, 29)) == datetime(2026, 9, 30)
    assert utc_day_end(date(2026, 9, 29)) == datetime(2026, 9, 29, 16, 0, tzinfo=UTC)


def test_timestamp_text_round_trip() -> None:
    value = datetime(2026, 9, 29, 23, 59, 59, 123456)
    assert parse_ts(format_ts(value), utc=False) == value
    assert parse_ts(format_ts(value), utc=True) == value.replace(tzinfo=UTC)
    assert parse_ts(None, utc=True) is None


def test_jdbc_options_from_dsn() -> None:
    options = jdbc_options("postgresql://ledgerlake:p%40ss@postgres:5432/source")
    assert options == {
        "url": "jdbc:postgresql://postgres:5432/source",
        "user": "ledgerlake",
        "password": "p@ss",
        "driver": "org.postgresql.Driver",
    }
    with pytest.raises(ValueError):
        jdbc_options("mysql://u:p@h/db")


def test_incremental_sql_first_load_has_no_lower_bound() -> None:
    sql = incremental_sql("transactions", "updated_at", None, datetime(2026, 9, 30))
    assert sql == "SELECT * FROM transactions WHERE updated_at < TIMESTAMP '2026-09-30T00:00:00.000000'"


def test_incremental_sql_window() -> None:
    sql = incremental_sql("customers", "updated_at", datetime(2026, 9, 29, 23, 58, 1), datetime(2026, 10, 1))
    assert sql == (
        "SELECT * FROM customers WHERE updated_at > TIMESTAMP '2026-09-29T23:58:01.000000'"
        " AND updated_at < TIMESTAMP '2026-10-01T00:00:00.000000'"
    )


def test_event_row_flattens_and_keeps_drifted_properties() -> None:
    ts = datetime(2026, 9, 14, 2, 0, tzinfo=UTC)
    doc = {
        "event_id": "e1",
        "customer_id": "C000001",
        "event_type": "view_offer",
        "event_ts": ts,
        "device": {"os": "ios", "app_version": "4.5.1"},
        "properties": {"offer_id": "OFF-ADV-050", "referral_source": "organic"},
    }
    assert event_row(doc) == (
        "e1",
        "C000001",
        "view_offer",
        ts,
        ("ios", "4.5.1"),
        {"offer_id": "OFF-ADV-050", "referral_source": "organic"},
    )
    assert event_row({"event_id": "e2"})[4:] == ((None, None), {})


def test_entities_cover_all_bronze_tables() -> None:
    assert sorted(e.bronze_table for e in ENTITIES.values()) == [
        "bronze.accounts",
        "bronze.app_events",
        "bronze.cash_advances",
        "bronze.customers",
        "bronze.transactions",
    ]
