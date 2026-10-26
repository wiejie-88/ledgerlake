"""Source definitions and incremental extraction helpers that need no Spark."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import unquote, urlsplit

from pymongo import MongoClient

from jobs.common.time_windows import format_ts


@dataclass(frozen=True)
class Entity:
    """One source table or collection extracted into bronze."""

    name: str
    source: str  # "postgres" or "mongodb"
    watermark_column: str
    source_ref: str  # recorded in bronze._source

    @property
    def bronze_table(self) -> str:
        return f"bronze.{self.name}"


ENTITIES: dict[str, Entity] = {
    e.name: e
    for e in (
        Entity("customers", "postgres", "updated_at", "postgres:source.public.customers"),
        Entity("accounts", "postgres", "updated_at", "postgres:source.public.accounts"),
        Entity("transactions", "postgres", "updated_at", "postgres:source.public.transactions"),
        Entity("cash_advances", "postgres", "updated_at", "postgres:source.public.cash_advances"),
        Entity("app_events", "mongodb", "event_ts", "mongodb:app.app_events"),
    )
}


def jdbc_options(dsn: str) -> dict[str, str]:
    """Convert ``postgresql://user:password@host:port/db`` into Spark JDBC reader options."""
    parts = urlsplit(dsn)
    if parts.scheme not in ("postgresql", "postgres") or not parts.hostname or not parts.path.strip("/"):
        raise ValueError("expected a DSN like postgresql://user:password@host:5432/dbname")
    return {
        "url": f"jdbc:postgresql://{parts.hostname}:{parts.port or 5432}/{parts.path.lstrip('/')}",
        "user": unquote(parts.username or ""),
        "password": unquote(parts.password or ""),
        "driver": "org.postgresql.Driver",
    }


def incremental_sql(table: str, column: str, low: datetime | None, high: datetime) -> str:
    """SQL selecting rows with ``low < column < high`` (no lower bound when ``low`` is None).

    ``table`` and ``column`` come from ``ENTITIES``, never from user input.
    """
    conditions = [f"{column} < TIMESTAMP '{format_ts(high)}'"]
    if low is not None:
        conditions.insert(0, f"{column} > TIMESTAMP '{format_ts(low)}'")
    return f"SELECT * FROM {table} WHERE {' AND '.join(conditions)}"


EVENT_FIELDS = ("event_id", "customer_id", "event_type", "event_ts", "device", "properties")


def event_row(doc: dict[str, Any]) -> tuple[Any, ...]:
    """Flatten a MongoDB app event into a tuple matching the bronze event schema.

    ``device`` becomes (os, app_version); ``properties`` stays a string-to-string map, so new keys
    (schema drift) land in bronze without a schema change.
    """
    device = doc.get("device") or {}
    properties = {k: None if v is None else str(v) for k, v in (doc.get("properties") or {}).items()}
    return (
        doc.get("event_id"),
        doc.get("customer_id"),
        doc.get("event_type"),
        doc.get("event_ts"),
        (device.get("os"), device.get("app_version")),
        properties,
    )


def fetch_events(uri: str, low: datetime | None, high: datetime) -> list[tuple[Any, ...]]:
    """Read app events with ``low < event_ts < high`` (aware UTC bounds) from MongoDB."""
    ts_filter: dict[str, datetime] = {"$lt": high}
    if low is not None:
        ts_filter["$gt"] = low
    with MongoClient(uri, tz_aware=True) as client:
        cursor = client["app"]["app_events"].find(
            {"event_ts": ts_filter}, projection={"_id": False}, sort=[("event_ts", 1), ("event_id", 1)]
        )
        return [event_row(doc) for doc in cursor]
