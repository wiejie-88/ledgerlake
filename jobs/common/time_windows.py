"""Time windows for a logical date. Pure functions, no Spark.

A logical date is a business day in Malaysia time (UTC+8). Postgres source timestamps are
naive local times; MongoDB event_ts values are UTC instants.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta

SOURCE_UTC_OFFSET = timedelta(hours=8)
TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


def local_day_end(logical_date: date) -> datetime:
    """Exclusive upper bound of ``logical_date`` as a naive Malaysia-time timestamp."""
    return datetime.combine(logical_date + timedelta(days=1), time())


def utc_day_end(logical_date: date) -> datetime:
    """Exclusive upper bound of ``logical_date`` (Malaysia day) as an aware UTC timestamp."""
    return (local_day_end(logical_date) - SOURCE_UTC_OFFSET).replace(tzinfo=UTC)


def format_ts(value: datetime) -> str:
    """Format a timestamp as ISO text (time zone dropped; callers know whether it is local or UTC)."""
    return value.strftime(TS_FORMAT)


def parse_ts(text: str | None, *, utc: bool) -> datetime | None:
    """Parse text from :func:`format_ts` (or Spark's equivalent); aware UTC if ``utc`` else naive."""
    if not text:
        return None
    value = datetime.strptime(text, TS_FORMAT)
    return value.replace(tzinfo=UTC) if utc else value
