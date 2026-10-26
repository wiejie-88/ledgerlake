"""Unit tests for the pure parts of jobs/bronze/load.py."""

from __future__ import annotations

from datetime import UTC, date, datetime

from jobs.bronze.load import add_ingestion_metadata, events_dataframe
from jobs.extract.sources import event_row


def test_add_ingestion_metadata(spark) -> None:
    df = spark.createDataFrame([("C1",)], "customer_id STRING")
    ingested = datetime(2026, 10, 7, 1, 2, 3, tzinfo=UTC)
    out = add_ingestion_metadata(
        df, source="postgres:source.public.customers", batch_id="run-1", logical_date=date(2026, 10, 6),
        ingested_at=ingested,
    ).first()  # fmt: skip
    assert out["customer_id"] == "C1"
    assert (out["_source"], out["_batch_id"], out["_logical_date"]) == (
        "postgres:source.public.customers",
        "run-1",
        date(2026, 10, 6),
    )
    assert out["_ingested_at"] == datetime(2026, 10, 7, 1, 2, 3)  # session time zone is UTC


def test_events_dataframe_keeps_drifted_properties_as_map(spark) -> None:
    ts = datetime(2026, 9, 14, 2, 0, tzinfo=UTC)
    docs = [
        {"event_id": "e1", "customer_id": "C1", "event_type": "login", "event_ts": ts,
         "device": {"os": "ios", "app_version": "4.5.0"}, "properties": {}},
        {"event_id": "e2", "customer_id": "C1", "event_type": "view_offer", "event_ts": ts,
         "device": {"os": "android", "app_version": "4.5.1"},
         "properties": {"offer_id": "O1", "referral_source": "organic"}},
    ]  # fmt: skip
    rows = {r["event_id"]: r for r in events_dataframe(spark, [event_row(d) for d in docs]).collect()}
    assert rows["e1"]["device"]["os"] == "ios"
    assert rows["e1"]["properties"] == {}
    assert rows["e2"]["properties"] == {"offer_id": "O1", "referral_source": "organic"}
