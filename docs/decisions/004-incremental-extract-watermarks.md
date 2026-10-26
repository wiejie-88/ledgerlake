# ADR 004: Incremental extraction windows and watermarks

**Status:** accepted (D1-EVE, 2026-10-26)

## Context
SPEC F2 asks for incremental extraction by watermark: `updated_at` for Postgres and `event_ts` for MongoDB. SPEC §5.2 asks that re-running a logical date replaces that date's bronze partition, and F7 asks for idempotent runs and backfills.

The classic pattern reads `last_value` from a watermark table, extracts newer rows, then advances the watermark. That pattern isn't idempotent. A re-run of the same date starts from the already-advanced watermark, finds nothing, and the partition overwrite then deletes the data the first run loaded.

## Decision
For logical date D (a Malaysia business day), each entity is extracted with `low < watermark_column < high`:
- **high** is the end of day D. For Postgres that's the naive local timestamp D+1 00:00. For MongoDB it's the same instant in UTC.
- **low** is the highest watermark value already landed in that entity's bronze partitions **before** D. If there are none, there's no lower bound, which makes the first run a full initial load.

The extracted rows replace partition D using Iceberg `overwrite(_logical_date = D)`. This also clears the partition when the new extract is empty.

After each entity loads, `ops.watermarks` (source, entity, last_value, updated_at) is upserted with `MERGE INTO`. `last_value` is the table's overall maximum, so a backfill of an older date never moves it backwards. It's used for observability and freshness checks, not as the lower bound.

Bronze keeps raw source types:
- Postgres timestamps stay naive (`TIMESTAMP_NTZ`) via the JDBC option `preferTimestampNTZ`.
- MongoDB `properties` is stored as `map<string,string>`, so the day-45 `referral_source` key lands without a schema change. Silver promotes it to a column using Iceberg schema evolution in D2-AM.
- MongoDB is read with pymongo in the Spark driver and needs no extra JAR. Daily volume is about 900 documents, and the initial load about 50k.

## Alternatives considered
- **Lower bound from `ops.watermarks`:** not idempotent on re-runs, as explained above.
- **Strict calendar window [D, D+1) with no watermark:** the first load would miss base customers created before the history window, and the design wouldn't be watermark-driven as SPEC asks.
- **MongoDB Spark connector:** an extra JAR and version to pin, which is unnecessary at this volume.

## Consequences
- Re-running D with an unchanged source gives identical row counts (the D1-EVE "Done when").
- Re-running D *after* later source changes can return fewer rows. A row updated again on D+1 no longer has an `updated_at` inside D's window. This is inherent to non-CDC extraction. The row isn't lost, because it lands in D+1's partition.
- Backfilling a date earlier than already-loaded partitions can land rows already present in later partitions. Silver deduplicates on the business key, keeping the latest `updated_at` (SPEC §5.3).
- The first date run carries the full history in one partition.
