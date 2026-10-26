# ADR 010: Time-travel audit demo and Iceberg table maintenance

**Status:** accepted (D3-MID, 2026-10-26)

## Context
SPEC should-haves 6–8 ask for:
- a schema-evolution demo;
- a time-travel demo, querying `fact_transactions` as of an earlier snapshot for an audit use case;
- a maintenance task that expires snapshots and compacts files, recording file counts before and after.

D3-MID is done when both run from documented `make` commands.

The lake had accumulated many small files:
- Spark's default 200 shuffle partitions produce many files per MERGE (`silver.transactions`: 841 current data files for about 105k rows).
- Copy-on-write MERGE rewrites whole tables, so old snapshots still reference thousands of superseded files.

## Decision
- **Time travel as an audit** (`make demo-time-travel`, `jobs/demo/time_travel.py`). The question it answers: *what did gold report for business date D when it was built, and what has been restated since?*
  - The snapshot "as built for D" is the newest snapshot whose `max(_logical_date)` equals D. Choosing by logical date rather than wall-clock time makes the demo independent of when runs happened.
  - The table is read `VERSION AS OF <id>` and `TIMESTAMP AS OF <committed_at>`, and the two must agree.
  - D's rows, completed spend and pending count are compared then and now, and status changes between the snapshot and now are listed.
  - The comparison logic is in pure functions (`jobs/demo/audit.py`) with unit tests.
- **Schema-evolution demo** (`make demo-schema-evolution`). It reads `silver.app_events.metadata_log_entries` to show the schema id change, the current columns, and old rows reading NULL for `referral_source`. It relies on metadata that maintenance doesn't remove, so it still works after snapshots expire.
- **Maintenance** (`make maintenance`, `jobs/maintenance/run.py`). For every lake table except `ops.maintenance_log` itself:
  1. `rewrite_data_files` (bin-pack, `min-input-files=2`);
  2. `rewrite_manifests`;
  3. `expire_snapshots(older_than => now, retain_last => 5)`. Expiry physically deletes files no snapshot references any more.
- **Measured before and after** for each table, written to `lake.ops.maintenance_log`, with a summary row in `ops.pipeline_runs`:
  - snapshots;
  - data files in the current snapshot;
  - data files referenced by any snapshot (`all_data_files`);
  - objects physically present in MinIO under `data/` and `metadata/` (boto3 listing).

  The physical count independently confirms that expiry deleted files and left no orphans.
- **Retention:** keep the last 5 snapshots per table (`make maintenance retain=N` to change). With one gold run per day, that's about 5 business days of time travel. The time-travel demo needs the previous day's snapshot, so 5 leaves margin.

## Alternatives considered
- **Age-based retention** (for example 7 days): the usual production policy, and supported with `--older-than-hours`. But every snapshot here is from the same day, so it would expire nothing and the demo would show no effect.
- **`remove_orphan_files`:** not needed today, because physical file counts match referenced counts after expiry. Worth scheduling with a multi-day `older_than` if writes ever fail midway.
- **Fixing the small-file problem at the source:** fewer shuffle partitions, merge-on-read, or pre-filtering MERGE sources to changed rows. All are better long term, but they change D2 code. Recorded as a follow-up; maintenance handles the symptom now. The first two of those, fewer shuffle partitions and MERGE only changed rows, were done afterwards in ADR 011.

## Consequences
- Time travel only reaches the snapshots maintenance keeps. The demo exits with a clear message if the requested date's snapshot has expired.
- Maintenance is a manual `make` command. It's easy to add as a weekly Airflow DAG later; it isn't part of `ledgerlake_daily`, so daily runs stay short.
