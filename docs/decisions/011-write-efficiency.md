# ADR 011: Fewer shuffle partitions and MERGE only changed rows

**Status:** accepted (follow-up to D3-MID, 2026-10-26)

## Context
Measurements during D3-MID (ADR 010) showed two write inefficiencies:
1. Spark's default `spark.sql.shuffle.partitions=200` made each MERGE write up to 200 small files.
2. MERGE sources contained every row, not just the changed ones. Gold rebuilt the fact from all of silver, and re-runs re-sent the same rows. With copy-on-write, every data file holding a *matched* key is rewritten, even when the `WHEN MATCHED AND s.updated_at > t.updated_at` condition then changes nothing. Every run therefore rewrote whole tables.

## Decision
- `spark.sql.shuffle.partitions=8`, set in `build_spark_conf` and overridable with `SPARK_SHUFFLE_PARTITIONS`. 8 matches the laptop's cores.
- **`merge_latest`** (silver tables, `gold.fact_transactions`):
  - First compute `changed_rows`, an anti-join against the target's `(key, updated_at)`. Only rows that are new or carry a newer `updated_at` reach the MERGE.
  - If there are none, the MERGE is skipped: no snapshot, no files.
  - It returns `(inserted, updated)` from the same pass, which replaces the separate `merge_preview`.
- **`dim_customer`'s hash MERGE** does the same, keeping only rows whose `_row_hash` changed or whose key is new.
- **Immutable events** (`silver.app_events`) use `insert_new`, a left anti-join plus a plain append, instead of an insert-only MERGE. No existing file is ever rewritten.

## Measured effect
Comparable days, full `ledgerlake_daily` runs. 2026-10-06 used the old code and 2026-10-07 the new. The counts are the sums of snapshot summaries over every bronze, quarantine, silver and gold table.

| | Old daily run | New daily run | Old same-date re-run | New same-date re-run |
|---|---|---|---|---|
| Data files added | 1,074 | 57 | 329 | 10 |
| Rows written | 237,428 | 78,467 | 229,307 | 9,125 |
| Rows rewritten (removed) | 224,378 | 65,586 | 222,811 | 2,600 |
| Snapshots | 21 | 21 | 21 | 13 |
| DAG wall time | 222 s | 170 s | 250 s | 248 s |

- **`gold.fact_transactions`:** from 68 files / 108,830 rows a day (the whole table) to 11 files / 18,423 rows. Only the day partitions holding updated transactions are rewritten.
- **`silver.accounts`, `customers`, `customers_pii`, `cash_advances`:** from about 200 files a day to 5–8.
- **Re-runs:** no silver or gold MERGE table commits anything. What remains is by design: the bronze and quarantine partition overwrites, and the mart rebuilds.

## Follow-up: merge-on-read for `silver.transactions` (2026-10-27)
After the changes above, `silver.transactions` still rewrote most of the table every day. It's unpartitioned, and copy-on-write must rewrite every file holding an updated key. Settlements update transactions from up to 14 days back, scattered across most files. Of the two options, (a) merge-on-read or (b) partitioning by `days(txn_ts)`, **(a) was applied**:
- `silver.transactions` gets `write.merge.mode`, `write.update.mode` and `write.delete.mode` = `merge-on-read`. These are declared on its `SilverEntity` and applied by `merge_latest(..., properties=MERGE_ON_READ)` through `ensure_properties`, which only alters properties that differ.
- Updates now write small position-delete files. Readers apply them, so DQ, gold and the reconciliation read the merged result.
- Maintenance compacts them: `rewrite_data_files` with `delete-file-threshold=1` and `remove-dangling-deletes=true`, then `rewrite_position_delete_files`. `ops.maintenance_log` gained `current_delete_files_before/after` and `rewritten_delete_files`, added to the existing table automatically.

Measured on comparable days, full DAG runs:

| `silver.transactions` | Copy-on-write (2026-10-16) | Merge-on-read (2026-10-17) |
|---|---|---|
| Data files added / removed | 8 / 10 | 1 / 0 |
| Delete files added | 0 | 11 |
| Rows written / rewritten | 95,287 / 93,561 | 1,898 / 0 |
| **All lake tables: rows written** | **130,225** | **30,936** |

- **Same-date re-run:** no silver or gold table committed anything.
- **DQ:** both runs passed, including `unique:txn_id` and all three gold reconciliations.
- **`make maintenance`:** `silver.transactions` went from 63 data files plus 11 delete files to **1 data file and 0 delete files**, with 126,323 rows before and after, all with unique `txn_id`. Silver DQ afterwards: 39/39.

**Trade-off:** until maintenance runs, reads apply the delete files, about 11 small files a day at this volume. Run `make maintenance` regularly, at least weekly, so they don't pile up.
- **`gold.dim_customer`** is a single file of about 2k rows, so any change rewrites it. That's negligible.
- **Files from before the change** stay until `make maintenance` compacts them (about 200 current files in some silver tables).
