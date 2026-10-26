# Runbook

All commands run from the repository root. Pipeline jobs run inside the `airflow` container, which is what `make` does. If `docker` isn't found, add `/Applications/Docker.app/Contents/Resources/bin` to your `PATH`.

## Start and stop

| Action | Command | Notes |
|---|---|---|
| Start everything | `make up` | Builds the image if needed and waits until healthy. Creates `.env` from `.env.example` on first use. |
| Status | `make ps` | `minio-init` should show `Exited (0)`; the others `Up` / `(healthy)` |
| Logs | `make logs s=airflow` | Use `s=postgres`, `s=minio` and so on for other services |
| Stop, keeping data | `make down` | |
| Delete everything | `make clean` | Asks first. Deletes the Postgres, MongoDB and MinIO volumes. |
| Drop lake tables only | `make reset-lake` | Asks first. Source data and serving are untouched. |

Airflow UI: http://localhost:8080 (no login; bound to localhost only). MinIO console: http://localhost:9001.

## Daily operation
- **One business date, full pipeline:** `make run date=YYYY-MM-DD` runs `ledgerlake_daily` through `airflow dags test` with an explicit `business_date`.
- **Scheduled:** unpause `ledgerlake_daily` in the UI. It runs at 00:00 Malaysia time for the day that just ended, one run at a time, without catchup.
- **Always pass `business_date` for manual triggers in the UI.** Without it, Airflow infers the data interval from the trigger time and processes that day instead.

## Re-runs and backfills
Every task is idempotent for its business date, so re-running a date is safe:
- the generator skips a date it already generated;
- bronze overwrites partition D using the same extract window;
- silver and gold MERGE only new or changed rows;
- publish replaces the serving tables in one transaction.

| Situation | Action |
|---|---|
| A task failed for a transient reason | It retries twice automatically (DQ tasks don't retry). Then clear the task in the UI, or run `make run date=D` again. |
| Backfill several days | Verified: `make run date=D` for each date, in order. Airflow's own `docker compose exec airflow airflow backfill create --dag-id ledgerlake_daily --from-date … --to-date …` should work, since every task is idempotent per date, but **it hasn't been exercised in this project's verification**. |
| Rebuild one layer by hand | `make bronze date=D`, `make silver date=D`, `make gold date=D`, `make publish date=D` (in that order) |

Backfilling a date *earlier* than already-loaded partitions can put a row into two bronze partitions. Silver keeps only the newest version (ADR 004). Gold is rebuilt from current silver, not as of the backfilled date (ADR 007).

## A DQ gate failed
1. **Find the failing checks.** The Airflow log of `dq_silver` or `dq_gold` ends with `DQ GATE FAILED: …`. All results are stored:
   ```sql
   SELECT logical_date, `table`, check_name, failed_rows, details
   FROM lake.ops.dq_results WHERE NOT passed AND severity = 'error' ORDER BY checked_at DESC;
   ```
   Query it in Spark: `docker compose exec airflow python -c "from jobs.common.spark import get_spark; get_spark('q').sql('…').show(truncate=False)"`.
2. **Interpret it:**
   - An `error` failure in **silver** means a defect got past quarantine, which is a pipeline bug, since silver is built to satisfy these checks. Check the rules in `jobs/silver/rules.py` against the new data.
   - **`reconcile_with_silver`** failing in gold means a mart no longer matches silver. Re-run `make gold date=D`; if it still fails, the mart logic and the reconciliation definitions disagree.
   - **`freshness`** failing in gold means no transactions arrived for the date. Check `generate_tick` / `extract_bronze` and `lake.ops.watermarks`.
3. **Re-run.** Fix, then re-run the date. Downstream tasks (gold, publish) were `upstream_failed`, so serving still holds the previous good publish.

`warn` failures (silver freshness, quarantine rate above 5%) only log. Review them, but they don't block.

## Reviewing quarantined data
`make silver-report` prints quarantine counts by reason and checks that every injected defect type is present. Rows are in `lake.quarantine.<entity>`, partitioned by `_quarantine_date`, with the original bronze columns. `superseded_by_late_update` rows are an audit trail of versions replaced by late updates, not errors.

## Serving and access
- `make publish date=D` re-applies `serving/*.sql` (tables, view, roles, grants) and replaces the data.
- **Verify access:** `make access-test` runs 30 checks logged in as each role.
- **Rotate role passwords:** change `ANALYST_PASSWORD` / `ENGINEER_PASSWORD` in `.env`, run `docker compose up -d airflow` so the container picks up the new values, then `make publish date=<latest>`.
- **Connect:** `psql -h localhost -U analyst serving`.

## Maintenance
- `make maintenance` compacts small files, rewrites manifests and expires snapshots, keeping the last 5 per table. File counts before and after go to `lake.ops.maintenance_log`. Run it weekly, or after many runs; it took 49–96 s on the verification data.
- Time travel (`make demo-time-travel date=D`) only reaches retained snapshots. Raise retention with `make maintenance retain=10`.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `docker: command not found` | Docker Desktop's CLI isn't on `PATH`. Add `/Applications/Docker.app/Contents/Resources/bin`. |
| `make up` fails with port already allocated | Another service uses 5432, 8080, 9000, 9001 or 27017. Change the `*_PORT` values in `.env`. |
| Spark tasks killed or very slow | Docker memory too low. Give Docker Desktop about 8 GB (Settings → Resources). |
| New databases or roles missing after editing init scripts | Postgres init scripts run only on an empty volume. Run `make clean`, then `make up`. Roles and grants are re-applied by every publish. |
| `PII_SALT is not set` or `Missing ANALYST_PASSWORD` | `.env` predates those keys. Copy them from `.env.example`, then `docker compose up -d airflow`. |
| Time-travel demo: `No retained snapshot … (expired by maintenance?)` | That date's snapshot was expired. Pick a later date, or keep more snapshots. |
| DAG test fails with `no such table: dag` | Use `DagBag.dags[...]`, not `get_dag()`, which needs a metadata database (ADR 012). |

## Fresh start (reproducible)
```bash
make clean            # asks first
make up
make seed
make run date=2026-09-29
```
The generator is deterministic. `make seed` logs a `transactions.fingerprint_md5`, which should be `10236acea734ec3e158b3f82776a3216` for the default settings.
