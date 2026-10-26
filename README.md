# LedgerLake

A batch lakehouse for a fictional Malaysian consumer fintech app. Postgres and MongoDB are extracted into Apache Iceberg tables on MinIO, transformed with PySpark (bronze → silver → gold), orchestrated by Airflow, and published to a Postgres serving schema with role-based access. The full specification is in [docs/SPEC.md](docs/SPEC.md).

> **Status:** Days 1 and 2, D3-AM and D3-MID are done and verified on Apple Silicon. That covers the platform, generator, bronze, silver, DQ, gold, the `ledgerlake_daily` Airflow DAG, serving with role-based access, and the time-travel, schema-evolution and maintenance commands. See [docs/PROGRESS.md](docs/PROGRESS.md). CI and the final docs come next.

## Prerequisites
- Docker Desktop (Apple Silicon and x86 both work). In Docker Desktop, go to **Settings → Resources** and set **Memory to about 8 GB**.
- The `docker` command on your PATH. Check with `docker version`. If it isn't found, either enable Docker Desktop's CLI tools installation or add `/Applications/Docker.app/Contents/Resources/bin` to your PATH.
- `make` (included with macOS Command Line Tools).
- Free host ports 5432, 8080, 9000, 9001 and 27017. To use different ports, change them in `.env`.

## Quick start
```bash
make up      # first run builds the Airflow image; creates .env from .env.example if missing
make smoke   # Iceberg smoke test
make seed    # 60 days of synthetic source data in Postgres and MongoDB
make run date=2026-09-29      # the whole pipeline in Airflow (ledgerlake_daily) for one business date
make run date=2026-09-30      # next day: tick, bronze, silver, DQ, gold, DQ, publish
make access-test              # prove what the analyst and engineer roles can read in the serving database
make demo                     # Iceberg time-travel audit + schema-evolution demo
make maintenance              # compact files, expire old snapshots; file counts before/after in ops.maintenance_log
```

Each layer can also be run on its own:
```bash
make bronze date=2026-09-29   # first run: full initial load into Iceberg bronze
make tick date=2026-09-30     # one more day of source data
make bronze date=2026-09-30   # incremental load (only rows changed since the previous high-water mark)
make silver date=2026-09-29   # silver + quarantine for each loaded date, in date order
make silver date=2026-09-30
make silver-report            # quarantine reasons, PII layout, schema evolution evidence
make dq layer=silver date=2026-09-30          # 32 DQ checks; results in lake.ops.dq_results
make gold date=2026-09-30                     # dims + fact (MERGE) and marts
make dq layer=gold date=2026-09-30            # 20 gold checks, incl. freshness and mart-vs-silver reconciliation
```

The first `make up` downloads about 1.5 GB of images and builds a 4.4 GB Airflow image. It took about 8 minutes on the reference MacBook Air.

| Service | URL / port | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | none (local-only, all users are admin) |
| MinIO console | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` from `.env` |
| Postgres | localhost:5432 (`source`, `iceberg_catalog`, `serving`, `airflow`) | `POSTGRES_USER` / `POSTGRES_PASSWORD` |
| MongoDB | localhost:27017 (`app.app_events`) | `MONGO_ROOT_USER` / `MONGO_ROOT_PASSWORD` |

## Make targets
| Target | What it does |
|---|---|
| `make up` | Builds and starts all services, and waits until they're healthy |
| `make down` | Stops services; data is kept |
| `make clean` | Stops services and **deletes all data volumes** (asks first) |
| `make ps` | Service status |
| `make logs` | Follows logs (`make logs s=airflow` for one service) |
| `make smoke` | Creates, writes and reads an Iceberg table on MinIO through the JDBC catalog |
| `make seed` | Rebuilds the synthetic source history (2026-08-01 to 2026-09-29), wiping existing source data. Logs row counts and defect counts. |
| `make tick date=2026-09-30` | Adds one day of data and updates some existing rows. A day that was already generated is skipped. |
| `make bronze date=2026-09-29` | Extracts all five sources incrementally into `lake.bronze.*` and replaces that date's partition. Re-running the same date gives the same counts. |
| `make silver date=2026-09-29` | Validates and deduplicates bronze partition D, writes rejected rows to `quarantine.*` with a `failure_reason`, and MERGEs the rest into `silver.*`. Re-runs change nothing. |
| `make silver-report` | Prints quarantine counts by reason, silver row counts, the PII column layout and schema-evolution evidence. Fails if an injected defect type is missing. |
| `make gold date=2026-09-30` | Builds `gold.dim_date`, `gold.dim_customer` (MERGE on a row hash), `gold.fact_transactions` (MERGE, partitioned by day) and the three marts (rebuilt) from current silver |
| `make dq layer=silver date=2026-09-30` | Runs the silver DQ suite and stores one row per check in `lake.ops.dq_results`. Exits 1 if any `error` check fails. |
| `make publish date=2026-10-04` | Publishes the 3 gold marts and `customer_profile` to the `serving` database in one transaction, and re-applies `serving/*.sql` (tables, masked view, roles, grants) |
| `make access-test` | Logs in as `analyst` and `engineer` and proves what each can and cannot read (30 tests). See [docs/access_control.md](docs/access_control.md). |
| `make demo-time-travel [date=2026-10-04]` | Audit demo: reads `gold.fact_transactions` as of the snapshot built for that business date (`VERSION AS OF` and `TIMESTAMP AS OF`) and shows what has been restated since. Defaults to the previous business date. |
| `make demo-schema-evolution` | Shows `silver.app_events`' schema id change when `referral_source` was added, and how old and new rows read |
| `make demo` | Both demos |
| `make maintenance [retain=5]` | For every lake table: compact data files, rewrite manifests, and expire snapshots, keeping the last N. Logs snapshot, current, referenced and physical file counts before and after to `lake.ops.maintenance_log`. |
| `make run date=2026-10-03` | Runs the whole `ledgerlake_daily` DAG for one business date (`airflow dags test`). Re-running a date changes nothing. |
| `make dq-gate-demo date=2026-10-04` | Runs `ledgerlake_daily` with a simulated `null_amount` defect: `dq_silver` fails, `gold` and `dq_gold` become `upstream_failed`, and the failure is logged in `lake.ops.pipeline_runs`. Stored data isn't changed; run `make run` for the same date afterwards. |
| `make reset-lake` | Drops every lake table, after asking. Use it before re-seeding for a fresh run. |
| `make test-container` | Runs the Spark unit tests and DAG integrity tests inside the Airflow container |
| `make test` / `make lint` | Unit tests and ruff, run locally (`pip install -r requirements-dev.txt` first) |

## Synthetic data
`make seed` generates the following, reproducibly (fixed seed):
- about 2,000 customers, 3,000 accounts, 100k transactions and 600 cash advances in Postgres `source`;
- about 50k app events in MongoDB `app.app_events`.

All names are fictional combinations of Malaysian names. Emails use reserved `example.*` domains. Amounts are in MYR.

About 2.2% of transactions carry a deliberate defect: duplicate row, null amount, negative amount, unknown category, or unknown account. About 0.5% of app events are duplicated. Pending transactions settle 1–14 days later, which produces late-arriving updates to old rows. From 2026-09-14 onward, app events gain `properties.referral_source` (schema drift).

Postgres timestamps are Malaysia local time (UTC+8) with no time zone. MongoDB `event_ts` is UTC. Each generated day is recorded in `source.generator_log` along with its injected defect counts. Design notes are in [ADR 003](docs/decisions/003-synthetic-generator.md).

## Verify the platform
1. Run `make up`. It exits 0 once postgres, mongodb and airflow are healthy and `minio-init` has exited 0.
2. Run `docker compose exec airflow uname -m`. It should print `aarch64` on Apple Silicon.
3. Run `make smoke`. It should end with `SMOKE TEST PASSED`, then show:
   - a `lake | ops | smoke_test` row from the Postgres catalog, and
   - the table's Parquet and metadata files under `warehouse/ops/smoke_test/` in MinIO.
4. Run `make seed`. It logs one line per generated day, then:
   - `seed total` lines with the rows inserted and defects injected, and
   - `seed measured in database` lines with counts taken directly from Postgres and MongoDB.

   For each defect type, the injected and measured counts should match. Running `make seed` again should give the same `transactions.fingerprint_md5`.
5. Run `make bronze date=2026-09-29` twice. Each run logs one line per table, with its window, `rows_extracted`, `partition_rows`, `table_rows` and the new watermark. The counts should be identical across the two runs.

## Bronze and watermarks
For logical date D (a Malaysia business day), each source is extracted with `low < updated_at < end of D`, using `event_ts` for MongoDB:
- `low` is the highest value already landed in bronze partitions **before** D, or no lower bound on the first load.
- The result replaces bronze partition D. A re-run of D therefore uses the same window and can't duplicate rows.
- `lake.ops.watermarks` records each entity's high-water mark after every run.

Bronze keeps raw source types and adds `_ingested_at`, `_source`, `_batch_id` and `_logical_date`. The details and trade-offs are in [ADR 004](docs/decisions/004-incremental-extract-watermarks.md).

## Silver and quarantine
For each logical date, silver reads that bronze partition and runs these steps for each entity:
- **Validate:** accepted values for status, category, direction and types; non-null keys; `amount > 0`; and parents that exist (transaction → account → customer).
- **Deduplicate:** keep the newest version per business key.
- **Quarantine:** write rejected rows to `quarantine.<entity>` with a `failure_reason`, for example `duplicate_row`, `null_amount`, `negative_amount`, `unknown_category` or `orphan_account`. When a transaction's late-arriving update replaces an older version, that older version is kept there as `superseded_by_late_update`.
- **Conform and merge:** convert timestamps to UTC and MERGE into `silver.*`.

Personal data is protected:
- `silver.customers` has only `email_hash` and `phone_hash` (salted SHA-256, with the salt from `PII_SALT` in `.env`) and an `age_band`.
- Name, email, phone and date of birth live only in `silver.customers_pii`.

`silver.app_events` gains `referral_source` through Iceberg schema evolution (`ALTER TABLE ADD COLUMN`) when the field first appears on 2026-09-14. Data files written earlier aren't rewritten. Details are in [ADR 005](docs/decisions/005-silver-quarantine-and-pii.md).

## Orchestration (Airflow)
`ledgerlake_daily` runs `generate_tick → extract_bronze → silver → dq_silver → gold → dq_gold → publish`.
- **Business date:** each run processes one Malaysia business day. That's the `business_date` param when given, otherwise the data interval of a midnight Malaysia-time schedule.
- **Idempotent:** every task is idempotent for its business date, so re-runs and backfills are safe.
- **Retries:** build tasks retry twice, 1 minute apart. DQ tasks don't retry, because a failed check won't fix itself.
- **Failure handling:** a failed `error` DQ check fails its task, and Airflow marks everything downstream `upstream_failed`.
- **Run log:** every task run is logged in `lake.ops.pipeline_runs` (run_id, dag_id, task_id, logical_date, rows_in, rows_out, duration_s, status). Jobs write their own success rows, and failure and retry callbacks write the rest.
- **Paused by default:** the DAG is paused at creation. Use `make run date=…` for one date, or unpause it in the UI for the daily schedule.

Details are in [ADR 008](docs/decisions/008-airflow-orchestration.md).

## Gold
| Table | Grain | Built by |
|---|---|---|
| `gold.fact_transactions` | one transaction (partitioned by `days(txn_ts)`) | MERGE, newest `updated_at` wins |
| `gold.dim_customer` | one customer (no PII) | MERGE INTO on a row hash (SCD type 1) |
| `gold.dim_date` | one day, 2024–2027 | created once |
| `gold.mart_daily_spend` | day × category | total and average spend, transaction count, customers |
| `gold.mart_advance_repayment` | monthly request cohort | repayment rate (overall and among due advances), average days late |
| `gold.mart_customer_features` | customer × month | spend level and daily volatility, salary credits and 3-month income regularity, advance usage |

Spend means completed debits, and days and months are Malaysia business dates. Every gold run is checked by `make dq layer=gold`, which recomputes each mart from silver and requires `mismatched_groups=0`, plus key, referential and freshness checks. Details are in [ADR 007](docs/decisions/007-gold-model-and-reconciliation.md).

## Time travel and maintenance
- **Time travel (audit):** "What did gold report for 2026-10-04 when it was built?" `make demo-time-travel date=2026-10-04` reads the fact table as of that build. In the verification run it reported 87,060.21 MYR of completed spend then, against 89,979.40 MYR now, because of 113 late-arriving status changes since.
- **Maintenance:** `make maintenance` compacts small files and expires snapshots beyond the last 5. In the verification run it reduced physical data files from 4,119 to 2,707, and `silver.transactions` went from 841 data files to 1. Physical object counts are checked against Iceberg's references, so orphaned files would show up.

Time travel reaches only snapshots that maintenance kept. Details are in [ADR 010](docs/decisions/010-time-travel-and-maintenance.md).

## Serving and access control
After `dq_gold` passes, `publish` replaces the contents of four tables in the Postgres `serving` database in one transaction: the three marts and `customer_profile`.

| Role (login, password from `.env`) | Can read | Cannot |
|---|---|---|
| `analyst` | the 3 marts, and `v_customer_profile_masked` (name initial + `***`, age band, state, income band) | `customer_profile` or any of its columns, other databases, any write or CREATE, `SET ROLE engineer` |
| `engineer` | everything in `serving`, including `customer_profile` personal data | write, CREATE, other databases |

For example, the same customer appears to `engineer` as `Siva a/l Subramaniam | siva.subramaniam3700@example.org` and to `analyst` as `S***`. Connect from the host with `psql -h localhost -U analyst serving`.

The matrix and the test that proves it are in [docs/access_control.md](docs/access_control.md). The design is in [ADR 009](docs/decisions/009-serving-and-access-control.md).

## Data quality
`quality/` is a lightweight PySpark DQ module. The gold suite has 20 `error` checks (see Gold above). The silver suite has 32 checks:
- **`error`:** not-null and unique business keys, accepted values for status, category and type columns, `amount > 0`, and referential integrity (transaction → account → customer, plus advances and events → customer).
- **`warn`:** freshness, and a quarantine rate of at most 5%.

Each run writes one row per check to `lake.ops.dq_results` (run_id, logical_date, table, check_name, severity, passed, failed_rows, details, checked_at). A failed `error` check makes the task exit 1, so Airflow marks downstream tasks `upstream_failed`. `warn` failures are only logged. Details are in [ADR 006](docs/decisions/006-data-quality-module.md).

## Notes
- Postgres and MongoDB init scripts run only on an empty volume. Run `make clean` to re-initialise them.
- Pinned versions and the reasons for them are in [docs/decisions/001-versions.md](docs/decisions/001-versions.md). The MinIO image choice is in [docs/decisions/002-minio-community-image.md](docs/decisions/002-minio-community-image.md).
- All data is synthetic. Secrets live in `.env`, which is git-ignored. `.env.example` holds placeholders only.
