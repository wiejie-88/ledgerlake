# LedgerLake

A batch lakehouse for a fictional Malaysian consumer fintech app. Postgres and MongoDB are extracted into Apache Iceberg tables on MinIO, transformed with PySpark (bronze → silver → gold), orchestrated by Airflow, and published to a Postgres serving schema with role-based access. The full specification is in [docs/SPEC.md](docs/SPEC.md).

> **Status:** Day 1 and silver (D2-AM) are done and verified on Apple Silicon. See [docs/PROGRESS.md](docs/PROGRESS.md). DQ, gold, the Airflow DAG and serving come in later blocks.

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
make bronze date=2026-09-29   # first run: full initial load into Iceberg bronze
make tick date=2026-09-30     # one more day of source data
make bronze date=2026-09-30   # incremental load (only rows changed since the previous high-water mark)
make silver date=2026-09-29   # silver + quarantine for each loaded date, in date order
make silver date=2026-09-30
make silver-report            # quarantine reasons, PII layout, schema evolution evidence
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
| `make reset-lake` | Drops every lake table, after asking. Use it before re-seeding for a fresh run. |
| `make test-spark` | Runs the Spark unit tests inside the Airflow container |
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

## Notes
- Postgres and MongoDB init scripts run only on an empty volume. Run `make clean` to re-initialise them.
- Pinned versions and the reasons for them are in [docs/decisions/001-versions.md](docs/decisions/001-versions.md). The MinIO image choice is in [docs/decisions/002-minio-community-image.md](docs/decisions/002-minio-community-image.md).
- All data is synthetic. Secrets live in `.env`, which is git-ignored. `.env.example` holds placeholders only.
