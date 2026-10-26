# LedgerLake — Progress

| Block | Status | Verified |
|---|---|---|
| D1-AM | Done | 2026-10-26 |
| D1-PM | Done | 2026-10-26 |
| D1-EVE | Done | 2026-10-26 |

All verification ran on a MacBook Air (Apple Silicon) with Docker Desktop: engine 29.9.2 on arm64, Compose v5.6.1, 8 CPUs and 8.3 GB of memory for Docker. Every number below is measured.

## D1-AM: scaffold, Docker Compose, Airflow image, Makefile, pinned versions

**What was built:**
- `docker-compose.yml` defines these services:
  - `postgres` (17.10), hosting four databases: `source`, `iceberg_catalog`, `serving` and `airflow`.
  - `mongodb` (8.0.32), with collection `app.app_events`.
  - `minio` (pgsty fork).
  - `minio-init`, which creates the `warehouse` bucket.
  - `airflow`, running `airflow standalone` on Postgres.
- `docker/airflow/Dockerfile`: Airflow 3.3.2 on Python 3.11, the Temurin 17.0.20 JRE, PySpark 3.5.9, and three SHA-256-verified JARs: Iceberg 1.10.2 Spark runtime, Iceberg 1.10.2 AWS bundle, and Postgres JDBC 42.7.14.
- `jobs/common/spark.py` holds the session factory for the `lake` JDBC catalog. `jobs/common/smoke.py` is the smoke test.
- Makefile, `.env.example`, README, and ADRs 001 and 002.

**Verification result:**
- **Image build** (inside `make up`):
  - All three JAR checksums printed `OK`.
  - `pip check` reported `No broken requirements found.`
  - `java -version` reported `17.0.20`.
  - PySpark ships `scala-library-2.12.18.jar`, which matches the Iceberg runtime's `_2.12`.
- **`make up`** exited 0 with postgres, mongodb, minio and airflow `Healthy` and `minio-init` `Exited (0)`.
  - The first run took 491 s wall time, including image downloads and the build.
  - The Airflow container is 4.38 GB.
- **Architecture:** `uname -m` in the Airflow container printed `aarch64`.
- **Databases** present: `airflow`, `iceberg_catalog`, `serving`, `source` (plus the default `postgres`).
- **MongoDB** collections: `[ 'app_events' ]`.
- **minio-init** logged ``Bucket created successfully `local/warehouse`.``
- **`/api/v2/monitor/health`** reported metadatabase, scheduler, triggerer and dag_processor all `healthy`.
- **`make smoke`** took 12.5 s and printed `versions spark=3.5.9 iceberg=1.10.2 java=17.0.20`, `rows_read=3 ids=[1, 2, 3]`, `snapshots=1 data_files=3` and `SMOKE TEST PASSED`.
  - The Postgres row was `lake | ops | smoke_test | s3://warehouse/ops/smoke_test/metadata/00001-….metadata.json`.
  - MinIO listed 3 Parquet files, 2 metadata JSON files, 1 manifest and 1 manifest list.
  - A second run gave the same result.

## D1-PM: synthetic generator (`seed`, `tick`) with defects

**What was built:**
- The `generator/` package, run as `python -m generator seed|tick --date`. It contains:
  - `synth.py`: pure planning of each day.
  - `store.py`: Postgres and MongoDB IO.
  - `run.py`: seed and tick orchestration.
  - `settings.py` and `reference.py`: the seed, volumes, defect rates and reference data.
- Source tables plus `source.generator_log`.
- Makefile targets `seed`, `tick`, `test` and `lint`. Also `pyproject.toml`, `requirements-dev.txt` and ADR 003.

**Verification result:**
- **`make seed`** took 10.8 s and exited 0. It logged one line per day for 60 days, plus totals. The **measured database counts equal the injected counts** for every defect type:

  | Measured in database | Value |
  |---|---|
  | customers / accounts / transactions / cash_advances | 2,000 / 2,994 / 99,897 / 597 |
  | app_events documents | 50,007 |
  | transactions: duplicate_row / null_amount / negative_amount | 584 / 397 / 416 |
  | transactions: unknown_category / orphan_account | 395 / 371 |
  | app_events: duplicate_row | 253 |
  | late-arriving updates injected / rows with updated_at after txn day | 6,454 / 6,502 (the difference is duplicate rows sharing a txn_id) |
  | app_events with referral_source / first one | 13,651 / 2026-09-13T16:16:17Z (= 2026-09-14 00:16 MYT, day 45) |

- **Reproducible:** a second `make seed` (11.4 s) gave the same `transactions.fingerprint_md5=10236acea734ec3e158b3f82776a3216` and 50,007 events.
- **`make tick date=2026-09-30`** took 0.2 s. It inserted 5 customers, 8 accounts, 1,589 transactions, 9 advances and 855 events, and updated 6 customers, 2 accounts, 101 transactions and 10 advances. A second tick for the same date logged `already generated; nothing to do`.
- **Local unit tests:** see D1-EVE (all suites run together).

## D1-EVE: extract and bronze jobs with watermarks

**What was built:**
- `jobs/extract/sources.py` (no Spark): entity definitions, DSN-to-JDBC options, incremental SQL, and the MongoDB reader.
- `jobs/common/time_windows.py`: Malaysia-day window bounds.
- `jobs/bronze/load.py`:
  - reads Postgres over JDBC, keeping timestamps as `TIMESTAMP_NTZ`;
  - adds the ingestion metadata columns `_ingested_at`, `_source`, `_batch_id`, `_logical_date`;
  - computes the lower bound from earlier bronze partitions;
  - replaces the date's partition (Iceberg `overwrite(_logical_date = D)`);
  - upserts `ops.watermarks` with `MERGE INTO`.
- `jobs/bronze/run.py` (`python -m jobs.bronze.run --date D`) and `make bronze date=D`.
- Bronze tables `lake.bronze.{customers, accounts, transactions, cash_advances, app_events}`, partitioned by `_logical_date`.
- 7 unit tests in `tests/unit/test_extract.py`, and ADR 004 (window and watermark design).

**Verification result** (after `make seed` and `make tick date=2026-09-30`):
- **`make bronze date=2026-09-29`, run twice.** This is the initial full load, with no lower bound. Run 1 took 36.2 s and run 2 took 26.1 s. Both runs gave identical counts:

  | Table | rows_extracted = partition_rows = table_rows |
  |---|---|
  | bronze.customers | 1,994 |
  | bronze.accounts | 2,992 |
  | bronze.transactions | 99,795 |
  | bronze.cash_advances | 587 |
  | bronze.app_events | 50,007 |

  The Postgres counts are below the seed totals because the 09-30 tick had already moved some rows' `updated_at` into 09-30. The difference is 6 customers, 2 accounts, 102 transaction rows (101 settlements, one on a duplicated row) and 10 advances. ADR 004 describes this behaviour.

- **`make bronze date=2026-09-30`, run twice.** This is the incremental case. The lower bound is the 09-29 high-water mark. Both runs gave identical counts:

  | Table | partition_rows | table_rows |
  |---|---|---|
  | bronze.customers | 11 | 2,005 |
  | bronze.accounts | 10 | 3,002 |
  | bronze.transactions | 1,691 | 101,486 |
  | bronze.cash_advances | 19 | 606 |
  | bronze.app_events | 855 | 50,862 |

  Per entity, each run took between 1.3 and 7.6 s.
- **`ops.watermarks`** has 5 rows. For example, `postgres | transactions | 2026-09-30T23:59:59.000000`.
- **Schema drift:** the 14,506 bronze events with `properties['referral_source']` equal the 13,651 seeded plus the 855 ticked events.
- **Bronze schema:** `bronze.transactions` keeps `txn_ts` and `updated_at` as `timestamp_ntz` and `amount` as `decimal(12,2)`, and is partitioned by `_logical_date`.
- **Local checks:** `pytest` gives 20 passed in 50.9 s (Python 3.11.5), and `ruff check` and `ruff format --check` are clean.

## Known issues / risks
- **Docker CLI path:** Docker Desktop's `docker` command wasn't on the shell PATH on this Mac, because no `/usr/local/bin/docker` link exists. Commands were run with `/Applications/Docker.app/Contents/Resources/bin` added to PATH. The README explains this.
- **MinIO image:** the official MinIO images are no longer published, so the project uses the pgsty fork (ADR 002). It worked unchanged.
- **pandas:** Airflow constraints pin pandas 3.0.5, which may not work with PySpark 3.5's pandas integration. No job uses pandas yet.
- **Airflow security:** the UI has no login (`SIMPLE_AUTH_MANAGER_ALL_ADMINS`) and its port is bound to 127.0.0.1 only. There is no Fernet key; no Airflow connections are stored.
- **Faker:** listed in SPEC §4 but not used, because it has no Malaysian names locale (ADR 003).
- **Phone numbers:** generated in the Malaysian mobile format with random digits, so some may match real numbers. Emails use reserved example domains.
- **`make seed` wipes source data:** it rebuilds the source but doesn't touch the lake. Re-seeding after bronze loads leaves bronze holding rows from the previous seed. Run `make clean` or drop the bronze tables for a fully fresh start.
- **Extraction limits** (ADR 004):
  - A re-run of an old date after newer source changes can return fewer rows, because those rows land in the newer partition instead.
  - Backfilling a date earlier than already-loaded partitions can duplicate rows across partitions. Silver deduplicates them.
- **No Spark unit tests:** the Spark functions in `jobs/bronze/load.py` were checked only by these end-to-end runs. Spark-based unit tests come with CI in D3-PM.
- **Slow tests:** the generator tests take about 50 s because they run the in-memory seed three times.
- **No `ops.pipeline_runs` yet:** that table and the run metrics written to it come with the Airflow DAG in D2-EVE.
