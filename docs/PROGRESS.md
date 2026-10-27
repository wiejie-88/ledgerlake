# LedgerLake — Progress

| Block | Status | Verified |
|---|---|---|
| D1-AM | Done | 2026-10-26 |
| D1-PM | Done | 2026-10-26 |
| D1-EVE | Done | 2026-10-26 |
| D2-AM | Done | 2026-10-26 |
| D2-MID | Done | 2026-10-26 |
| D2-PM | Done | 2026-10-26 |
| D2-EVE | Done | 2026-10-26 |
| D3-AM | Done | 2026-10-26 |
| D3-MID | Done | 2026-10-26 |
| D3-PM | Done locally; **GitHub run pending** (no remote yet) | 2026-10-26 |
| D3-EVE | Done | 2026-10-26 |
| Could-haves 9, 11 | Done (10 is ADR 013) | 2026-10-27 |

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

## D2-AM: silver (dedupe, typing, PII hashing, quarantine, schema evolution)

**What was built:**
- `jobs/silver/rules.py`: the data contract. It defines accepted values, plus NULL-safe validation rules that each map a failure reason to a condition.
- `jobs/silver/transforms.py`: pure DataFrame functions:
  - validation flags, parent-key checks, deduplication, and late-update detection;
  - conversion from Malaysia time to UTC, salted SHA-256 hashing, and age bands;
  - conform functions per entity, and property-key detection for schema evolution.
- `jobs/silver/run.py` (`make silver date=D`): per entity, it validates, deduplicates, audits late updates, overwrites quarantine partition D, then MERGEs into silver. Event loading handles schema evolution.
- `jobs/common/iceberg.py`: shared helpers for partition overwrite, `merge_latest` and `merge_insert_only`. Bronze now uses them too.
- `jobs/silver/report.py` (`make silver-report`): prints quarantine reasons, silver counts, PII column layout and schema-evolution evidence, and exits 1 if an injected defect type is missing.
- `jobs/common/reset_lake.py` (`make reset-lake`) drops all lake tables.
- `make test-spark` runs the Spark unit tests inside the container. `pytest==9.1.1` was added to the image, and `tests/` is mounted into the container.
- The Airflow service gets `PII_SALT`.
- ADR 005.

**Decision:** late-arriving updates are applied in silver, and the version they replace is written to quarantine as `superseded_by_late_update`, as an audit trail. This was chosen by the project owner.

**Verification result.** Run from a clean lake: `reset_lake`, then `seed`, then for each of 2026-09-29, 09-30 and 10-01, `bronze` then `silver`, with a `tick` before each later date.
- **`make test-spark`:** 9 passed in 22.0 s.
- **Silver runs:** 78 s for 09-29 (the initial load), 85 s for 09-30 and 72 s for 10-01.

  | Logical date | silver.transactions | quarantine.transactions reasons |
  |---|---|---|
  | 09-29 | rows_in 99,897, valid 97,734, inserted 97,734 | duplicate_row 584, negative_amount 416, null_amount 397, unknown_category 395, orphan_account 371 |
  | 09-30 | rows_in 1,691, valid 1,658, inserted 1,559, updated 99 | superseded_by_late_update 99, duplicate_row 17, unknown_category 7, negative_amount 4, orphan_account 3, null_amount 2 |
  | 10-01 | rows_in 1,707, valid 1,671, inserted 1,567, updated 104 | superseded_by_late_update 104, duplicate_row 8, negative_amount 8, orphan_account 7, unknown_category 7, null_amount 6 |

- **App events:** 253, 6 and 4 duplicate rows were quarantined on the three dates.
- **Other tables:** customers, accounts and cash_advances had no quarantined rows. Updates were applied, for example customers 5 inserted and 6 updated on 09-30.
- **Re-run of silver for 10-01:** every table reported `inserted=0 updated=0` with unchanged table rows and quarantine counts.
- **`make silver-report`:** `CHECK PASSED: every injected defect type is in quarantine with a reason`.
- **Reconciliation with the source** (after the ticks). Distinct quarantined keys equal the source's measured defect counts:

  | Defect | Count |
  |---|---|
  | duplicate_row | 608 |
  | null_amount | 405 |
  | negative_amount | 427 |
  | unknown_category | 406 |
  | orphan_account | 381 |

  `silver.transactions` holds 0 null, negative, unknown-category or orphan rows.
- **Silver row counts:** customers 2,010, accounts 3,008, transactions 100,860, cash_advances 614, app_events 51,359. Each is one row per key.
- **PII:** `silver.customers` columns are `customer_id, email_hash, phone_hash, age_band, state, income_band, created_at, updated_at, _logical_date, _processed_at`. Name, email, phone and date of birth exist only in `silver.customers_pii`.
- **Schema evolution:** the run logged `added column referral_source (first seen at event_ts=2026-09-13 16:16:17)`, which is 2026-09-14 Malaysia time.
  - The first data append (36,174 records in 45 files) has the schema without `referral_source`.
  - All 45 of those files are still in the current table (66 files in total), so none were rewritten.
  - Rows without `referral_source`: 36,174, event dates 2026-08-01 to 09-13. Rows with it: 15,185, from 2026-09-14 to 10-01.

## D2-MID: DQ module and `ops.dq_results`

**What was built:**
- `quality/checks.py`: check kinds `not_null`, `unique`, `accepted_values`, `positive`, `referential`, `freshness` and `quarantine_rate`.
- `quality/suites.py`: the silver suite of 32 checks. 30 are `error`; freshness and quarantine rate are `warn`.
- `quality/gate.py`: the severity gate, with no Spark.
- `quality/run.py`: `make dq layer=silver date=D`. It runs the suite, stores results in `lake.ops.dq_results` (replacing the same run_id's rows), and exits 1 on an error failure. `--simulate-defect` adds a bad row in memory only.
- `dags/ledgerlake_dq_gate_demo.py`: `dq_silver >> gold`, with a `simulate_defect` param. `make dq-gate-demo date=D` runs it.
- Tests: 3 gate unit tests run locally, and 11 check tests run in the container.
- `quality/` is copied into the image and mounted in Compose.
- ADR 006.

**Verification result** (lake state from the D2-AM run, logical date 2026-10-01):
- **Tests:** `make test-spark` gives 20 passed in 34.0 s (9 silver, 11 quality). Locally, `pytest` gives 23 passed and ruff is clean.
- **`make dq layer=silver date=2026-10-01`:** all 32 checks passed and the log printed `DQ GATE PASSED` (46.0 s).
  - Freshness logged `latest=2026-10-01 required>=2026-09-30`.
  - Quarantine rate logged `rejected=36 bronze_rows=1707 rate=0.0211`.
  - The re-run (43.8 s) replaced the run's rows. `ops.dq_results` still holds exactly 32 rows for `manual__silver__2026-10-01`.
- **`airflow dags test ledgerlake_dq_gate_demo 2026-10-01`** (clean): `dq_silver` was `success` (32/32 passed, 39.2 s), `gold` was `success`, and the DagRun was successful. Exit 0.
- **The same DAG with `{"simulate_defect": "null_amount"}`:**
  - The task logged `SIMULATED DEFECT null_amount added in memory (stored data unchanged)`, then `check=not_null:amount severity=error passed=False failed_rows=1`, then `DQ GATE FAILED: 1 error-severity DQ check(s) failed: silver.transactions:not_null:amount (1 rows)`.
  - `airflow tasks states-for-dag-run` showed **`dq_silver failed` and `gold upstream_failed`**. The gold task never started, and the DagRun was marked failed. Exit 1.
- **`ops.dq_results`** holds 3 runs: the manual run (32/32), the Airflow clean run (32/32), and the Airflow defect run (31/32), whose single failure row reads `silver.transactions | not_null:amount | error | false | 1`.
- **Stored data is unchanged** after the simulation: `silver.transactions` has 100,860 rows, 0 NULL amounts and 0 `SIMULATED%` rows.

## D2-PM: gold fact, dimensions (MERGE), marts

**What was built:**
- `jobs/gold/transforms.py`, pure functions:
  - `build_dim_date`, `build_dim_customer` (account roll-ups plus `_row_hash`), `build_fact_transactions`;
  - `mart_daily_spend`, `mart_advance_repayment`, `mart_customer_features`.
- `jobs/gold/run.py` (`make gold date=D`):
  - `dim_date` is created once;
  - `dim_customer` uses a hash-based SCD1 MERGE;
  - `fact_transactions` uses `merge_latest`, partitioned by `days(txn_ts)`;
  - marts are rebuilt in full.
- `quality/reconcile.py`: per-group reconciliation of each mart against silver.
- Gold DQ suite of 20 `error` checks: keys, referential integrity to the dimensions, accepted categories, gold freshness, mart grain uniqueness and `reconcile_with_silver` for each mart. The `unique` check now accepts composite keys.
- 7 Spark unit tests in `tests/spark/test_gold.py`, including reconciliation and tamper detection.
- ADR 007.

**Verification result.** Run on the lake from D2-AM, which holds silver through 2026-10-01. Then one more full day, 2026-10-02: tick, bronze, silver, silver DQ, gold, gold DQ.
- **Tests:** `make test-spark` gives 27 passed in 47.4 s. Locally, `pytest` gives 23 passed and ruff is clean.
- **`make gold date=2026-10-01`** (44 s wall):

  | Table | Rows | Notes |
  |---|---|---|
  | dim_date | 1,461 | created |
  | dim_customer | 2,010 | inserted 2,010 |
  | fact_transactions | 100,860 | inserted 100,860 |
  | mart_daily_spend | 620 | |
  | mart_advance_repayment | 3 | |
  | mart_customer_features | 4,828 | |

- **Gold DQ for 2026-10-01:** 20/20 passed in 33.9 s.
  - `reconcile_with_silver` reported `mismatched_groups=0` for all three marts.
  - Freshness logged `latest=2026-10-01 required>=2026-09-30`.
- **Re-run of gold for 2026-10-01:** `dim_customer` and `fact_transactions` reported `inserted=0 updated=0`, and the marts kept the same row counts.
- **Next day, 2026-10-02:**
  - Silver DQ: 32/32 passed.
  - Gold: `dim_customer` inserted 5 and **updated 8**. That's 6 customer attribute changes plus 2 customers whose account roll-up changed when an account went dormant. `fact_transactions` inserted 1,772 and updated 107, for 102,632 rows. The marts have 630, 3 and 5,368 rows.
  - Gold DQ: 20/20 passed in 25.1 s, with all three reconciliations at `mismatched_groups=0` and freshness `latest=2026-10-02`.
- **Grand total cross-check:** `mart_daily_spend` holds 7,071,522.71 MYR across 87,806 transactions, matching silver's completed debits exactly.
- **`mart_advance_repayment`** as of 2026-10-02:

  | Cohort | Advances | Repaid | Overdue | Repayment rate | Rate among due | Avg days late |
  |---|---|---|---|---|---|---|
  | 2026-08 | 302 | 242 | 60 | 0.8013 | 0.8013 | 1.64 |
  | 2026-09 | 304 | 113 | 5 | 0.3717 | 0.75 (24 due) | 0.0 |
  | 2026-10 | 16 | 0 | 0 | 0.0 | none yet | — |

## D2-EVE: Airflow DAG `ledgerlake_daily`

**What was built:**
- `dags/ledgerlake_daily.py`: `generate_tick → extract_bronze → silver → dq_silver → gold → dq_gold`.
  - Tasks are BashOperators calling the existing CLIs.
  - The business date is the `business_date` param, else the data interval start in Malaysia time (`CronDataIntervalTimetable`, midnight Malaysia time).
  - Retries: 2 (1-minute delay) for build tasks, 0 for DQ tasks. A 30-minute timeout, `max_active_runs=1` and `catchup=False`.
  - A `simulate_defect` param drives the gate demo.
- `jobs/common/pipeline_runs.py`: `lake.ops.pipeline_runs` (run_id, dag_id, task_id, logical_date, rows_in, rows_out, duration_s, status, recorded_at).
  - Spark jobs append their own success row.
  - The generator prints `ROWS_OUT=` and the task records it through the CLI.
- `jobs/common/airflow_callbacks.py`: failure and retry callbacks that record `failed` / `up_for_retry` rows in a subprocess.
- `tests/dags/test_ledgerlake_daily.py`: 5 DAG integrity tests. `make test-container` runs these and the Spark tests.
- New Make targets `make run date=D` and `make dq-gate-demo date=D` (the latter now uses `ledgerlake_daily`).
- `dags/ledgerlake_dq_gate_demo.py` was removed; `ledgerlake_daily` replaces it.
- ADR 008.

**Verification result.** Runs with `airflow dags test`, continuing from the D2-PM lake state through 2026-10-02.
- **Tests:** `make test-container` gives 32 passed in 35.0 s (27 Spark, 5 DAG integrity). After the summary, py4j prints a harmless "Logging error" at interpreter shutdown. Locally, `pytest` gives 23 passed and ruff is clean.
- **`make run date=2026-10-03`:** all 6 tasks `success`, run marked successful, 210 s wall.

  | Task | rows_in | rows_out | duration_s |
  |---|---|---|---|
  | generate_tick | — | 2,698 | 1.0 |
  | extract_bronze | 2,830 | 2,830 | 22.6 |
  | silver | 2,830 | 2,787 | 58.4 |
  | dq_silver | 32 | 32 | 33.7 |
  | gold | 104,383 | 8,146 | 38.6 |
  | dq_gold | 20 | 20 | 25.2 |

  The reconciliation checks reported `mismatched_groups=0`.
- **Re-run of `make run date=2026-10-03`:** success in 196 s.
  - The tick logged `already generated; nothing to do` (rows_out 0).
  - Every silver table and `gold.dim_customer` / `gold.fact_transactions` reported `inserted=0 updated=0`.
  - **Row counts of all 26 tables (bronze, silver, quarantine, gold and the source tables) were identical before and after the re-run.** For example, silver.transactions 104,383, gold.fact_transactions 104,383, quarantine.transactions 2,735, gold.mart_customer_features 5,625.
- **`make dq-gate-demo date=2026-10-04`** (simulated `null_amount`, 150 s):
  - `generate_tick`, `extract_bronze` and `silver` succeeded.
  - `dq_silver` logged `DQ GATE FAILED: 1 error-severity DQ check(s) failed: silver.transactions:not_null:amount (1 rows)`. The failure callback logged `recorded status=failed for task_id=dq_silver`.
  - `airflow tasks states-for-dag-run` showed **`dq_silver failed`, `gold upstream_failed`, `dq_gold upstream_failed`**, and the run was marked failed.
- **Recovery, `make run date=2026-10-04`:** all 6 tasks `success` (215 s). The tick was skipped as already generated, and both DQ gates passed.
- **`lake.ops.pipeline_runs`** holds 22 rows for these four runs: one per task attempt, including `2026-10-04 | dq_silver | failed | duration_s 34.0` from the callback.

## D3-AM: publish step, serving roles, masked view, access test

**What was built:**
- `serving/01_tables.sql`: schema `serving`, the 3 mart tables and `customer_profile`.
- `serving/02_views.sql`: `v_customer_profile_masked`, with the name initial + `***`, age band, state and income band.
- `serving/03_roles_grants.sql`: the `analyst` and `engineer` login roles. PUBLIC privileges are revoked on all 4 databases. `analyst` gets SELECT on the marts and the masked view; `engineer` gets SELECT on everything in `serving`.
- `jobs/publish/` (`make publish date=D`, and the new DAG task `publish` after `dq_gold`):
  - applies the SQL files and sets role passwords from `.env`;
  - builds `customer_profile` from `silver.customers_pii` joined to `gold.dim_customer`;
  - TRUNCATEs and inserts all 4 tables in one transaction;
  - records its row in `ops.pipeline_runs`.
- Tests:
  - `tests/unit/test_serving_contract.py` (3 tests): the publish columns match the DDL, only `customer_profile` holds PII, and the masked view exposes only 5 columns.
  - `tests/spark/test_publish.py` (2 tests).
  - `tests/integration/test_serving_access.py` (30 tests, `make access-test`): logs in as each role.
- `docs/access_control.md` (access matrix) and ADR 009.
- Configuration: `.env.example` gained `ANALYST_PASSWORD` and `ENGINEER_PASSWORD`, and the local `.env` had the same placeholders appended. Compose passes `SERVING_PG_DSN` and both passwords to Airflow and mounts `serving/`.

**Verification result:**
- **Tests:** `make test-container` gives 34 passed (29 Spark, 5 DAG integrity, with `publish` in the chain). Locally, the contract tests pass and the 30 integration tests skip without a database.
- **`make publish date=2026-10-04`** (13 s wall, job 8.8 s): applied the 3 SQL files and published mart_daily_spend 650, mart_advance_repayment 3, mart_customer_features 5,712 and customer_profile 2,025 rows.
- **`make access-test`:** **30 passed.**
  - The analyst reads the 3 marts and the masked view, and every `name_masked` matches `^\S\*\*\*$`.
  - The analyst is denied `customer_profile` for `*`, `customer_id`, `full_name`, `email`, `phone` and `date_of_birth`, and `has_column_privilege` is false for all 4 PII columns.
  - The objects the analyst can read are exactly the 3 marts plus the masked view, with no PII column among them.
  - The analyst can't INSERT, DELETE, CREATE (in `serving` or `public`) or `SET ROLE engineer`.
  - Both roles get `permission denied for database` on `source`, `iceberg_catalog` and `airflow`.
  - The engineer reads all PII columns, and UPDATE, TRUNCATE and CREATE are denied.
  - The first run had 1 failure, caused by a bug in my test query (`has_table_privilege` was evaluated on `serving.pg_statistic`). After the fix, all 30 passed.
- **`\dp serving.*`** shows `customer_profile` with `engineer=r` only. The marts and the view have `analyst=r` and `engineer=r`.
- **Same customer, two roles:**
  - engineer: `C000001 | Siva a/l Subramaniam | siva.subramaniam3700@example.org | 25_34 | Pahang`
  - analyst: `C000001 | S*** | 25_34 | Pahang | 2k_4k`
- **Re-publishing 2026-10-04:** row counts and an MD5 over all rows were identical for all 4 serving tables.
- **`make run date=2026-10-05`:** all **7** tasks `success`, including `publish` (660 / 3 / 5,787 / 2,030 rows), in 237 s wall. `make access-test` afterwards: 30 passed.

## D3-MID: time-travel demo, maintenance task

**What was built:**
- `jobs/demo/time_travel.py` (`make demo-time-travel [date=D]`): an audit of `gold.fact_transactions`.
  - It finds the snapshot built for business date D (the newest snapshot with `max(_logical_date) = D`).
  - It reads it `VERSION AS OF` and `TIMESTAMP AS OF`, and compares D's rows, completed spend and pending count then and now.
  - It lists status changes since that snapshot.
  - The pure helpers are in `jobs/demo/audit.py`.
- `jobs/demo/schema_evolution.py` (`make demo-schema-evolution`): the schema id change from the metadata log, the current columns, and old and new rows. `make demo` runs both demos.
- `jobs/maintenance/run.py` (`make maintenance [retain=N]`): for every lake table, `rewrite_data_files`, `rewrite_manifests`, then `expire_snapshots(retain_last=5)`.
  - It measures snapshots, current data files, files referenced by any snapshot, and physical MinIO objects before and after.
  - Results go to `lake.ops.maintenance_log`, plus a row in `ops.pipeline_runs`.
- 2 Spark unit tests in `tests/spark/test_demo_audit.py`. ADR 010.

**Verification result:**
- **Tests:** `make test-container` gives 36 passed (31 Spark, 5 DAG). Ruff is clean.
- **`make demo`** (25 s wall, before maintenance):
  - The time-travel audit of 2026-10-04 used snapshot `2244984067148293756` (committed 11:04:53).
  - `VERSION AS OF` and `TIMESTAMP AS OF` both returned **105,813** rows; there are 107,300 now.
  - As reported then: 1,430 rows, completed spend **87,060.21 MYR**, 97 pending. Now: 1,430 rows, **89,979.40 MYR**, 50 pending. That's a restatement of **+2,919.19 MYR**.
  - 113 transactions changed status since that snapshot: 103 pending → completed, 5 → failed, 5 → reversed. One example: `T20260921001137` was pending then and completed now (updated 2026-10-05 02:18 UTC).
  - Schema evolution: the metadata log shows `schema_id=0` at 10:01:26 and `schema_id=1` at 10:01:32. Rows without `referral_source`: 36,174 (event dates 2026-08-01 to 09-13). Rows with it: 18,524 (09-14 to 10-05).
- **`make maintenance`, run 1** (96 s wall, 25 tables): physical data files went from **4,119 to 2,707**.
  - `silver.transactions`: current files **841 → 1**, physical files 2,000 → 1,141 (the remainder is referenced by the 5 retained snapshots).
  - `silver.accounts`, `customers`, `customers_pii` and `cash_advances`: about 230–241 current files → 1 each.
  - `gold.fact_transactions`: physical files 453 → 263. Its current 67 files are already one per day partition, so there was nothing to compact.
  - `ops.dq_results`: current files 88 → 1.
  - Snapshots were trimmed to 5 per table.
  - Physical object counts equal referenced counts for every table, so **no orphan files** were left.
- **`make maintenance`, run 2** (49 s): converged. Physical files 2,708 → 2,678, only from compacting the run log's own new rows.
- **After maintenance:**
  - The time-travel demo still found the 10-04 snapshot (among the 5 retained) and gave identical figures.
  - Silver DQ: 32/32 passed in **18.9 s**, against 28.5–34.5 s in earlier runs. Gold DQ: 20/20 passed in **20.4 s**, against 24.3–30.7 s. Single samples, not a benchmark.
- **`lake.ops.maintenance_log`** holds 50 rows (25 tables × 2 runs). The before and after sums are 6,827 and 5,385 physical data files.

## D3-MID follow-up: write efficiency (shuffle partitions, MERGE only changed rows)

**What changed:**
- `spark.sql.shuffle.partitions=8` (`SPARK_SHUFFLE_PARTITIONS`).
- `merge_latest` now MERGEs only new or newer rows and skips the MERGE when there are none. It returns `(inserted, updated)` and replaces `merge_preview`.
- The `dim_customer` MERGE does the same by row hash.
- `silver.app_events` uses `insert_new` (anti-join plus append).
- 3 new Spark tests in `tests/spark/test_iceberg_writes.py`. ADR 011.

**Verification result.** Full DAG runs on comparable days, with writes summed from every lake table's snapshot summaries.

| | Old code, 2026-10-06 run | New code, 2026-10-07 run | Old re-run | New re-run |
|---|---|---|---|---|
| Data files added | 1,074 | **57** | 329 | **10** |
| Rows written | 237,428 | 78,467 | 229,307 | 9,125 |
| Rows rewritten | 224,378 | 65,586 | 222,811 | 2,600 |
| DAG wall time | 222 s | 170 s | 250 s | 248 s |

- `gold.fact_transactions` went from 68 files / 108,830 rows to 11 files / 18,423 rows per day.
- `silver.transactions` went from 200 files / 108,830 rows to 8 files / 47,863 rows.
- Other silver tables went from about 200 files to 5–8.
- On the new re-run, every silver and gold MERGE table committed **no snapshot**.
- Both runs succeeded with the DQ gates passing, including the reconciliation.
- `make test-container`: 39 passed.

## D3-PM: unit tests, DAG import test, GitHub Actions, pre-commit

**What was built:**
- `.github/workflows/ci.yml`, triggered on every push and pull request:
  - `lint` job: ruff check and format check.
  - `test` job: Temurin 17 and Python 3.11, installing Airflow 3.3.2, PySpark 3.5.9 and the dev tools under the official Airflow constraints (the same pins as the image), then `pytest`.
  - Actions are pinned to release tags, permissions are read-only, and superseded runs are cancelled.
- `.pre-commit-config.yaml`: ruff-check and ruff-format (v0.17.0), plus trailing-whitespace, end-of-file, check-yaml, merge-conflict, large-file and private-key hooks (v6.0.0). `pre-commit==4.6.2` added to `requirements-dev.txt`.
- Missing transformation unit tests added:
  - `tests/spark/test_bronze_load.py` (`add_ingestion_metadata`, `events_dataframe`);
  - `conform_accounts` and `conform_cash_advances` in `test_silver_transforms.py`.

  Every function in the `transforms`, `audit`, `rules`, `checks` and `reconcile` modules, plus bronze's metadata helpers, now has Spark unit tests. The catalog-bound bronze functions (watermark, partition write) are covered by the end-to-end runs.
- DAG test fix: it read DAGs with `DagBag.get_dag()`, which in Airflow 3 queries the metadata database. It now uses `DagBag.dags` (in memory).
- ADR 012.

**Verification result:**
- **Local CI stand-in.** The workflow steps ran in a clean `python:3.11.16-slim-bookworm` container with `openjdk-17-jre-headless` (17.0.21), against a clean copy of the repo with no `.env`.
  - **First run: 3 DAG tests failed** with `sqlite3.OperationalError: no such table: dag`. That's the `get_dag()` issue above, which the Airflow container hid because its metadata database exists.
  - **After the fix, from scratch:** ruff `All checks passed!` and `85 files already formatted`; install 195 s; pytest **69 passed, 30 skipped** (the access tests, which need the live stack) in 97.7 s; 344 s wall in total.
- **`pre-commit run --all-files`:** all 8 hooks `Passed`, with no files modified.
- **Other suites:**
  - `make test-container` (Airflow container): **43 passed**.
  - `make access-test`: **30 passed**.
  - Local `pytest`: 26 passed, 30 skipped.
- **Not verified:** the workflow hasn't run on GitHub Actions. The repository has no GitHub remote, and the GitHub CLI isn't installed. The SPEC's "Done when: CI passes" is met only by the local stand-in until it's pushed.

## D3-EVE: README, architecture diagram, data dictionary, ADRs, runbook, measured metrics

**What was built:**
- `README.md` rewritten:
  - a pitch and an architecture diagram (Mermaid);
  - a "What it demonstrates" table mapping features to the SPEC §1 skills (chosen by the project owner; no job posting is in the repo);
  - prerequisites, a quick start, measured results only, Make targets, a short how-it-works section, project layout, tests and CI.
- `docs/architecture.md`: data-flow, DAG and runtime diagrams (Mermaid), a step-by-step idempotency table and a layer table.
- `docs/data_dictionary.md`: every source, MongoDB, bronze, quarantine, silver, gold, ops and serving table, with columns and types from the live schemas, keys, grain, PII markers and failure reasons.
- `docs/runbook.md`: start and stop, daily runs, re-runs and backfills, DQ failure triage, quarantine review, serving and password rotation, maintenance, troubleshooting, fresh start.
- ADRs:
  - 013, MinIO and the AWS migration path (SPEC key decision 1, could-have 10);
  - 014, the Iceberg JDBC catalog (key decision 2);
  - 015, Spark local mode in Airflow (key decision 3);
  - `docs/decisions/README.md`, an index of all 15.
- `docker-compose.yml` no longer hard-codes the project `name`. The project name is the folder name, so a clone elsewhere gets its own volumes.
- **Fix:** `quality/run.py` replaced a run's DQ results with `DELETE … WHERE run_id = X`. Because `dq_silver` and `dq_gold` share the Airflow run id, `dq_gold` deleted the silver results. The delete now also filters on the suite's own tables.

**Verification result:**
- **All 4 Mermaid diagrams** parse and render to SVG with Mermaid 11.4.1, checked in a browser. The README diagram's subgraph title was shortened after it overlapped a node.
- **Fresh copy following only the README.** The working tree was copied to a new folder, without `.env`, as its own Compose project with its own volumes. The main stack was stopped (data kept) to free the ports. All 8 quick-start steps exited 0:

  | Step | Wall time | Result |
  |---|---|---|
  | `make up` | 37 s | cached image layers |
  | `make smoke` | 19 s | |
  | `make seed` | 15 s | fingerprint `10236acea734ec3e158b3f82776a3216`, identical |
  | `make run date=2026-09-29` | 178 s | 7/7 tasks, both DQ gates passed, reconciliation 0 |
  | `make run date=2026-09-30` | 171 s | same |
  | `make access-test` | 1 s | 30 passed |
  | `make demo` | 24 s | |
  | `make maintenance` | 49 s | 26 tables; physical data files 292 → 289 |

  The demo's time-travel audit showed 2026-09-29 completed spend at 91,960.60 MYR as first reported, against 95,201.51 now: +3,240.91 from 99 status changes. Schema evolution showed `schema_id` 0 → 1, with 36,174 events without `referral_source` and 14,429 with it.
- **The fresh-run metrics exposed the DQ results bug:** `ops.dq_results` held 20 rows per DAG run instead of 52. After the fix, on the fresh stack, running silver, gold, then silver again under one run id left **52 rows (32 silver, 20 gold)**.
- **Cleanup:** the fresh copy's stack and its 3 volumes were removed, and the main stack was restarted with its data.
- **First-build time:** the 491 s for a first-ever `make up` is the D1-AM measurement. The fresh-copy run reused cached image layers, so it doesn't re-measure a cold build.

## Follow-up: per-defect-type counts in `ops.dq_results` (SPEC §9 acceptance)

**What changed:**
- New check kind `quarantine_reason` (`quality/checks.py`). For each injected defect type it records that day's quarantined row count in `failed_rows`, and warns when that type exceeds a share of the day's bronze rows. The thresholds are 2% for defects, and 10% for `superseded_by_late_update` audit rows.
- The silver suite gained 7 `warn` checks (`quality/suites.py: DEFECT_TYPES`): `duplicate_row`, `null_amount`, `negative_amount`, `unknown_category`, `orphan_account` and `superseded_by_late_update` for transactions, plus `duplicate_row` for app events. That brings the suite from 32 to 39 checks.
- 2 Spark tests:
  - the count logic (multi-reason rows, other days excluded);
  - every defect type the generator injects has a check.
- ADR 006 updated.

**Verification result:**
- **`make dq layer=silver date=2026-09-29`** (initial load): 39/39 passed in 31.3 s. Recorded counts were `duplicate_row` 584, `null_amount` 397, `negative_amount` 416, `unknown_category` 395, `orphan_account` 371, `superseded_by_late_update` 0 (the initial load has no earlier versions) and `app_events` `duplicate_row` 253. Those equal the defects the generator reported injecting at seed.
- **`make dq layer=silver date=2026-10-07`:** 39/39 passed in 32.6 s. Recorded 12 / 7 / 4 / 7 / 4 / 111 / 5 (late-update audit rate 6.91%, under 10%).
- **Independent cross-check:** a direct count from `lake.quarantine.*` by `_quarantine_date` and exploded reason equals the `failed_rows` recorded in `ops.dq_results` for **all 14 checks** on both dates.
- **Tests:** `make test-container` gives 45 passed.

## Could-haves 9 and 11: Streamlit dashboard, 7-day backfill demo

**What was built:**
- `dashboard/app.py` plus `docker/dashboard/`: a `dashboard` Compose service (Streamlit 1.65.0, Altair 6.3.0, pandas 3.0.6, psycopg2-binary 2.9.13; `pip check` at build; 851 MB image), on `127.0.0.1:8501`.
  - It reads the serving database **as `analyst`**.
  - The page has:
    - KPIs as plain numbers: completed spend, transactions, average per day, and the repayment rate of the oldest due cohort;
    - charts: completed spend per day (line), spend by category (bars), and the repayment rate among due advances per cohort;
    - tables: the cohort table, customer features by month, and 10 rows of the masked customer view.
  - It's read-only, with the developer toolbar hidden and usage statistics off.
- `make backfill from=D1 to=D2`: one `airflow dags test` per business date, in order, stopping at the first failure.
- `DASHBOARD_PORT` in `.env.example`. Runbook, README and ADR 016 updated.

**Findings while building the backfill** (ADR 016):
- `airflow backfill create --from-date 2026-10-08 --to-date 2026-10-14` (dry run) reads plain dates as UTC. It produced business dates **10-09 to 10-14, 6 runs, skipping 10-08**. Malaysia-time boundaries (`T00:00:00+08:00`) give exactly 10-08 to 10-14.
- The real native backfill created 7 runs that stayed **`queued`**, because the scheduler only queues tasks for unpaused DAGs (`scheduler_job_runner.py`: `.where(~DM.is_paused)`). Unpausing would also start the daily schedule and process the current day out of order.
  - The backfill was cancelled through `PUT /api/v2/backfills/1/cancel`. Its 7 runs were marked `failed` without any task running, so no data changed.
  - `make backfill` was then implemented as sequential `dags test` runs.

**Verification result:**
- **`make backfill from=2026-10-08 to=2026-10-14`:** exit 0 in **1,657 s** wall.

  | Business date | State | Wall time |
  |---|---|---|
  | 2026-10-08 | success | 290 s (dashboard image building at the same time) |
  | 2026-10-09 | success | 194 s |
  | 2026-10-10 | success | 177 s |
  | 2026-10-11 | success | 181 s |
  | 2026-10-12 | success | 207 s |
  | 2026-10-13 | success | 360 s |
  | 2026-10-14 | success | 247 s |

  - `ops.pipeline_runs` shows 7/7 successful tasks for every date.
  - `ops.dq_results` shows **59 checks per date, all passed** (39 silver plus 20 gold). That confirms in a live DAG that silver results are no longer deleted by `dq_gold`.
  - Bronze has partitions for every date from 10-08 to 10-14, and `gold.fact_transactions` reaches `txn_date` 2026-10-14.
- **Dashboard,** checked in the built-in browser at 1400×1000:
  - Every section rendered. KPIs for the default 30 days were completed spend MYR 3,334,805, 41,284 transactions, MYR 111,160 per day, and 82.5% repayment for the oldest due cohort.
  - The line-chart tooltip showed date, spend and transactions.
  - The masked table showed initials only (`S***`, `L***`, …).
  - Two fixes after the first look: the truncated `food_and_beverage` axis label (`labelLimit`), and the developer toolbar (`toolbarMode = "viewer"`). Customer-feature averages are now rounded.
  - The health endpoint returns HTTP 200.
- **Tests:** `make test-container` 45 passed, `make access-test` 30 passed, local `pytest` 26 passed and 30 skipped, ruff and all pre-commit hooks clean.

## Follow-up: merge-on-read for `silver.transactions` (2026-10-27)

**What changed:**
- `MERGE_ON_READ` table properties plus `properties_to_set` and `ensure_properties` in `jobs/common/iceberg.py`. `merge_latest` gained a `properties` argument, and the transactions entity declares `table_properties=MERGE_ON_READ`.
- Maintenance: `rewrite_data_files` with `delete-file-threshold=1` and `remove-dangling-deletes=true`, plus `rewrite_position_delete_files`. Delete files are counted before and after, and new columns are added to `ops.maintenance_log` automatically.
- 2 Spark tests (`tests/spark/test_iceberg_writes.py`).
- ADR 011 updated.

**Verification result:**
- **Docker restart first:** Docker Desktop had quit overnight, so the containers had exited. `make up` restarted them in 32 s with all data intact.
- **Baseline, copy-on-write, `make run date=2026-10-16`** (205 s): `silver.transactions` added 8 data files, removed 10, and wrote **95,287 rows while rewriting 93,561**. All tables together wrote 130,225 rows.
- **Merge-on-read, `make run date=2026-10-17`** (194 s): `silver.transactions` added **1 data file, 0 removed, and 11 delete files**; it wrote **1,898 rows and rewrote 0**. All tables together wrote 30,936 rows. Both DQ gates passed, and all reconciliations were `mismatched_groups=0`.
- **Same-date re-run** (184 s): no silver or gold table committed anything.
- **Table properties:** `SHOW TBLPROPERTIES` shows all three `write.*.mode = merge-on-read`.
- **`make maintenance`:** `silver.transactions` had 63 data files and 11 delete files, and ended with **1 data file and 0 delete files**. Rows were 126,323 both before and after, with 126,323 distinct `txn_id`. Lake-wide physical data files went from 5,225 to 1,679. `ops.maintenance_log` gained `current_delete_files_before/after` and `rewritten_delete_files`.
- **Silver DQ after maintenance:** 39/39 passed.
- **Tests:** `make test-container` gives 47 passed.

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
- **Spark tests run only in the container:** the silver transforms have Spark unit tests, but they run only in the container (`make test-spark`). The bronze Spark functions are covered only by end-to-end runs. Running Spark tests in CI comes in D3-PM.
- **Late-update auditing starts on day two:** `superseded_by_late_update` is detected only from the second loaded day onward, because the initial full load contains only the latest versions (ADR 005).
- **Silver run time:** silver takes 70–85 s per day. Most of it is Spark start-up and one MERGE per table. Acceptable at this scale.
- **Empty snapshots:** a no-op MERGE (a re-run) still commits an empty Iceberg snapshot.
- **DQ run time:** the silver DQ suite takes 40–46 s, because each check is a separate Spark action (ADR 006).
- **Demo DAG removed:** the D2-MID demo DAG `ledgerlake_dq_gate_demo` was removed in D2-EVE, and `make dq-gate-demo` now runs `ledgerlake_daily` with `simulate_defect`. The test command `make test-spark` was renamed `make test-container`.
- **Driver-side publish:** publish collects the serving tables to the Spark driver. That's fine for about 8.4k rows; larger volumes would need COPY plus a table swap (ADR 009).
- **PII copy:** `customer_profile` is a second copy of personal data, readable by `engineer` only and rebuilt on every publish.
- **Access-control scope:** access control covers the serving database only. Lake storage (MinIO, Iceberg) uses platform credentials that analysts don't have.
- **`silver.transactions` delete files:** the daily rewrite was fixed with merge-on-read (ADR 011 follow-up). Delete files then build up, about 11 a day, until `make maintenance` compacts them. Maintenance is a manual command, so run it regularly.
- **Mart history:** gold marts are rebuilt with `createOrReplace`, so expiry keeps only their latest snapshot (7 → 1). Time travel on marts isn't retained; the audit demo uses `fact_transactions`.
- **Limited time-travel window:** time travel reaches only the last 5 snapshots per table after maintenance.
- **Shared run id:** manual maintenance runs share `run_id = manual` in `ops.maintenance_log`; `maintained_at` tells them apart.
- **Maintenance isn't scheduled:** it's a manual `make` command, not part of `ledgerlake_daily`.
- **Lost silver DQ results in earlier DAG runs:** DAG runs before the D3-EVE fix (D2-EVE through D3-MID) lost their `dq_silver` rows in `ops.dq_results`. The gate itself worked: `dq_silver` passed or failed before `dq_gold` ran.
- **Native backfill needs an unpaused DAG:** `make backfill` (sequential DAG tests) is verified over 7 dates. Airflow's native `backfill create` was tested and needs an unpaused DAG (ADR 016). The cancelled attempt left 7 `failed` backfill runs in the Airflow history.
- **Stale demo DAG entry:** Airflow's metadata database still lists `ledgerlake_dq_gate_demo`, whose file was deleted in D2-EVE. It's paused and has no file, so it's harmless; `airflow dags delete ledgerlake_dq_gate_demo` would remove it.
- **No dashboard tests:** the dashboard has no unit tests (presentation only; the queries are plain SQL over the serving tables). It was checked in the browser.
- **No GitHub CI run yet:** CI hasn't run on GitHub Actions (no remote); it passed in a local clean-container stand-in.
- **Acceptance gap (SPEC §9, third criterion): closed** by the per-defect-type checks below.
- **Run time:** a full DAG run takes about 3.5 minutes, because each task starts its own Spark driver.
- **Coarse tick duration:** `generate_tick` records `duration_s` from bash `$SECONDS`, which has 1-second resolution.
- **Manual runs need the business date:** manual runs without `business_date` take the data interval Airflow infers from the trigger time. Seen in the test: the 10-24 to 10-25 interval for a run triggered on 2026-10-26. Always pass `business_date` for manual runs; `make run` does this.
- **Gold uses current silver:** gold is rebuilt from current silver, so a backfill of an older date doesn't produce point-in-time marts (ADR 007).
- **Short history:** `income_regularity_3m` is low for the first months, because there are fewer than 3 months of history.
- **Slow tests:** the generator tests take about 50 s because they run the in-memory seed three times.
- **No `ops.pipeline_runs` yet:** that table and the run metrics written to it come with the Airflow DAG in D2-EVE.
