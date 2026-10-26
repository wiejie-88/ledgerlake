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
- **No publish task yet:** `ledgerlake_daily` has no `publish` task until D3-AM.
- **Run time:** a full DAG run takes about 3.5 minutes, because each task starts its own Spark driver.
- **Coarse tick duration:** `generate_tick` records `duration_s` from bash `$SECONDS`, which has 1-second resolution.
- **Manual runs need the business date:** manual runs without `business_date` take the data interval Airflow infers from the trigger time. Seen in the test: the 10-24 to 10-25 interval for a run triggered on 2026-10-26. Always pass `business_date` for manual runs; `make run` does this.
- **Gold uses current silver:** gold is rebuilt from current silver, so a backfill of an older date doesn't produce point-in-time marts (ADR 007).
- **Short history:** `income_regularity_3m` is low for the first months, because there are fewer than 3 months of history.
- **Slow tests:** the generator tests take about 50 s because they run the in-memory seed three times.
- **No `ops.pipeline_runs` yet:** that table and the run metrics written to it come with the Airflow DAG in D2-EVE.
