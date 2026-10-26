# LedgerLake — Project Specification

## 1. Summary
LedgerLake is a batch data platform for a fictional Malaysian consumer fintech app.
It extracts data from PostgreSQL (operational database) and MongoDB (app events) into an Apache Iceberg lakehouse stored on MinIO (S3-compatible storage).
PySpark processes the data through three layers: bronze (raw), silver (cleaned) and gold (business-ready). Airflow orchestrates the pipeline.
Data quality checks run after the silver and gold layers. Gold marts are published to a PostgreSQL serving schema with role-based access and masking of personal data.

Built to demonstrate: Iceberg, Spark, Airflow, SQL and NoSQL sources, data quality, data governance and access control, documentation, and reproducibility.

## 2. Requirements

### 2.1 Functional requirements
| ID | Requirement |
|---|---|
| F1 | Generate synthetic fintech data (customers, accounts, transactions, cash advances, app events) with deliberate defects. |
| F2 | Extract incrementally: Postgres by `updated_at` watermark, MongoDB by `event_ts` watermark. |
| F3 | Land raw data in Iceberg bronze tables with ingestion metadata. |
| F4 | Clean, deduplicate, type and conform data into silver tables. Hash personal data. |
| F5 | Build gold: one fact table, two dimensions, three marts. |
| F6 | Run DQ checks after silver and gold. Quarantine failed rows. Store results. |
| F7 | Orchestrate end to end in Airflow. Runs must be idempotent and support backfills. |
| F8 | Publish gold marts to a Postgres serving schema with role-based access (analyst: masked; engineer: full). |
| F9 | Demonstrate Iceberg schema evolution and time travel. |

### 2.2 Non-functional requirements
- One-command startup on a laptop with 16 GB RAM. Zero cost. No cloud account.
- All versions pinned. Secrets in `.env` only; `.env.example` committed.
- CI (GitHub Actions) runs lint and unit tests on every push.
- A new user can run the project end to end from the README in about 15 minutes.

### 2.3 Out of scope
Streaming, Trino, BI tools, AWS deployment, ML models, Kubernetes, real personal data.

## 3. Architecture

```
Postgres (source OLTP)     MongoDB (app_events)        Generator CLI (seed / tick)
        │ incremental              │ incremental
        ▼                          ▼
Airflow (standalone container; PySpark runs in local mode inside it)
  generate_tick → extract → bronze → silver → dq_silver → gold → dq_gold → publish
        │
        ▼
Iceberg tables on MinIO (s3://warehouse)
  bronze.* | silver.* | quarantine.* | gold.* | ops.*
  Catalog: Iceberg JDBC catalog stored in Postgres
        │ publish
        ▼
Postgres "serving" database — roles: analyst (masked views), engineer (full)
```

### 3.1 Containers
- `postgres`: one instance with four databases: `source`, `iceberg_catalog`, `serving`, `airflow`.
- `mongodb`: database `app`, collection `app_events`.
- `minio`: bucket `warehouse`; a bucket-creation step runs on startup.
- `airflow`: custom image with Java (required by Spark), PySpark, Iceberg JARs, project code. Runs `airflow standalone` with Postgres as the metadata database.

### 3.2 Spark and Iceberg wiring notes
- Required JARs: the Iceberg Spark runtime matching the Spark version, the Iceberg AWS bundle (for S3FileIO), and the PostgreSQL JDBC driver.
- S3FileIO points at the MinIO endpoint with path-style access enabled.
- Catalog name: `lake`. Namespaces: `bronze`, `silver`, `quarantine`, `gold`, `ops`.

### 3.3 Key decisions (each one gets an ADR)
1. MinIO instead of AWS S3. Same S3 API, so migrating to S3 with the Glue catalog is a configuration change. The ADR documents this migration path.
2. Iceberg JDBC catalog on Postgres: fewest moving parts.
3. Spark in local mode inside the Airflow container: no separate cluster. The same jobs would run on EMR or Glue.
4. Serving through Postgres roles and views: access control can be demonstrated in hours instead of days.
5. Custom lightweight DQ module in PySpark instead of a framework, to fit the 3-day timeline.

## 4. Technology stack
Python 3.11 · PySpark 3.5.x · Apache Iceberg · MinIO · Apache Airflow (standalone) · PostgreSQL · MongoDB · Faker · pytest · ruff · pre-commit · GitHub Actions · Docker Compose · Makefile

## 5. Data model

### 5.1 Sources (synthetic; currency MYR; Malaysian names, states and merchants)

**Postgres `source` database**
| Table | Columns |
|---|---|
| customers | customer_id, full_name, email, phone, date_of_birth, state, income_band, created_at, updated_at |
| accounts | account_id, customer_id, account_type (spending, savings), status, opened_at, updated_at |
| transactions | txn_id, account_id, txn_ts, amount, direction (debit, credit), merchant_name, category, channel, status, updated_at |
| cash_advances | advance_id, customer_id, amount, requested_at, due_date, repaid_at, status, updated_at |

**MongoDB `app.app_events`**
event_id, customer_id, event_type (login, view_offer, click_offer, apply_offer), event_ts, device {os, app_version}, properties {offer_id}.
Schema drift: events dated from day 45 of the history onward also include `properties.referral_source`.

**Volume:** about 2,000 customers, 60 days of history, about 100k transactions, about 50k events.

**Injected defects (about 2–3% of rows):**
- duplicate rows
- null amounts
- negative amounts
- unknown categories
- transactions referencing accounts that don't exist
- late-arriving updates (changed `updated_at` on old rows)

**Generator CLI**
- `seed`: creates the full history.
- `tick --date YYYY-MM-DD`: inserts one new day of data and updates some existing rows.
- Uses a fixed random seed, so output is reproducible.

### 5.2 Bronze (append-only)
Tables: `bronze.customers`, `bronze.accounts`, `bronze.transactions`, `bronze.cash_advances`, `bronze.app_events`.
Source columns plus `_ingested_at`, `_source`, `_batch_id` and `_logical_date`. Partitioned by `_logical_date`.
Idempotency: re-running a logical date replaces that date's partition.

### 5.3 Silver
- Deduplicate on the business key, keeping the latest `updated_at`.
- Cast types; convert timestamps to UTC.
- Personal data:
  - `email` and `phone` → SHA-256 with a salt from `.env`.
  - `date_of_birth` → `age_band`.
  - `full_name` exists only in `silver.customers_pii`.
- Failed rows go to `quarantine.<table>` with a `failure_reason` column.
- Schema evolution: `referral_source` is added to `silver.app_events` through Iceberg schema evolution, without rewriting the table.

### 5.4 Gold
| Table | Grain | Notes |
|---|---|---|
| fact_transactions | one transaction | partitioned by days(txn_ts) |
| dim_customer | one customer | upserted with Iceberg MERGE INTO |
| dim_date | one day | calendar dimension |
| mart_daily_spend | day × category | total and average spend, transaction count |
| mart_advance_repayment | monthly cohort | repayment rate, average days late |
| mart_customer_features | customer × month | spend volatility, income regularity, advance usage (ML-ready) |

### 5.5 Ops tables
- `ops.watermarks` (source, entity, last_value, updated_at)
- `ops.dq_results` (run_id, table, check_name, severity, passed, failed_rows, checked_at)
- `ops.pipeline_runs` (run_id, dag_id, task_id, logical_date, rows_in, rows_out, duration_s, status)

### 5.6 Serving (Postgres `serving` database)
- The three gold marts are published by overwriting the full table on each run.
- `customer_profile` (from `silver.customers_pii` joined to `dim_customer`): readable by engineer only.
- View `v_customer_profile_masked`: name shown as first letter plus `***`, plus age_band, state and income_band. Readable by analyst.
- Roles:
  - `analyst`: SELECT on the marts and the masked view only.
  - `engineer`: SELECT on everything.

## 6. Features

### Must have
1. Incremental, idempotent extraction and loading with watermarks.
2. Medallion transformations as pure, testable PySpark functions.
3. DQ checks:
   - not-null on keys and amounts
   - uniqueness of business keys
   - referential integrity (transaction → account → customer)
   - accepted values for status and category
   - `amount > 0`
   - gold freshness
   - Severity: `error` stops downstream tasks; `warn` logs and continues.
4. Airflow DAG `ledgerlake_daily` with retries and a failure callback that writes to `ops.pipeline_runs`.
5. Role-based access in the serving database, with a documented test proving what each role can and cannot read.

### Should have
6. Schema evolution demo.
7. Time-travel demo: query `fact_transactions` as of an earlier snapshot (audit use case).
8. Maintenance task: expire snapshots and compact files, recording file counts before and after.

### Could have (only if ahead of schedule)
9. A one-page Streamlit view of the marts.
10. An ADR describing the AWS migration path.
11. A documented 7-day backfill demo.

## 7. Repository layout
```
ledgerlake/
├── CLAUDE.md
├── docker-compose.yml
├── Makefile              # up, down, seed, tick, run, test, lint, demo
├── .env.example
├── docker/               # Airflow image Dockerfile, Postgres init scripts
├── generator/
├── jobs/                 # common/, extract/, bronze/, silver/, gold/, publish/, maintenance/
├── quality/
├── dags/
├── serving/              # roles, grants, views
├── tests/                # unit/, dags/
├── docs/
│   ├── SPEC.md
│   ├── PROGRESS.md
│   ├── architecture.md
│   ├── data_dictionary.md
│   ├── runbook.md
│   └── decisions/
└── README.md
```

## 8. Delivery plan (blocks)

| Block | Work | Done when |
|---|---|---|
| D1-AM | Scaffold, Docker Compose, Airflow image, Makefile, `.env.example`, pinned versions, PROGRESS.md | `make up` starts all services; a smoke test creates, writes and reads an Iceberg table on MinIO through the JDBC catalog |
| D1-PM | Synthetic generator (`seed`, `tick`) with defects | `make seed` fills Postgres and MongoDB; row counts and defect counts are logged |
| D1-EVE | Extract and bronze jobs with watermarks | Running bronze twice for the same date leaves row counts unchanged |
| D2-AM | Silver: dedupe, typing, PII hashing, quarantine, schema evolution | Every injected defect type appears in quarantine with a reason |
| D2-MID | DQ module and `ops.dq_results` | A failing `error` check stops downstream tasks |
| D2-PM | Gold fact, dimensions (MERGE), marts | Mart totals reconcile with silver (checked by a test) |
| D2-EVE | Airflow DAG `ledgerlake_daily` | A full DAG run succeeds; re-running the same date is idempotent |
| D3-AM | Publish step, serving roles, masked view, access test | The analyst role is denied access to personal-data columns and tables |
| D3-MID | Time-travel demo, maintenance task | Both run from documented `make` commands |
| D3-PM | Unit tests, DAG import test, GitHub Actions, pre-commit | CI passes |
| D3-EVE | README, architecture diagram, data dictionary, ADRs, runbook, measured metrics | A fresh clone runs end to end by following only the README |

**Fallbacks**
- If the Iceberg JDBC catalog wiring is still blocked after about 2 hours in D1-AM, switch to the Iceberg Hadoop catalog on MinIO and write an ADR explaining why.

**Cut order if behind schedule:** maintenance task → time-travel demo → `mart_customer_features`.
**Never cut:** idempotency, DQ checks, access control, README.

## 9. Acceptance criteria
- [ ] `make up && make seed && make run` completes on a clean machine.
- [ ] Re-running the same date gives identical gold row counts.
- [ ] Every injected defect type is quarantined and recorded in `ops.dq_results`.
- [ ] The analyst role cannot read raw personal data; the engineer role can.
- [ ] The schema evolution and time-travel demos run from documented commands.
- [ ] CI is green; every transformation function has unit tests.
- [ ] The README has an architecture diagram, quick start, a table mapping features to the job posting, and measured metrics only.
