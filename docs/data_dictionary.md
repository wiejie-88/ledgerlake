# Data dictionary

Column names and types are taken from the live tables (2026-10-26). Lake tables are in the Iceberg catalog `lake`; Postgres tables are in the `source` and `serving` databases.

**Conventions**
- **Business key:** the first column in each table (marked 🔑).
- **Time:** source Postgres timestamps are naive **Malaysia time (UTC+8)**. From silver onward, every `*_ts` and `*_at` column is **UTC**. `txn_date` and `event_date` are Malaysia business dates.
- **Money:** amounts are MYR, `decimal(12,2)`; sums are wider decimals.
- **Personal data (PII)** is marked 🔒. Only `source.customers`, `bronze.customers`, `quarantine.customers`, `silver.customers_pii` and `serving.customer_profile` contain it (see [access_control.md](access_control.md)).

## Source — Postgres `source` (synthetic, written by `generator/`)

| Table | Grain | Columns |
|---|---|---|
| `customers` | one customer | 🔑 `customer_id` text (`C000001`), 🔒 `full_name`, 🔒 `email` (reserved `example.*` domains), 🔒 `phone` (`+60 1X-…`), 🔒 `date_of_birth` date, `state` (16 Malaysian states and territories), `income_band` (`below_2k`, `2k_4k`, `4k_6k`, `6k_10k`, `above_10k`), `created_at`, `updated_at` timestamp |
| `accounts` | one account | 🔑 `account_id` text (`A0000001`), `customer_id` → customers, `account_type` (`spending`, `savings`), `status` (`active`, `dormant`, `closed`), `opened_at`, `updated_at` |
| `transactions` | one transaction **version**, with no primary key (injected duplicates and orphans are allowed) | 🔑 `txn_id` text (`T20260929000123`), `account_id`, `txn_ts` timestamp, `amount` numeric(12,2) (nullable: injected defect), `direction` (`debit`, `credit`), `merchant_name`, `category` (11 accepted values: `groceries`, `food_and_beverage`, `transport`, `utilities`, `telco`, `shopping`, `entertainment`, `health`, `education`, `transfer`, `salary`), `channel` (`card`, `duitnow_qr`, `fpx`, `ewallet`, `transfer`), `status` (`completed`, `pending`, `failed`, `reversed`), `updated_at` |
| `cash_advances` | one advance | 🔑 `advance_id` text (`ADV000001`), `customer_id`, `amount` numeric(12,2), `requested_at`, `due_date` date (requested + 30 days), `repaid_at` (nullable), `status` (`active`, `repaid`, `overdue`), `updated_at` |
| `generator_log` | one generated day | 🔑 `run_date` date, `kind` (`base`, `seed`, `tick`), `inserted`, `updated`, `defects` jsonb (counts per table and defect type), `generated_at` timestamptz |

**Injected defects** (about 2.2% of transaction rows, at most one per row): duplicate row, null amount, negative amount, unknown category (`unknown`, `crypto_exchange`, `MISC`), orphan account (`A9…`). Also about 0.5% duplicate app events, and late-arriving updates (pending transactions settle 1–14 days later).

## Source — MongoDB `app.app_events`

| Field | Type | Meaning |
|---|---|---|
| 🔑 `event_id` | string (UUID) | Event id; about 0.5% of documents are injected duplicates |
| `customer_id` | string | → `customers` |
| `event_type` | string | `login`, `view_offer`, `click_offer`, `apply_offer` |
| `event_ts` | date (UTC) | When the event happened |
| `device` | {`os`, `app_version`} | `android`, `ios`, `harmonyos`; `4.x.y` |
| `properties` | object | `offer_id` on offer events. **Schema drift:** from 2026-09-14 (day 45) every event also has `referral_source` (`organic`, `friend_referral`, `social_media`, `search_ads`, `partner`) |

## Bronze — `lake.bronze.*` (raw, append by partition)

Same columns and types as the source (Postgres timestamps kept as `timestamp_ntz`; MongoDB `device` as `struct<os,app_version>` and `properties` as `map<string,string>`), plus:

| Column | Type | Meaning |
|---|---|---|
| `_ingested_at` | timestamp | When the extract ran (UTC) |
| `_source` | string | e.g. `postgres:source.public.transactions`, `mongodb:app.app_events` |
| `_batch_id` | string | Airflow `run_id`, or `manual__<uuid>` |
| `_logical_date` | date | **Partition column.** The business date whose extract window produced the row |

Tables: `customers`, `accounts`, `transactions`, `cash_advances`, `app_events`. A row appears in partition D if its `updated_at` (or `event_ts`) was after the high-water mark of earlier partitions and before the end of D.

## Quarantine — `lake.quarantine.*`

Bronze columns of the rejected row, plus:

| Column | Type | Meaning |
|---|---|---|
| `failure_reason` | string | One or more of `missing_key`, `null_amount`, `negative_amount`, `zero_amount`, `unknown_category`, `invalid_direction`, `invalid_status`, `invalid_account_type`, `invalid_income_band`, `orphan_account`, `orphan_customer`, `missing_event_ts`, `unknown_event_type`, `duplicate_row`, `superseded_in_batch`, separated by `; `. Also `superseded_by_late_update`, which is an **audit** row: the older version a late update replaced, not a data failure |
| `_quarantine_date` | date | **Partition column.** The silver run that quarantined the row |
| `_quarantined_at` | timestamp | When it was quarantined |

## Silver — `lake.silver.*` (one row per business key)

All silver tables add `_logical_date` (the last silver run that wrote the row) and `_processed_at`.

| Table | Columns |
|---|---|
| `customers` | 🔑 `customer_id`, `email_hash` (SHA-256 of `PII_SALT` + lower(trim(email))), `phone_hash` (SHA-256 of salt + digits only), `age_band` (`under_18`, `18_24`, `25_34`, `35_44`, `45_54`, `55_64`, `65_plus`, `unknown`; as of the logical date), `state`, `income_band`, `created_at`, `updated_at` (UTC) |
| `customers_pii` 🔒 | 🔑 `customer_id`, `full_name`, `email`, `phone`, `date_of_birth`, `updated_at` — the only silver table with personal data |
| `accounts` | 🔑 `account_id`, `customer_id`, `account_type`, `status`, `opened_at`, `updated_at` (UTC) |
| `transactions` | 🔑 `txn_id`, `account_id`, `txn_ts` (UTC), `txn_date` (Malaysia business date), `amount` decimal(12,2) (> 0), `direction`, `merchant_name`, `category`, `channel`, `status`, `updated_at` (UTC) |
| `cash_advances` | 🔑 `advance_id`, `customer_id`, `amount`, `requested_at`, `due_date`, `repaid_at`, `status`, `updated_at` |
| `app_events` | 🔑 `event_id`, `customer_id`, `event_type`, `event_ts` (UTC), `event_date` (Malaysia), `os`, `app_version`, `offer_id`, `referral_source` (**added by schema evolution**; NULL before 2026-09-14). Partitioned by `days(event_ts)` |

## Gold — `lake.gold.*`

| Table | Grain | Columns |
|---|---|---|
| `fact_transactions` | one transaction; partitioned by `days(txn_ts)` | 🔑 `txn_id`, `account_id`, `customer_id`, `account_type`, `txn_ts`, `txn_date`, `date_key` int (yyyymmdd → `dim_date`), `amount`, `direction`, `merchant_name`, `category`, `channel`, `status`, `updated_at`, `_logical_date`, `_built_at` |
| `dim_customer` | one customer, SCD type 1, no PII | 🔑 `customer_id`, `age_band`, `state`, `income_band`, `customer_since` (Malaysia date), `account_count`, `has_savings_account`, `active_account_count`, `_row_hash` (SHA-256 of the attributes; drives the MERGE), `_logical_date`, `_built_at` |
| `dim_date` | one day, 2024-01-01 to 2027-12-31 | 🔑 `date_key`, `date`, `year`, `quarter`, `month`, `month_name`, `day_of_month`, `day_of_week` (ISO, Mon = 1), `day_name`, `iso_week`, `is_weekend`, `month_start` |
| `mart_daily_spend` | day × category | 🔑 (`txn_date`, `category`), `date_key`, `total_spend` decimal(22,2), `avg_spend` decimal(12,2), `txn_count`, `customer_count`. **Spend = completed debits** |
| `mart_advance_repayment` | monthly request cohort (Malaysia month) | 🔑 `cohort_month`, `advance_count`, `amount_total`, `repaid_count`, `overdue_count`, `active_count`, `due_count` (due by `as_of_date`), `due_repaid_count`, `late_repaid_count`, `avg_days_late` (mean of max(0, repaid − due) over repaid advances), `repayment_rate` (repaid / all), `repayment_rate_due` (repaid / due), `as_of_date` |
| `mart_customer_features` | customer × month | 🔑 (`customer_id`, `month`), `spend_total`, `spend_txn_count`, `spend_days`, `daily_spend_stddev`, `daily_spend_cv` (stddev / mean of daily spend), `salary_credit_count`, `salary_total`, `income_regularity_3m` (share of the last 3 calendar months with a salary credit), `advance_count`, `advance_amount` |

Every mart is reconciled against silver on every run (`reconcile_with_silver` in `dq_gold`).

## Ops — `lake.ops.*`

| Table | Grain | Columns |
|---|---|---|
| `watermarks` | source × entity | 🔑 (`source`, `entity`), `last_value` (ISO text: naive Malaysia time for Postgres, UTC for MongoDB), `updated_at` |
| `dq_results` | run × check | `run_id` (Airflow run id or `manual__<layer>__<date>`), `logical_date`, `table`, `check_name` (e.g. `not_null:amount`, `referential:account_id->silver.accounts.account_id`, `reconcile_with_silver`), `severity` (`error`, `warn`), `passed`, `failed_rows`, `details`, `checked_at` |
| `pipeline_runs` | task attempt | `run_id`, `dag_id`, `task_id`, `logical_date`, `rows_in`, `rows_out`, `duration_s`, `status` (`success`, `failed`, `up_for_retry`), `recorded_at`. rows_in / rows_out per task: generate_tick — / source rows inserted; extract_bronze rows extracted / partition rows written; silver rows read / valid rows merged; dq_* checks evaluated / checks passed; gold silver transactions read / dimension and fact changes plus mart rows; publish rows published / rows published |
| `maintenance_log` | maintenance run × table | `run_id`, `table`, `retain_last`; `snapshots_*`, `current_data_files_*`, `current_delete_files_*` (merge-on-read), `current_bytes_*`, `referenced_data_files_*`, `physical_data_files_*`, `physical_metadata_files_*` (each `_before` and `_after`); `rewritten_data_files`, `added_data_files`, `rewritten_delete_files`, `rewritten_manifests`, `expired_data_files_deleted`, `expired_manifest_lists_deleted`, `duration_s`, `maintained_at` |

## Serving — Postgres `serving.*`

| Object | Readable by | Columns |
|---|---|---|
| `mart_daily_spend` | analyst, engineer | as gold, plus `as_of_date` |
| `mart_advance_repayment` | analyst, engineer | as gold (with `as_of_date`) |
| `mart_customer_features` | analyst, engineer | as gold, plus `as_of_date` |
| `customer_profile` 🔒 | **engineer only** | 🔑 `customer_id`, `full_name`, `email`, `phone`, `date_of_birth`, `age_band`, `state`, `income_band`, `customer_since`, `as_of_date` |
| `v_customer_profile_masked` (view) | analyst, engineer | `customer_id`, `name_masked` (first letter + `***`), `age_band`, `state`, `income_band` |
