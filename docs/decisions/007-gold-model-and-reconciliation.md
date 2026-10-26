# ADR 007: Gold model, MERGE strategy and mart reconciliation

**Status:** accepted (D2-PM, 2026-10-26)

## Context
SPEC §5.4 asks for:
- `fact_transactions`, partitioned by `days(txn_ts)`;
- `dim_customer`, upserted with Iceberg MERGE INTO;
- `dim_date`;
- three marts: daily spend, monthly advance-repayment cohorts, and customer × month features.

D2-PM is done when mart totals reconcile with silver, checked by a test. SPEC §6 also asks for a gold freshness DQ check.

## Decision
- **Gold is built from the current silver state** on every run (`jobs/gold/run.py --date D`). The logical date sets the as-of date for advance repayment and the freshness check. Every silver row at or before D is already merged, so a daily run and a re-run produce the same gold.
- **`dim_customer` is SCD type 1, upserted with MERGE on a row hash.**
  - `_row_hash` = SHA-256 over the business attributes: age band, state, income band, customer-since date, account count, has-savings flag, active-account count.
  - `WHEN MATCHED AND t._row_hash <> s._row_hash THEN UPDATE`. A row changes when *derived* attributes change too, such as an account going dormant, even if the customer's `updated_at` didn't move.
  - The dimension has no PII. Names and contacts stay in `silver.customers_pii` for the engineer-only serving table (D3-AM).
- **`fact_transactions`** is upserted with `merge_latest` (a newer `updated_at` wins) and partitioned by `days(txn_ts)`. It adds `customer_id`, `account_type` and `date_key` from accounts. It's an inner join, so a transaction without an account can't enter gold; silver's quarantine already removes them.
- **`dim_date`** is a fixed calendar from 2024-01-01 to 2027-12-31: ISO day of week, ISO week, weekend flag, month start. It's created once.
- **Marts** are rebuilt in full every run (`createOrReplace`). That's deterministic and idempotent, and they're small (hundreds to thousands of rows). Iceberg keeps the previous versions as snapshots.
  - **Shared definitions** (`jobs/gold/transforms.py`): *spend* is completed debits, *salary* is completed salary credits, and months and days are Malaysia business dates.
  - `mart_daily_spend`: day × category, with total, average, count and distinct customers.
  - `mart_advance_repayment`: by request-month cohort. It has counts, amounts, repaid / overdue / active counts and `repayment_rate`. `repayment_rate_due` counts only advances due by the as-of date, so young cohorts look fair. `avg_days_late` is the mean of max(0, repaid − due) over repaid advances.
  - `mart_customer_features`: customer × month. Spend total, count and days; daily-spend standard deviation and coefficient of variation; salary credits; `income_regularity_3m`, the share of the last 3 *calendar* months with a salary, using a date-range window rather than a row window; and advance count and amount.
- **Reconciliation** (`quality/reconcile.py`) recomputes each mart's measures directly from silver and counts mismatched groups. A group mismatches when it's missing on either side or any measure differs, compared null-safe with exact decimals:
  - daily spend: `total_spend` and `txn_count` per day × category;
  - advance repayment: count, amount and repaid count per cohort;
  - customer features: monthly spend, count, advance count and advance amount. Months with only salary credits have all-zero measures, so they're excluded.

  This runs in two places:
  - **Spark unit tests** check that marts built from fixtures reconcile, and that a tampered mart or an orphan are caught.
  - The **gold DQ suite** (20 checks, all `error`) runs it on every run as `reconcile_with_silver`. The suite also covers keys, referential integrity to `dim_customer` and `dim_date`, and **gold freshness** (`txn_date` within 1 day). Freshness is `error` because stale marts must not be published.

## Alternatives considered
- **Incremental marts (MERGE per day):** more moving parts for tables this small, and harder to keep exactly reconciled after late-arriving updates.
- **SCD type 2 for `dim_customer`:** keeps history, but SPEC asks for an upsert, and the marts don't need point-in-time customer attributes.
- **Reconciling only grand totals:** compensating errors between groups would cancel out. Per-group comparison catches them.

## Consequences
- A gold run takes about 40 s. A no-op MERGE still commits an Iceberg snapshot (`overwrite` with nothing changed).
- Backfilling an older date rebuilds marts from current silver, not as of that date. That's acceptable for this project; point-in-time marts would need silver time travel (`VERSION AS OF`).
- `income_regularity_3m` is low for the first months of history because there are fewer than 3 prior months.
