-- Serving tables in the `serving` database. Applied on every publish; safe to re-run.
-- Column lists must match jobs/publish/contract.py (checked by tests/unit/test_serving_contract.py).

CREATE SCHEMA IF NOT EXISTS serving;

CREATE TABLE IF NOT EXISTS serving.mart_daily_spend (
    date_key       integer       NOT NULL,
    txn_date       date          NOT NULL,
    category       text          NOT NULL,
    total_spend    numeric(22,2) NOT NULL,
    avg_spend      numeric(12,2) NOT NULL,
    txn_count      bigint        NOT NULL,
    customer_count bigint        NOT NULL,
    as_of_date     date          NOT NULL,
    PRIMARY KEY (txn_date, category)
);

CREATE TABLE IF NOT EXISTS serving.mart_advance_repayment (
    cohort_month       date          PRIMARY KEY,
    advance_count      bigint        NOT NULL,
    amount_total       numeric(22,2) NOT NULL,
    repaid_count       bigint        NOT NULL,
    overdue_count      bigint        NOT NULL,
    active_count       bigint        NOT NULL,
    due_count          bigint        NOT NULL,
    due_repaid_count   bigint        NOT NULL,
    late_repaid_count  bigint        NOT NULL,
    avg_days_late      double precision,
    repayment_rate     double precision,
    repayment_rate_due double precision,
    as_of_date         date          NOT NULL
);

CREATE TABLE IF NOT EXISTS serving.mart_customer_features (
    customer_id          text          NOT NULL,
    month                date          NOT NULL,
    spend_total          numeric(32,2) NOT NULL,
    spend_txn_count      bigint        NOT NULL,
    spend_days           bigint        NOT NULL,
    daily_spend_stddev   double precision,
    daily_spend_cv       double precision,
    salary_credit_count  bigint        NOT NULL,
    salary_total         numeric(22,2) NOT NULL,
    income_regularity_3m double precision,
    advance_count        bigint        NOT NULL,
    advance_amount       numeric(22,2) NOT NULL,
    as_of_date           date          NOT NULL,
    PRIMARY KEY (customer_id, month)
);

-- Personal data. Readable by the engineer role only (see 03_roles_grants.sql).
CREATE TABLE IF NOT EXISTS serving.customer_profile (
    customer_id    text PRIMARY KEY,
    full_name      text NOT NULL,
    email          text,
    phone          text,
    date_of_birth  date,
    age_band       text,
    state          text,
    income_band    text,
    customer_since date,
    as_of_date     date NOT NULL
);
