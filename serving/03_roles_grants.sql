-- Roles and grants. Applied on every publish; safe to re-run.
-- Passwords are set separately from .env by jobs/publish/run.py (never stored in this file).

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'analyst') THEN
        CREATE ROLE analyst LOGIN;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'engineer') THEN
        CREATE ROLE engineer LOGIN;
    END IF;
END
$$;

ALTER ROLE analyst  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
ALTER ROLE engineer NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;

-- Nobody but the owner gets anything by default: no connecting to the other databases, no temp tables.
REVOKE ALL ON DATABASE source, iceberg_catalog, airflow, serving FROM PUBLIC;
GRANT CONNECT ON DATABASE serving TO analyst, engineer;
REVOKE ALL ON SCHEMA public FROM PUBLIC;

-- Reset table privileges in the serving schema, then grant exactly what each role may read.
REVOKE ALL ON ALL TABLES IN SCHEMA serving FROM PUBLIC, analyst, engineer;
GRANT USAGE ON SCHEMA serving TO analyst, engineer;

-- analyst: the three marts and the masked view only.
GRANT SELECT ON serving.mart_daily_spend,
                serving.mart_advance_repayment,
                serving.mart_customer_features,
                serving.v_customer_profile_masked
    TO analyst;

-- engineer: everything in serving, including personal data. Read-only.
GRANT SELECT ON ALL TABLES IN SCHEMA serving TO engineer;
