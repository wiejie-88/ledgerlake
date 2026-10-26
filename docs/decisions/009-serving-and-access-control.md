# ADR 009: Serving through Postgres roles, a masked view and a transactional publish

**Status:** accepted (D3-AM, 2026-10-26)

## Context
SPEC §3.3 (decision 4), §5.6 and F8 ask that:
- the three gold marts are published to a Postgres `serving` database by full overwrite;
- a `customer_profile` table (`silver.customers_pii` joined to `dim_customer`) is readable by the engineer only;
- a view `v_customer_profile_masked` (name initial + `***`, age band, state, income band) is readable by the analyst;
- the analyst role gets the marts and the masked view only, and the engineer role gets everything;
- a documented test proves the access.

## Decision
- **SQL as files** in `serving/`: tables (`01`), view (`02`), roles and grants (`03`). The publish job applies them on every run.
  - Everything is idempotent: `IF NOT EXISTS`, `CREATE OR REPLACE VIEW`, a `DO` block for roles, and REVOKE-then-GRANT. Re-applying always converges to the documented state.
  - Role passwords are set from `.env` with `ALTER ROLE … PASSWORD` (parameterised with `psycopg2.sql.Literal`) and never appear in the files.
- **Least privilege:**
  - `analyst` and `engineer` are LOGIN roles with no superuser, createdb, createrole, inherit, replication or bypass-RLS attributes.
  - PUBLIC loses all database privileges on the four LedgerLake databases and on `public` in `serving`.
  - Both roles get `CONNECT` on `serving` and `USAGE` on schema `serving`.
  - `analyst` gets SELECT on the 3 marts and the masked view. `engineer` gets SELECT on all tables in `serving`.
  - Neither role can write.
- **Masked view, not column grants.** The view runs with its owner's privileges, so analysts never need any privilege on `customer_profile`. A future column added to that table can't leak by accident through a forgotten column-level REVOKE.
- **Transactional publish.** Spark collects the four small tables (about 8.4k rows in total). One psycopg2 transaction then runs `TRUNCATE` on all four and `execute_values` inserts.
  - Readers see either the previous or the new publish, never a mix.
  - Grants survive, because tables aren't dropped and recreated.
  - Re-publishing a date produces byte-identical tables.
- **Contract test.** `jobs/publish/contract.py` holds the column lists. `tests/unit/test_serving_contract.py` checks that they match `serving/01_tables.sql`, that only `customer_profile` holds personal-data columns, and that the masked view exposes only the five allowed columns.
- **Access test.** `tests/integration/test_serving_access.py` (`make access-test`) logs in as each role. `docs/access_control.md` documents the access matrix.

## Alternatives considered
- **Spark JDBC `mode("overwrite")`:** drops or truncates per partition without a single transaction, and loses grants when it recreates tables.
- **Column-level privileges on `customer_profile` for analysts:** works, but it's easier to get wrong as the table evolves.
- **Row-level security:** not needed. Access here is by column sensitivity, not by row ownership.
- **A separate schema for personal data:** clearer at larger scale. SPEC names objects in one serving schema, and the grants already isolate them.

## Consequences
- Publishing collects to the Spark driver. That's fine for thousands of rows; at larger volumes, use COPY from staging tables plus a rename swap.
- `customer_profile` is a full copy of personal data in a second store. Its access is limited to `engineer`, and it's rebuilt on every publish, so deletions in silver propagate on the next run.
- The analyst can see pseudonymous `customer_id` in `mart_customer_features` and the masked view. That's needed to join them, and it isn't personal data on its own.
