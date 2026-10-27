# Access control — serving database

Gold data is published to the Postgres database `serving`, schema `serving`, by the `publish` task (`jobs/publish/run.py`). Two login roles read it. Their passwords come from `.env` (`ANALYST_PASSWORD`, `ENGINEER_PASSWORD`). The SQL that creates the objects, roles and grants is in [`serving/`](../serving) and is re-applied on every publish.

## Objects

| Object | Contents | Personal data |
|---|---|---|
| `serving.mart_daily_spend` | day × category spend | no |
| `serving.mart_advance_repayment` | monthly advance cohorts | no |
| `serving.mart_customer_features` | customer × month features (pseudonymous `customer_id`) | no |
| `serving.customer_profile` | name, email, phone, date of birth, age band, state, income band, customer since | **yes** |
| `serving.v_customer_profile_masked` | `customer_id`, `name_masked` (first letter + `***`), age band, state, income band | no |

The masked view runs with its owner's privileges, so analysts can read it without any privilege on `customer_profile`.

## Access matrix

| | analyst | engineer |
|---|---|---|
| 3 marts (SELECT) | ✅ | ✅ |
| `v_customer_profile_masked` (SELECT) | ✅ | ✅ |
| `customer_profile`: any column, including `full_name`, `email`, `phone`, `date_of_birth` | ❌ permission denied | ✅ |
| INSERT / UPDATE / DELETE / TRUNCATE on serving tables | ❌ | ❌ |
| CREATE TABLE in `serving` or `public` | ❌ | ❌ |
| `SET ROLE` to the other role | ❌ | not tested |
| Connect to `source`, `iceberg_catalog`, `airflow` | ❌ permission denied for database | ❌ |

Both roles are `NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT`. PUBLIC has no privileges on any LedgerLake database or on the `public` schema.

**Scope:** this covers the serving database. The lakehouse (MinIO and the Iceberg catalog) is reached only with the platform's own credentials. Analysts have no lake credentials.

## How it is proven

`make access-test` runs [`tests/integration/test_serving_access.py`](../tests/integration/test_serving_access.py) inside the Airflow container after a publish. **Each test logs in as the role itself**, using that role's password. The tests check:
- **Reads:** row counts on every object the role may read, and that every `name_masked` value matches `^\S\*\*\*$`.
- **Denied table reads:** `permission denied` (`InsufficientPrivilege`) for `SELECT *` and for each column of `customer_profile`, run as analyst.
- **Column privileges:** `has_column_privilege('analyst', 'serving.customer_profile', <col>, 'SELECT')` is false for every personal-data column.
- **Readable objects:** the set of tables and views the analyst can read is exactly the 3 marts plus the masked view, and none of their columns is a personal-data column.
- **Writes and escalation:** writes, CREATE and `SET ROLE` fail. Connecting to the other databases fails for both roles.
- **Engineer:** can read every personal-data column, and is read-only.

The latest result (30 passed) is in the README's measured results.
