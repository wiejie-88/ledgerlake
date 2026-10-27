# ADR 016: Backfills as sequential DAG tests, and a dashboard that reads as the analyst

**Status:** accepted (could-haves 9 and 11, 2026-10-26)

## Context
SPEC could-have 11 asks for a documented 7-day backfill demo, and could-have 9 for a one-page Streamlit view of the marts.

Trying Airflow's native `airflow backfill create` on `ledgerlake_daily` showed two things:
1. **Plain dates are read as UTC.** Each run's logical date is a Malaysia midnight (16:00 UTC the previous day). So `--from-date 2026-10-08 --to-date 2026-10-14` created runs for business dates 10-09 to 10-14 only: 6 runs, with 10-08 missing. Passing `2026-10-08T00:00:00+08:00` boundaries gives exactly 10-08 to 10-14.
2. **Backfill runs of a paused DAG stay `queued`.** The scheduler only queues tasks for unpaused DAGs (`.where(~DM.is_paused)` in `scheduler_job_runner.py`). Unpausing `ledgerlake_daily` would also make the scheduler create a regular run for the latest complete interval (catchup is off). That would process the current day out of order and leave a gap in the synthetic source. The 7 queued runs were cancelled through the REST API (`PUT /api/v2/backfills/1/cancel`); no task had started.

## Decision
- **`make backfill from=D1 to=D2`** runs `airflow dags test ledgerlake_daily` once per business date, in order. It passes `business_date` explicitly, prints one result line per date, and stops at the first failure, since later dates depend on earlier ones.
  - This is the same verified path as `make run`. The DAG stays paused and the date mapping is explicit.
- **Airflow-native backfill is documented, not wired up.** In production, with the DAG unpaused and scheduling, `airflow backfill create --dag-id ledgerlake_daily --from-date <D1>T00:00:00+08:00 --to-date <D2>T00:00:00+08:00` is the native route.
- **Dashboard:** a separate `dashboard` Compose service (`python:3.11.16-slim-bookworm`, with Streamlit 1.65.0, Altair 6.3.0, pandas 3.0.6 and psycopg2-binary 2.9.13 pinned, and `pip check` at build).
  - It connects to the serving database **as `analyst`**, so the page can only reach the marts and the masked view: governance by construction, not by convention.
  - It's read-only, with the developer toolbar hidden and usage statistics disabled. It's bound to `127.0.0.1:8501`.
  - Charts follow the dataviz rules: single-series charts (no legend, one color), thin marks, hover tooltips, a table view for every chart, and KPIs as plain numbers.

## Consequences
- `make backfill` runs one date at a time. The 7-day demo (2026-10-08 to 10-14) took 177–360 s per date, 1,657 s in total (measured).
- The Airflow history keeps 7 `failed` (cancelled) backfill runs from the native attempt. They never executed tasks and changed no data.
- The dashboard image (851 MB) builds separately from the Airflow image. It's part of `make up`.
