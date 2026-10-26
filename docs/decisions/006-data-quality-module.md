# ADR 006: Lightweight data-quality module and severity gate

**Status:** accepted (D2-MID, 2026-10-26)

## Context
SPEC F6 and §6 ask for DQ checks after silver and gold, with results stored in `ops.dq_results`. The required checks are:
- not-null on keys and amounts;
- uniqueness of business keys;
- referential integrity (transaction → account → customer);
- accepted values for status and category;
- `amount > 0`;
- gold freshness.

`error` severity must stop downstream tasks, and `warn` must log and continue. SPEC decision 5 asks for a custom PySpark module rather than a framework.

## Decision
- **`quality/`** has four parts:
  - `checks.py`: a `Check` dataclass and one PySpark counting function per kind: `not_null`, `unique`, `accepted_values`, `positive`, `referential`, `freshness`, `quarantine_rate`. Checks read tables through an injected loader, so they're unit-testable with in-memory DataFrames.
  - `suites.py`: suites per layer. Silver is defined now; gold is added in D2-PM. Accepted values come from the silver contract in `jobs/silver/rules.py`, so the contract and its checks can't drift apart.
  - `gate.py`: `CheckResult`, `Severity`, and `enforce()`, which raises `DataQualityError` on any failed `error` check. It contains no Spark code.
  - `run.py`: the CLI, `python -m quality.run --layer silver --date D --run-id R`. It evaluates the suite, replaces run R's rows in `ops.dq_results`, and exits 1 on a blocking failure.
- **Stopping downstream work is Airflow's job.** A task that exits non-zero fails, and its downstream tasks become `upstream_failed`. The DQ task just has to exit 1. The `ledgerlake_daily` DAG (D2-EVE) puts `dq_silver` and `dq_gold` tasks between layers.
- **Severities:**
  - Every contract check (keys, uniqueness, referential integrity, accepted values, amounts) is `error`. Silver is built to satisfy these, so a failure means a pipeline bug, and it must not reach gold.
  - Freshness (`txn_date` within 1 day of the logical date) is `warn`. So is quarantine rate (at most 5% of the day's bronze transactions rejected, not counting `superseded_by_late_update` audit rows).
- **`ops.dq_results`** has the SPEC columns (run_id, table, check_name, severity, passed, failed_rows, checked_at), plus `logical_date` and `details` (for example `latest=2026-10-01 required>=2026-09-30`). Re-running the same run_id replaces its rows (`DELETE` then append), so results are idempotent per run.
- **Demonstrating a failure.** Silver is clean by construction, so `--simulate-defect` adds one bad row to the in-memory DataFrame of `silver.transactions` that the checks read. Stored data is never changed. The `ledgerlake_dq_gate_demo` DAG uses this to show the gate stopping a downstream task.

## Alternatives considered
- **Great Expectations or Soda:** more features, but extra dependencies, setup and pinning that SPEC decision 5 rules out for a 3-day build.
- **Injecting a real bad row into silver and rolling back with an Iceberg snapshot:** realistic, but it risks leaving corrupted data if the demo is interrupted.
- **One result row per failing record:** too large. Counts plus quarantine tables give both the summary and the evidence.

## Consequences
- The silver suite (32 checks) takes about 40–46 s, because each check runs its own Spark action over the full current silver tables. Fine at this scale; at larger scale, checks could be batched into one aggregation per table.
- An Airflow task's `run_id` is used as the DQ `run_id`, so results can be joined back to DAG runs.
