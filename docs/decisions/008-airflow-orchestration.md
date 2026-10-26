# ADR 008: Airflow orchestration (`ledgerlake_daily`) and the run log

**Status:** accepted (D2-EVE, 2026-10-26)

## Context
SPEC §3 and §6 ask for an Airflow DAG `ledgerlake_daily` running generate_tick → extract → bronze → silver → dq_silver → gold → dq_gold → publish, with retries and a failure callback that writes to `ops.pipeline_runs` (run_id, dag_id, task_id, logical_date, rows_in, rows_out, duration_s, status). Runs must be idempotent and support backfills.

## Decision
- **Tasks are thin `BashOperator`s** calling the same CLIs as the Makefile (`python -m generator tick`, `jobs.bronze.run`, `jobs.silver.run`, `quality.run`, `jobs.gold.run`). The DAG file only wires tasks together (CLAUDE.md), and a task behaves the same in Airflow as by hand.
- **Extract and bronze are one task** (`extract_bronze`). The extract is Spark JDBC or pymongo reading directly into the bronze write, so there's no intermediate landing area to hand over between two tasks.
- **`publish`** is added in D3-AM, together with the serving database it writes to.
- **Business date:**
  - The schedule is `CronDataIntervalTimetable("0 0 * * *", Asia/Kuala_Lumpur)`. Each run covers one Malaysia business day and starts when that day ends.
  - Tasks use the `business_date` param if given, else the data interval start converted to Malaysia time. Airflow's `{{ ds }}` is in UTC, which would be a day early.
  - The param makes manual runs and demos explicit: `make run date=2026-10-03`.
- **Retries:** 2 with a 1-minute delay for build tasks, to absorb transient failures. 0 for the DQ tasks, because a failed check is deterministic and retrying only delays the alert.
- **Limits:** a 30-minute task timeout, `max_active_runs=1` (each task starts a Spark driver of about 2 GB, and days must be processed in order), and `catchup=False`. New DAGs start paused (`dags_are_paused_at_creation=True`), so nothing runs until it's enabled.
- **`lake.ops.pipeline_runs`** (Iceberg, one row per task run, append-only):
  - Spark jobs append their own **success** row at the end, using their existing session, with measured `rows_in`, `rows_out` and `duration_s`:
    - bronze: rows extracted / partition rows written;
    - silver: rows read / valid rows merged;
    - DQ: checks evaluated / checks passed;
    - gold: silver transactions read / dimension and fact changes plus mart rows written.
  - `generate_tick` records through the recorder CLI. The generator prints `ROWS_OUT=<rows inserted>` on stdout.
  - **Failure and retry callbacks** (`jobs/common/airflow_callbacks.py`) record `failed` and `up_for_retry` rows by running `python -m jobs.common.pipeline_runs` in a subprocess. The DAG file and scheduler therefore never import Spark, and a recording error is logged without hiding the task's real failure.
  - Ids come from `AIRFLOW_CTX_*` variables, which the BashOperator exports, or from the callback context.
- **The demo DAG is retired.** `ledgerlake_daily` has a `simulate_defect` param that passes `--simulate-defect` to `dq_silver`, so the gate is demonstrated on the real pipeline (`make dq-gate-demo date=D`). `ledgerlake_dq_gate_demo` is removed.

## Alternatives considered
- **`SparkSubmitOperator` or a Spark connection:** needs a Spark cluster or connection setup. Local-mode Spark through the CLIs is simpler (SPEC decision 3).
- **`ops.pipeline_runs` in Postgres instead of Iceberg:** cheaper writes from callbacks, but SPEC places `ops.*` in the lakehouse.
- **pyiceberg for callback writes without Spark:** a new dependency beyond SPEC. Rejected without approval.
- **Recording every success from a callback:** about 10–15 s of Spark start-up per task, and rows_in/rows_out would have to travel via XCom. The jobs already know their counts.

## Consequences
- A failure costs one extra short Spark session, about 15 s, in the failing task's callback.
- Each attempt is logged, so a re-run date has several rows in `ops.pipeline_runs` (an audit trail, not a state table).
- Backfills: `airflow backfill create --dag-id ledgerlake_daily --from-date … --to-date …` creates one run per Malaysia day. Every task is idempotent per business date.
