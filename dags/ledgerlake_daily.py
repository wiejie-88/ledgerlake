"""ledgerlake_daily: generate one day of source data, then bronze -> silver -> DQ -> gold -> DQ -> publish.

Business date = the ``business_date`` param when given, else the data interval start in Malaysia time (the
schedule runs at midnight Malaysia time for the day that just ended). Every task is idempotent for its
business date, so re-running a date or backfilling is safe.

A failed error-severity DQ check fails its task, and Airflow marks every downstream task upstream_failed.
Failures and retries are written to lake.ops.pipeline_runs by callbacks; successful jobs write their own rows.
The ``simulate_defect`` param injects one bad row (in memory only) into the data the silver checks read.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param
from airflow.timetables.interval import CronDataIntervalTimetable
from pendulum import timezone

from jobs.common.airflow_callbacks import on_failure, on_retry

BUSINESS_DATE = (
    "{{ params.business_date or data_interval_start.in_timezone('Asia/Kuala_Lumpur').strftime('%Y-%m-%d') }}"
)
MYT = timezone("Asia/Kuala_Lumpur")

default_args = {
    "owner": "ledgerlake",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": on_failure,
    "on_retry_callback": on_retry,
    "cwd": "/opt/ledgerlake",
}

with DAG(
    dag_id="ledgerlake_daily",
    description="LedgerLake daily batch: source tick, bronze, silver, DQ, gold, DQ, publish to serving",
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone=MYT),
    start_date=datetime(2026, 10, 1, tzinfo=MYT),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ledgerlake"],
    params={
        "business_date": Param(None, type=["null", "string"], format="date", description="Override, YYYY-MM-DD"),
        "simulate_defect": Param(
            "", type="string", enum=["", "null_amount", "duplicate_key", "orphan_account", "unknown_category"]
        ),
    },
) as dag:
    generate_tick = BashOperator(
        task_id="generate_tick",
        bash_command=(
            f'OUT=$(python -m generator tick --date "{BUSINESS_DATE}") && '
            f'python -m jobs.common.pipeline_runs --status success --date "{BUSINESS_DATE}" --task-id generate_tick '
            '--rows-out "${OUT#ROWS_OUT=}" --duration-s "$SECONDS"'
        ),
    )
    extract_bronze = BashOperator(
        task_id="extract_bronze",
        bash_command=f'python -m jobs.bronze.run --date "{BUSINESS_DATE}" --batch-id "{{{{ run_id }}}}"',
    )
    silver = BashOperator(task_id="silver", bash_command=f'python -m jobs.silver.run --date "{BUSINESS_DATE}"')
    dq_silver = BashOperator(
        task_id="dq_silver",
        retries=0,  # a failed check is deterministic; retrying would only delay the alert
        bash_command=(
            f'python -m quality.run --layer silver --date "{BUSINESS_DATE}" --run-id "{{{{ run_id }}}}"'
            "{% if params.simulate_defect %} --simulate-defect {{ params.simulate_defect }}{% endif %}"
        ),
    )
    gold = BashOperator(task_id="gold", bash_command=f'python -m jobs.gold.run --date "{BUSINESS_DATE}"')
    dq_gold = BashOperator(
        task_id="dq_gold",
        retries=0,
        bash_command=f'python -m quality.run --layer gold --date "{BUSINESS_DATE}" --run-id "{{{{ run_id }}}}"',
    )

    publish = BashOperator(task_id="publish", bash_command=f'python -m jobs.publish.run --date "{BUSINESS_DATE}"')

    generate_tick >> extract_bronze >> silver >> dq_silver >> gold >> dq_gold >> publish
