"""DAG integrity tests: dags/ imports cleanly and ledgerlake_daily has the expected shape. Runs in the container."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum
import pytest

try:
    from airflow.dag_processing.dagbag import DagBag
except ImportError:  # older import path
    from airflow.models.dagbag import DagBag

from jobs.common.airflow_callbacks import business_date, on_failure, on_retry

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"
ORDER = ["generate_tick", "extract_bronze", "silver", "dq_silver", "gold", "dq_gold", "publish"]


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR))  # examples are off via AIRFLOW__CORE__LOAD_EXAMPLES


def test_no_import_errors(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}
    assert set(dagbag.dag_ids) == {"ledgerlake_daily"}


def test_task_chain(dagbag: DagBag) -> None:
    dag = dagbag.dags["ledgerlake_daily"]  # in memory; get_dag() queries the metadata DB
    assert sorted(dag.task_ids) == sorted(ORDER)
    for upstream, downstream in zip(ORDER, ORDER[1:], strict=False):
        assert dag.get_task(downstream).upstream_task_ids == {upstream}


def test_retries_and_callbacks(dagbag: DagBag) -> None:
    dag = dagbag.dags["ledgerlake_daily"]  # in memory; get_dag() queries the metadata DB
    assert dag.max_active_runs == 1 and dag.catchup is False
    for task in dag.tasks:
        expected_retries = 0 if task.task_id.startswith("dq_") else 2
        assert task.retries == expected_retries, task.task_id
        assert task.retry_delay == timedelta(minutes=1)
        assert on_failure in _as_list(task.on_failure_callback), task.task_id
        assert on_retry in _as_list(task.on_retry_callback), task.task_id


def test_every_task_uses_the_business_date(dagbag: DagBag) -> None:
    dag = dagbag.dags["ledgerlake_daily"]  # in memory; get_dag() queries the metadata DB
    for task in dag.tasks:
        assert "params.business_date or data_interval_start.in_timezone('Asia/Kuala_Lumpur')" in task.bash_command


def test_business_date_from_param_or_malaysia_interval() -> None:
    interval_start = pendulum.datetime(2026, 10, 2, 16, 0, tz="UTC")  # 2026-10-03 00:00 in Malaysia
    assert business_date({"params": {"business_date": None}, "data_interval_start": interval_start}) == "2026-10-03"
    assert business_date({"params": {"business_date": "2026-10-04"}, "data_interval_start": interval_start}) == (
        "2026-10-04"
    )


def _as_list(callback: object) -> list:
    if callback is None:
        return []
    return list(callback) if isinstance(callback, list | tuple) else [callback]
