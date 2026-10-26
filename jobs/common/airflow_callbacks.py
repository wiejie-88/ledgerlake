"""Airflow task callbacks that write failure and retry rows to lake.ops.pipeline_runs.

They shell out to ``python -m jobs.common.pipeline_runs`` (a short Spark session), so the scheduler and the
DAG file never import Spark. A recording problem is logged and never masks the task's own failure.
"""

from __future__ import annotations

import logging
import subprocess
import sys
from datetime import UTC, datetime
from typing import Any

SOURCE_TZ = "Asia/Kuala_Lumpur"
PROJECT_DIR = "/opt/ledgerlake"

log = logging.getLogger(__name__)


def business_date(context: dict[str, Any]) -> str:
    """The run's business date: the ``business_date`` param, else the data interval start in Malaysia time."""
    override = (context.get("params") or {}).get("business_date")
    if override:
        return str(override)
    return context["data_interval_start"].in_timezone(SOURCE_TZ).strftime("%Y-%m-%d")


def _record(context: dict[str, Any], status: str) -> None:
    ti = context["ti"]
    start = getattr(ti, "start_date", None)
    duration = round((datetime.now(UTC) - start).total_seconds(), 1) if start else None
    cmd = [
        sys.executable, "-m", "jobs.common.pipeline_runs",
        "--status", status,
        "--date", business_date(context),
        "--dag-id", ti.dag_id,
        "--task-id", ti.task_id,
        "--run-id", ti.run_id,
    ]  # fmt: skip
    if duration is not None:
        cmd += ["--duration-s", str(duration)]
    try:
        result = subprocess.run(cmd, cwd=PROJECT_DIR, capture_output=True, text=True, timeout=300, check=False)
        if result.returncode != 0:
            log.error("pipeline_runs recording failed (%s): %s", result.returncode, result.stderr[-2000:])
        else:
            log.info("pipeline_runs: recorded status=%s for task_id=%s", status, ti.task_id)
    except Exception:  # noqa: BLE001 - never let run logging hide the task's real failure
        log.exception("pipeline_runs recording raised")


def on_failure(context: dict[str, Any]) -> None:
    """Record a failed task in ops.pipeline_runs."""
    _record(context, "failed")


def on_retry(context: dict[str, Any]) -> None:
    """Record a task that will be retried in ops.pipeline_runs."""
    _record(context, "up_for_retry")
