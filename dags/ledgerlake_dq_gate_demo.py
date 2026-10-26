"""Demo: a failing error-severity DQ check stops downstream tasks.

Trigger with conf {"simulate_defect": "null_amount"} to inject one bad row (in memory only) into the data the
silver checks read: ``dq_silver`` then fails and ``gold`` ends as upstream_failed. Without conf, both succeed.
The production pipeline DAG (ledgerlake_daily) arrives in block D2-EVE and uses the same quality.run gate.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

with DAG(
    dag_id="ledgerlake_dq_gate_demo",
    schedule=None,
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["ledgerlake", "demo", "data-quality"],
    params={
        "simulate_defect": Param(
            "", type="string", enum=["", "null_amount", "duplicate_key", "orphan_account", "unknown_category"]
        )
    },
) as dag:
    dq_silver = BashOperator(
        task_id="dq_silver",
        cwd="/opt/ledgerlake",
        bash_command=(
            "python -m quality.run --layer silver --date {{ ds }} --run-id '{{ run_id }}'"
            "{% if params.simulate_defect %} --simulate-defect {{ params.simulate_defect }}{% endif %}"
        ),
    )
    gold = BashOperator(task_id="gold", bash_command="echo 'gold would build here (block D2-PM)'")

    dq_silver >> gold
