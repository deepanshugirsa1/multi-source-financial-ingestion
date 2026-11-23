"""Airflow DAG for the multi-source financial ingestion platform.

Point an Airflow deployment at this folder:

    airflow standalone
    # airflow.cfg: dags_folder = <repo>/orchestration/dags

The callables live in finingest.tasks and are the same functions CI runs.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from airflow import DAG
from airflow.operators.python import PythonOperator

from finingest.tasks import (
    dbt_build,
    publish_events,
    replay_log,
    run_quality_checks,
    stream_to_lake,
    validate_contracts,
)

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="financial_multi_source_ingestion",
    description="Publish five financial sources, stream to the lake, test with dbt, and write lineage.",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["finance", "ingestion", "medallion"],
) as dag:
    contracts = PythonOperator(task_id="validate_contracts", python_callable=validate_contracts)
    publish = PythonOperator(task_id="publish_events", python_callable=publish_events)
    stream = PythonOperator(task_id="stream_to_lake", python_callable=stream_to_lake)
    replay = PythonOperator(task_id="replay_history", python_callable=replay_log)
    transform = PythonOperator(task_id="dbt_build", python_callable=dbt_build)
    quality = PythonOperator(task_id="data_quality", python_callable=run_quality_checks)
    contracts >> publish >> stream >> replay >> transform >> quality
