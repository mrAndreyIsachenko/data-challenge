from datetime import datetime, timedelta
import os
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline_anomaly.presentation.cli import run_pipeline

DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "config" / "pipeline.yaml"


def _run_pipeline_from_dag(**_: object) -> None:
    config_path = Path(os.environ.get("PIPELINE_CONFIG", DEFAULT_CONFIG))
    run_pipeline(config_path)


with DAG(
    dag_id="pipeline_anomaly",
    description="ClickHouse ETL + anomaly detection pipeline",
    schedule="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "oracul-data",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["clickhouse", "anomaly", "etl"],
) as dag:
    run_pipeline_task = PythonOperator(
        task_id="run_pipeline_cli",
        python_callable=_run_pipeline_from_dag,
    )
