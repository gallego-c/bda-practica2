from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from data_collector import collect, get_spark, DATASETS


DEFAULT_ARGS = {
    "owner": "data-engineering-team",
    "depends_on_past": False,       
    "email_on_failure": True,
    "email": ["data-eng-alerts@example.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

def run_collector(kaggle_id: str, dataset_name: str, **context) -> None:
    os.environ["KAGGLE_USERNAME"] = Variable.get("KAGGLE_USERNAME")
    os.environ["KAGGLE_KEY"] = Variable.get("KAGGLE_KEY")

    spark = get_spark()
    try:
        collect(dataset_name, kaggle_id, spark)
    finally:
        spark.stop()

with DAG(
    dag_id="landing_zone_data_collector",
    description="Ingesta periódica de datasets cardíacos desde Kaggle → Landing Zone (Parquet raw)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule="0 2 * * *",  
    catchup=False,
    max_active_runs=1,
    tags=["landing-zone", "data-collection", "bda-gia"],
) as dag:

    tasks = []
    for kaggle_id, dataset_name in DATASETS:
        task = PythonOperator(
            task_id=f"collect_{dataset_name}",
            python_callable=run_collector,
            op_kwargs={
                "kaggle_id": kaggle_id,
                "dataset_name": dataset_name,
            },
        )
        tasks.append(task)

