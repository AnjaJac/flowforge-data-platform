from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
from pathlib import Path

import polars as pl
import yaml

from src.core.dedup import dedup_entity
from src.staging.quality_check import load_quality_config

STAGING_DIR = "/opt/airflow/data/staging"
CORE_DIR = "/opt/airflow/data/core"
VALIDATION_CONFIG_PATH = "/opt/airflow/config/validation.yaml"

ENTITIES = ["customers", "orders", "products", "sellers", "reviews", "payments", "order_items"]


def run_dedup_for_entity(entity_name: str):
    Path(CORE_DIR).mkdir(parents=True, exist_ok=True)

    quality_config = load_quality_config(VALIDATION_CONFIG_PATH)
    primary_keys = quality_config["primary_keys"][entity_name]

    with open(VALIDATION_CONFIG_PATH) as f:
        full_config = yaml.safe_load(f)
    tiebreaker = full_config["deduplication"]["tiebreaker_columns"][entity_name]

    df = pl.read_parquet(f"{STAGING_DIR}/stg_{entity_name}.parquet")
    deduped_df, removed_count = dedup_entity(df, entity_name, primary_keys, tiebreaker)

    print(f"[{entity_name}] rows removed during deduplication: {removed_count}")

    deduped_df.write_parquet(f"{CORE_DIR}/core_{entity_name}.parquet")


with DAG(
    dag_id="core_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    wait_for_staging = ExternalTaskSensor(
        task_id="wait_for_staging",
        external_dag_id="staging_dag",
        external_task_id="publish_metadata",
        allowed_states=["success"],
        poke_interval=30,
    )

    dedup_tasks = []
    for entity in ENTITIES:
        task = PythonOperator(
            task_id=f"dedup_{entity}_task",
            python_callable=run_dedup_for_entity,
            op_kwargs={"entity_name": entity},
        )
        dedup_tasks.append(task)

    publish_metadata = EmptyOperator(
        task_id="publish_metadata"
    )

    wait_for_staging >> dedup_tasks >> publish_metadata