from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
from pathlib import Path

import polars as pl
import yaml

from src.core.dedup import dedup_entity
from src.staging.quality_check import load_quality_config
from src.core.relationship_validation import run_relationship_validation
from src.core.reconciliation import run_reconciliation
from src.core.publish_metadata import publish_metadata as write_core_metadata
from src.core.quality_report import generate_quality_report

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

    return {"entity": entity_name, "removed_count": removed_count}

def run_relationship_validation_task():
    reports = run_relationship_validation(
        core_dir=CORE_DIR,
        config_path=VALIDATION_CONFIG_PATH,
    )
    for entity_name, entity_report in reports.items():
        for column, result in entity_report.items():
            print(f"[{entity_name}.{column}] removed: {result['removed_count']}")
    return reports

def run_reconciliation_task():
    summary = run_reconciliation(
        core_dir=CORE_DIR,
        config_path=VALIDATION_CONFIG_PATH,
    )
    print(f"[reconciliation] {summary}")
    return summary

def run_reconciliation_task():
    summary = run_reconciliation(
        core_dir=CORE_DIR,
        config_path=VALIDATION_CONFIG_PATH,
    )
    print(f"[reconciliation] {summary}")
    return summary


def run_publish_metadata_task(**context):
    ti = context["ti"]
    dedup_results = [
        ti.xcom_pull(task_ids=f"dedup_{entity}_task") for entity in ENTITIES
    ]
    fk_results = ti.xcom_pull(task_ids="relationship_validation_task")
    reconciliation_summary = ti.xcom_pull(task_ids="reconciliation_task")

    execution_summary = {
        "dedup_results": dedup_results,
        "fk_results": fk_results,
        "reconciliation_summary": reconciliation_summary,
    }

    write_core_metadata(
        execution_summary=execution_summary,
        dag_id=context["dag"].dag_id,
        run_id=context["run_id"],
        execution_date=str(context.get("logical_date", context.get("ds", "unknown"))),
        output_directory=CORE_DIR,
    )


def run_quality_report_task(**context):
    ti = context["ti"]
    dedup_results = [
        ti.xcom_pull(task_ids=f"dedup_{entity}_task") for entity in ENTITIES
    ]
    fk_results = ti.xcom_pull(task_ids="relationship_validation_task")
    reconciliation_summary = ti.xcom_pull(task_ids="reconciliation_task")

    path = generate_quality_report(
        dedup_results=dedup_results,
        fk_results=fk_results,
        reconciliation_summary=reconciliation_summary,
        output_directory=CORE_DIR,
    )
    print(f"[quality_report] written to {path}")
    return path

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

    relationship_validation_task = PythonOperator(
        task_id="relationship_validation_task",
        python_callable=run_relationship_validation_task,
    )
    reconciliation_task = PythonOperator(
        task_id="reconciliation_task",
        python_callable=run_reconciliation_task,
    )

    publish_metadata = PythonOperator(
        task_id="publish_metadata",
        python_callable=run_publish_metadata_task,
    )

    quality_report_task = PythonOperator(
        task_id="quality_report_task",
        python_callable=run_quality_report_task,
    )

    wait_for_staging >> dedup_tasks >> relationship_validation_task >> reconciliation_task >> publish_metadata >> quality_report_task