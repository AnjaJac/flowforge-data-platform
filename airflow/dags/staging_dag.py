from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from src.staging.schema_validation import validate_all_schemas

RAW_DIR = "/opt/airflow/data/raw"
VALIDATION_CONFIG_PATH = "/opt/airflow/config/validation.yaml"


def run_schema_validation():
    validate_all_schemas(raw_dir=RAW_DIR, config_path=VALIDATION_CONFIG_PATH)


with DAG(
    dag_id="staging_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    schema_validation_task = PythonOperator(
        task_id="schema_validation_task",
        python_callable=run_schema_validation,
    )

    publish_metadata = EmptyOperator(
        task_id="publish_metadata"
    )

    start >> schema_validation_task >> publish_metadata