from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime

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

    process_core = EmptyOperator(
        task_id="process_core"
    )

    publish_metadata = EmptyOperator(
        task_id="publish_metadata"
    )

    wait_for_staging >> process_core >> publish_metadata
        