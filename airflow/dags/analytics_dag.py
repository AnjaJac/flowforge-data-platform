from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime

with DAG(
    dag_id="analytics_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    
    wait_for_core = ExternalTaskSensor(
        task_id="wait_for_core",
        external_dag_id="core_dag",
        external_task_id="publish_metadata",
        allowed_states=["success"],
        poke_interval=30,
    )

    process_analytics = EmptyOperator(
        task_id="process_analytics"
    )

    publish_metadata = EmptyOperator(
        task_id="publish_metadata"
    )

    wait_for_core >> process_analytics >> publish_metadata