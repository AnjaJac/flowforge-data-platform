from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="staging_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    
    start = EmptyOperator(
        task_id="start"
    )
    publish_metadata = EmptyOperator(
        task_id="publish_metadata"
    )

    start >> publish_metadata
