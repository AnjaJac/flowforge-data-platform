from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from src.ingestion.discovery import dataset_discovery

with DAG(
    dag_id="ingestion_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    @task
    def dataset_discovery_task():
        return dataset_discovery()
    
    dataset_discovery_task()
        
