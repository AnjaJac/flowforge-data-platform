from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group

from src.ingestion.discovery import dataset_discovery
from src.ingestion.raw_ingestion import ingest_file

with DAG(
    dag_id="ingestion_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    @task
    def dataset_discovery_task():
        return dataset_discovery()
    
    @task
    def ingest_dataset(source_path: str):
        return ingest_file(source_path)
        
    @task_group(group_id="ingestion_task_group")
    def ingestion_task_group(validated_paths):
        ingest_dataset.expand(
            source_path=validated_paths
        )
    validated_paths = dataset_discovery_task()
    ingestion_task_group(validated_paths)