from datetime import datetime

from airflow import DAG
from airflow.decorators import (
    task,
    task_group
)
from airflow.sdk import get_current_context

from src.ingestion.discovery import dataset_discovery
from src.ingestion.raw_ingestion import ingest_file
from src.ingestion.raw_validation import validate_raw_layer
from src.ingestion.publish_metadata import publish_metadata


RAW_DIRECTORY = "/opt/airflow/data/raw"

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

    @task
    def raw_validation_task():
        validate_raw_layer(RAW_DIRECTORY)

    @task
    def publish_metadata_task(
        ingestion_results
    ):
        context = get_current_context()

        return publish_metadata(
            ingestion_results=ingestion_results,
            dag_id=context["dag"].dag_id,
            run_id=context["run_id"],
            execution_date=context["logical_date"].isoformat(),
            output_directory=RAW_DIRECTORY,
        )
    

    @task_group(group_id="ingestion_task_group")
    def ingestion_task_group(validated_paths):
        
        return ingest_dataset.expand(
            source_path=validated_paths
        )
    validated_paths = dataset_discovery_task()
    ingestion_results = ingestion_task_group(validated_paths)
    validation = raw_validation_task()
    metadata = publish_metadata_task(ingestion_results)

    ingestion_results >> validation >> metadata