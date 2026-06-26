from pathlib import Path

from src.utils.metadata import (
    generate_metadata,
    write_metadata,
)


def publish_metadata(
        ingestion_results: list[dict],
        dag_id: str,
        run_id: str,
        execution_date: str,
        output_directory: str,
) -> str:
    """
    Generate and publish metadata for the Raw layer.

    Args:
        ingestion_results:
            Metadata dictionaries returned by the ingestion tasks.

        dag_id:
            Airflow DAG identifier.

        run_id:
            Airflow DAG Run identifier.

        execution_date:
            Logical execution date.

        output_directory:
            Directory where metadata.json will be written.

    Returns:
        Path to the generated metadata file.
    """
    entities = {}

    for result in ingestion_results:

        dataset_name = (
           Path(result["output_file_path"]).stem  
        )

        entities[dataset_name] = result
    metadata = generate_metadata(
        layer = "raw",
        dag_id=dag_id,
        run_id=run_id,
        execution_date=execution_date,
        status="success",
        validation_passed=True,
        entities=entities,
    )

    output_path = (
        Path(output_directory)
        / "metadata.json"
    )

    write_metadata(
        metadata,
        str(output_path),
    )

    return str(output_path)