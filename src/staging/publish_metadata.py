from pathlib import Path

from src.utils.metadata import (
    generate_metadata,
    write_metadata,
)


def publish_metadata(
    quality_results: dict[str, dict],
    dag_id: str,
    run_id: str,
    execution_date: str,
    output_directory: str,
) -> str:
    """
    Generate and publish metadata for the Staging layer.

    Args:
        quality_results:
            Results dict returned by run_quality_check(), keyed by
            entity name (e.g. {"customers": {...}, "orders": {...}}).
            Unlike Ingestion's list-of-dicts shape, this is already
            keyed by entity name, so no name-derivation step is needed.

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
    entities = quality_results

    metadata = generate_metadata(
        layer="staging",
        dag_id=dag_id,
        run_id=run_id,
        execution_date=execution_date,
        status="success",
        validation_passed=True,
        entities=entities,
    )

    output_path = Path(output_directory) / "metadata.json"

    write_metadata(
        metadata,
        str(output_path),
    )

    return str(output_path)
