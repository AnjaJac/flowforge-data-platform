from pathlib import Path

from src.utils.metadata import generate_metadata, write_metadata


def publish_analytics(
    execution_summary: dict,
    dag_id: str,
    run_id: str,
    execution_date: str,
    output_directory: str,
) -> str:
    """
    Generate and publish final execution metadata for the Analytics
    layer - the last metadata artifact in the entire pipeline,
    marking the end-to-end run as successful.
    """
    metadata = generate_metadata(
        layer="analytics",
        dag_id=dag_id,
        run_id=run_id,
        execution_date=execution_date,
        status="success",
        validation_passed=True,
        entities=execution_summary,
    )

    output_path = Path(output_directory) / "metadata.json"
    write_metadata(metadata, str(output_path))

    return str(output_path)