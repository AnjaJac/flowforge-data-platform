from pathlib import Path

from src.utils.metadata import (
    generate_metadata,
    write_metadata,
)


def publish_metadata(
        execution_summary: dict,
        dag_id: str,
        run_id: str,
        execution_date: str,
        output_directory: str,
) -> str:
    """
    Generate and publish execution metadata for the Core layer.

    Args:
        execution_summary:
            A dict describing what ran - e.g. dedup/FK/reconciliation
            task completion status. This is execution metadata only
            ("did the pipeline run") - business quality findings
            belong in entity_quality_report.json (see
            src/core/quality_report.py), not here.

        dag_id, run_id, execution_date, output_directory:
            Same as the Ingestion/Staging publish_metadata functions.

    Returns:
        Path to the generated metadata.json file.
    """
    metadata = generate_metadata(
        layer="core",
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