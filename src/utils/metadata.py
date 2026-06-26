import json
from pathlib import Path
from datetime import UTC, datetime

def write_metadata(metadata: dict, output_path: str) -> None:
    """
    Write metadata dictionary to a JSON file.

    Args:
        metadata: Metadata payload.
        output_path: Destination JSON file path.
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(metadata, file, indent=4)


def generate_metadata(
        layer: str,
        dag_id: str,
        run_id: str,
        execution_date: str,
        status: str,
        entities: dict,
        validation_passed: bool = True,
) -> dict:
    """
    Generate standardized metadata payload.

    Args:
        layer: Pipeline layer.
        dag_id: Airflow DAG ID.
        run_id: Airflow run identifier.
        execution_date: Logical execution date.
        status: success or failed.
        entities: Entity-level metadata.
        validation_passed: Whether the validation passed.

    Returns:
        Standardized metadata dictionary.
    """
    return {
        "layer": layer,
        "dag_id": dag_id,
        "run_id": run_id,
        "execution_date": execution_date,
        "completed_at": datetime.now(UTC).isoformat(),
        "status": status,
        "validation_passed": validation_passed,
        "entities": entities,
        "pipeline_meta": {},
    }
