from src.utils.metadata import generate_metadata, write_metadata
import json


def test_generate_metadata():
    metadata = generate_metadata(
        layer="staging",
        dag_id="staging_dag",
        run_id="manual__1",
        execution_date="2025-01-01T00:00:00",
        status="success",
        entities={"customers": 100},
        validation_passed=True,
    )
    assert metadata["layer"] == "staging"
    assert metadata["dag_id"] == "staging_dag"
    assert metadata["run_id"] == "manual__1"
    assert metadata["execution_date"] == "2025-01-01T00:00:00"
    assert metadata["status"] == "success"
    assert metadata["validation_passed"] is True
    assert metadata["entities"] == {"customers": 100}

    assert "completed_at" in metadata
    assert "pipeline_meta" in metadata


def test_write_metadata(tmp_path):
    output_file = tmp_path / "metadata.json"

    metadata = generate_metadata(
        layer="staging",
        dag_id="staging_dag",
        run_id="manual__1",
        execution_date="2025-01-01T00:00:00",
        status="success",
        entities={"customers": 100},
    )

    write_metadata(metadata, str(output_file))

    assert output_file.exists()
    assert output_file.stat().st_size > 0

    with open(output_file, "r", encoding="utf-8") as file:
        loaded = json.load(file)

    assert loaded["layer"] == "staging"
    assert loaded["dag_id"] == "staging_dag"


def test_generate_metadata_failed_validation():
    metadata = generate_metadata(
        layer="staging",
        dag_id="staging_dag",
        run_id="manual__1",
        execution_date="2025-01-01T00:00:00",
        status="failed",
        entities={},
        validation_passed=False,
    )

    assert metadata["status"] == "failed"
    assert metadata["validation_passed"] is False
