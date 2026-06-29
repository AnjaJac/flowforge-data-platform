"""
Unit tests for src/core/publish_metadata.py.

publish_metadata writes execution metadata (not data quality findings)
for the Core layer. It follows the same pattern as the ingestion and
staging equivalents - the key contract is that the file lands inside
the given output directory and contains layer="core".
"""

import json

from src.core.publish_metadata import publish_metadata


def test_publish_metadata_writes_json_file(tmp_path):
    out_dir = tmp_path / "core"
    out_dir.mkdir()

    returned_path = publish_metadata(
        execution_summary={"dedup": "done"},
        dag_id="core_dag",
        run_id="run_1",
        execution_date="2024-01-15",
        output_directory=str(out_dir),
    )

    expected = out_dir / "metadata.json"
    assert (
        expected.exists()
    ), "metadata.json must be written inside the given output_directory (tmp_path)"
    assert returned_path == str(expected)


def test_publish_metadata_json_has_layer_core(tmp_path):
    out_dir = tmp_path / "core"
    out_dir.mkdir()

    publish_metadata(
        execution_summary={},
        dag_id="core_dag",
        run_id="run_1",
        execution_date="2024-01-15",
        output_directory=str(out_dir),
    )

    with open(out_dir / "metadata.json") as f:
        data = json.load(f)

    assert data["layer"] == "core"
    assert data["status"] == "success"


def test_publish_metadata_json_includes_execution_summary(tmp_path):
    out_dir = tmp_path / "core"
    out_dir.mkdir()

    summary = {"dedup_removed": 3, "fk_removed": 0}
    publish_metadata(
        execution_summary=summary,
        dag_id="d",
        run_id="r",
        execution_date="2024-01-01",
        output_directory=str(out_dir),
    )

    with open(out_dir / "metadata.json") as f:
        data = json.load(f)

    assert data["entities"]["dedup_removed"] == 3
    assert data["entities"]["fk_removed"] == 0
