"""
Unit tests for src/analytics/publish_metadata.py.

publish_analytics writes the final metadata artifact for the entire
pipeline - the last JSON file produced in an end-to-end run. Same
pattern as ingestion/staging/core publish functions: confirms the
file lands in the given output_directory (within tmp_path) and that
layer="analytics" is set in the payload.
"""

import json

from src.analytics.publish_metadata import publish_analytics


def test_publish_analytics_writes_metadata_json(tmp_path):
    out_dir = tmp_path / "analytics"
    out_dir.mkdir()

    returned_path = publish_analytics(
        execution_summary={"clv_rows": 99000},
        dag_id="analytics_dag",
        run_id="run_1",
        execution_date="2024-01-15",
        output_directory=str(out_dir),
    )

    expected = out_dir / "metadata.json"
    assert expected.exists(), (
        "metadata.json must be written inside tmp_path/analytics/, not a system path"
    )
    assert returned_path == str(expected)


def test_publish_analytics_json_has_layer_analytics(tmp_path):
    out_dir = tmp_path / "analytics"
    out_dir.mkdir()

    publish_analytics(
        execution_summary={},
        dag_id="analytics_dag",
        run_id="run_1",
        execution_date="2024-01-15",
        output_directory=str(out_dir),
    )

    with open(out_dir / "metadata.json") as f:
        data = json.load(f)

    assert data["layer"] == "analytics"
    assert data["status"] == "success"


def test_publish_analytics_json_includes_execution_summary(tmp_path):
    out_dir = tmp_path / "analytics"
    out_dir.mkdir()

    publish_analytics(
        execution_summary={"clv_rows": 99000, "sales_rows": 24},
        dag_id="d",
        run_id="r",
        execution_date="2024-01-01",
        output_directory=str(out_dir),
    )

    with open(out_dir / "metadata.json") as f:
        data = json.load(f)

    assert data["entities"]["clv_rows"] == 99000
    assert data["entities"]["sales_rows"] == 24
