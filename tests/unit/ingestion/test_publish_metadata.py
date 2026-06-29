"""
Unit tests for src/ingestion/publish_metadata.py.

publish_metadata extracts dataset names from output_file_path stems
and writes a metadata.json into the given output directory. Tests
verify the file lands in tmp_path, that the returned path matches
the real file location, and that entity names in the JSON are keyed
by dataset stem (e.g. "customers"), not by full path.
"""

import json

import pytest

from src.ingestion.publish_metadata import publish_metadata


def _make_result(tmp_path, dataset_name):
    """Build a minimal ingestion result dict for one dataset."""
    return {
        "source_file_path": str(tmp_path / "source" / f"{dataset_name}.csv"),
        "output_file_path": str(tmp_path / "raw" / f"{dataset_name}.parquet"),
        "source_row_count": 10,
        "output_row_count": 10,
    }


def test_publish_metadata_writes_json_to_output_directory(tmp_path):
    results = [_make_result(tmp_path, "customers")]
    out_dir = tmp_path / "raw"
    out_dir.mkdir(parents=True)

    returned_path = publish_metadata(results, "dag1", "run1", "2024-01-01", str(out_dir))

    expected = out_dir / "metadata.json"
    assert expected.exists(), (
        "metadata.json must be written inside tmp_path, not a system default path"
    )
    assert returned_path == str(expected)


def test_publish_metadata_json_entities_keyed_by_dataset_stem(tmp_path):
    """The JSON must contain entities keyed by stem (e.g. 'customers'),
    not by the full file path - this is what downstream tasks consume."""
    results = [
        _make_result(tmp_path, "customers"),
        _make_result(tmp_path, "orders"),
    ]
    out_dir = tmp_path / "raw"
    out_dir.mkdir(parents=True)

    publish_metadata(results, "dag1", "run1", "2024-01-01", str(out_dir))

    with open(out_dir / "metadata.json") as f:
        data = json.load(f)

    assert "customers" in data["entities"]
    assert "orders" in data["entities"]


def test_publish_metadata_json_has_correct_layer_and_status(tmp_path):
    results = [_make_result(tmp_path, "payments")]
    out_dir = tmp_path / "raw"
    out_dir.mkdir(parents=True)

    publish_metadata(results, "my_dag", "run_42", "2024-06-01", str(out_dir))

    with open(out_dir / "metadata.json") as f:
        data = json.load(f)

    assert data["layer"] == "raw"
    assert data["dag_id"] == "my_dag"
    assert data["status"] == "success"
