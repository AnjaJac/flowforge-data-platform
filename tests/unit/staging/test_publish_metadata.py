"""
Unit tests for src/staging/publish_metadata.py (Card 4.3).

Covers the Staging-specific metadata wrapper: confirms it correctly
shapes quality_check's results into the standard metadata contract
and writes a real, parseable JSON file - using src/utils/metadata.py's
real functions, not a mock, since the actual JSON shape on disk is
the thing that matters here.
"""

import json

from src.staging.publish_metadata import publish_metadata


def test_publish_metadata_writes_real_json_file(tmp_path):
    quality_results = {
        "customers": {"passed": True, "duplicate_count": 0, "null_rates": {"customer_id": 0.0}},
    }

    output_path = publish_metadata(
        quality_results=quality_results,
        dag_id="staging_dag",
        run_id="test-run-123",
        execution_date="2026-06-28",
        output_directory=str(tmp_path),
    )

    with open(output_path) as f:
        written = json.load(f)

    assert written["layer"] == "staging"
    assert written["dag_id"] == "staging_dag"
    assert written["run_id"] == "test-run-123"
    assert written["status"] == "success"
    assert written["validation_passed"] is True


def test_publish_metadata_preserves_entity_results_unchanged(tmp_path):
    """The quality_check results dict must pass through into the
    "entities" field exactly as given - no reshaping, no renaming,
    no name-derivation step (unlike Ingestion's publish_metadata,
    which derives entity names from a file path)."""
    quality_results = {
        "orders": {"passed": True, "duplicate_count": 0, "null_rates": {"order_id": 0.0}},
        "reviews": {"passed": True, "duplicate_count": 0, "null_rates": {"review_id": 0.0, "review_comment_message": 0.58}},
    }

    output_path = publish_metadata(
        quality_results=quality_results,
        dag_id="staging_dag",
        run_id="test-run-456",
        execution_date="2026-06-28",
        output_directory=str(tmp_path),
    )

    with open(output_path) as f:
        written = json.load(f)

    assert written["entities"] == quality_results


def test_publish_metadata_returns_correct_file_path(tmp_path):
    output_path = publish_metadata(
        quality_results={},
        dag_id="staging_dag",
        run_id="test-run-789",
        execution_date="2026-06-28",
        output_directory=str(tmp_path),
    )

    assert output_path == str(tmp_path / "metadata.json")