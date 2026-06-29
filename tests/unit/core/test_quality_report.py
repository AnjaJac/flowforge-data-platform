"""
Unit tests for src/core/quality_report.py.

generate_quality_report assembles dedup, FK, and reconciliation
findings into entity_quality_report.json. Distinct from metadata.json,
which records pipeline execution; this file records data quality.
"""

import json

from src.core.quality_report import generate_quality_report


def test_generate_quality_report_writes_json_with_all_three_sections(tmp_path):
    returned_path = generate_quality_report(
        dedup_results=[{"entity": "customers", "removed_count": 0}],
        fk_results={"orders": {"customer_id": {"removed_count": 1, "missing_values": ["x"]}}},
        reconciliation_summary={"orders_passed": 10, "orders_failed": 0},
        output_directory=str(tmp_path),
    )

    expected = tmp_path / "entity_quality_report.json"
    assert expected.exists(), (
        "report must be written inside tmp_path, not a system default path"
    )
    assert returned_path == str(expected)

    with open(expected) as f:
        report = json.load(f)

    assert "deduplication" in report
    assert "foreign_key_validation" in report
    assert "financial_reconciliation" in report


def test_generate_quality_report_json_contains_correct_values(tmp_path):
    generate_quality_report(
        dedup_results=[{"entity": "orders", "removed_count": 5}],
        fk_results={},
        reconciliation_summary={"orders_passed": 100},
        output_directory=str(tmp_path),
    )

    with open(tmp_path / "entity_quality_report.json") as f:
        report = json.load(f)

    assert report["deduplication"][0]["removed_count"] == 5
    assert report["financial_reconciliation"]["orders_passed"] == 100
