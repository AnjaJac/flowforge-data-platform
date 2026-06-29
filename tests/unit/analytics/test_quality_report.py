"""
Unit tests for src/analytics/quality_report.py (Card 6.3).

generate_quality_report reads all 5 analytics parquets and writes
analytics_quality_report.json. Tests write the parquets to tmp_path
and confirm the JSON lands there too - no real analytics/ path is
touched. The overall_status field is derived from validation_result["passed"].
"""

import json

import polars as pl

from src.analytics.quality_report import generate_quality_report


def _write_analytics_parquets(analytics_dir):
    """Write all 5 analytics parquets with the minimum columns that
    generate_quality_report accesses for value_ranges."""
    pl.DataFrame({"month": ["2024-01"], "gmv": [1000.0], "aov": [100.0], "order_count": [10]}).write_parquet(
        analytics_dir / "sales_performance.parquet"
    )
    pl.DataFrame({"customer_id": ["c1"], "clv": [500.0], "order_count": [5], "avg_order_value": [100.0]}).write_parquet(
        analytics_dir / "customer_lifetime_value.parquet"
    )
    pl.DataFrame({"seller_id": ["s1"], "revenue": [800.0], "order_count": [8], "avg_review_score": [4.5]}).write_parquet(
        analytics_dir / "seller_performance.parquet"
    )
    pl.DataFrame({"product_id": ["p1"], "revenue": [600.0], "order_count": [6], "avg_review_score": [4.0]}).write_parquet(
        analytics_dir / "product_performance.parquet"
    )
    pl.DataFrame({
        "cohort_month": ["2024-01"],
        "activity_month": ["2024-01"],
        "cohort_size": [10],
        "active_customers": [10],
        "retention_rate": [1.0],
    }).write_parquet(analytics_dir / "customer_retention.parquet")


def test_generate_quality_report_writes_json_with_all_five_reports(tmp_path):
    analytics_dir = tmp_path / "analytics"
    analytics_dir.mkdir()
    _write_analytics_parquets(analytics_dir)

    returned_path = generate_quality_report(
        analytics_dir=str(analytics_dir),
        validation_result={"passed": True},
    )

    expected = analytics_dir / "analytics_quality_report.json"
    assert expected.exists(), (
        "quality report must be written inside tmp_path/analytics/, not a system path"
    )
    assert returned_path == str(expected)

    with open(expected) as f:
        report = json.load(f)

    for name in ["sales_performance", "customer_lifetime_value", "seller_performance",
                 "product_performance", "customer_retention"]:
        assert name in report["reports"], f"report for {name} must be present"


def test_generate_quality_report_overall_status_passed_when_validation_passed(tmp_path):
    analytics_dir = tmp_path / "analytics"
    analytics_dir.mkdir()
    _write_analytics_parquets(analytics_dir)

    generate_quality_report(analytics_dir=str(analytics_dir), validation_result={"passed": True})

    with open(analytics_dir / "analytics_quality_report.json") as f:
        report = json.load(f)

    assert report["overall_status"] == "passed"


def test_generate_quality_report_overall_status_failed_when_validation_not_passed(tmp_path):
    analytics_dir = tmp_path / "analytics"
    analytics_dir.mkdir()
    _write_analytics_parquets(analytics_dir)

    generate_quality_report(analytics_dir=str(analytics_dir), validation_result={"passed": False})

    with open(analytics_dir / "analytics_quality_report.json") as f:
        report = json.load(f)

    assert report["overall_status"] == "failed"
