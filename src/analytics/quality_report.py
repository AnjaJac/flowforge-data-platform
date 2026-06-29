"""
Analytics-layer final quality report (Card 6.3).

Closes out the entire pipeline. Combines row counts and value ranges
for all 5 analytics reports with Card 6.2's validation results into
one final summary - the last quality artifact produced by the whole
platform.
"""

import json
from pathlib import Path

import polars as pl


def generate_quality_report(
    analytics_dir: str | Path,
    validation_result: dict,
) -> str:
    """
    Build the final analytics quality report: row counts and value
    ranges for each of the 5 reports, plus Card 6.2's validation
    results and an overall matching/pass status.

    Args:
        analytics_dir: Directory containing the 5 analytics parquet files.
        validation_result: The dict returned by run_analytics_validation
            (Card 6.2).

    Returns:
        Path to the written analytics_quality_report.json file.
    """
    analytics_dir = Path(analytics_dir)

    report_files = {
        "sales_performance": ("gmv", "aov"),
        "customer_lifetime_value": ("clv",),
        "seller_performance": ("revenue",),
        "product_performance": ("revenue",),
        "customer_retention": ("retention_rate",),
    }

    report_summaries = {}
    for report_name, value_columns in report_files.items():
        df = pl.read_parquet(analytics_dir / f"{report_name}.parquet")
        value_ranges = {
            col: {"min": df[col].min(), "max": df[col].max()} for col in value_columns
        }
        report_summaries[report_name] = {
            "row_count": df.height,
            "value_ranges": value_ranges,
        }

    report = {
        "reports": report_summaries,
        "validation": validation_result,
        "overall_status": "passed" if validation_result.get("passed") else "failed",
    }

    output_path = analytics_dir / "analytics_quality_report.json"
    with open(output_path, "w") as f:
        json.dump(report, f, indent=4, default=str)

    return str(output_path)
