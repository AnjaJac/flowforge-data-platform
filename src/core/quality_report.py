"""
Core-layer comprehensive quality report (Card 5.3).

Distinct from publish_metadata.py / metadata.json, which answers
"did the pipeline run" (execution metadata). This module answers
"is the resulting data trustworthy" - combining results from all
three Core cards (5.1 deduplication, 5.2 FK validation, 5.3
reconciliation) into one comprehensive artifact, per the card's own
requirement for a "comprehensive validation report."
"""

import json
from pathlib import Path


def generate_quality_report(
    dedup_results: list[dict],
    fk_results: dict,
    reconciliation_summary: dict,
    output_directory: str | Path,
) -> str:
    """
    Combine all three Core validation cards' findings into a single
    entity_quality_report.json.

    Args:
        dedup_results: list of {"entity": ..., "removed_count": ...}
            from each dedup_*_task (Card 5.1).
        fk_results: the dict returned by relationship_validation_task
            (Card 5.2) - {entity: {column: {removed_count, missing_values}}}.
        reconciliation_summary: the dict returned by reconciliation_task
            (Card 5.3).
        output_directory: directory to write entity_quality_report.json into.

    Returns:
        Path to the written report file.
    """
    report = {
        "deduplication": dedup_results,
        "foreign_key_validation": fk_results,
        "financial_reconciliation": reconciliation_summary,
    }

    output_path = Path(output_directory) / "entity_quality_report.json"
    with open(output_path, "w") as f:
        json.dump(report, f, indent=4, default=str)

    return str(output_path)