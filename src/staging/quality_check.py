"""
Staging-layer quality gate (Card 4.3).

Checks primary-key uniqueness and null thresholds on the CLEANED
staging data (data/staging/*.parquet), after Card 4.2's
transformations have already run. This is a content-quality check,
distinct from Card 4.1's structural schema gate.

Policy, per the Card 4.3 design discussion:
- Primary key duplicates: ZERO tolerance, always. Not configurable -
  the card's own wording has no tolerance language for this, unlike
  nulls which explicitly get a threshold.
- Null thresholds: per-column, read from validation.yaml. Exceeding
  the threshold always fails - the threshold value is configurable,
  but "fail if exceeded" is not.
"""

from pathlib import Path

import polars as pl
import yaml


class QualityCheckError(Exception):
    """Raised when a staging dataset fails a quality check."""


def load_quality_config(config_path: str | Path) -> dict:
    """
    Load the quality policy (primary_keys, null_thresholds) from
    validation.yaml.

    Args:
        config_path: Path to validation.yaml.

    Returns:
        The "quality" section as a dict, e.g.:
            {
                "primary_keys": {"customers": ["customer_id"], ...},
                "null_thresholds": {"customers": {"customer_id": 0.0}, ...},
            }
    """
    config_path = Path(config_path)
    with open(config_path) as f:
        config = yaml.safe_load(f)
    return config["quality"]


def check_primary_key_uniqueness(
    df: pl.DataFrame,
    entity_name: str,
    primary_key_columns: list[str],
) -> int:
    """
    Check that the given primary key column(s) have no duplicate
    combinations. Supports composite keys (e.g. payments uses
    [order_id, payment_sequential]).

    Args:
        df: The staging DataFrame to check.
        entity_name: Used only for the error message.
        primary_key_columns: One or more column names forming the key.

    Returns:
        0, if no duplicates exist.

    Raises:
        QualityCheckError: if any duplicate key combination is found.
            Zero tolerance - this is not configurable.
    """
    total_rows = df.height
    unique_rows = df.select(primary_key_columns).unique().height
    duplicate_count = total_rows - unique_rows

    if duplicate_count > 0:
        raise QualityCheckError(
            f"[{entity_name}] {duplicate_count} duplicate primary key "
            f"combination(s) found on {primary_key_columns}"
        )

    return duplicate_count


def check_null_thresholds(
    df: pl.DataFrame,
    entity_name: str,
    column_thresholds: dict[str, float],
) -> dict[str, float]:
    """
    Check that each named column's null rate does not exceed its
    configured threshold.

    Args:
        df: The staging DataFrame to check.
        entity_name: Used only for the error message.
        column_thresholds: {column_name: max_allowed_null_rate}.

    Returns:
        {column_name: actual_null_rate} for every checked column.

    Raises:
        QualityCheckError: if any column's null rate exceeds its
            threshold. Exceeding the threshold always fails - this
            is not configurable.
    """
    total_rows = df.height
    null_rates = {}

    for column, max_allowed in column_thresholds.items():
        null_count = df[column].null_count()
        null_rate = null_count / total_rows if total_rows > 0 else 0.0
        null_rates[column] = null_rate

        if null_rate > max_allowed:
            raise QualityCheckError(
                f"[{entity_name}] column '{column}' null rate "
                f"{null_rate:.4f} exceeds threshold {max_allowed:.4f}"
            )

    return null_rates


def run_quality_check(staging_dir: str | Path, config_path: str | Path) -> dict:
    """
    Run the full Staging quality gate across all entities defined in
    validation.yaml's quality.primary_keys section.

    Args:
        staging_dir: Directory containing stg_*.parquet files.
        config_path: Path to validation.yaml.

    Returns:
        A results dict, e.g.:
            {
                "customers": {
                    "duplicate_count": 0,
                    "null_rates": {"customer_id": 0.0},
                    "passed": True,
                },
                ...
            }
        This dict is returned by the calling Airflow task and
        automatically pushed to XCom.

    Raises:
        QualityCheckError: on the first entity that fails either
            check - same fail-fast philosophy as Card 4.1.
    """
    staging_dir = Path(staging_dir)
    quality_config = load_quality_config(config_path)
    primary_keys = quality_config["primary_keys"]
    null_thresholds = quality_config.get("null_thresholds", {})

    results = {}

    for entity_name, pk_columns in primary_keys.items():
        stg_path = staging_dir / f"stg_{entity_name}.parquet"
        df = pl.read_parquet(stg_path)

        duplicate_count = check_primary_key_uniqueness(df, entity_name, pk_columns)

        column_thresholds = null_thresholds.get(entity_name, {})
        null_rates = check_null_thresholds(df, entity_name, column_thresholds)

        results[entity_name] = {
            "duplicate_count": duplicate_count,
            "null_rates": null_rates,
            "passed": True,
        }

    return results
