"""
Core-layer cross-entity foreign key validation gate (Card 5.2).

Unlike Cards 4.1 and 4.3's gates, this is NOT a fail-and-stop check -
per the card's own wording, orphaned rows must be filtered out of
the clean dataset, with counts recorded, while the pipeline keeps
running. This is a cleanup filter, not a hard gate.

Design decision: an orphaned row is dropped entirely (not partially
kept) if it fails ANY of its configured FK checks - a row referencing
a nonexistent parent is treated as defective child data, the more
likely real-world cause versus a systematically incomplete parent
table. To support investigating the less likely cause too, each FK
rule's missing values are reported individually (not collapsed into
one entity-wide total), so a pattern of orphans concentrated on one
specific relationship is visible rather than hidden.

Follows this project's established SCD Type 1 approach (see Card
5.1): output overwrites the existing core_*.parquet files in place.
Core remains one coherent, current-truth layer; no parallel
"pre-FK-cleaned" version of the data is retained.
"""

from pathlib import Path

import polars as pl
import yaml


def load_fk_config(config_path: str | Path) -> dict:
    """
    Load the foreign_keys section from validation.yaml.

    Returns:
        {entity_name: [{"column": ..., "references_entity": ...,
                         "references_column": ...}, ...]}
    """
    config_path = Path(config_path)
    with open(config_path) as f:
        config = yaml.safe_load(f)
    return config.get("foreign_keys", {})


def check_foreign_key(
    child_df: pl.DataFrame,
    child_column: str,
    parent_df: pl.DataFrame,
    parent_column: str,
) -> tuple[pl.Series, list]:
    """
    Check one FK relationship: does every value in child_column exist
    in parent_df's parent_column?

    Args:
        child_df: The DataFrame containing the foreign key.
        child_column: The FK column name in child_df.
        parent_df: The DataFrame the FK should reference.
        parent_column: The primary key column name in parent_df.

    Returns:
        A tuple of (valid_mask, missing_values):
        - valid_mask: a boolean Series, True where child_df's row
          has a value that DOES exist in the parent table.
        - missing_values: the distinct child values that did NOT
          exist in the parent table (for reporting/investigation).
    """
    parent_keys = set(parent_df[parent_column].to_list())
    child_values = child_df[child_column]

    valid_mask = child_values.is_in(list(parent_keys))

    missing_values = (
        child_df.filter(~valid_mask)
        .select(child_column)
        .unique()[child_column]
        .to_list()
    )

    return valid_mask, missing_values


def validate_entity_relationships(
    entity_df: pl.DataFrame,
    entity_name: str,
    fk_rules: list[dict],
    parent_dataframes: dict[str, pl.DataFrame],
) -> tuple[pl.DataFrame, dict]:
    """
    Apply all of an entity's FK rules, dropping any row that fails
    ANY rule, and build a per-relationship report.

    Args:
        entity_df: The entity's DataFrame to validate.
        entity_name: Used only for logging/reporting.
        fk_rules: This entity's list of FK rule dicts from
            validation.yaml.
        parent_dataframes: {entity_name: DataFrame} for every parent
            entity referenced by fk_rules - loaded once by the caller
            and passed in, to avoid re-reading the same parent table
            multiple times if it's referenced by more than one rule.

    Returns:
        A tuple of (cleaned_df, report):
        - cleaned_df: entity_df with all orphaned rows removed.
        - report: {child_column: {"removed_count": int,
                                   "missing_values": [...]}}
    """
    total_rows = entity_df.height
    combined_valid_mask = pl.Series([True] * total_rows)
    report = {}

    for rule in fk_rules:
        child_column = rule["column"]
        parent_entity = rule["references_entity"]
        parent_column = rule["references_column"]
        parent_df = parent_dataframes[parent_entity]

        valid_mask, missing_values = check_foreign_key(
            entity_df, child_column, parent_df, parent_column
        )

        removed_count = total_rows - valid_mask.sum()
        report[child_column] = {
            "removed_count": removed_count,
            "missing_values": missing_values,
        }

        combined_valid_mask = combined_valid_mask & valid_mask

    cleaned_df = entity_df.filter(combined_valid_mask)
    return cleaned_df, report


def run_relationship_validation(core_dir: str | Path, config_path: str | Path) -> dict:
    """
    Run FK validation for every entity that has foreign_keys rules
    configured, overwriting each entity's core_*.parquet file in
    place with orphaned rows removed (SCD Type 1 - see module
    docstring).

    Args:
        core_dir: Directory containing core_*.parquet files.
        config_path: Path to validation.yaml.

    Returns:
        {entity_name: {child_column: {"removed_count": ..., "missing_values": [...]}}}
        for every entity that had FK rules to check.
    """
    core_dir = Path(core_dir)
    fk_config = load_fk_config(config_path)

    all_reports = {}

    for entity_name, fk_rules in fk_config.items():
        entity_df = pl.read_parquet(core_dir / f"core_{entity_name}.parquet")

        needed_parents = {rule["references_entity"] for rule in fk_rules}
        parent_dataframes = {
            parent_name: pl.read_parquet(core_dir / f"core_{parent_name}.parquet")
            for parent_name in needed_parents
        }

        cleaned_df, report = validate_entity_relationships(
            entity_df, entity_name, fk_rules, parent_dataframes
        )

        cleaned_df.write_parquet(core_dir / f"core_{entity_name}.parquet")
        all_reports[entity_name] = report

    return all_reports
