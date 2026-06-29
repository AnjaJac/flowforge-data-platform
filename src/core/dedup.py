"""
Core-layer chronological deduplication engine (Card 5.1).

Olist is a static snapshot dataset, not a CDC stream - there is no
genuine "most recently updated" timestamp for any entity (verified:
zero duplicate primary keys exist anywhere in real Staging data, see
Card 5.1 design discussion). This function is therefore built as a
safety net that will always take the pass-through branch on this
dataset, while remaining a real, correct, tested algorithm for any
future dataset that does contain duplicates.

Deliberately does NOT use a business timestamp (e.g.
order_purchase_timestamp) as a stand-in tiebreaker - that would be
semantically incorrect, since a business event timestamp does not
mean "this record supersedes another."
"""

import polars as pl


class DeduplicationError(Exception):
    """Raised when duplicates are found but no tiebreaker is configured
    to determine which row should be kept."""


def dedup_entity(
    df: pl.DataFrame,
    entity_name: str,
    primary_key_columns: list[str],
    tiebreaker_column: str | None = None,
) -> tuple[pl.DataFrame, int]:
    """
    Deduplicate a DataFrame on its primary key columns.

    If no duplicates exist, returns the DataFrame unchanged. If
    duplicates exist and a tiebreaker_column is configured, keeps the
    row with the maximum tiebreaker value per primary key combination.
    If duplicates exist and no tiebreaker is configured, raises -
    there is no safe default winner to pick automatically.

    Args:
        df: The staging DataFrame to deduplicate.
        entity_name: Used only for error messages and logging.
        primary_key_columns: One or more columns forming the key.
        tiebreaker_column: Column to sort by (descending) when
            duplicates exist. None if no such column exists for
            this entity.

    Returns:
        A tuple of (deduplicated DataFrame, number of rows removed).

    Raises:
        DeduplicationError: if duplicates exist but no
            tiebreaker_column was provided.
    """
    total_rows = df.height
    unique_rows = df.select(primary_key_columns).unique().height
    duplicate_count = total_rows - unique_rows

    if duplicate_count == 0:
        return df, 0

    if tiebreaker_column is None:
        raise DeduplicationError(
            f"[{entity_name}] {duplicate_count} duplicate primary key "
            f"combination(s) found on {primary_key_columns}, but no "
            f"tiebreaker_column is configured to determine which row "
            f"to keep. Configure one in validation.yaml under "
            f"deduplication.tiebreaker_columns.{entity_name}."
        )

    deduped = df.sort(tiebreaker_column, descending=True).unique(
        subset=primary_key_columns, keep="first"
    )

    rows_removed = total_rows - deduped.height
    return deduped, rows_removed
