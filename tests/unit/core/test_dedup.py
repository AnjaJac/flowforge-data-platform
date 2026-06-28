"""
Unit tests for src/core/dedup.py (Card 5.1).

Real Staging data contains zero duplicate primary keys on every
entity (verified directly - see Card 5.1 design discussion), so
these synthetic tests are the ONLY way the duplicate-handling and
no-tiebreaker-error branches of dedup_entity are ever actually
exercised. Without these tests, that logic would be unverified code
that has never genuinely run.
"""

import pytest
import polars as pl

from src.core.dedup import dedup_entity, DeduplicationError


def test_dedup_entity_passes_through_unchanged_when_no_duplicates():
    df = pl.DataFrame({"customer_id": ["c1", "c2", "c3"]})
    result_df, removed = dedup_entity(df, "customers", ["customer_id"])
    assert result_df.height == 3
    assert removed == 0


def test_dedup_entity_raises_when_duplicates_exist_and_no_tiebreaker():
    """This is the core safety property: if there's no real way to
    decide a winner, dedup_entity must refuse to guess."""
    df = pl.DataFrame({"customer_id": ["c1", "c1", "c2"]})
    with pytest.raises(DeduplicationError, match="no tiebreaker_column"):
        dedup_entity(df, "customers", ["customer_id"])


def test_dedup_entity_keeps_most_recent_when_tiebreaker_provided():
    """The only way to prove this branch works, since no real
    dataset in this project ever reaches it."""
    df = pl.DataFrame({
        "customer_id": ["c1", "c1", "c2"],
        "processing_timestamp": [1, 2, 1],
        "customer_city": ["OLD CITY", "NEW CITY", "OTHER"],
    })
    result_df, removed = dedup_entity(
        df, "customers", ["customer_id"], tiebreaker_column="processing_timestamp"
    )
    assert result_df.height == 2
    assert removed == 1
    kept_row = result_df.filter(pl.col("customer_id") == "c1")
    assert kept_row["customer_city"].to_list() == ["NEW CITY"]


def test_dedup_entity_handles_composite_primary_key():
    """Must work identically for composite keys (e.g. payments,
    order_items, reviews) as for single-column keys."""
    df = pl.DataFrame({
        "order_id": ["o1", "o1", "o2"],
        "payment_sequential": [1, 1, 1],
        "processing_timestamp": [1, 2, 1],
    })
    result_df, removed = dedup_entity(
        df, "payments", ["order_id", "payment_sequential"],
        tiebreaker_column="processing_timestamp",
    )
    assert result_df.height == 2
    assert removed == 1