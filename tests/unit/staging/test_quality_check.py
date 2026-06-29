"""
Unit tests for src/staging/quality_check.py (Card 4.3).

Covers primary key uniqueness (including composite keys), null
threshold enforcement, and the fail-fast behaviour across multiple
entities - derived from the card's own Acceptance and Failure
Criteria, not from the implementation's current behaviour.
"""

import pytest
import polars as pl
import yaml

from src.staging.quality_check import (
    QualityCheckError,
    check_primary_key_uniqueness,
    check_null_thresholds,
    run_quality_check,
)

# ---------------------------------------------------------------------------
# check_primary_key_uniqueness
# ---------------------------------------------------------------------------


def test_check_primary_key_uniqueness_passes_on_unique_single_column():
    df = pl.DataFrame({"customer_id": ["c1", "c2", "c3"]})
    duplicate_count = check_primary_key_uniqueness(df, "customers", ["customer_id"])
    assert duplicate_count == 0


def test_check_primary_key_uniqueness_raises_on_duplicate_single_column():
    df = pl.DataFrame({"customer_id": ["c1", "c1", "c2"]})
    with pytest.raises(QualityCheckError, match="duplicate primary key"):
        check_primary_key_uniqueness(df, "customers", ["customer_id"])


def test_check_primary_key_uniqueness_passes_on_unique_composite_key():
    """This is the real-world case discovered during this card:
    review_id alone repeats legitimately, but (review_id, order_id)
    is genuinely unique. Composite keys must be checked as a
    combination, not column by column."""
    df = pl.DataFrame(
        {
            "review_id": ["r1", "r1", "r2"],
            "order_id": ["o1", "o2", "o3"],
        }
    )
    duplicate_count = check_primary_key_uniqueness(
        df, "reviews", ["review_id", "order_id"]
    )
    assert duplicate_count == 0


def test_check_primary_key_uniqueness_raises_on_duplicate_composite_key():
    df = pl.DataFrame(
        {
            "review_id": ["r1", "r1"],
            "order_id": ["o1", "o1"],
        }
    )
    with pytest.raises(QualityCheckError, match="duplicate primary key"):
        check_primary_key_uniqueness(df, "reviews", ["review_id", "order_id"])


# ---------------------------------------------------------------------------
# check_null_thresholds
# ---------------------------------------------------------------------------


def test_check_null_thresholds_passes_within_threshold():
    df = pl.DataFrame({"category": ["a", "b", None, "d"]})
    result = check_null_thresholds(df, "products", {"category": 0.5})
    assert result["category"] == 0.25


def test_check_null_thresholds_raises_when_exceeded():
    df = pl.DataFrame({"category": [None, None, "c", "d"]})
    with pytest.raises(QualityCheckError, match="exceeds threshold"):
        check_null_thresholds(df, "products", {"category": 0.1})


def test_check_null_thresholds_zero_tolerance_column():
    """A primary-key-like column with a 0.0 threshold must fail on
    even a single null."""
    df = pl.DataFrame({"customer_id": ["c1", None, "c3"]})
    with pytest.raises(QualityCheckError, match="exceeds threshold"):
        check_null_thresholds(df, "customers", {"customer_id": 0.0})


def test_check_null_thresholds_only_checks_named_columns():
    """A column with no configured threshold must be ignored, even
    if it has nulls - same explicit-list discipline as Card 4.2."""
    df = pl.DataFrame(
        {
            "customer_id": ["c1", "c2"],
            "some_other_column": [None, None],
        }
    )
    result = check_null_thresholds(df, "customers", {"customer_id": 0.0})
    assert "some_other_column" not in result


# ---------------------------------------------------------------------------
# run_quality_check
# ---------------------------------------------------------------------------


def test_run_quality_check_passes_when_all_entities_pass(tmp_path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    pl.DataFrame({"customer_id": ["c1", "c2"]}).write_parquet(
        staging_dir / "stg_customers.parquet"
    )

    config_path = tmp_path / "validation.yaml"
    config_path.write_text(
        yaml.dump(
            {
                "quality": {
                    "primary_keys": {"customers": ["customer_id"]},
                    "null_thresholds": {"customers": {"customer_id": 0.0}},
                }
            }
        )
    )

    results = run_quality_check(staging_dir, config_path)
    assert results["customers"]["passed"] is True
    assert results["customers"]["duplicate_count"] == 0


def test_run_quality_check_fails_fast_on_first_bad_entity(tmp_path):
    """One entity failing must stop the whole gate - same design
    decision as Card 4.1's schema validation."""
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    pl.DataFrame({"customer_id": ["c1", "c2"]}).write_parquet(
        staging_dir / "stg_customers.parquet"
    )
    pl.DataFrame({"order_id": ["o1", "o1"]}).write_parquet(
        staging_dir / "stg_orders.parquet"
    )

    config_path = tmp_path / "validation.yaml"
    config_path.write_text(
        yaml.dump(
            {
                "quality": {
                    "primary_keys": {
                        "customers": ["customer_id"],
                        "orders": ["order_id"],
                    },
                    "null_thresholds": {},
                }
            }
        )
    )

    with pytest.raises(QualityCheckError, match=r"\[orders\]"):
        run_quality_check(staging_dir, config_path)
