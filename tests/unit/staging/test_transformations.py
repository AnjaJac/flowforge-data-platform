"""
Unit tests for src/staging/transformations.py 

These cover the shared building blocks used by all 6 entity staging
modules: column renaming, uppercasing, date parsing, and null-filling.
Each function is tested for its correct behaviour AND for the
specific failure mode it must guard against (e.g. parse_dates must
raise on a bad value, not silently return null).
"""

import pytest
import polars as pl

from src.staging.transformations import (
    to_snake_case,
    uppercase_columns,
    parse_dates,
    fill_null_with,
)


# ---------------------------------------------------------------------------
# to_snake_case
# ---------------------------------------------------------------------------

def test_to_snake_case_renames_messy_headers():
    """A column with a space and one with a hyphen should both come
    out as clean, lowercase, underscore-separated names."""
    df = pl.DataFrame({"Customer ID": [1], "customer-city": ["sp"]})
    result = to_snake_case(df)
    assert result.columns == ["customer_id", "customer_city"]


def test_to_snake_case_leaves_already_clean_headers_unchanged():
    """If a column is already snake_case, this function should be a
    no-op for it - it shouldn't accidentally mangle clean names."""
    df = pl.DataFrame({"customer_id": [1], "customer_city": ["sp"]})
    result = to_snake_case(df)
    assert result.columns == ["customer_id", "customer_city"]


# ---------------------------------------------------------------------------
# uppercase_columns
# ---------------------------------------------------------------------------

def test_uppercase_columns_only_affects_named_columns():
    """This is the key safety property: uppercase_columns must NOT
    uppercase every string column it sees - only the ones explicitly
    named. 'status' here represents a column like order_status, which
    we deliberately decided should NOT be uppercased."""
    df = pl.DataFrame({"city": ["sao paulo"], "status": ["delivered"]})
    result = uppercase_columns(df, ["city"])
    assert result["city"].to_list() == ["SAO PAULO"]
    assert result["status"].to_list() == ["delivered"]


def test_uppercase_columns_missing_column_raises():
    """Asking to uppercase a column that doesn't exist must fail
    loudly, not silently do nothing - same 'don't hide a mismatch'
    principle used in the Card 4.1 schema validation gate."""
    df = pl.DataFrame({"city": ["sp"]})
    with pytest.raises(ValueError, match="not found"):
        uppercase_columns(df, ["nonexistent"])


# ---------------------------------------------------------------------------
# parse_dates
# ---------------------------------------------------------------------------

def test_parse_dates_converts_valid_format():
    """A correctly formatted timestamp string should become a real
    Polars Datetime type, not stay as a string."""
    df = pl.DataFrame({"order_date": ["2024-01-15 10:30:00"]})
    result = parse_dates(df, ["order_date"])
    assert result["order_date"].dtype == pl.Datetime


def test_parse_dates_raises_on_malformed_value_instead_of_returning_null():
    """This is the most important test in this file. Card 4.2's
    Failure Criterion explicitly says a date conversion that produces
    nulls or leaves unparsed strings is a failure. This test proves
    parse_dates raises an exception on bad input rather than quietly
    converting it to null - which is what a non-strict parse would do."""
    df = pl.DataFrame({"order_date": ["2024-01-15 10:30:00", "NOT A DATE"]})
    with pytest.raises(Exception):
        parse_dates(df, ["order_date"])


def test_parse_dates_missing_column_raises():
    """Same missing-column safety check as uppercase_columns."""
    df = pl.DataFrame({"order_date": ["2024-01-15 10:30:00"]})
    with pytest.raises(ValueError, match="not found"):
        parse_dates(df, ["nonexistent"])


# ---------------------------------------------------------------------------
# fill_null_with
# ---------------------------------------------------------------------------

def test_fill_null_with_replaces_only_nulls():
    """Only the actual null value should be replaced - real existing
    values ('electronics', 'books') must be left untouched."""
    df = pl.DataFrame({"category": ["electronics", None, "books"]})
    result = fill_null_with(df, "category", "unknown")
    assert result["category"].to_list() == ["electronics", "unknown", "books"]


def test_fill_null_with_missing_column_raises():
    """Same missing-column safety check as the other functions."""
    df = pl.DataFrame({"category": ["electronics"]})
    with pytest.raises(ValueError, match="not found"):
        fill_null_with(df, "nonexistent", "unknown")