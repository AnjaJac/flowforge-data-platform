"""
Unit tests for src/staging/customers.py

These test the customers-specific transformation choices, not the
shared building blocks (already covered in test_transformations.py).
"""

import polars as pl

from src.staging.customers import clean_customers


def test_clean_customers_uppercases_city_and_state():
    df = pl.DataFrame(
        {
            "customer_id": ["c1"],
            "customer_unique_id": ["u1"],
            "customer_zip_code_prefix": [1001],
            "customer_city": ["sao paulo"],
            "customer_state": ["sp"],
        }
    )
    result = clean_customers(df)
    assert result["customer_city"].to_list() == ["SAO PAULO"]
    assert result["customer_state"].to_list() == ["SP"]


def test_clean_customers_leaves_zip_code_untouched():
    """Deliberate scope decision: zip code type conversion is NOT
    part of Card 4.2 - it was flagged as follow-up work in the
    Card 4.1 report. This test guards against someone accidentally
    "fixing" it here without that being a deliberate decision."""
    df = pl.DataFrame(
        {
            "customer_id": ["c1"],
            "customer_unique_id": ["u1"],
            "customer_zip_code_prefix": [1001],
            "customer_city": ["sp"],
            "customer_state": ["sp"],
        }
    )
    result = clean_customers(df)
    assert result["customer_zip_code_prefix"].dtype == pl.Int64
    assert result["customer_zip_code_prefix"].to_list() == [1001]


def test_clean_customers_preserves_row_count():
    df = pl.DataFrame(
        {
            "customer_id": ["c1", "c2", "c3"],
            "customer_unique_id": ["u1", "u2", "u3"],
            "customer_zip_code_prefix": [1001, 1002, 1003],
            "customer_city": ["sp", "rj", "ba"],
            "customer_state": ["sp", "rj", "ba"],
        }
    )
    result = clean_customers(df)
    assert result.height == df.height
