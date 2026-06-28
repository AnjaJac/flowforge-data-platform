"""
Unit tests for src/staging/sellers.py 

Structurally identical concern to customers - uppercase city/state,
leave zip code untouched, preserve row count.
"""

import polars as pl

from src.staging.sellers import clean_sellers


def test_clean_sellers_uppercases_city_and_state():
    df = pl.DataFrame({
        "seller_id": ["s1"],
        "seller_zip_code_prefix": [2001],
        "seller_city": ["rio de janeiro"],
        "seller_state": ["rj"],
    })
    result = clean_sellers(df)
    assert result["seller_city"].to_list() == ["RIO DE JANEIRO"]
    assert result["seller_state"].to_list() == ["RJ"]


def test_clean_sellers_leaves_zip_code_untouched():
    df = pl.DataFrame({
        "seller_id": ["s1"],
        "seller_zip_code_prefix": [2001],
        "seller_city": ["rj"],
        "seller_state": ["rj"],
    })
    result = clean_sellers(df)
    assert result["seller_zip_code_prefix"].dtype == pl.Int64
    assert result["seller_zip_code_prefix"].to_list() == [2001]


def test_clean_sellers_preserves_row_count():
    df = pl.DataFrame({
        "seller_id": ["s1", "s2"],
        "seller_zip_code_prefix": [2001, 2002],
        "seller_city": ["rj", "sp"],
        "seller_state": ["rj", "sp"],
    })
    result = clean_sellers(df)
    assert result.height == df.height