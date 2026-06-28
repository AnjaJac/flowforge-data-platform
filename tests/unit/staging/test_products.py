"""
Unit tests for src/staging/products.py (Card 4.2).

Products has two distinctive transformations not shared by any other
entity: filling missing categories with 'unknown' (the card's
explicit, named requirement), and correcting the misspelled
"lenght" columns from the raw source data.
"""

import polars as pl

from src.staging.products import clean_products


def test_clean_products_fills_missing_category_with_unknown():
    df = pl.DataFrame({
        "product_id": ["p1", "p2"],
        "product_category_name": ["electronics", None],
        "product_name_lenght": [40, 35],
        "product_description_lenght": [200, 150],
        "product_photos_qty": [2, 1],
        "product_weight_g": [500, 300],
        "product_length_cm": [20, 15],
        "product_height_cm": [10, 8],
        "product_width_cm": [15, 12],
    })
    result = clean_products(df)
    assert result["product_category_name"].to_list() == ["electronics", "unknown"]


def test_clean_products_does_not_overwrite_existing_category():
    """The fill must only affect actual nulls - a real category value
    must never be replaced."""
    df = pl.DataFrame({
        "product_id": ["p1"],
        "product_category_name": ["electronics"],
        "product_name_lenght": [40],
        "product_description_lenght": [200],
        "product_photos_qty": [2],
        "product_weight_g": [500],
        "product_length_cm": [20],
        "product_height_cm": [10],
        "product_width_cm": [15],
    })
    result = clean_products(df)
    assert result["product_category_name"].to_list() == ["electronics"]


def test_clean_products_corrects_misspelled_column_names():
    """Deliberate decision from this card's design discussion: the
    source data's misspelled 'lenght' columns are corrected to
    'length' as part of standardization."""
    df = pl.DataFrame({
        "product_id": ["p1"],
        "product_category_name": ["electronics"],
        "product_name_lenght": [40],
        "product_description_lenght": [200],
        "product_photos_qty": [2],
        "product_weight_g": [500],
        "product_length_cm": [20],
        "product_height_cm": [10],
        "product_width_cm": [15],
    })
    result = clean_products(df)
    assert "product_name_length" in result.columns
    assert "product_description_length" in result.columns
    assert "product_name_lenght" not in result.columns
    assert "product_description_lenght" not in result.columns


def test_clean_products_preserves_row_count():
    df = pl.DataFrame({
        "product_id": ["p1", "p2"],
        "product_category_name": ["electronics", None],
        "product_name_lenght": [40, 35],
        "product_description_lenght": [200, 150],
        "product_photos_qty": [2, 1],
        "product_weight_g": [500, 300],
        "product_length_cm": [20, 15],
        "product_height_cm": [10, 8],
        "product_width_cm": [15, 12],
    })
    result = clean_products(df)
    assert result.height == df.height