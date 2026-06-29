"""
Unit tests for src/staging/payments.py (Card 4.2).

Payments needs no uppercasing, no date parsing, and no null-filling -
per the real raw schema reviewed during this card's design discussion.
This test confirms clean_payments is correctly a near-no-op, not that
it's missing logic it should have.
"""

import polars as pl

from src.staging.payments import clean_payments


def test_clean_payments_leaves_values_unchanged():
    df = pl.DataFrame(
        {
            "order_id": ["o1"],
            "payment_sequential": [1],
            "payment_type": ["credit_card"],
            "payment_installments": [3],
            "payment_value": [99.90],
        }
    )
    result = clean_payments(df)
    assert result["payment_type"].to_list() == ["credit_card"]
    assert result["payment_value"].to_list() == [99.90]


def test_clean_payments_preserves_row_count():
    df = pl.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "payment_sequential": [1, 1],
            "payment_type": ["credit_card", "boleto"],
            "payment_installments": [3, 1],
            "payment_value": [99.90, 50.00],
        }
    )
    result = clean_payments(df)
    assert result.height == df.height


def test_clean_payments_column_names_unchanged():
    """Confirms the snake_case pass is a true no-op on already-clean
    column names, since payments has no other transformation to
    verify against."""
    df = pl.DataFrame(
        {
            "order_id": ["o1"],
            "payment_sequential": [1],
            "payment_type": ["credit_card"],
            "payment_installments": [3],
            "payment_value": [99.90],
        }
    )
    result = clean_payments(df)
    assert result.columns == df.columns
