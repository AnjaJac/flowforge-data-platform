"""
Unit tests for src/ingestion/validation.py.

validate_payments has three distinct logical paths: all-positive
(silent pass), zero-value present (pass but emit a warning), and
any negative value (hard raise). All three must be covered since
the zero-value path is easy to miss - it looks like a no-op but
the warning is the observable contract.
"""

import logging

import polars as pl
import pytest

from src.ingestion.validation import validate_payments


def test_validate_payments_passes_on_all_positive_values():
    df = pl.DataFrame({
        "order_id": ["o1", "o2"],
        "payment_value": [100.0, 50.0],
    })
    validate_payments(df)  # must not raise


def test_validate_payments_raises_on_negative_payment_value():
    """Any row with payment_value < 0 must raise ValueError, naming
    the count of invalid rows so a caller can log it meaningfully."""
    df = pl.DataFrame({
        "order_id": ["o1", "o2"],
        "payment_value": [100.0, -5.0],
    })
    with pytest.raises(ValueError, match="1 payment"):
        validate_payments(df)


def test_validate_payments_raises_lists_all_negative_rows():
    df = pl.DataFrame({
        "order_id": ["o1", "o2", "o3"],
        "payment_value": [-1.0, 50.0, -2.0],
    })
    with pytest.raises(ValueError, match="2 payment"):
        validate_payments(df)


def test_validate_payments_passes_but_logs_warning_on_zero_value(caplog):
    """payment_value == 0 is suspicious but not invalid - it must
    not raise, but the caller must be warned so the case is auditable."""
    df = pl.DataFrame({
        "order_id": ["o1", "o2"],
        "payment_value": [0.0, 100.0],
    })
    with caplog.at_level(logging.WARNING, logger="src.ingestion.validation"):
        validate_payments(df)

    assert any("0" in record.message for record in caplog.records)
