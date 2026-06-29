"""
Unit tests for src/ingestion/schemas.py.

get_schema returns a hardcoded Polars dtype dict for datasets that
need an enforced schema on CSV read, or None for datasets where
Polars should infer types freely. Both return paths need coverage
so that a future edit (e.g. accidentally removing a dataset from
the lookup) is caught by tests rather than by a runtime crash.
"""

import polars as pl

from src.ingestion.schemas import get_schema


def test_get_schema_returns_schema_for_orders():
    schema = get_schema("orders")
    assert schema is not None
    assert schema["order_id"] == pl.String
    assert schema["order_purchase_timestamp"] == pl.String


def test_get_schema_returns_schema_for_payments():
    schema = get_schema("payments")
    assert schema is not None
    assert schema["payment_value"] == pl.Float64
    assert schema["payment_sequential"] == pl.Int64


def test_get_schema_returns_none_for_dataset_without_explicit_schema():
    """Datasets like customers, products, and sellers have no enforced
    schema - Polars infers types from the CSV. get_schema must return
    None for these, not raise or return an empty dict."""
    assert get_schema("customers") is None
    assert get_schema("products") is None
    assert get_schema("sellers") is None


def test_get_schema_returns_none_for_unknown_name():
    assert get_schema("does_not_exist") is None
