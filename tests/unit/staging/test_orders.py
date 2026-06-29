"""
Unit tests for src/staging/orders.py (Card 4.2).

orders.py contains two functions, not one - clean_orders and
clean_order_items - reflecting the deliberate decision to fold
order_items into the order_processing TaskGroup while keeping them
as two separate DataFrames/functions, since they come from two
different raw files with different columns.
"""

import polars as pl

from src.staging.orders import clean_orders, clean_order_items


def test_clean_orders_parses_all_five_date_columns():
    df = pl.DataFrame(
        {
            "order_id": ["o1"],
            "customer_id": ["c1"],
            "order_status": ["delivered"],
            "order_purchase_timestamp": ["2024-01-01 10:00:00"],
            "order_approved_at": ["2024-01-01 11:00:00"],
            "order_delivered_carrier_date": ["2024-01-02 09:00:00"],
            "order_delivered_customer_date": ["2024-01-05 14:00:00"],
            "order_estimated_delivery_date": ["2024-01-10 00:00:00"],
        }
    )
    result = clean_orders(df)
    date_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col in date_columns:
        assert result[col].dtype == pl.Datetime


def test_clean_orders_leaves_status_uppercase_untouched():
    """Deliberate decision: order_status is a category label, not a
    code like city/state, so it must NOT be uppercased."""
    df = pl.DataFrame(
        {
            "order_id": ["o1"],
            "customer_id": ["c1"],
            "order_status": ["delivered"],
            "order_purchase_timestamp": ["2024-01-01 10:00:00"],
            "order_approved_at": ["2024-01-01 11:00:00"],
            "order_delivered_carrier_date": ["2024-01-02 09:00:00"],
            "order_delivered_customer_date": ["2024-01-05 14:00:00"],
            "order_estimated_delivery_date": ["2024-01-10 00:00:00"],
        }
    )
    result = clean_orders(df)
    assert result["order_status"].to_list() == ["delivered"]


def test_clean_orders_preserves_row_count():
    df = pl.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "customer_id": ["c1", "c2"],
            "order_status": ["delivered", "shipped"],
            "order_purchase_timestamp": ["2024-01-01 10:00:00", "2024-02-01 10:00:00"],
            "order_approved_at": ["2024-01-01 11:00:00", "2024-02-01 11:00:00"],
            "order_delivered_carrier_date": [
                "2024-01-02 09:00:00",
                "2024-02-02 09:00:00",
            ],
            "order_delivered_customer_date": [
                "2024-01-05 14:00:00",
                "2024-02-05 14:00:00",
            ],
            "order_estimated_delivery_date": [
                "2024-01-10 00:00:00",
                "2024-02-10 00:00:00",
            ],
        }
    )
    result = clean_orders(df)
    assert result.height == df.height


def test_clean_order_items_parses_shipping_limit_date():
    df = pl.DataFrame(
        {
            "order_id": ["o1"],
            "order_item_id": [1],
            "product_id": ["p1"],
            "seller_id": ["s1"],
            "shipping_limit_date": ["2024-01-05 09:00:00"],
            "price": [99.90],
            "freight_value": [10.00],
        }
    )
    result = clean_order_items(df)
    assert result["shipping_limit_date"].dtype == pl.Datetime


def test_clean_order_items_preserves_row_count():
    df = pl.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "order_item_id": [1, 1],
            "product_id": ["p1", "p2"],
            "seller_id": ["s1", "s2"],
            "shipping_limit_date": ["2024-01-05 09:00:00", "2024-02-05 09:00:00"],
            "price": [99.90, 50.00],
            "freight_value": [10.00, 8.00],
        }
    )
    result = clean_order_items(df)
    assert result.height == df.height
