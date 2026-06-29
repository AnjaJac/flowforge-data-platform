"""
Unit tests for src/analytics/customer_lifetime_value.py.

generate_customer_lifetime_value joins payments to customers via
orders (inner join), so a payment for an order_id not present in
orders is silently excluded - which is correct for post-quarantine
Core data but must be confirmed explicitly.
"""

import polars as pl

from src.analytics.customer_lifetime_value import generate_customer_lifetime_value


def test_generate_clv_aggregates_spend_per_customer():
    orders = pl.DataFrame({
        "order_id": ["o1", "o2"],
        "customer_id": ["c1", "c2"],
    })
    payments = pl.DataFrame({
        "order_id": ["o1", "o2"],
        "payment_value": [100.0, 50.0],
    })

    result = generate_customer_lifetime_value(orders, payments)

    c1 = result.filter(pl.col("customer_id") == "c1")
    assert c1["clv"].to_list() == [100.0]
    assert c1["order_count"].to_list() == [1]
    assert c1["avg_order_value"].to_list() == [100.0]


def test_generate_clv_multi_order_customer_sums_correctly():
    """A customer with two orders must have clv = sum of both payments
    and order_count = 2, not two separate rows."""
    orders = pl.DataFrame({
        "order_id": ["o1", "o2"],
        "customer_id": ["c1", "c1"],
    })
    payments = pl.DataFrame({
        "order_id": ["o1", "o2"],
        "payment_value": [60.0, 40.0],
    })

    result = generate_customer_lifetime_value(orders, payments)

    assert result.height == 1
    assert result["clv"].to_list() == [100.0]
    assert result["order_count"].to_list() == [2]
    assert result["avg_order_value"].to_list() == [50.0]


def test_generate_clv_sorted_by_clv_descending():
    orders = pl.DataFrame({
        "order_id": ["o1", "o2", "o3"],
        "customer_id": ["c1", "c2", "c3"],
    })
    payments = pl.DataFrame({
        "order_id": ["o1", "o2", "o3"],
        "payment_value": [30.0, 200.0, 75.0],
    })

    result = generate_customer_lifetime_value(orders, payments)

    clv_values = result["clv"].to_list()
    assert clv_values == sorted(clv_values, reverse=True)
