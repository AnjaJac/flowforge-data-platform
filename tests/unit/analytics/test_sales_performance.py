"""
Unit tests for src/analytics/sales_performance.py.

generate_sales_performance requires order_purchase_timestamp to be
a Polars Datetime type (uses .dt.strftime), not a String. Tests
build DataFrames with Python datetime objects, which Polars infers
as Datetime automatically.
"""

from datetime import datetime

import polars as pl

from src.analytics.sales_performance import generate_sales_performance


def test_generate_sales_performance_groups_gmv_by_month():
    orders = pl.DataFrame({
        "order_id": ["o1", "o2"],
        "customer_id": ["c1", "c2"],
        "order_purchase_timestamp": [datetime(2024, 1, 15), datetime(2024, 1, 28)],
    })
    payments = pl.DataFrame({
        "order_id": ["o1", "o2"],
        "payment_value": [60.0, 40.0],
    })

    result = generate_sales_performance(orders, payments)

    assert result.height == 1
    assert result["month"].to_list() == ["2024-01"]
    assert result["gmv"].to_list() == [100.0]


def test_generate_sales_performance_aov_is_gmv_over_order_count():
    """AOV = GMV / order_count per month. With two orders totalling
    200, AOV must be exactly 100, not rounded or truncated."""
    orders = pl.DataFrame({
        "order_id": ["o1", "o2"],
        "customer_id": ["c1", "c2"],
        "order_purchase_timestamp": [datetime(2024, 3, 1), datetime(2024, 3, 15)],
    })
    payments = pl.DataFrame({
        "order_id": ["o1", "o2"],
        "payment_value": [80.0, 120.0],
    })

    result = generate_sales_performance(orders, payments)

    assert result["order_count"].to_list() == [2]
    assert result["aov"].to_list() == [100.0]


def test_generate_sales_performance_sorted_by_month_ascending():
    orders = pl.DataFrame({
        "order_id": ["o1", "o2", "o3"],
        "customer_id": ["c1", "c2", "c3"],
        "order_purchase_timestamp": [
            datetime(2024, 3, 1),
            datetime(2024, 1, 1),
            datetime(2024, 2, 1),
        ],
    })
    payments = pl.DataFrame({
        "order_id": ["o1", "o2", "o3"],
        "payment_value": [10.0, 20.0, 30.0],
    })

    result = generate_sales_performance(orders, payments)

    assert result["month"].to_list() == ["2024-01", "2024-02", "2024-03"]
