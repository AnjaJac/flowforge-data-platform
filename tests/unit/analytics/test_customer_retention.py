"""
Unit tests for src/analytics/customer_retention.py.

The critical design decision is that months where a cohort has zero
activity must appear explicitly in the output with active_customers=0,
not be omitted. Omitting them would make retention curves misleading
by connecting non-adjacent data points.

Requires order_purchase_timestamp as Polars Datetime - built here
with Python datetime objects, which Polars infers as pl.Datetime.
"""

from datetime import datetime

import polars as pl

from src.analytics.customer_retention import generate_customer_retention


def test_generate_customer_retention_assigns_correct_cohort_month():
    """A customer's cohort is their FIRST purchase month, not any
    subsequent month. c1 buys in Jan and Mar; cohort must be Jan."""
    orders = pl.DataFrame({
        "order_id": ["o1", "o2"],
        "customer_id": ["c1", "c1"],
        "order_purchase_timestamp": [datetime(2024, 1, 15), datetime(2024, 3, 10)],
    })

    result = generate_customer_retention(orders)

    assert "2024-01" in result["cohort_month"].to_list()
    assert "2024-03" not in result["cohort_month"].to_list()


def test_generate_customer_retention_zero_activity_months_appear_explicitly():
    """The no-gaps design decision: a cohort month with no activity in
    a later month must appear as active_customers=0, not be absent from
    the result. Skipping it would make retention visualisations
    incorrectly connect non-adjacent points."""
    orders = pl.DataFrame({
        "order_id": ["o1", "o2"],
        "customer_id": ["c1", "c2"],
        "order_purchase_timestamp": [datetime(2024, 1, 10), datetime(2024, 2, 20)],
    })

    result = generate_customer_retention(orders)

    # Cohort 2024-01 (c1) has no activity in 2024-02 - that row must exist with 0
    jan_cohort_feb = result.filter(
        (pl.col("cohort_month") == "2024-01") & (pl.col("activity_month") == "2024-02")
    )
    assert jan_cohort_feb.height == 1, "zero-activity month must appear, not be omitted"
    assert jan_cohort_feb["active_customers"].to_list() == [0]
    assert jan_cohort_feb["retention_rate"].to_list() == [0.0]


def test_generate_customer_retention_rate_is_one_on_cohort_month():
    """Every customer in a cohort made at least one purchase that month
    by definition, so retention_rate must be exactly 1.0 for every
    cohort in its own month."""
    orders = pl.DataFrame({
        "order_id": ["o1", "o2", "o3"],
        "customer_id": ["c1", "c2", "c3"],
        "order_purchase_timestamp": [
            datetime(2024, 1, 5),
            datetime(2024, 1, 12),
            datetime(2024, 1, 20),
        ],
    })

    result = generate_customer_retention(orders)

    jan_self = result.filter(
        (pl.col("cohort_month") == "2024-01") & (pl.col("activity_month") == "2024-01")
    )
    assert jan_self.height == 1
    assert jan_self["cohort_size"].to_list() == [3]
    assert jan_self["retention_rate"].to_list() == [1.0]


def test_generate_customer_retention_sorted_by_cohort_then_activity_month():
    orders = pl.DataFrame({
        "order_id": ["o1", "o2", "o3"],
        "customer_id": ["c1", "c2", "c1"],
        "order_purchase_timestamp": [
            datetime(2024, 1, 1),
            datetime(2024, 2, 1),
            datetime(2024, 2, 1),
        ],
    })

    result = generate_customer_retention(orders)

    cohort_months = result["cohort_month"].to_list()
    activity_months = result["activity_month"].to_list()
    pairs = list(zip(cohort_months, activity_months))
    assert pairs == sorted(pairs)
