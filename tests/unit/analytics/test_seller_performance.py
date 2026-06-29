"""
Unit tests for src/analytics/seller_performance.py.

Revenue is item-level (order_items.price), not payment-level, since
sellers are paid per item. The left join on reviews means a seller
with no reviews gets a null avg_review_score - that null must be
preserved, not defaulted to 0, which would distort aggregates.
"""

import polars as pl

from src.analytics.seller_performance import generate_seller_performance


def test_generate_seller_performance_aggregates_revenue_and_order_count(
    sample_order_items_df, sample_reviews_df
):
    result = generate_seller_performance(sample_order_items_df, sample_reviews_df)

    s1 = result.filter(pl.col("seller_id") == "s1")
    assert s1["revenue"].to_list() == [130.0]  # 50+50+30
    assert s1["order_count"].to_list() == [2]  # o1 and o3


def test_generate_seller_performance_seller_with_no_reviews_gets_null_avg_score(
    sample_order_items_df,
):
    """A seller present in order_items but absent from reviews must
    produce a null avg_review_score, not 0 or an error, because the
    left join must preserve all sellers regardless of review coverage."""
    reviews = pl.DataFrame(
        {
            "order_id": ["o999"],  # order not in order_items
            "review_score": [5],
        }
    )

    result = generate_seller_performance(sample_order_items_df, reviews)

    for row in result.iter_rows(named=True):
        assert (
            row["avg_review_score"] is None
        ), f"seller {row['seller_id']} has no reviews and must have null avg_review_score"


def test_generate_seller_performance_sorted_by_revenue_descending(
    sample_order_items_df, sample_reviews_df
):
    result = generate_seller_performance(sample_order_items_df, sample_reviews_df)

    revenues = result["revenue"].to_list()
    assert revenues == sorted(revenues, reverse=True)
