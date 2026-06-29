"""
Unit tests for src/analytics/product_performance.py.

Revenue is item-level (order_items.price), matching seller_performance's
reasoning - products are priced per item, not per whole-order payment.
The left join on reviews is intentional: products with no reviews must
still appear in the result with null avg_review_score.
"""

import polars as pl

from src.analytics.product_performance import generate_product_performance


def test_generate_product_performance_aggregates_revenue_and_order_count(
    sample_order_items_df, sample_reviews_df
):
    result = generate_product_performance(sample_order_items_df, sample_reviews_df)

    p1 = result.filter(pl.col("product_id") == "p1")
    assert p1["revenue"].to_list() == [130.0]  # 50+50+30
    assert p1["order_count"].to_list() == [2]  # o1 and o3 (unique)

    p2 = result.filter(pl.col("product_id") == "p2")
    assert p2["revenue"].to_list() == [80.0]
    assert p2["order_count"].to_list() == [1]


def test_generate_product_performance_product_with_no_reviews_gets_null_avg_score(
    sample_order_items_df,
):
    """A product in order_items but absent from reviews must produce
    null avg_review_score, not 0 - a left join preserves all products."""
    reviews = pl.DataFrame({
        "order_id": ["o_not_in_items"],
        "review_score": [5],
    })

    result = generate_product_performance(sample_order_items_df, reviews)

    for row in result.iter_rows(named=True):
        assert row["avg_review_score"] is None, (
            f"product {row['product_id']} has no reviews and must have null avg_review_score"
        )


def test_generate_product_performance_sorted_by_revenue_descending(
    sample_order_items_df, sample_reviews_df
):
    result = generate_product_performance(sample_order_items_df, sample_reviews_df)

    revenues = result["revenue"].to_list()
    assert revenues == sorted(revenues, reverse=True)
