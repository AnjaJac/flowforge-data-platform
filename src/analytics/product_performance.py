"""
Product Performance report (Card 6.1).

Revenue is item-level (order_items.price), consistent with
seller_performance's reasoning - products are sold per item, not
per whole-order payment.
"""

import polars as pl


def generate_product_performance(
    order_items_df: pl.DataFrame,
    reviews_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Aggregate revenue, order count, and average review score per
    product.

    Args:
        order_items_df: Core order_items data (post-quarantine).
        reviews_df: Core reviews data (post-quarantine).

    Returns:
        One row per product_id: product_id, revenue, order_count, avg_review_score.
    """
    product_revenue = order_items_df.group_by("product_id").agg(
        [
            pl.col("price").sum().alias("revenue"),
            pl.col("order_id").n_unique().alias("order_count"),
        ]
    )

    order_to_product = order_items_df.select(["order_id", "product_id"]).unique()
    reviews_with_product = reviews_df.join(order_to_product, on="order_id", how="inner")

    product_reviews = reviews_with_product.group_by("product_id").agg(
        pl.col("review_score").mean().alias("avg_review_score")
    )

    result = product_revenue.join(product_reviews, on="product_id", how="left")

    return result.sort("revenue", descending=True)
