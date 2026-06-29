"""
Seller Performance report (Card 6.1).

Revenue here is item-level (order_items.price), not payment-level,
since sellers are compensated per item sold, not per whole-order
payment (which can include freight, multiple installments, etc. at
the order level rather than the seller level).
"""

import polars as pl


def generate_seller_performance(
    order_items_df: pl.DataFrame,
    reviews_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Aggregate revenue, order count, and average review score per
    seller.

    Args:
        order_items_df: Core order_items data (post-quarantine).
        reviews_df: Core reviews data (post-quarantine).

    Returns:
        One row per seller_id: seller_id, revenue, order_count, avg_review_score.
    """
    seller_revenue = order_items_df.group_by("seller_id").agg(
        [
            pl.col("price").sum().alias("revenue"),
            pl.col("order_id").n_unique().alias("order_count"),
        ]
    )

    order_to_seller = order_items_df.select(["order_id", "seller_id"]).unique()
    reviews_with_seller = reviews_df.join(order_to_seller, on="order_id", how="inner")

    seller_reviews = reviews_with_seller.group_by("seller_id").agg(
        pl.col("review_score").mean().alias("avg_review_score")
    )

    result = seller_revenue.join(seller_reviews, on="seller_id", how="left")

    return result.sort("revenue", descending=True)
