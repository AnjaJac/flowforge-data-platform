"""
Customer Lifetime Value report (Card 6.1).
"""

import polars as pl


def generate_customer_lifetime_value(
    orders_df: pl.DataFrame,
    payments_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Aggregate total spend, order count, and average order value per
    customer.

    Args:
        orders_df: Core orders data (post-quarantine).
        payments_df: Core payments data (post-quarantine).

    Returns:
        One row per customer_id: customer_id, clv, order_count, avg_order_value.
    """
    order_to_customer = orders_df.select(["order_id", "customer_id"])

    payments_with_customer = payments_df.join(order_to_customer, on="order_id", how="inner")

    clv = (
        payments_with_customer
        .group_by("customer_id")
        .agg(pl.col("payment_value").sum().alias("clv"))
    )

    order_counts = (
        orders_df
        .group_by("customer_id")
        .agg(pl.col("order_id").n_unique().alias("order_count"))
    )

    result = clv.join(order_counts, on="customer_id", how="inner")
    result = result.with_columns(
        (pl.col("clv") / pl.col("order_count")).alias("avg_order_value")
    )

    return result.sort("clv", descending=True)