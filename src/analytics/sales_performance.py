"""
Sales Performance report (Card 6.1).

GMV is defined as a pure pass-through of core_payments.payment_value,
summed - never recomputed from order_items. This is deliberate: per
the Medallion architecture principle, Analytics consumes the already
validated Core layer's business truth rather than re-deriving it.
This also makes Card 6.2's GMV reconciliation check correct by
construction, since both sides ultimately trace to the same source.
"""

import polars as pl


def generate_sales_performance(
    orders_df: pl.DataFrame,
    payments_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Aggregate GMV, order count, and AOV by month.

    Args:
        orders_df: Core orders data (post-quarantine).
        payments_df: Core payments data (post-quarantine).

    Returns:
        One row per month: month, gmv, order_count, aov.
    """
    orders_with_month = orders_df.with_columns(
        pl.col("order_purchase_timestamp").dt.strftime("%Y-%m").alias("month")
    )

    order_to_month = orders_with_month.select(["order_id", "month"])

    payments_with_month = payments_df.join(order_to_month, on="order_id", how="inner")

    monthly = payments_with_month.group_by("month").agg(
        [
            pl.col("payment_value").sum().alias("gmv"),
        ]
    )

    order_counts = orders_with_month.group_by("month").agg(
        pl.col("order_id").n_unique().alias("order_count")
    )

    result = monthly.join(order_counts, on="month", how="inner")
    result = result.with_columns((pl.col("gmv") / pl.col("order_count")).alias("aov"))

    return result.sort("month")
