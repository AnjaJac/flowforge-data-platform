"""
Customer Retention report (Card 6.1).

True cohort analysis: each customer is assigned to a cohort based on
their first purchase month. For every month across the FULL dataset
date range, we compute what fraction of each cohort placed an order
that month. Months with zero activity for a cohort must still appear
explicitly as 0 - omitting them would make visualizations misleading
and would violate the card's explicit no-gaps Failure Criterion.
"""

import polars as pl


def generate_customer_retention(orders_df: pl.DataFrame) -> pl.DataFrame:
    """
    Build monthly cohort retention, with no gaps across the full
    dataset history.

    Args:
        orders_df: Core orders data (post-quarantine).

    Returns:
        One row per (cohort_month, activity_month): cohort_month,
        activity_month, cohort_size, active_customers, retention_rate.
        Includes explicit 0 rows for months with no activity.
    """
    orders_with_month = orders_df.with_columns(
        pl.col("order_purchase_timestamp").dt.strftime("%Y-%m").alias("order_month")
    )

    first_purchase = (
        orders_with_month
        .group_by("customer_id")
        .agg(pl.col("order_month").min().alias("cohort_month"))
    )

    cohort_sizes = (
        first_purchase
        .group_by("cohort_month")
        .agg(pl.col("customer_id").n_unique().alias("cohort_size"))
    )

    all_months = sorted(orders_with_month["order_month"].unique().to_list())

    customer_activity = orders_with_month.join(first_purchase, on="customer_id", how="left")
    activity_counts = (
        customer_activity
        .group_by(["cohort_month", "order_month"])
        .agg(pl.col("customer_id").n_unique().alias("active_customers"))
        .rename({"order_month": "activity_month"})
    )

    all_cohorts = cohort_sizes["cohort_month"].to_list()
    full_grid = pl.DataFrame(
        [(c, m) for c in all_cohorts for m in all_months if m >= c],
        schema=["cohort_month", "activity_month"],
        orient="row",
    )

    result = full_grid.join(
        activity_counts, on=["cohort_month", "activity_month"], how="left"
    )
    result = result.with_columns(pl.col("active_customers").fill_null(0))

    result = result.join(cohort_sizes, on="cohort_month", how="left")
    result = result.with_columns(
        (pl.col("active_customers") / pl.col("cohort_size")).alias("retention_rate")
    )

    return result.sort(["cohort_month", "activity_month"])