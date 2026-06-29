"""
Analytics fan-in validation gate (Card 6.2).

Hard gate, consistent with Cards 4.1/4.3/5.3's raise behaviour - no
tolerance language in the card's null-check criterion, and GMV must
"reconcile perfectly" (within floating-point precision, not bit-for-
bit equality - see Card 6.1's verified ~5e-9 summation-order noise).
"""

import polars as pl


class AnalyticsValidationError(Exception):
    """Raised when analytics outputs fail GMV reconciliation or contain
    nulls in key financial columns."""


GMV_TOLERANCE = 0.01


def validate_gmv_reconciliation(
    sales_performance_df: pl.DataFrame,
    payments_df: pl.DataFrame,
) -> dict:
    """
    Confirm sales_performance's total GMV reconciles with the sum of
    all Core payments, within floating-point tolerance.

    Returns:
        {"analytics_gmv": ..., "core_payments_total": ..., "difference": ...}

    Raises:
        AnalyticsValidationError: if the difference exceeds GMV_TOLERANCE.
    """
    analytics_gmv = sales_performance_df["gmv"].sum()
    core_total = payments_df["payment_value"].sum()
    difference = abs(analytics_gmv - core_total)

    result = {
        "analytics_gmv": analytics_gmv,
        "core_payments_total": core_total,
        "difference": difference,
    }

    if difference > GMV_TOLERANCE:
        raise AnalyticsValidationError(
            f"GMV reconciliation failed: analytics_gmv={analytics_gmv:.2f}, "
            f"core_payments_total={core_total:.2f}, difference={difference:.4f} "
            f"exceeds tolerance={GMV_TOLERANCE}"
        )

    return result


def validate_no_nulls_in_key_columns(
    sales_performance_df: pl.DataFrame,
    clv_df: pl.DataFrame,
) -> dict:
    """
    Confirm gmv/aov (sales_performance) and clv (customer_lifetime_value)
    contain no nulls.

    Raises:
        AnalyticsValidationError: naming the specific column and count
            if any null is found.
    """
    checks = {
        "gmv": sales_performance_df["gmv"].null_count(),
        "aov": sales_performance_df["aov"].null_count(),
        "clv": clv_df["clv"].null_count(),
    }

    for column, null_count in checks.items():
        if null_count > 0:
            raise AnalyticsValidationError(
                f"Null check failed: column '{column}' has {null_count} null value(s)"
            )

    return checks


def run_analytics_validation(analytics_dir: str, core_dir: str) -> dict:
    """
    Run both Card 6.2 checks: GMV reconciliation and null checks on
    key columns.

    Raises:
        AnalyticsValidationError: on the first check that fails.
    """
    sales_performance = pl.read_parquet(f"{analytics_dir}/sales_performance.parquet")
    clv = pl.read_parquet(f"{analytics_dir}/customer_lifetime_value.parquet")
    payments = pl.read_parquet(f"{core_dir}/core_payments.parquet")

    gmv_result = validate_gmv_reconciliation(sales_performance, payments)
    null_result = validate_no_nulls_in_key_columns(sales_performance, clv)

    return {
        "gmv_reconciliation": gmv_result,
        "null_checks": null_result,
        "passed": True,
    }