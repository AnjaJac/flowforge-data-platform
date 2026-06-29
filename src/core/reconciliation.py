"""
Core-layer financial reconciliation gate (Card 5.3).

Two failure modes, controlled by financial_reconciliation.on_failure
in validation.yaml:
  - "raise": true gate behaviour, matching the card's literal Failure
    Criterion - any order exceeding tolerance stops the DAG.
  - "warn": soft-fail/quarantine mode - failing and excluded orders
    are removed from core_orders/core_order_items/core_payments and
    written, with full diagnostic detail, to
    data/core/excluded_orders.parquet. The pipeline completes
    successfully. This is a standard production pattern (sometimes
    called soft-fail or warn-and-continue) used when a known,
    explained data characteristic should not block an entire
    pipeline run, while still being fully auditable rather than
    silently dropped.

Both modes use IDENTICAL detection logic - the on_failure setting
only changes what happens once a violation is found, never what
counts as one.

Expected total per order: SUM(order_items.price + order_items.freight_value)
Actual paid per order: SUM(payments.payment_value)

DESIGN NOTE - excluded orders (orders with payments but ZERO
order_items): real-data investigation found 775 such orders, 99% of
which have order_status "unavailable" (78%) or "canceled" (21%) -
i.e. orders that never reached a fulfillment state where order_items
would exist, even though payment had already been captured. These
have no real "expected total" to compare against and are excluded
from the tolerance check itself (never compared against a fabricated
expected_total of 0.0), but ARE included in the quarantine output in
warn mode, since their payment value still represents real,
un-reconciled money.

DESIGN NOTE - tolerance failures (orders WITH order_items whose
difference exceeds tolerance): real-data investigation of the
remaining ~379 such orders found a statistically clear pattern -
failed orders have roughly double the median payment_installments of
passed orders (4 vs 1), and discrepancies are overwhelmingly
overpayments. This is consistent with Brazilian credit card
installment financing including interest, which payment_value
captures but the cash-price formula (price + freight_value) cannot.
This is a documented, EXPLAINED LIMITATION of the reconciliation
formula, not a data defect - and was deliberately NOT "fixed" by
reverse-engineering an inferred interest rate from outcome data, since
doing so would mean encoding an unverifiable guess about a real
financial institution's lending terms directly into a financial
validation gate.
"""

from pathlib import Path

import polars as pl
import yaml


class ReconciliationError(Exception):
    """Raised (only in on_failure="raise" mode) when one or more orders'
    payment totals do not reconcile with their order_items totals
    within the configured tolerance."""


def load_reconciliation_config(config_path: str | Path) -> dict:
    """Load the financial_reconciliation section from validation.yaml."""
    config_path = Path(config_path)
    with open(config_path) as f:
        config = yaml.safe_load(f)
    return config["financial_reconciliation"]


def reconcile_payments(
    order_items_df: pl.DataFrame,
    payments_df: pl.DataFrame,
    tolerance: float,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Compare expected order totals (from order_items) against actual
    paid totals (from payments), per order_id.

    Returns:
        A tuple of (comparison, excluded):
        - comparison: one row per order_id that HAS order_items -
          order_id, expected_total, actual_total, difference, passed.
        - excluded: one row per order_id with payments but ZERO
          order_items - order_id, actual_total only.
    """
    expected = (
        order_items_df.with_columns(
            (pl.col("price") + pl.col("freight_value")).alias("item_total")
        )
        .group_by("order_id")
        .agg(pl.col("item_total").sum().alias("expected_total"))
    )

    actual = payments_df.group_by("order_id").agg(
        pl.col("payment_value").sum().alias("actual_total")
    )

    orders_with_items = set(expected["order_id"].to_list())
    orders_with_payments = set(actual["order_id"].to_list())
    excluded_ids = orders_with_payments - orders_with_items

    excluded = actual.filter(pl.col("order_id").is_in(list(excluded_ids)))

    comparison = expected.join(actual, on="order_id", how="left")
    comparison = comparison.with_columns(pl.col("actual_total").fill_null(0.0))
    comparison = comparison.with_columns(
        (pl.col("expected_total") - pl.col("actual_total")).abs().alias("difference")
    )
    comparison = comparison.with_columns(
        (pl.col("difference") <= tolerance).alias("passed")
    )

    return comparison, excluded


def build_quarantine_records(
    comparison: pl.DataFrame,
    excluded: pl.DataFrame,
    orders_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Build the full quarantine record set: every order that either
    failed tolerance OR was excluded for having zero order_items.
    Includes order_status and a "reason" column so the quarantine
    file is self-explanatory without needing this module's source.

    Returns:
        A DataFrame with columns: order_id, order_status, reason,
        expected_total, actual_total, difference.
    """
    failed = comparison.filter(~pl.col("passed")).with_columns(
        pl.lit("exceeded_tolerance").alias("reason")
    )

    excluded_with_reason = excluded.with_columns(
        [
            pl.lit(None, dtype=pl.Float64).alias("expected_total"),
            pl.lit(None, dtype=pl.Float64).alias("difference"),
            pl.lit("no_order_items").alias("reason"),
        ]
    ).select(["order_id", "expected_total", "actual_total", "difference", "reason"])

    failed_aligned = failed.select(
        ["order_id", "expected_total", "actual_total", "difference", "reason"]
    )

    quarantine = pl.concat([failed_aligned, excluded_with_reason])

    quarantine = quarantine.join(
        orders_df.select(["order_id", "order_status"]), on="order_id", how="left"
    )

    return quarantine


def run_reconciliation(core_dir: str | Path, config_path: str | Path) -> dict:
    """
    Run financial reconciliation across all orders.

    In "raise" mode: raises ReconciliationError if any order with
    order_items exceeds tolerance. Core data is left untouched.

    In "warn" mode: never raises. All quarantined orders (tolerance
    failures + zero-order_items exclusions) are removed from
    core_orders.parquet, core_order_items.parquet, and
    core_payments.parquet, and written with full diagnostic detail
    to data/core/excluded_orders.parquet.

    Returns:
        A summary dict including total_orders_checked, orders_passed,
        orders_failed, max_difference, tolerance_used,
        excluded_order_count, excluded_order_status_breakdown,
        on_failure_mode, and (in warn mode) quarantined_order_count
        and quarantine_file_path.
    """
    core_dir = Path(core_dir)
    recon_config = load_reconciliation_config(config_path)
    tolerance = recon_config["tolerance"]
    on_failure = recon_config.get("on_failure", "raise")

    order_items_df = pl.read_parquet(core_dir / "core_order_items.parquet")
    payments_df = pl.read_parquet(core_dir / "core_payments.parquet")
    orders_df = pl.read_parquet(core_dir / "core_orders.parquet")

    comparison, excluded = reconcile_payments(order_items_df, payments_df, tolerance)

    failed = comparison.filter(~pl.col("passed"))
    total_orders_checked = comparison.height
    orders_failed = failed.height
    orders_passed = total_orders_checked - orders_failed
    max_difference = comparison["difference"].max()

    excluded_with_status = excluded.join(
        orders_df.select(["order_id", "order_status"]), on="order_id", how="left"
    )
    status_breakdown = (
        excluded_with_status.group_by("order_status")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .to_dicts()
    )

    summary = {
        "total_orders_checked": total_orders_checked,
        "orders_passed": orders_passed,
        "orders_failed": orders_failed,
        "max_difference": max_difference,
        "tolerance_used": tolerance,
        "excluded_order_count": excluded.height,
        "excluded_order_status_breakdown": status_breakdown,
        "on_failure_mode": on_failure,
    }

    if orders_failed == 0 and excluded.height == 0:
        return summary

    if on_failure == "raise":
        if orders_failed > 0:
            worst = failed.sort("difference", descending=True).row(0, named=True)
            raise ReconciliationError(
                f"{orders_failed} of {total_orders_checked} orders (with order_items) "
                f"failed reconciliation (tolerance={tolerance}). Worst case: "
                f"order_id={worst['order_id']} difference={worst['difference']:.2f}. "
                f"{excluded.height} additional orders were excluded from this check "
                f"because they have payments but zero order_items."
            )
        return summary

    # on_failure == "warn": quarantine and continue.
    quarantine = build_quarantine_records(comparison, excluded, orders_df)
    quarantine_path = core_dir / "excluded_orders.parquet"
    quarantine.write_parquet(quarantine_path)

    quarantined_ids = quarantine["order_id"].to_list()

    orders_df.filter(~pl.col("order_id").is_in(quarantined_ids)).write_parquet(
        core_dir / "core_orders.parquet"
    )
    order_items_df.filter(~pl.col("order_id").is_in(quarantined_ids)).write_parquet(
        core_dir / "core_order_items.parquet"
    )
    payments_df.filter(~pl.col("order_id").is_in(quarantined_ids)).write_parquet(
        core_dir / "core_payments.parquet"
    )

    summary["quarantined_order_count"] = quarantine.height
    summary["quarantine_file_path"] = str(quarantine_path)

    print(
        f"[reconciliation] WARN mode: {quarantine.height} orders quarantined "
        f"({orders_failed} exceeded tolerance, {excluded.height} had zero "
        f"order_items). Written to {quarantine_path}. Pipeline continuing."
    )

    return summary
