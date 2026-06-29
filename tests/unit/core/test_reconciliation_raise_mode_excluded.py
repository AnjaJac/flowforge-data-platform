"""
Unit tests for the uncovered branch in src/core/reconciliation.py:
line 228 - the `return summary` inside on_failure="raise" when
orders_failed == 0 but excluded.height > 0.

This branch is reached when: raise mode is active, every order
that has order_items reconciles within tolerance, BUT at least one
order has a payment with zero order_items (an "excluded" order).
The existing tests cover raise-with-failure and warn-with-exclusions,
but not raise-with-exclusions-and-no-failures - this file closes
that specific gap.
"""

import polars as pl

from src.core.reconciliation import run_reconciliation


def _write_core_files(core_dir, orders, order_items, payments):
    orders.write_parquet(core_dir / "core_orders.parquet")
    order_items.write_parquet(core_dir / "core_order_items.parquet")
    payments.write_parquet(core_dir / "core_payments.parquet")


def test_run_reconciliation_raise_mode_returns_summary_when_no_failures_but_some_excluded(
    tmp_path,
):
    """Line 228: on_failure="raise", orders_failed == 0, excluded.height > 0.

    o1 reconciles perfectly (100 + 10 = 110 from order_items; 110 in payments).
    o_excluded has a payment but no order_items - it goes into excluded,
    not into the comparison set. Since no reconciliation failure exists,
    the function must return the summary dict without raising."""
    core_dir = tmp_path / "core"
    core_dir.mkdir()

    orders = pl.DataFrame(
        {
            "order_id": ["o1", "o_excluded"],
            "order_status": ["delivered", "canceled"],
        }
    )
    order_items = pl.DataFrame(
        {
            "order_id": ["o1"],
            "price": [100.0],
            "freight_value": [10.0],
        }
    )
    payments = pl.DataFrame(
        {
            "order_id": ["o1", "o_excluded"],
            "payment_value": [110.0, 50.0],
        }
    )
    _write_core_files(core_dir, orders, order_items, payments)

    config_path = tmp_path / "validation.yaml"
    config_path.write_text(
        "financial_reconciliation:\n  tolerance: 0.01\n  on_failure: raise\n"
    )

    summary = run_reconciliation(core_dir, config_path)

    assert summary["orders_failed"] == 0
    assert summary["excluded_order_count"] == 1
    assert summary["on_failure_mode"] == "raise"
    assert (
        "quarantined_order_count" not in summary
    ), "raise mode must not write a quarantine - that is warn mode's responsibility"
