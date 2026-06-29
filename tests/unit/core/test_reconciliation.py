"""
Unit tests for src/core/reconciliation.py (Card 5.3).

Real Core data only ever exercises the "warn" path with the specific
1154 orders this project's real-data investigation found. These
synthetic tests prove the underlying logic generally: exclusion of
zero-order_items orders, tolerance-based pass/fail, both on_failure
modes, and quarantine file correctness.
"""

import pytest
import polars as pl

from src.core.reconciliation import (
    ReconciliationError,
    reconcile_payments,
    run_reconciliation,
)


def _write_core_files(tmp_path, orders, order_items, payments):
    orders.write_parquet(tmp_path / "core_orders.parquet")
    order_items.write_parquet(tmp_path / "core_order_items.parquet")
    payments.write_parquet(tmp_path / "core_payments.parquet")


# ---------------------------------------------------------------------------
# reconcile_payments
# ---------------------------------------------------------------------------


def test_reconcile_payments_passes_within_tolerance():
    order_items = pl.DataFrame(
        {"order_id": ["o1"], "price": [100.0], "freight_value": [10.0]}
    )
    payments = pl.DataFrame({"order_id": ["o1"], "payment_value": [110.0]})

    comparison, excluded = reconcile_payments(order_items, payments, tolerance=0.01)

    assert comparison["passed"].to_list() == [True]
    assert excluded.height == 0


def test_reconcile_payments_fails_when_exceeding_tolerance():
    order_items = pl.DataFrame(
        {"order_id": ["o1"], "price": [100.0], "freight_value": [10.0]}
    )
    payments = pl.DataFrame({"order_id": ["o1"], "payment_value": [150.0]})

    comparison, excluded = reconcile_payments(order_items, payments, tolerance=0.01)

    assert comparison["passed"].to_list() == [False]
    assert comparison["difference"].to_list() == [40.0]


def test_reconcile_payments_excludes_orders_with_zero_order_items():
    """The key real-world finding: an order with a real payment but
    NO order_items must be excluded from the tolerance check, not
    compared against a fabricated expected_total of 0.0."""
    order_items = pl.DataFrame(
        {"order_id": [], "price": [], "freight_value": []},
        schema={
            "order_id": pl.String,
            "price": pl.Float64,
            "freight_value": pl.Float64,
        },
    )
    payments = pl.DataFrame({"order_id": ["o1"], "payment_value": [500.0]})

    comparison, excluded = reconcile_payments(order_items, payments, tolerance=0.01)

    assert comparison.height == 0
    assert excluded.height == 1
    assert excluded["order_id"].to_list() == ["o1"]


# ---------------------------------------------------------------------------
# run_reconciliation - raise mode
# ---------------------------------------------------------------------------


def test_run_reconciliation_raise_mode_raises_on_violation(tmp_path):
    orders = pl.DataFrame({"order_id": ["o1"], "order_status": ["delivered"]})
    order_items = pl.DataFrame(
        {"order_id": ["o1"], "price": [100.0], "freight_value": [10.0]}
    )
    payments = pl.DataFrame({"order_id": ["o1"], "payment_value": [200.0]})
    _write_core_files(tmp_path, orders, order_items, payments)

    config_path = tmp_path / "validation.yaml"
    config_path.write_text(
        "financial_reconciliation:\n  tolerance: 0.01\n  on_failure: raise\n"
    )

    with pytest.raises(ReconciliationError, match="1 of 1 orders"):
        run_reconciliation(tmp_path, config_path)


def test_run_reconciliation_raise_mode_does_not_modify_core_files(tmp_path):
    """In raise mode, Core data must be left untouched - the
    exception itself is the signal, nothing should be silently
    filtered before the error is raised."""
    orders = pl.DataFrame({"order_id": ["o1"], "order_status": ["delivered"]})
    order_items = pl.DataFrame(
        {"order_id": ["o1"], "price": [100.0], "freight_value": [10.0]}
    )
    payments = pl.DataFrame({"order_id": ["o1"], "payment_value": [200.0]})
    _write_core_files(tmp_path, orders, order_items, payments)

    config_path = tmp_path / "validation.yaml"
    config_path.write_text(
        "financial_reconciliation:\n  tolerance: 0.01\n  on_failure: raise\n"
    )

    with pytest.raises(ReconciliationError):
        run_reconciliation(tmp_path, config_path)

    untouched_orders = pl.read_parquet(tmp_path / "core_orders.parquet")
    assert untouched_orders.height == 1


# ---------------------------------------------------------------------------
# run_reconciliation - warn mode
# ---------------------------------------------------------------------------


def test_run_reconciliation_warn_mode_never_raises_and_quarantines(tmp_path):
    orders = pl.DataFrame(
        {
            "order_id": ["o1", "o2"],
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
            "order_id": ["o1", "o2"],
            "payment_value": [110.0, 50.0],
        }
    )
    _write_core_files(tmp_path, orders, order_items, payments)

    config_path = tmp_path / "validation.yaml"
    config_path.write_text(
        "financial_reconciliation:\n  tolerance: 0.01\n  on_failure: warn\n"
    )

    summary = run_reconciliation(tmp_path, config_path)

    assert summary["quarantined_order_count"] == 1
    assert summary["orders_passed"] == 1

    remaining_orders = pl.read_parquet(tmp_path / "core_orders.parquet")
    assert remaining_orders["order_id"].to_list() == ["o1"]

    quarantine = pl.read_parquet(tmp_path / "excluded_orders.parquet")
    assert quarantine["order_id"].to_list() == ["o2"]
    assert quarantine["reason"].to_list() == ["no_order_items"]


def test_run_reconciliation_warn_mode_removes_quarantined_orders_from_all_tables(
    tmp_path,
):
    """Confirms the order is removed from orders, order_items, AND
    payments - not just one of the three tables."""
    orders = pl.DataFrame(
        {"order_id": ["o1", "o2"], "order_status": ["delivered", "delivered"]}
    )
    order_items = pl.DataFrame(
        {
            "order_id": ["o1", "o2"],
            "price": [100.0, 100.0],
            "freight_value": [10.0, 10.0],
        }
    )
    payments = pl.DataFrame({"order_id": ["o1", "o2"], "payment_value": [110.0, 999.0]})
    _write_core_files(tmp_path, orders, order_items, payments)

    config_path = tmp_path / "validation.yaml"
    config_path.write_text(
        "financial_reconciliation:\n  tolerance: 0.01\n  on_failure: warn\n"
    )

    run_reconciliation(tmp_path, config_path)

    remaining_orders = pl.read_parquet(tmp_path / "core_orders.parquet")
    remaining_items = pl.read_parquet(tmp_path / "core_order_items.parquet")
    remaining_payments = pl.read_parquet(tmp_path / "core_payments.parquet")

    assert "o2" not in remaining_orders["order_id"].to_list()
    assert "o2" not in remaining_items["order_id"].to_list()
    assert "o2" not in remaining_payments["order_id"].to_list()


def test_run_reconciliation_no_violations_returns_clean_summary(tmp_path):
    orders = pl.DataFrame({"order_id": ["o1"], "order_status": ["delivered"]})
    order_items = pl.DataFrame(
        {"order_id": ["o1"], "price": [100.0], "freight_value": [10.0]}
    )
    payments = pl.DataFrame({"order_id": ["o1"], "payment_value": [110.0]})
    _write_core_files(tmp_path, orders, order_items, payments)

    config_path = tmp_path / "validation.yaml"
    config_path.write_text(
        "financial_reconciliation:\n  tolerance: 0.01\n  on_failure: warn\n"
    )

    summary = run_reconciliation(tmp_path, config_path)

    assert summary["orders_failed"] == 0
    assert summary["excluded_order_count"] == 0
    assert "quarantined_order_count" not in summary
