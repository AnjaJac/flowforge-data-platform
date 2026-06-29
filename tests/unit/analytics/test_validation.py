"""
Unit tests for src/analytics/validation.py (Card 6.2).

Three functions:
- validate_gmv_reconciliation: raises AnalyticsValidationError if
  abs(analytics_gmv - core_payments_total) > GMV_TOLERANCE (0.01).
- validate_no_nulls_in_key_columns: raises on any null in gmv, aov, or clv.
- run_analytics_validation: file-based orchestrator, reads three parquets.

For run_analytics_validation, both analytics/ and core/ paths are
tmp_path sub-directories to avoid any real project paths.
"""

import polars as pl
import pytest

from src.analytics.validation import (
    AnalyticsValidationError,
    validate_gmv_reconciliation,
    validate_no_nulls_in_key_columns,
    run_analytics_validation,
)

# ---------------------------------------------------------------------------
# validate_gmv_reconciliation
# ---------------------------------------------------------------------------


def test_validate_gmv_reconciliation_passes_within_tolerance():
    sales = pl.DataFrame({"gmv": [100.0, 50.0]})
    payments = pl.DataFrame({"payment_value": [80.0, 70.0]})

    result = validate_gmv_reconciliation(sales, payments)

    assert result["analytics_gmv"] == 150.0
    assert result["core_payments_total"] == 150.0
    assert result["difference"] == 0.0


def test_validate_gmv_reconciliation_raises_when_difference_exceeds_tolerance():
    """Any absolute difference above GMV_TOLERANCE (0.01) must raise
    AnalyticsValidationError, named explicitly so the caller can handle
    it as a distinct failure type rather than a generic exception."""
    sales = pl.DataFrame({"gmv": [100.0]})
    payments = pl.DataFrame({"payment_value": [200.0]})

    with pytest.raises(AnalyticsValidationError, match="GMV reconciliation failed"):
        validate_gmv_reconciliation(sales, payments)


# ---------------------------------------------------------------------------
# validate_no_nulls_in_key_columns
# ---------------------------------------------------------------------------


def test_validate_no_nulls_passes_when_no_nulls_present():
    sales = pl.DataFrame({"gmv": [100.0, 200.0], "aov": [50.0, 100.0]})
    clv = pl.DataFrame({"clv": [150.0, 250.0]})

    result = validate_no_nulls_in_key_columns(sales, clv)

    assert result["gmv"] == 0
    assert result["aov"] == 0
    assert result["clv"] == 0


def test_validate_no_nulls_raises_on_null_gmv():
    sales = pl.DataFrame({"gmv": [100.0, None], "aov": [50.0, 50.0]})
    clv = pl.DataFrame({"clv": [200.0]})

    with pytest.raises(AnalyticsValidationError, match="'gmv'"):
        validate_no_nulls_in_key_columns(sales, clv)


def test_validate_no_nulls_raises_on_null_clv():
    sales = pl.DataFrame({"gmv": [100.0], "aov": [100.0]})
    clv = pl.DataFrame({"clv": [None]})

    with pytest.raises(AnalyticsValidationError, match="'clv'"):
        validate_no_nulls_in_key_columns(sales, clv)


# ---------------------------------------------------------------------------
# run_analytics_validation
# ---------------------------------------------------------------------------


def test_run_analytics_validation_passes_end_to_end(tmp_path):
    """Both GMV reconciliation and null checks must pass for returned
    dict to have passed=True. Files must be in tmp_path sub-directories
    so no real project data paths are touched."""
    analytics_dir = tmp_path / "analytics"
    analytics_dir.mkdir()
    core_dir = tmp_path / "core"
    core_dir.mkdir()

    pl.DataFrame(
        {"month": ["2024-01"], "gmv": [150.0], "aov": [75.0], "order_count": [2]}
    ).write_parquet(analytics_dir / "sales_performance.parquet")
    pl.DataFrame(
        {
            "customer_id": ["c1"],
            "clv": [150.0],
            "order_count": [2],
            "avg_order_value": [75.0],
        }
    ).write_parquet(analytics_dir / "customer_lifetime_value.parquet")
    pl.DataFrame(
        {"order_id": ["o1", "o2"], "payment_value": [80.0, 70.0]}
    ).write_parquet(core_dir / "core_payments.parquet")

    result = run_analytics_validation(str(analytics_dir), str(core_dir))

    assert result["passed"] is True
    assert "gmv_reconciliation" in result
    assert "null_checks" in result
