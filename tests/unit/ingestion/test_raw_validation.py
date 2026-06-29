"""
Unit tests for src/ingestion/raw_validation.py.

validate_raw_layer checks that the raw Parquet layer is complete
and non-empty after ingestion. Unlike discovery.py which checks
byte size, this function actually reads each Parquet and checks
row count - a Parquet file can be non-zero bytes but contain
zero rows, and that case must also be caught.
"""

import polars as pl
import pytest

from src.ingestion.raw_validation import EXPECTED_RAW_FILES, validate_raw_layer


def _write_all_parquets(raw_dir):
    """Write all required Parquet files, each with one row of data."""
    for name in EXPECTED_RAW_FILES:
        pl.DataFrame({"id": ["x"]}).write_parquet(raw_dir / name)


def test_validate_raw_layer_passes_when_all_files_present_and_non_empty(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    _write_all_parquets(raw_dir)

    validate_raw_layer(str(raw_dir))  # must not raise


def test_validate_raw_layer_raises_on_missing_parquet(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    _write_all_parquets(raw_dir)
    (raw_dir / "orders.parquet").unlink()

    with pytest.raises(FileNotFoundError, match="orders.parquet"):
        validate_raw_layer(str(raw_dir))


def test_validate_raw_layer_raises_on_empty_parquet(tmp_path):
    """A Parquet that exists but has zero rows must be rejected - this
    is a distinct failure mode from a missing file and must be checked
    separately (byte-size alone does not detect it)."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    _write_all_parquets(raw_dir)
    pl.DataFrame({"id": []}, schema={"id": pl.String}).write_parquet(
        raw_dir / "customers.parquet"
    )

    with pytest.raises(ValueError, match="customers.parquet"):
        validate_raw_layer(str(raw_dir))
