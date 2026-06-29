"""
Unit tests for src/ingestion/raw_ingestion.py.

ingest_file derives its output path from the source path:
  output = source_file.parent.parent / "raw" / f"{name}.parquet"
Tests explicitly confirm the written file lives inside tmp_path,
not at any hardcoded system path, by asserting both that
tmp_path / "raw" / "{name}.parquet" exists and that
result["output_file_path"] matches that path exactly.

The row-count mismatch branch (lines 53-57) cannot be triggered
without mocking Polars I/O - it compares a count taken before
write_parquet against a count read back immediately after, so
they will always agree in any real filesystem test.
"""

import polars as pl
import pytest

from src.ingestion.raw_ingestion import ingest_file


def test_ingest_file_no_schema_dataset_writes_parquet_and_returns_stats(tmp_path):
    """For datasets without an explicit schema (e.g. customers), Polars
    infers types freely. The output parquet must land inside tmp_path/raw/,
    not at any absolute default path."""
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    csv_path = source_dir / "customers.csv"
    csv_path.write_text("customer_id,customer_city\nc1,sao paulo\nc2,rio\n")

    result = ingest_file(str(csv_path))

    expected_output = tmp_path / "raw" / "customers.parquet"
    assert (
        expected_output.exists()
    ), "output parquet must be written to tmp_path/raw/, not a system default path"
    assert result["output_file_path"] == str(
        expected_output
    ), "returned output_file_path must match the actual tmp_path location"
    assert result["source_file_path"] == str(csv_path)
    assert result["source_row_count"] == 2
    assert result["output_row_count"] == 2


def test_ingest_file_applies_typed_schema_for_payments(tmp_path):
    """For payments, ingest_file applies the PAYMENTS_SCHEMA on CSV
    read, enforcing Int64 for sequential/installments and Float64 for
    value - not leaving them as inferred types."""
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    csv_path = source_dir / "payments.csv"
    csv_path.write_text(
        "order_id,payment_sequential,payment_type,payment_installments,payment_value\n"
        "o1,1,credit_card,1,100.0\n"
        "o2,1,boleto,1,50.0\n"
    )

    result = ingest_file(str(csv_path))

    expected_output = tmp_path / "raw" / "payments.parquet"
    assert (
        expected_output.exists()
    ), "payments parquet must be written to tmp_path/raw/, not a system default path"
    assert result["output_file_path"] == str(expected_output)
    parquet_df = pl.read_parquet(expected_output)
    assert parquet_df["payment_value"].dtype == pl.Float64
    assert parquet_df["payment_sequential"].dtype == pl.Int64


def test_ingest_file_raises_on_negative_payment_value(tmp_path):
    """validate_payments is called only for the payments dataset;
    a negative payment_value must propagate as ValueError."""
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    csv_path = source_dir / "payments.csv"
    csv_path.write_text(
        "order_id,payment_sequential,payment_type,payment_installments,payment_value\n"
        "o1,1,credit_card,1,-50.0\n"
    )

    with pytest.raises(ValueError, match="payment"):
        ingest_file(str(csv_path))
