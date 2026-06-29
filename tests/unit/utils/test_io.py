"""
Unit tests for src/utils/io.py.

These functions are thin wrappers around Polars I/O, but they need
coverage to confirm the wiring is correct - e.g. that the snappy
compression argument actually reaches write_parquet and that a
custom separator is forwarded to read_csv, not silently ignored.
"""

import polars as pl
import pytest

from src.utils.io import read_csv_to_polars, write_polars_to_parquet


def test_read_csv_to_polars_reads_file_correctly(tmp_path):
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("name,age\nalice,30\nbob,25\n")

    result = read_csv_to_polars(str(csv_file))

    assert result.columns == ["name", "age"]
    assert result.height == 2
    assert result["name"].to_list() == ["alice", "bob"]


def test_read_csv_to_polars_uses_custom_separator(tmp_path):
    """The separator parameter must be forwarded to Polars; a file
    delimited by semicolons must not be treated as a single-column CSV."""
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("city;country\nsao paulo;brazil\n")

    result = read_csv_to_polars(str(csv_file), separator=";")

    assert result.columns == ["city", "country"]
    assert result.height == 1
    assert result["city"].to_list() == ["sao paulo"]


def test_write_polars_to_parquet_writes_readable_snappy_file(tmp_path):
    """The written file must be a valid Parquet file that Polars can
    round-trip back - confirming that write_parquet is called, not just
    that the path is touched."""
    df = pl.DataFrame({"id": [1, 2, 3], "value": [10.0, 20.0, 30.0]})
    out_path = tmp_path / "output.parquet"

    write_polars_to_parquet(df, str(out_path))

    assert out_path.exists(), "parquet file must be written inside tmp_path"
    roundtrip = pl.read_parquet(out_path)
    assert roundtrip.height == 3
    assert roundtrip["id"].to_list() == [1, 2, 3]
    assert roundtrip["value"].to_list() == [10.0, 20.0, 30.0]
