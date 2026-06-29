from pathlib import Path

import polars as pl

EXPECTED_RAW_FILES = [
    "customers.parquet",
    "orders.parquet",
    "order_items.parquet",
    "products.parquet",
    "sellers.parquet",
    "payments.parquet",
    "reviews.parquet",
]


def validate_raw_layer(raw_directory: str) -> None:
    """
    Validate the completeness of the Raw layer.

    Validation rules:
        - All expected Parquet files must exist.
        - All Parquet files must contain at least one row.

    Args:
        raw_directory:
            Path to the raw data directory.

    Raises:
        FileNotFoundError:
            If any expected Parquet file is missing.

        ValueError:
            If a Parquet file exists but contains no rows.
    """
    raw_path = Path(raw_directory)

    for filename in EXPECTED_RAW_FILES:
        parquet_file = raw_path / filename

        if not parquet_file.exists():
            raise FileNotFoundError(f"Missing raw dataset: {filename}")
        df = pl.read_parquet(parquet_file)

        if df.height == 0:
            raise ValueError(f"Raw dataset is empty: {filename}")
