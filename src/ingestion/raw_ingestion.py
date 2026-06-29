from pathlib import Path

import polars as pl

from src.ingestion.schemas import get_schema
from src.ingestion.validation import validate_payments


def ingest_file(source_path: str) -> dict:
    """
    Ingest a source CSV file into the raw layer.

    Args:
        source_path: Source CSV path.

    Returns:
        Ingestion statistics.
    """
    source_file = Path(source_path)

    dataset_name = source_file.stem
    schema = get_schema(dataset_name)

    output_path = source_file.parent.parent / "raw" / f"{dataset_name}.parquet"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    if schema is None:
        df = pl.read_csv(source_file)
    else:
        df = pl.read_csv(
            source_file,
            schema=schema,
        )
    if dataset_name == "payments":
        validate_payments(df)

    row_count = df.height

    df.write_parquet(output_path)

    parquet_df = pl.read_parquet(output_path)
    parquet_row_count = parquet_df.height

    if row_count != parquet_row_count:
        raise ValueError(
            f"Row count mismatch for '{source_file.name}'. "
            f"CSV rows: {row_count}, "
            f"Parquet rows: {parquet_row_count}."
        )

    return {
        "source_file_path": str(source_file),
        "output_file_path": str(output_path),
        "source_row_count": row_count,
        "output_row_count": parquet_row_count,
    }
