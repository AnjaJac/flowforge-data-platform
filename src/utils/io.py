import polars as pl

def read_csv_to_polars(file_path: str, separator: str = ",") -> pl.DataFrame:
    """
    Read a CSV file into a Polars DataFrame

    Args:
        file_path: Path to the CSV file.
        separator: CSV delimiter

    Returns:
        Polars DataFrame.
    """
    return pl.read_csv(file_path, separator=separator)


def write_polars_to_parquet(df: pl.DataFrame, file_path: str) -> None:
    """
    Write a Polars DataFrame to a compressed Parquet file.

    Args:
        df: Polars DataFrame.
        file_path: Path to the output Parquet file.
        
    """
    df.write_parquet(file_path, compression="snappy")

