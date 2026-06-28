import polars as pl

from src.staging.transformations import to_snake_case


def clean_payments(df: pl.DataFrame) -> pl.DataFrame:
    df = to_snake_case(df)
    return df