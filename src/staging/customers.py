import polars as pl

from src.staging.transformations import to_snake_case, uppercase_columns


def clean_customers(df: pl.DataFrame) -> pl.DataFrame:
    df = to_snake_case(df)
    df = uppercase_columns(df, ["customer_city", "customer_state"])
    return df