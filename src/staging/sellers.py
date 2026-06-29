import polars as pl

from src.staging.transformations import to_snake_case, uppercase_columns


def clean_sellers(df: pl.DataFrame) -> pl.DataFrame:
    df = to_snake_case(df)
    df = uppercase_columns(df, ["seller_city", "seller_state"])
    return df
