import polars as pl

from src.staging.transformations import to_snake_case, parse_dates


def clean_reviews(df: pl.DataFrame) -> pl.DataFrame:
    df = to_snake_case(df)
    df = parse_dates(df, ["review_creation_date", "review_answer_timestamp"])
    return df
