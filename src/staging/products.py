import polars as pl

from src.staging.transformations import to_snake_case, fill_null_with


def clean_products(df: pl.DataFrame) -> pl.DataFrame:
    df = to_snake_case(df)
    df = df.rename(
        {
            "product_name_lenght": "product_name_length",
            "product_description_lenght": "product_description_length",
        }
    )
    df = fill_null_with(df, "product_category_name", "unknown")
    return df
