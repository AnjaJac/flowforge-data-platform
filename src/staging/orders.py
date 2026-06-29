import polars as pl

from src.staging.transformations import to_snake_case, parse_dates


def clean_orders(df: pl.DataFrame) -> pl.DataFrame:
    df = to_snake_case(df)
    df = parse_dates(
        df,
        [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    )
    return df


def clean_order_items(df: pl.DataFrame) -> pl.DataFrame:
    df = to_snake_case(df)
    df = parse_dates(df, ["shipping_limit_date"])
    return df
