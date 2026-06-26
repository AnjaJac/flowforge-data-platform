import polars as pl

ORDERS_SCHEMA = {
    "order_id": pl.String,
    "customer_id": pl.String,
    "order_status": pl.String,
    "order_purchase_timestamp": pl.String,
    "order_approved_at": pl.String,
    "order_delivered_carrier_date": pl.String,
    "order_delivered_customer_date": pl.String,
    "order_estimated_delivery_date": pl.String
}

PAYMENTS_SCHEMA = {
    "order_id": pl.String,
    "payment_sequential": pl.Int64,
    "payment_type": pl.String,
    "payment_installments": pl.Int64,
    "payment_value": pl.Float64
}


def get_schema(dataset_name: str) -> dict[str, pl.DataType] | None:
    """
    Return the explicit schema for a dataset if one exists.
    """
    schemas = {
        "orders": ORDERS_SCHEMA,
        "payments": PAYMENTS_SCHEMA,
    }

    return schemas.get(dataset_name)