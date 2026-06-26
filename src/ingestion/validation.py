import polars as pl
import logging

logger = logging.getLogger(__name__)


def validate_payments(df: pl.DataFrame) -> None:
    """
    Validate the payments dataset.

    Rules:
        - payment_value must not be negative.
        - payment_value equal to zero is allowed but logged as a warning.

    Raises:
        ValueError: If negative payment values are found.
    """
    invalid_rows = df.filter(
        pl.col("payment_value") < 0
    )

    zero_value_rows = df.filter(
        pl.col("payment_value") == 0
    )

    if zero_value_rows.height > 0:
        logger.warning(
            "Found %d payment(s) with payment_value == 0.",
            zero_value_rows.height
        )

    if invalid_rows.height > 0:
        raise ValueError(
            f"Found {invalid_rows.height} payment(s) with "
            "payment_value < 0."
        )