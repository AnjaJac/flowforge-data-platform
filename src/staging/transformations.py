"""
Shared Staging-layer transformation building blocks

These functions are intentionally generic in the SAFE sense: each one
takes an explicit list of columns to act on, rather than inferring
which columns to touch based on dtype or name pattern. This is a
deliberate design choice - see the Card 4.2 design discussion - to
avoid the "false generic" trap where a shared function quietly grows
hidden per-entity special cases. Entity-specific decisions (which
columns to uppercase, which to date-parse, which need null-filling)
live in each entity's own module, not here.
"""

import polars as pl


def to_snake_case(df: pl.DataFrame) -> pl.DataFrame:
    """
    Rename every column in the DataFrame to a clean snake_case form:
    lowercase, internal whitespace/hyphens replaced with underscores,
    leading/trailing whitespace stripped.

    This dataset's raw column names are already lowercase-with-
    underscores, so in practice this function mostly guarantees the
    contract going forward rather than fixing today's data - it
    protects against future drift (e.g. a source system renaming a
    column to "Customer ID") rather than correcting an existing
    problem.

    Args:
        df: The DataFrame to rename columns on.

    Returns:
        A new DataFrame with renamed columns.
    """
    rename_map = {}
    for col in df.columns:
        clean = col.strip().lower().replace(" ", "_").replace("-", "_")
        rename_map[col] = clean
    return df.rename(rename_map)


def uppercase_columns(df: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    """
    Uppercase the string values in the given columns.

    Only acts on the explicitly named columns - this function never
    infers "all string columns should be uppercased". That decision
    belongs to each entity module (e.g. city/state codes are
    uppercased; status labels like order_status are deliberately not).

    Args:
        df: The DataFrame to transform.
        columns: Exact column names to uppercase.

    Returns:
        A new DataFrame with the given columns uppercased.

    Raises:
        ValueError: if any named column does not exist in df.
    """
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(
            f"uppercase_columns: column(s) not found in DataFrame: {missing}"
        )

    return df.with_columns(
        [pl.col(c).str.to_uppercase() for c in columns]
    )


def parse_dates(
    df: pl.DataFrame,
    columns: list[str],
    format: str = "%Y-%m-%d %H:%M:%S",
) -> pl.DataFrame:
    """
    Parse the given columns from raw timestamp strings into proper
    Polars Datetime values.

    Uses strict=True, which causes Polars itself to raise if any
    value in the column does not match the expected format - this is
    deliberate. Card 4.2's Failure Criterion explicitly states that a
    date conversion producing nulls or leaving unparsed strings in
    critical columns is a failure. A non-strict parse can silently
    turn a malformed value into null, which is exactly the failure
    mode this must not allow.

    Args:
        df: The DataFrame to transform.
        columns: Exact column names to parse as datetimes.
        format: The expected strptime-style format string. Verify
            this against the actual raw data before relying on the
            default - do not assume it matches every date column.

    Returns:
        A new DataFrame with the given columns converted to Datetime.

    Raises:
        ValueError: if any named column does not exist in df.
        polars.exceptions.InvalidOperationError (or similar): if any
            value in a named column does not match the given format -
            propagated from Polars' strict parsing, not caught here.
    """
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(
            f"parse_dates: column(s) not found in DataFrame: {missing}"
        )

    return df.with_columns(
        [
            pl.col(c).str.to_datetime(format, strict=True)
            for c in columns
        ]
    )


def fill_null_with(df: pl.DataFrame, column: str, value: str) -> pl.DataFrame:
    """
    Fill null values in a single column with a given placeholder.

    Deliberately scoped to ONE column at a time, with an explicit
    placeholder value supplied by the caller - this is not a generic
    "fill all nulls" utility. Card 4.2 names exactly one case for this
    (products.product_category_name -> 'unknown'); applying a
    placeholder elsewhere (e.g. review comment fields) was explicitly
    considered and rejected, since a missing review comment is a
    meaningful real state, not missing data.

    Args:
        df: The DataFrame to transform.
        column: Exact column name to fill nulls in.
        value: The placeholder string to use.

    Returns:
        A new DataFrame with nulls in the given column replaced.

    Raises:
        ValueError: if the named column does not exist in df.
    """
    if column not in df.columns:
        raise ValueError(f"fill_null_with: column not found in DataFrame: {column}")

    return df.with_columns(pl.col(column).fill_null(value))