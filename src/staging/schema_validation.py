"""
Raw Schema Conformance Validation Gate 

This module validates that raw Parquet files match the structural schema
expected by the Staging layer, BEFORE any transformation logic runs.

It answers exactly one question per entity: "does raw look like what raw
is supposed to look like right now" - it does not attempt to coerce,
fix, or transform anything. Type casting, renaming, and cleanup belong
to Card 4.2 (Entity Cleansing & Normalization), not here.

Design decisions (see Card 4.1 design discussion):
- Type comparison is EXACT, not coercible. "Compatible" was interpreted
  strictly: if validation.yaml says Int64, raw must be Int64, not "close
  enough to cast later."
- A single entity's mismatch fails the ENTIRE validation task. Staging
  has not started yet, so there is nothing partially salvageable to let
  through.
- An extra column present in raw but not listed in validation.yaml does
  NOT cause a failure. Only missing-or-wrong-typed EXPECTED columns do.
"""

from pathlib import Path

import polars as pl
import yaml


class SchemaValidationError(Exception):
    """Raised when a raw dataset's structure does not match its expected schema."""


# Maps the plain-string type names used in validation.yaml to real Polars
# dtypes. YAML has no native concept of a Polars dtype, so this is the one
# small piece of indirection required by storing schemas as configuration.
_TYPE_MAP: dict[str, pl.DataType] = {
    "String": pl.String,
    "Int64": pl.Int64,
    "Float64": pl.Float64,
}


def load_expected_schemas(config_path: str | Path) -> dict[str, dict[str, str]]:
    """
    Load the expected per-entity schemas from validation.yaml.

    Args:
        config_path: Path to validation.yaml.

    Returns:
        A dict shaped like:
            {
                "customers": {"customer_id": "String", ...},
                "orders": {"order_id": "String", ...},
                ...
            }

    Raises:
        FileNotFoundError: if config_path does not exist.
        KeyError: if the file does not contain a top-level "schemas" key.
    """
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Validation config not found: {config_path}")

    with open(config_path) as f:
        config = yaml.safe_load(f)

    if "schemas" not in config:
        raise KeyError(
            f"'{config_path}' does not contain a top-level 'schemas' key"
        )

    return config["schemas"]


def resolve_polars_dtype(type_name: str) -> pl.DataType:
    """
    Translate a plain-string type name (as written in validation.yaml)
    into the corresponding Polars dtype object.

    Args:
        type_name: e.g. "String", "Int64", "Float64".

    Returns:
        The matching pl.DataType.

    Raises:
        ValueError: if type_name is not a recognised type.
    """
    if type_name not in _TYPE_MAP:
        known = ", ".join(sorted(_TYPE_MAP))
        raise ValueError(
            f"Unknown type name '{type_name}' in validation.yaml. "
            f"Known types: {known}"
        )
    return _TYPE_MAP[type_name]


def validate_entity_schema(
    entity_name: str,
    raw_path: str | Path,
    expected_schema: dict[str, str],
) -> None:
    """
    Validate a single entity's raw Parquet file against its expected schema.

    Reads only the schema of the Parquet file (not the data itself), so
    this is cheap even for large files.

    Args:
        entity_name: e.g. "customers" - used only for error messages.
        raw_path: Path to the raw Parquet file for this entity.
        expected_schema: dict of {column_name: type_name} from validation.yaml.

    Raises:
        SchemaValidationError: if any expected column is missing, or if
            any expected column's type does not exactly match.
    """
    raw_path = Path(raw_path)
    if not raw_path.exists():
        raise SchemaValidationError(
            f"[{entity_name}] raw file not found: {raw_path}"
        )

    actual_schema = pl.read_parquet_schema(raw_path)

    for column_name, expected_type_name in expected_schema.items():
        if column_name not in actual_schema:
            raise SchemaValidationError(
                f"[{entity_name}] missing expected column '{column_name}' "
                f"in {raw_path.name}"
            )

        expected_dtype = resolve_polars_dtype(expected_type_name)
        actual_dtype = actual_schema[column_name]

        if actual_dtype != expected_dtype:
            raise SchemaValidationError(
                f"[{entity_name}] column '{column_name}' has type "
                f"{actual_dtype}, expected {expected_dtype}"
            )

    # Note: columns present in raw but NOT in expected_schema are
    # intentionally allowed through. See module docstring.


def validate_all_schemas(
    raw_dir: str | Path,
    config_path: str | Path,
) -> None:
    """
    Validate every entity's raw Parquet file against validation.yaml.

    Entities are checked in a fixed, predictable order. The first
    mismatch encountered raises immediately and stops validation -
    by design, one bad entity fails the whole gate rather than
    collecting all errors and continuing.

    Args:
        raw_dir: Directory containing the raw *.parquet files
            (e.g. data/raw/).
        config_path: Path to validation.yaml.

    Raises:
        SchemaValidationError: on the first entity that fails to match
            its expected schema.
    """
    raw_dir = Path(raw_dir)
    expected_schemas = load_expected_schemas(config_path)

    for entity_name, expected_schema in expected_schemas.items():
        raw_path = raw_dir / f"{entity_name}.parquet"
        validate_entity_schema(entity_name, raw_path, expected_schema)