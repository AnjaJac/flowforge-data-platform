"""
Unit tests for src/staging/schema_validation.py (Card 4.1).

Test cases are derived directly from Card 4.1's Acceptance and Failure
Criteria, not from the implementation's current behaviour:

  Acceptance Criterion 1: matching schema -> validation passes silently.
  Failure Criterion: missing columns must NOT pass undetected.
  Failure Criterion: altered datatypes must NOT pass undetected.
  Design decision: an unexpected EXTRA column does not fail validation.
  Design decision: one entity failing must fail the whole gate.

All fixtures are real Parquet files written to a temp directory via
pytest's tmp_path fixture - this exercises the actual Polars read path,
not a mocked stand-in for it.
"""

import pytest
import polars as pl
import yaml

from src.staging.schema_validation import (
    SchemaValidationError,
    load_expected_schemas,
    resolve_polars_dtype,
    validate_entity_schema,
    validate_all_schemas,
)


# ---------------------------------------------------------------------------
# resolve_polars_dtype
# ---------------------------------------------------------------------------

def test_resolve_polars_dtype_known_types():
    """Each type name used in validation.yaml must resolve to the matching
    Polars dtype. This is checked individually rather than just trusting
    one example, since a typo in the mapping table could silently swap
    two types."""
    assert resolve_polars_dtype("String") == pl.String
    assert resolve_polars_dtype("Int64") == pl.Int64
    assert resolve_polars_dtype("Float64") == pl.Float64


def test_resolve_polars_dtype_unknown_type_raises():
    """An unrecognised type name (e.g. a typo in validation.yaml) must
    raise clearly, rather than silently resolving to None or crashing
    with an unrelated error later."""
    with pytest.raises(ValueError, match="Unknown type name"):
        resolve_polars_dtype("NotARealType")


# ---------------------------------------------------------------------------
# load_expected_schemas
# ---------------------------------------------------------------------------

def test_load_expected_schemas_parses_real_yaml_structure(tmp_path):
    """Parses a real validation.yaml-shaped file, including the unrelated
    top-level 'validation' block, and returns only the 'schemas' section
    in the expected shape."""
    config_path = tmp_path / "validation.yaml"
    config_path.write_text(
        yaml.dump(
            {
                "validation": {"enabled": True},
                "schemas": {
                    "customers": {
                        "customer_id": "String",
                        "customer_zip_code_prefix": "Int64",
                    }
                },
            }
        )
    )

    result = load_expected_schemas(config_path)

    assert result == {
        "customers": {
            "customer_id": "String",
            "customer_zip_code_prefix": "Int64",
        }
    }


def test_load_expected_schemas_missing_file_raises(tmp_path):
    missing_path = tmp_path / "does_not_exist.yaml"
    with pytest.raises(FileNotFoundError):
        load_expected_schemas(missing_path)


def test_load_expected_schemas_missing_schemas_key_raises(tmp_path):
    """A config file that exists but has no 'schemas' key at all is a
    distinct failure mode from a missing file, and should be reported
    as such rather than crashing with a generic KeyError on later use."""
    config_path = tmp_path / "validation.yaml"
    config_path.write_text(yaml.dump({"validation": {"enabled": True}}))

    with pytest.raises(KeyError, match="schemas"):
        load_expected_schemas(config_path)


# ---------------------------------------------------------------------------
# validate_entity_schema
# ---------------------------------------------------------------------------

def _write_parquet(path, data: dict) -> None:
    """Helper: write a tiny real Parquet file from a dict of column data."""
    pl.DataFrame(data).write_parquet(path)


def test_validate_entity_schema_passes_on_exact_match(tmp_path):
    """Acceptance Criterion 1: when raw matches the expected schema
    exactly, validation must pass silently (no exception)."""
    raw_path = tmp_path / "customers.parquet"
    _write_parquet(
        raw_path,
        {
            "customer_id": ["c1", "c2"],
            "customer_zip_code_prefix": [1001, 1002],
        },
    )
    expected_schema = {
        "customer_id": "String",
        "customer_zip_code_prefix": "Int64",
    }

    # Should not raise.
    validate_entity_schema("customers", raw_path, expected_schema)


def test_validate_entity_schema_missing_column_raises(tmp_path):
    """Failure Criterion: a missing expected column must not pass
    undetected - it must raise SchemaValidationError."""
    raw_path = tmp_path / "customers.parquet"
    _write_parquet(raw_path, {"customer_id": ["c1", "c2"]})  # zip column absent
    expected_schema = {
        "customer_id": "String",
        "customer_zip_code_prefix": "Int64",
    }

    with pytest.raises(SchemaValidationError, match="missing expected column"):
        validate_entity_schema("customers", raw_path, expected_schema)


def test_validate_entity_schema_wrong_type_raises(tmp_path):
    """Failure Criterion: an altered datatype on an expected column must
    not pass undetected. Here the zip column is written as a String
    instead of the expected Int64."""
    raw_path = tmp_path / "customers.parquet"
    _write_parquet(
        raw_path,
        {
            "customer_id": ["c1", "c2"],
            "customer_zip_code_prefix": ["1001", "1002"],  # wrong type
        },
    )
    expected_schema = {
        "customer_id": "String",
        "customer_zip_code_prefix": "Int64",
    }

    with pytest.raises(SchemaValidationError, match="has type"):
        validate_entity_schema("customers", raw_path, expected_schema)


def test_validate_entity_schema_extra_column_is_allowed(tmp_path):
    """Design decision: a column present in raw but NOT listed in the
    expected schema must NOT cause a failure. Source systems may add
    columns over time without breaking the pipeline."""
    raw_path = tmp_path / "customers.parquet"
    _write_parquet(
        raw_path,
        {
            "customer_id": ["c1", "c2"],
            "customer_zip_code_prefix": [1001, 1002],
            "some_new_column_nobody_expected": ["x", "y"],
        },
    )
    expected_schema = {
        "customer_id": "String",
        "customer_zip_code_prefix": "Int64",
    }

    # Should not raise, even though raw has more columns than expected.
    validate_entity_schema("customers", raw_path, expected_schema)


def test_validate_entity_schema_missing_file_raises(tmp_path):
    raw_path = tmp_path / "does_not_exist.parquet"
    expected_schema = {"customer_id": "String"}

    with pytest.raises(SchemaValidationError, match="not found"):
        validate_entity_schema("customers", raw_path, expected_schema)


# ---------------------------------------------------------------------------
# validate_all_schemas
# ---------------------------------------------------------------------------

def test_validate_all_schemas_passes_when_every_entity_matches(tmp_path):
    """All entities matching their expected schema -> the whole gate
    passes silently."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    _write_parquet(raw_dir / "customers.parquet", {"customer_id": ["c1"]})
    _write_parquet(raw_dir / "orders.parquet", {"order_id": ["o1"]})

    config_path = tmp_path / "validation.yaml"
    config_path.write_text(
        yaml.dump(
            {
                "schemas": {
                    "customers": {"customer_id": "String"},
                    "orders": {"order_id": "String"},
                }
            }
        )
    )

    # Should not raise.
    validate_all_schemas(raw_dir, config_path)


def test_validate_all_schemas_one_bad_entity_fails_the_whole_gate(tmp_path):
    """Design decision: if even one entity fails, the entire validation
    task must fail - good entities do not get a partial pass. This test
    also checks that the failing entity's name is identifiable in the
    error, which matters for debugging a real failed DAG run."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    # customers is fine...
    _write_parquet(raw_dir / "customers.parquet", {"customer_id": ["c1"]})
    # ...but orders is missing its required column entirely.
    _write_parquet(raw_dir / "orders.parquet", {"some_other_column": ["x"]})

    config_path = tmp_path / "validation.yaml"
    config_path.write_text(
        yaml.dump(
            {
                "schemas": {
                    "customers": {"customer_id": "String"},
                    "orders": {"order_id": "String"},
                }
            }
        )
    )

    with pytest.raises(SchemaValidationError, match=r"\[orders\]"):
        validate_all_schemas(raw_dir, config_path)