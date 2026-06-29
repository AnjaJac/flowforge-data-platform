"""
Unit tests for the file-I/O layer of src/core/relationship_validation.py:
load_fk_config and run_relationship_validation (lines 38-41, 149-170).

The pure-logic functions (check_foreign_key, validate_entity_relationships)
are already covered in test_relationship_validation.py. These tests
cover only the orchestration layer: reading config from disk, reading
and writing core parquets, and confirming orphan removal is persisted.
"""

import polars as pl
import yaml

from src.core.relationship_validation import load_fk_config, run_relationship_validation

# ---------------------------------------------------------------------------
# load_fk_config
# ---------------------------------------------------------------------------


def test_load_fk_config_returns_foreign_keys_section(tmp_path):
    config = {
        "foreign_keys": {
            "orders": [
                {
                    "column": "customer_id",
                    "references_entity": "customers",
                    "references_column": "customer_id",
                }
            ]
        }
    }
    config_path = tmp_path / "validation.yaml"
    config_path.write_text(yaml.dump(config))

    result = load_fk_config(config_path)

    assert "orders" in result
    assert result["orders"][0]["column"] == "customer_id"


def test_load_fk_config_returns_empty_dict_when_key_absent(tmp_path):
    """A validation.yaml without a foreign_keys section must return {},
    not raise - some pipeline stages call load_fk_config on a shared
    config file that may not yet define any FK rules."""
    config_path = tmp_path / "validation.yaml"
    config_path.write_text(yaml.dump({"schemas": {}}))

    result = load_fk_config(config_path)

    assert result == {}


# ---------------------------------------------------------------------------
# run_relationship_validation
# ---------------------------------------------------------------------------


def test_run_relationship_validation_filters_orphans_and_overwrites_file(tmp_path):
    """run_relationship_validation must remove orphaned rows from the
    entity's parquet on disk (SCD Type 1 in-place overwrite), not just
    return a report while leaving the bad data in place. The output
    parquet must live inside tmp_path, not at any system default path."""
    core_dir = tmp_path / "core"
    core_dir.mkdir()

    customers = pl.DataFrame({"customer_id": ["c1", "c2"]})
    orders = pl.DataFrame(
        {
            "order_id": ["o1", "o2", "o3"],
            "customer_id": ["c1", "c2", "c_orphan"],
        }
    )
    customers.write_parquet(core_dir / "core_customers.parquet")
    orders.write_parquet(core_dir / "core_orders.parquet")

    config = {
        "foreign_keys": {
            "orders": [
                {
                    "column": "customer_id",
                    "references_entity": "customers",
                    "references_column": "customer_id",
                }
            ]
        }
    }
    config_path = tmp_path / "validation.yaml"
    config_path.write_text(yaml.dump(config))

    report = run_relationship_validation(core_dir, config_path)

    orders_on_disk = pl.read_parquet(core_dir / "core_orders.parquet")
    assert (
        orders_on_disk.height == 2
    ), "orphaned row must be removed from the file on disk"
    assert "c_orphan" not in orders_on_disk["customer_id"].to_list()

    assert report["orders"]["customer_id"]["removed_count"] == 1
    assert report["orders"]["customer_id"]["missing_values"] == ["c_orphan"]


def test_run_relationship_validation_returns_empty_report_when_no_fk_rules(tmp_path):
    """If validation.yaml has no foreign_keys section, the function
    must return an empty dict (no entities processed), not raise."""
    core_dir = tmp_path / "core"
    core_dir.mkdir()

    config_path = tmp_path / "validation.yaml"
    config_path.write_text(yaml.dump({"schemas": {}}))

    report = run_relationship_validation(core_dir, config_path)

    assert report == {}
