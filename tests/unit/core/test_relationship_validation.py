"""
Unit tests for src/core/relationship_validation.py (Card 5.2).

Real Core data has zero orphaned rows on any configured FK
relationship (verified directly against the real pipeline output),
so these synthetic tests are the ONLY place the actual filtering,
per-relationship reporting, and whole-row-drop-on-any-failure logic
is ever genuinely exercised.
"""

import polars as pl

from src.core.relationship_validation import (
    check_foreign_key,
    validate_entity_relationships,
)


def test_check_foreign_key_identifies_valid_and_orphaned_rows():
    child_df = pl.DataFrame({"order_id": ["o1", "o2", "o3"]})
    parent_df = pl.DataFrame({"order_id": ["o1", "o3"]})

    valid_mask, missing_values = check_foreign_key(
        child_df, "order_id", parent_df, "order_id"
    )

    assert valid_mask.to_list() == [True, False, True]
    assert missing_values == ["o2"]


def test_check_foreign_key_no_orphans_returns_empty_missing_list():
    child_df = pl.DataFrame({"order_id": ["o1", "o2"]})
    parent_df = pl.DataFrame({"order_id": ["o1", "o2", "o3"]})

    valid_mask, missing_values = check_foreign_key(
        child_df, "order_id", parent_df, "order_id"
    )

    assert valid_mask.to_list() == [True, True]
    assert missing_values == []


def test_validate_entity_relationships_drops_orphaned_rows():
    entity_df = pl.DataFrame({
        "order_id": ["o1", "o2", "o3"],
        "customer_id": ["c1", "c2", "c1"],
    })
    customers_df = pl.DataFrame({"customer_id": ["c1"]})

    fk_rules = [
        {"column": "customer_id", "references_entity": "customers", "references_column": "customer_id"},
    ]
    parent_dataframes = {"customers": customers_df}

    cleaned_df, report = validate_entity_relationships(
        entity_df, "orders", fk_rules, parent_dataframes
    )

    assert cleaned_df.height == 2
    assert cleaned_df["order_id"].to_list() == ["o1", "o3"]
    assert report["customer_id"]["removed_count"] == 1
    assert report["customer_id"]["missing_values"] == ["c2"]


def test_validate_entity_relationships_drops_row_failing_any_single_rule():
    """The key design decision: order_items has 3 FK rules. A row
    failing even ONE of them must be dropped entirely, even if the
    other 2 relationships are perfectly valid for that row."""
    entity_df = pl.DataFrame({
        "order_id": ["o1", "o2"],
        "product_id": ["p1", "p_does_not_exist"],
        "seller_id": ["s1", "s1"],
    })
    orders_df = pl.DataFrame({"order_id": ["o1", "o2"]})
    products_df = pl.DataFrame({"product_id": ["p1"]})
    sellers_df = pl.DataFrame({"seller_id": ["s1"]})

    fk_rules = [
        {"column": "order_id", "references_entity": "orders", "references_column": "order_id"},
        {"column": "product_id", "references_entity": "products", "references_column": "product_id"},
        {"column": "seller_id", "references_entity": "sellers", "references_column": "seller_id"},
    ]
    parent_dataframes = {"orders": orders_df, "products": products_df, "sellers": sellers_df}

    cleaned_df, report = validate_entity_relationships(
        entity_df, "order_items", fk_rules, parent_dataframes
    )

    assert cleaned_df.height == 1
    assert cleaned_df["order_id"].to_list() == ["o1"]
    # order_id and seller_id rules both passed for all rows
    assert report["order_id"]["removed_count"] == 0
    assert report["seller_id"]["removed_count"] == 0
    # product_id rule correctly identifies the one bad row
    assert report["product_id"]["removed_count"] == 1
    assert report["product_id"]["missing_values"] == ["p_does_not_exist"]


def test_validate_entity_relationships_reports_per_relationship_not_combined():
    """Confirms reports are kept separate per FK rule, not merged
    into one entity-wide total - this is what lets an engineer see
    that orphans are concentrated on one specific relationship."""
    entity_df = pl.DataFrame({
        "order_id": ["o1"],
        "product_id": ["p_missing"],
    })
    orders_df = pl.DataFrame({"order_id": ["o1"]})
    products_df = pl.DataFrame({"product_id": []}, schema={"product_id": pl.String})

    fk_rules = [
        {"column": "order_id", "references_entity": "orders", "references_column": "order_id"},
        {"column": "product_id", "references_entity": "products", "references_column": "product_id"},
    ]
    parent_dataframes = {"orders": orders_df, "products": products_df}

    _, report = validate_entity_relationships(
        entity_df, "order_items", fk_rules, parent_dataframes
    )

    assert set(report.keys()) == {"order_id", "product_id"}
    assert report["order_id"]["removed_count"] == 0
    assert report["product_id"]["removed_count"] == 1