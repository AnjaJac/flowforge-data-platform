"""
Shared pytest fixtures for the FlowForge test suite (Card 7.1).
"""

import polars as pl
import pytest


@pytest.fixture
def sample_customers_df() -> pl.DataFrame:
    return pl.DataFrame({
        "customer_id": ["c1", "c2", "c3"],
        "customer_unique_id": ["u1", "u2", "u3"],
        "customer_zip_code_prefix": [1001, 1002, 1003],
        "customer_city": ["sao paulo", "rio de janeiro", "salvador"],
        "customer_state": ["sp", "rj", "ba"],
    })


@pytest.fixture
def sample_orders_df() -> pl.DataFrame:
    return pl.DataFrame({
        "order_id": ["o1", "o2", "o3"],
        "customer_id": ["c1", "c2", "c3"],
        "order_status": ["delivered", "delivered", "shipped"],
        "order_purchase_timestamp": [
            "2024-01-15 10:00:00",
            "2024-02-20 11:00:00",
            "2024-03-05 09:30:00",
        ],
    })


@pytest.fixture
def sample_payments_df() -> pl.DataFrame:
    return pl.DataFrame({
        "order_id": ["o1", "o2", "o3"],
        "payment_sequential": [1, 1, 1],
        "payment_type": ["credit_card", "boleto", "credit_card"],
        "payment_installments": [1, 1, 3],
        "payment_value": [100.0, 50.0, 200.0],
    })


@pytest.fixture
def core_dir(tmp_path):
    d = tmp_path / "core"
    d.mkdir()
    return d


@pytest.fixture
def staging_dir(tmp_path):
    d = tmp_path / "staging"
    d.mkdir()
    return d


@pytest.fixture
def sample_order_items_df() -> pl.DataFrame:
    return pl.DataFrame({
        "order_id": ["o1", "o1", "o2", "o3"],
        "product_id": ["p1", "p1", "p2", "p1"],
        "seller_id": ["s1", "s1", "s2", "s1"],
        "price": [50.0, 50.0, 80.0, 30.0],
        "freight_value": [5.0, 5.0, 8.0, 3.0],
    })


@pytest.fixture
def sample_reviews_df() -> pl.DataFrame:
    return pl.DataFrame({
        "order_id": ["o1", "o2"],
        "review_score": [5, 3],
    })
