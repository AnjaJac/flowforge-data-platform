"""
Unit tests for src/staging/reviews.py (Card 4.2).

Reviews needs date parsing on 2 columns, and - per this card's
deliberate decision - must leave the comment fields' nulls
untouched, since a missing review comment is a meaningful real
state, not missing data needing a placeholder.
"""

import polars as pl

from src.staging.reviews import clean_reviews


def test_clean_reviews_parses_date_columns():
    df = pl.DataFrame({
        "review_id": ["r1"],
        "order_id": ["o1"],
        "review_score": [5],
        "review_comment_title": ["Great"],
        "review_comment_message": ["Loved it"],
        "review_creation_date": ["2024-01-15 00:00:00"],
        "review_answer_timestamp": ["2024-01-16 10:30:00"],
    })
    result = clean_reviews(df)
    assert result["review_creation_date"].dtype == pl.Datetime
    assert result["review_answer_timestamp"].dtype == pl.Datetime


def test_clean_reviews_leaves_null_comments_untouched():
    """Deliberate decision: no placeholder is applied to missing
    review comments. A null here means the reviewer wrote nothing,
    which is a real, meaningful state - not equivalent to products'
    missing category, which IS filled with 'unknown'."""
    df = pl.DataFrame({
        "review_id": ["r1"],
        "order_id": ["o1"],
        "review_score": [3],
        "review_comment_title": [None],
        "review_comment_message": [None],
        "review_creation_date": ["2024-01-15 00:00:00"],
        "review_answer_timestamp": ["2024-01-16 10:30:00"],
    })
    result = clean_reviews(df)
    assert result["review_comment_title"].to_list() == [None]
    assert result["review_comment_message"].to_list() == [None]


def test_clean_reviews_preserves_row_count():
    df = pl.DataFrame({
        "review_id": ["r1", "r2"],
        "order_id": ["o1", "o2"],
        "review_score": [5, 3],
        "review_comment_title": ["Great", None],
        "review_comment_message": ["Loved it", None],
        "review_creation_date": ["2024-01-15 00:00:00", "2024-02-01 00:00:00"],
        "review_answer_timestamp": ["2024-01-16 10:30:00", "2024-02-02 11:00:00"],
    })
    result = clean_reviews(df)
    assert result.height == df.height