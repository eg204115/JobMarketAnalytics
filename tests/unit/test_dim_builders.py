"""
Unit tests for the Gold dimension builders.

Only generate_dim_date is covered here. The upsert_* builders drive Delta
MERGE against a real table path, which needs a Delta-configured session and
on-disk state — that belongs in an integration test, not a unit test.
"""

from datetime import date

from transformation.dim_builders import generate_dim_date


def test_generate_dim_date_row_count_matches_range(spark):
    df = generate_dim_date(spark, date(2026, 1, 1), date(2026, 1, 31))
    assert df.count() == 31


def test_generate_dim_date_range_is_inclusive_of_both_ends(spark):
    df = generate_dim_date(spark, date(2026, 7, 15), date(2026, 7, 15))
    assert df.count() == 1


def test_generate_dim_date_key_format(spark):
    df = generate_dim_date(spark, date(2026, 7, 15), date(2026, 7, 15))
    row = df.collect()[0]
    assert row["date_key"] == 20260715
    assert row["quarter"] == 3


def test_generate_dim_date_has_expected_columns(spark):
    df = generate_dim_date(spark, date(2026, 1, 1), date(2026, 1, 2))
    assert df.columns == [
        "date_key",
        "full_date",
        "year",
        "month",
        "quarter",
        "day_name",
    ]


def test_generate_dim_date_key_is_unique_across_a_full_year(spark):
    df = generate_dim_date(spark, date(2026, 1, 1), date(2026, 12, 31))
    # date_key is the surrogate key the fact table joins on — a collision
    # would silently fan out every posting into duplicate rows.
    assert df.count() == 365
    assert df.select("date_key").distinct().count() == 365


def test_generate_dim_date_maps_every_month_to_the_right_quarter(spark):
    df = generate_dim_date(spark, date(2026, 1, 1), date(2026, 12, 31))
    mapping = {
        row["month"]: row["quarter"]
        for row in df.select("month", "quarter").distinct().collect()
    }
    assert mapping == {
        1: 1, 2: 1, 3: 1,
        4: 2, 5: 2, 6: 2,
        7: 3, 8: 3, 9: 3,
        10: 4, 11: 4, 12: 4,
    }


def test_generate_dim_date_day_name_is_correct(spark):
    df = generate_dim_date(spark, date(2026, 7, 15), date(2026, 7, 15))
    assert df.collect()[0]["day_name"] == "Wednesday"
