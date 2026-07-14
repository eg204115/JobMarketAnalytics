from pyspark.sql import Row

from transformation.bronze_to_silver import deduplicate_postings


def test_deduplication_prefers_adzuna_over_jooble(spark):
    df = spark.createDataFrame([
        Row(title="Data Engineer", company="Acme", canonical_country="Sri Lanka",
            source="jooble", posted_date="2026-07-10"),
        Row(title="Data Engineer", company="Acme", canonical_country="Sri Lanka",
            source="adzuna", posted_date="2026-07-09"),
    ])

    result = deduplicate_postings(df)

    assert result.count() == 1
    assert result.collect()[0]["source"] == "adzuna"


def test_deduplication_keeps_distinct_jobs_separate(spark):
    df = spark.createDataFrame([
        Row(title="Data Engineer", company="Acme", canonical_country="Sri Lanka",
            source="adzuna", posted_date="2026-07-10"),
        Row(title="Data Analyst", company="Acme", canonical_country="Sri Lanka",
            source="adzuna", posted_date="2026-07-10"),
    ])

    result = deduplicate_postings(df)

    assert result.count() == 2