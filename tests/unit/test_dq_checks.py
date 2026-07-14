
from pyspark.sql import Row

from transformation.dq_checks import apply_dq_rules


def test_row_with_empty_title_is_quarantined(spark):
    df = spark.createDataFrame([
        Row(title="", company="Acme", source_job_id="1",
            final_salary_min=None, final_salary_max=None, description="A" * 30),
        Row(title="Data Engineer", company="Acme", source_job_id="2",
            final_salary_min=None, final_salary_max=None, description="A" * 30),
    ])

    clean_df, quarantined_df = apply_dq_rules(df)

    assert clean_df.count() == 1
    assert quarantined_df.count() == 1
    assert quarantined_df.collect()[0]["title"] == ""


def test_inverted_salary_range_is_quarantined(spark):
    df = spark.createDataFrame([
        Row(title="Data Engineer", company="Acme", source_job_id="1",
            final_salary_min=100000.0, final_salary_max=50000.0, description="A" * 30),
    ])

    clean_df, quarantined_df = apply_dq_rules(df)

    assert clean_df.count() == 0
    assert quarantined_df.count() == 1


def test_missing_company_is_warning_not_quarantine(spark):
    df = spark.createDataFrame([
        Row(title="Data Engineer", company=None, source_job_id="1",
            final_salary_min=None, final_salary_max=None, description="A" * 30),
    ])

    clean_df, quarantined_df = apply_dq_rules(df)

    assert clean_df.count() == 1
    assert quarantined_df.count() == 0
    assert "company_present" in clean_df.collect()[0]["dq_warnings"]
