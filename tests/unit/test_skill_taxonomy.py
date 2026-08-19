"""
Unit tests for keyword-based skill extraction. Deliberately Spark-free —
extract_skills was kept as a pure function precisely so the matching logic
can be tested in milliseconds without a JVM.
"""

from transformation.skill_taxonomy import SKILL_TAXONOMY, extract_skills


def test_extracts_multiple_skills_from_description():
    text = (
        "We need someone strong in Python, SQL, and PySpark for our "
        "Azure Data Factory pipelines."
    )
    # "spark" and "azure" match too: the taxonomy does plain substring
    # matching, and "pyspark" contains "spark" while "azure data factory"
    # contains "azure". Asserted explicitly so the behaviour is documented
    # rather than discovered later as a surprise in the Gold skill counts.
    assert set(extract_skills(text)) == {
        "python",
        "sql",
        "pyspark",
        "spark",
        "azure data factory",
        "azure",
    }


def test_returns_empty_list_for_no_matches():
    assert extract_skills("Looking for a great communicator and team player.") == []


def test_returns_empty_list_for_none():
    assert extract_skills(None) == []


def test_returns_empty_list_for_empty_string():
    assert extract_skills("") == []


def test_case_insensitive_matching():
    assert "python" in extract_skills("Experience with PYTHON required")


def test_result_is_sorted_and_deduplicated():
    assert extract_skills("SQL, sql, and more SQL. Also Python.") == ["python", "sql"]


def test_multi_word_skills_are_matched_whole():
    assert "power bi" in extract_skills("Dashboards built in Power BI")


def test_every_extracted_skill_is_a_taxonomy_key():
    text = "Python, Kafka, Docker, Snowflake and Power BI"
    for skill in extract_skills(text):
        assert skill in SKILL_TAXONOMY
