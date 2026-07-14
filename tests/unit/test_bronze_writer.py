"""
Unit tests for bronze_writer.py using a local SparkSession (no Delta write —
tested against the DataFrame transformations only, which don't require a
real Delta/Fabric environment to verify).
"""

import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from transformation.bronze_writer import add_partition_column, load_raw_json_as_dataframe


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("test_bronze_writer")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def sample_json_dir(tmp_path: Path) -> Path:
    records = [
        {
            "source": "adzuna",
            "source_job_id": "123",
            "title": "Data Engineer",
            "company": "Acme Corp",
            "location_raw": "Colombo",
            "country": "lk",
            "description": "Build pipelines",
            "salary_min": 50000.0,
            "salary_max": 70000.0,
            "currency": "USD",
            "remote": True,
            "posted_date": "2026-07-10",
            "url": "https://example.com/job/123",
            "ingestion_timestamp": "2026-07-12T08:00:00+00:00",
            "run_id": "abc123",
            "raw_payload": {"id": 123, "extra_field": "adzuna-specific"},
        }
    ]
    file_path = tmp_path / "adzuna_lk_abc123.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(records, f)
    return tmp_path


def test_load_raw_json_serializes_raw_payload_to_string(spark, sample_json_dir):
    df = load_raw_json_as_dataframe(spark, str(sample_json_dir))
    row = df.collect()[0]

    assert isinstance(row["raw_payload"], str)
    assert json.loads(row["raw_payload"])["extra_field"] == "adzuna-specific"


def test_load_raw_json_empty_dir_returns_empty_df(spark, tmp_path):
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()

    df = load_raw_json_as_dataframe(spark, str(empty_dir))

    assert df.count() == 0


def test_add_partition_column_derives_date_from_timestamp(spark, sample_json_dir):
    df = load_raw_json_as_dataframe(spark, str(sample_json_dir))
    df = add_partition_column(df)
    row = df.collect()[0]

    assert str(row["ingestion_date"]) == "2026-07-12"