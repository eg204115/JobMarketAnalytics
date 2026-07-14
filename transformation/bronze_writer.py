"""
Reads normalized JobPosting JSON files (produced by ingestion/run_ingestion.py)
and writes them to the Bronze Delta table, partitioned by ingestion_date.

This module is imported by notebooks/01_bronze_ingestion.ipynb — the notebook
itself stays thin (a few cells calling these functions), so this logic is
unit-testable without a notebook runtime.
"""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from transformation.schemas import JOB_POSTING_SCHEMA
from utils.logger import get_logger

logger = get_logger(__name__)

BRONZE_TABLE_NAME = "bronze_job_postings"


def load_raw_json_as_dataframe(spark: SparkSession, json_dir: str) -> DataFrame:
    """
    Reads all connector-output JSON files from a directory into one DataFrame,
    flattening raw_payload into a JSON string column rather than letting Spark
    infer its (source-specific, inconsistent) nested structure.
    """
    files = list(Path(json_dir).glob("*.json"))
    if not files:
        logger.warning("No JSON files found in %s — returning empty DataFrame.", json_dir)
        return spark.createDataFrame([], schema=JOB_POSTING_SCHEMA)

    # Each connector output file is a JSON *array* of records (see run_ingestion.py),
    # which Spark's multiline JSON reader handles directly.
    records = []
    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            file_records = json.load(f)
            for r in file_records:
                # raw_payload gets serialized to a string here, in Python,
                # rather than relying on Spark to infer a consistent nested
                # schema across two structurally different source APIs.
                r["raw_payload"] = json.dumps(r.get("raw_payload", {}))
            records.extend(file_records)

    logger.info("Loaded %d raw records from %d files in %s", len(records), len(files), json_dir)

    # Build DataFrame from the explicit schema + raw_payload as a plain string column.
    from pyspark.sql.types import StringType, StructField

    schema_with_payload = JOB_POSTING_SCHEMA.add(
        StructField("raw_payload", StringType(), nullable=True)
    )
    return spark.createDataFrame(records, schema=schema_with_payload)


def add_partition_column(df: DataFrame) -> DataFrame:
    """
    Derives ingestion_date (a plain date, for partitioning) from the
    finer-grained ingestion_timestamp string written by the connectors.
    """
    return df.withColumn(
        "ingestion_date",
        F.to_date(F.col("ingestion_timestamp")),
    )


def write_bronze_delta(
    df: DataFrame,
    table_path: str,
    partition_col: str = "ingestion_date",
) -> None:
    """
    Writes to the Bronze Delta table, overwriting only the partitions present
    in `df` (via replaceWhere-equivalent partition overwrite mode). This is
    what makes re-running the same day's ingestion idempotent: it replaces
    that day's data rather than appending duplicates, while leaving every
    other day's partition untouched.
    """
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")   # only touches partitions present in df
        .option("mergeSchema", "true")                  # allows additive schema evolution
        .partitionBy(partition_col)
        .save(table_path)
    )

    row_count = df.count()
    partitions = df.select(partition_col).distinct().collect()
    logger.info(
        "Wrote %d rows to Bronze Delta table at %s across partitions: %s",
        row_count, table_path, [r[partition_col] for r in partitions],
    )


def run_bronze_ingestion(
    spark: SparkSession,
    json_dir: str,
    table_path: str,
) -> DataFrame:
    """Orchestrates the full Bronze write: read -> partition -> write. Returns the written DataFrame for notebook display/inspection."""
    df = load_raw_json_as_dataframe(spark, json_dir)
    if df.rdd.isEmpty():
        logger.warning("No records to write — skipping Bronze write.")
        return df

    df = add_partition_column(df)
    write_bronze_delta(df, table_path)
    return df