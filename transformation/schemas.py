"""
Central registry of explicit PySpark schemas. Every read operation in this
project uses one of these — never spark.read with inferSchema=True.

Keeping schemas here (not inline in notebooks) means Silver/Gold code can
import and reference the same StructType, so "what does a JobPosting record
look like" has exactly one source of truth in the codebase.
"""

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    DoubleType,
    BooleanType,
)

# Mirrors ingestion/base_connector.py's JobPosting dataclass field-for-field.
# Nullable=True on almost everything because source APIs are inconsistent —
# we enforce "this field exists" here, and enforce "this field is populated"
# as a data quality RULE in Chapter 4, not as a schema-level NOT NULL.
# Rationale: a schema-level failure aborts the whole read; a DQ rule flags
# and quarantines just the bad row, which is what we want for messy web APIs.
JOB_POSTING_SCHEMA = StructType([
    StructField("source", StringType(), nullable=False),
    StructField("source_job_id", StringType(), nullable=False),
    StructField("title", StringType(), nullable=True),
    StructField("company", StringType(), nullable=True),
    StructField("location_raw", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("description", StringType(), nullable=True),
    StructField("salary_min", DoubleType(), nullable=True),
    StructField("salary_max", DoubleType(), nullable=True),
    StructField("currency", StringType(), nullable=True),
    StructField("remote", BooleanType(), nullable=True),
    StructField("posted_date", StringType(), nullable=True),
    StructField("url", StringType(), nullable=True),
    StructField("ingestion_timestamp", StringType(), nullable=True),
    StructField("run_id", StringType(), nullable=True),
    # raw_payload is intentionally excluded from the Bronze *table* schema —
    # storing arbitrary nested per-source JSON in a shared table causes
    # schema conflicts across sources. It's preserved separately; see 4.3.
])