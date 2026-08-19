"""
Builds fact_job_postings and bridge_job_skill by resolving Silver rows
against the dimension tables built in dim_builders.py, then MERGEs into
the fact table (upsert on source_job_id — a posting re-appearing in a
later Silver run updates rather than duplicates).
"""

from __future__ import annotations

from datetime import date

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from transformation.skill_taxonomy import extract_skills
from utils.logger import get_logger

logger = get_logger(__name__)


def build_fact_and_bridge(
    spark: SparkSession,
    silver_df: DataFrame,
    dim_company_path: str,
    dim_location_path: str,
    dim_date_path: str,
    dim_skill_path: str,
) -> tuple[DataFrame, DataFrame]:
    dim_company = spark.read.format("delta").load(dim_company_path).filter(F.col("is_current"))
    dim_location = spark.read.format("delta").load(dim_location_path)
    dim_date = spark.read.format("delta").load(dim_date_path)
    dim_skill = spark.read.format("delta").load(dim_skill_path)

    # Built here rather than at module scope: F.udf() needs a live
    # SparkSession, so a module-level UDF makes importing this module
    # impossible before the session exists (and breaks test collection).
    extract_skills_udf = F.udf(extract_skills, "array<string>")

    with_skills = silver_df.withColumn("skills", extract_skills_udf(F.col("description")))

    resolved = (
        with_skills
        .join(dim_company, with_skills["company"] == dim_company["company_natural_key"], "left")
        .join(dim_location, with_skills["canonical_country"] == dim_location["canonical_country"], "left")
        .join(
            dim_date,
            F.to_date(with_skills["posted_date"]) == dim_date["full_date"],
            "left",
        )
    )

    fact_df = resolved.select(
        F.col("source_job_id"),
        F.col("source").alias("source_name"),
        dim_company["company_key"],
        dim_location["location_key"],
        dim_date["date_key"],
        F.col("final_salary_min").alias("salary_min"),
        F.col("final_salary_max").alias("salary_max"),
        F.col("final_currency").alias("currency"),
        F.col("remote").alias("is_remote"),
        F.size(F.col("skills")).alias("skill_count"),
        F.current_timestamp().alias("loaded_at"),
        F.col("skills"),   # kept temporarily to build the bridge table below, dropped before fact write
    )

    bridge_df = (
        fact_df.select("source_job_id", F.explode("skills").alias("skill_name"))
        .join(dim_skill, "skill_name")
        .select("source_job_id", "skill_key")
    )

    return fact_df.drop("skills"), bridge_df


def merge_fact_table(spark: SparkSession, fact_df: DataFrame, table_path: str) -> None:
    """Upsert on source_job_id — re-ingesting a posting updates its fact row instead of duplicating it."""
    if not DeltaTable.isDeltaTable(spark, table_path):
        fact_df.write.format("delta").partitionBy("date_key").save(table_path)
        logger.info("Initialized fact_job_postings with %d rows", fact_df.count())
        return

    target = DeltaTable.forPath(spark, table_path)
    (
        target.alias("target")
        .merge(fact_df.alias("source"), "target.source_job_id = source.source_job_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info("Merged %d rows into fact_job_postings", fact_df.count())


def optimize_fact_table(spark: SparkSession, table_path: str) -> None:
    """
    Periodic maintenance — not run every single pipeline execution (too
    expensive), scheduled weekly in Chapter 6's orchestration instead.
    Shown here for completeness of the Delta Lake operations this chapter
    is teaching.
    """
    spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY (company_key, location_key)")
    logger.info("Ran OPTIMIZE + ZORDER on fact_job_postings")


def vacuum_fact_table(spark: SparkSession, table_path: str, retention_hours: int = 168) -> None:
    """Default retention_hours=168 (7 days) — never lowered without a specific, deliberate reason (see Theory 1.5)."""
    spark.sql(f"VACUUM delta.`{table_path}` RETAIN {retention_hours} HOURS")
    logger.info("Ran VACUUM on fact_job_postings (retention=%dh)", retention_hours)