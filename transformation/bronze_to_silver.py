"""
Main Bronze -> Silver transformation: standardize location (broadcast join),
parse salary (native cast for Adzuna, UDF for Jooble), deduplicate across
sources (window function), then run the DQ engine and write both outputs.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from transformation.dq_checks import apply_dq_rules
from transformation.salary_parser import parse_salary_text_udf
from utils.logger import get_logger

logger = get_logger(__name__)

# Business rule: when the same job appears in both sources, which do we trust more?
# Adzuna provides structured salary data directly from employers/aggregators with
# fewer parsing errors than Jooble's free-text salary, so it wins ties.
SOURCE_PRIORITY = {"adzuna": 2, "jooble": 1}


def standardize_location(df: DataFrame, spark: SparkSession, reference_path: str) -> DataFrame:
    """
    Broadcast join against the small country reference table. F.broadcast()
    is explicit here rather than relying on Spark's auto-broadcast threshold,
    because this join happens on every Silver run against a table small
    enough (~250 rows) to always want broadcast behavior, regardless of
    cluster config changes over time.
    """
    reference_df = (
        spark.read.option("header", True).csv(reference_path)
    )

    df_lower_location = df.withColumn("location_lower", F.lower(F.trim(F.col("location_raw"))))

    joined = df_lower_location.join(
        F.broadcast(reference_df),
        df_lower_location["location_lower"].contains(reference_df["raw_location_contains"]),
        how="left",
    )

    return joined.select(
        df["*"],
        F.coalesce(joined["canonical_country"], df["country"]).alias("canonical_country"),
        joined["region"],
    )


def parse_salaries(df: DataFrame) -> DataFrame:
    """
    Adzuna already has numeric salary_min/salary_max — no parsing needed,
    just casting/coalescing. Jooble's salary lives as free text (if present
    at all) inside the raw description, requiring the Pandas UDF.
    """
    return (
        df.withColumn(
            "adzuna_salary_min", F.col("salary_min").cast("double")
        )
        .withColumn("adzuna_salary_max", F.col("salary_max").cast("double"))
        .withColumn(
            "jooble_parsed",
            F.when(
                F.col("source") == "jooble",
                parse_salary_text_udf(F.col("description")),
            ),
        )
        .withColumn(
            "final_salary_min",
            F.coalesce(F.col("adzuna_salary_min"), F.col("jooble_parsed.parsed_salary_min")),
        )
        .withColumn(
            "final_salary_max",
            F.coalesce(F.col("adzuna_salary_max"), F.col("jooble_parsed.parsed_salary_max")),
        )
        .withColumn(
            "final_currency",
            F.coalesce(F.col("currency"), F.col("jooble_parsed.parsed_currency")),
        )
        .drop("jooble_parsed", "adzuna_salary_min", "adzuna_salary_max")
    )


def deduplicate_postings(df: DataFrame) -> DataFrame:
    """
    Builds a natural key from normalized title+company+location (lowercased,
    trimmed, whitespace-collapsed — native string functions, not a UDF, since
    this is fully expressible with built-ins) and keeps exactly one row per
    key using a window-function rank, preferring higher source priority and
    more recent posted_date.
    """
    with_key = (
        df.withColumn(
            "natural_key",
            F.concat_ws(
                "|",
                F.lower(F.trim(F.regexp_replace(F.col("title"), r"\s+", " "))),
                F.lower(F.trim(F.coalesce(F.col("company"), F.lit("")))),
                F.lower(F.trim(F.coalesce(F.col("canonical_country"), F.lit("")))),
            ),
        )
        .withColumn(
            "source_priority",
            F.when(F.col("source") == "adzuna", SOURCE_PRIORITY["adzuna"])
             .when(F.col("source") == "jooble", SOURCE_PRIORITY["jooble"])
             .otherwise(0),
        )
    )

    dedup_window = Window.partitionBy("natural_key").orderBy(
        F.desc("source_priority"), F.desc("posted_date")
    )

    ranked = with_key.withColumn("_dedup_rank", F.row_number().over(dedup_window))
    deduped = ranked.filter(F.col("_dedup_rank") == 1).drop("_dedup_rank", "source_priority")

    dropped_count = with_key.count() - deduped.count()
    logger.info("Deduplication: removed %d duplicate postings across sources", dropped_count)

    return deduped


def run_silver_transformation(
    spark: SparkSession,
    bronze_table_path: str,
    reference_path: str,
    silver_table_path: str,
    quarantine_table_path: str,
    ingestion_date: str,
) -> None:
    bronze_df = (
        spark.read.format("delta").load(bronze_table_path)
        .filter(F.col("ingestion_date") == ingestion_date)
    )

    if bronze_df.rdd.isEmpty():
        logger.warning("No Bronze rows found for ingestion_date=%s — skipping Silver run.", ingestion_date)
        return

    standardized = standardize_location(bronze_df, spark, reference_path)
    salaried = parse_salaries(standardized)
    deduped = deduplicate_postings(salaried)

    # Cache here: DQ engine + both write paths below all act on this same
    # DataFrame — without caching, each action would recompute the full
    # broadcast-join + UDF + window-function chain from scratch.
    deduped.cache()

    try:
        clean_df, quarantined_df = apply_dq_rules(deduped)

        (
            clean_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(silver_table_path)
        )
        logger.info("Wrote %d clean rows to Silver: %s", clean_df.count(), silver_table_path)

        if not quarantined_df.rdd.isEmpty():
            (
                quarantined_df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(quarantine_table_path)
            )
            logger.warning(
                "Wrote %d quarantined rows to: %s", quarantined_df.count(), quarantine_table_path
            )
    finally:
        deduped.unpersist()