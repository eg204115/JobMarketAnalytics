"""
Builds/upserts each Gold dimension table. dim_company uses true SCD Type 2
via Delta's MERGE; dim_location and dim_skill use simple Type-1 upserts;
dim_date is pre-generated once (dates don't need incremental logic).
"""

from __future__ import annotations

from datetime import date, timedelta

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from transformation.skill_taxonomy import SKILL_TAXONOMY, extract_skills
from utils.logger import get_logger

logger = get_logger(__name__)


def generate_dim_date(spark: SparkSession, start: date, end: date) -> DataFrame:
    """
    dim_date is generated once for a wide date range (e.g., 5 years) rather
    than incrementally — date dimensions are static and cheap to fully
    regenerate, so there's no upsert complexity needed here at all.
    """
    days = (end - start).days + 1
    dates = [start + timedelta(days=i) for i in range(days)]
    rows = [
        (
            int(d.strftime("%Y%m%d")),   # date_key: YYYYMMDD integer, standard convention
            d,
            d.year,
            d.month,
            (d.month - 1) // 3 + 1,
            d.strftime("%A"),
        )
        for d in dates
    ]
    return spark.createDataFrame(
        rows, schema=["date_key", "full_date", "year", "month", "quarter", "day_name"]
    )


def upsert_dim_location(spark: SparkSession, silver_df: DataFrame, table_path: str) -> None:
    """Type-1 upsert: new (country, region) pairs are inserted; existing ones are left as-is."""
    new_locations = (
        silver_df.select(
            F.col("canonical_country"), F.col("region")
        )
        .distinct()
        .filter(F.col("canonical_country").isNotNull())
    )

    if not DeltaTable.isDeltaTable(spark, table_path):
        (
            new_locations
            .withColumn("location_key", F.monotonically_increasing_id())
            .write.format("delta").save(table_path)
        )
        logger.info("Initialized dim_location with %d rows", new_locations.count())
        return

    dim_table = DeltaTable.forPath(spark, table_path)
    (
        dim_table.alias("target")
        .merge(
            new_locations.alias("source"),
            "target.canonical_country = source.canonical_country",
        )
        .whenNotMatchedInsert(values={
            "canonical_country": "source.canonical_country",
            "region": "source.region",
            "location_key": F.expr("abs(hash(source.canonical_country))"),  # deterministic surrogate key
        })
        .execute()
    )
    logger.info("Upserted dim_location (Type 1)")


def upsert_dim_skill(spark: SparkSession, table_path: str) -> None:
    """
    dim_skill is built directly from the static taxonomy (not derived from
    Silver data) since the taxonomy IS the dimension's source of truth —
    a skill exists in the dimension whether or not it currently appears
    in any posting, which matters for building a complete Power BI slicer.
    """
    rows = [
        (F.abs(F.hash(F.lit(name))), name, category)
        for name, category in SKILL_TAXONOMY.items()
    ]
    # Build via a plain DataFrame rather than the F.hash-in-a-tuple trick above
    # (kept simple/explicit rather than clever):
    df = spark.createDataFrame(
        [(name, category) for name, category in SKILL_TAXONOMY.items()],
        schema=["skill_name", "skill_category"],
    ).withColumn("skill_key", F.abs(F.hash(F.col("skill_name"))))

    df.write.format("delta").mode("overwrite").save(table_path)
    logger.info("Rebuilt dim_skill with %d rows", df.count())


def upsert_dim_company_scd2(
    spark: SparkSession,
    silver_df: DataFrame,
    table_path: str,
    run_date: date,
) -> None:
    """
    True SCD Type 2 upsert for dim_company. For each company_natural_key:
    - If it's new, insert a fresh current row.
    - If size_bucket has CHANGED since the current row, close out the old
      row (is_current=false, effective_end_date=run_date) and insert a new
      current row with a new surrogate key.
    - If nothing changed, leave the existing current row untouched.
    """
    postings_per_company = silver_df.groupBy("company").count()
    incoming = postings_per_company.withColumn(
        "size_bucket",
        F.when(F.col("count") >= 20, "Large")
         .when(F.col("count") >= 5, "Medium")
         .otherwise("Small"),
    ).select(
        F.col("company").alias("company_natural_key"),
        F.col("company").alias("company_name"),
        "size_bucket",
    ).filter(F.col("company_natural_key").isNotNull())

    if not DeltaTable.isDeltaTable(spark, table_path):
        initial = (
            incoming
            .withColumn("company_key", F.monotonically_increasing_id())
            .withColumn("effective_start_date", F.lit(run_date))
            .withColumn("effective_end_date", F.lit(None).cast("date"))
            .withColumn("is_current", F.lit(True))
        )
        initial.write.format("delta").save(table_path)
        logger.info("Initialized dim_company (SCD2) with %d rows", initial.count())
        return

    dim_table = DeltaTable.forPath(spark, table_path)
    current_rows = dim_table.toDF().filter(F.col("is_current"))

    # Detect changes: join incoming vs. current rows on natural key, flag
    # where size_bucket differs — this is the actual "slowly changing" check.
    changes = incoming.alias("src").join(
        current_rows.alias("cur"),
        "company_natural_key",
        "left",
    ).select(
        "src.*",
        F.col("cur.size_bucket").alias("current_size_bucket"),
        F.col("cur.company_key").alias("current_company_key"),
    )

    changed_or_new = changes.filter(
        F.col("current_size_bucket").isNull()
        | (F.col("current_size_bucket") != F.col("size_bucket"))
    )

    if changed_or_new.rdd.isEmpty():
        logger.info("dim_company: no SCD2 changes detected this run.")
        return

    # Step 1: close out current rows for companies whose size_bucket changed
    # (skip the ones that are brand new — current_company_key is null there).
    keys_to_close = [
        r["current_company_key"]
        for r in changed_or_new.filter(F.col("current_company_key").isNotNull()).collect()
    ]
    if keys_to_close:
        dim_table.update(
            condition=F.col("company_key").isin(keys_to_close) & F.col("is_current"),
            set={
                "is_current": F.lit(False),
                "effective_end_date": F.lit(run_date),
            },
        )

    # Step 2: insert new current rows (both brand-new companies and changed ones).
    new_rows = (
        changed_or_new
        .select("company_natural_key", "company_name", "size_bucket")
        .withColumn("company_key", F.monotonically_increasing_id() + F.lit(int(run_date.strftime("%Y%m%d"))) * 100000)
        .withColumn("effective_start_date", F.lit(run_date))
        .withColumn("effective_end_date", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
    )
    new_rows.write.format("delta").mode("append").save(table_path)

    logger.info(
        "dim_company SCD2: closed %d rows, inserted %d new/changed rows",
        len(keys_to_close), new_rows.count(),
    )

def write_dim_company_current(
    spark: SparkSession,
    dim_company_path: str,
    out_path: str,
) -> DataFrame:
    current = (
        spark.read.format("delta").load(dim_company_path)
        .filter(F.col("is_current"))
        .select("company_key", "company_natural_key", "company_name", "size_bucket")
    )

    current.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(out_path)

    logger.info("Wrote %d current companies to %s", current.count(), out_path)
    return current
