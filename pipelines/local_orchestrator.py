"""
Local/CI equivalent of master_pipeline.json — the same orchestration decisions
(per-source failure isolation, gate Silver/Gold on partial success, alert on
partial or total failure) expressed as plain Python so the pipeline's *logic*
can be unit tested without a live Fabric workspace.

This is NOT a replacement for the Fabric pipeline in production. It is a
development aid and a CI smoke-test target (Chapter 8 runs it against sample
data). Every stage below calls the same functions the Chapter 3-5 notebooks
call — this file sequences work, it does not implement any of it.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from datetime import date

import requests
from delta.tables import DeltaTable

from ingestion.connector_factory import build_enabled_connectors
from ingestion.run_ingestion import write_postings_json
from transformation.bronze_to_silver import run_silver_transformation
from transformation.bronze_writer import run_bronze_ingestion
from transformation.dim_builders import (
    generate_dim_date,
    upsert_dim_company_scd2,
    upsert_dim_location,
    upsert_dim_skill,
)
from transformation.silver_to_gold import (
    build_fact_and_bridge,
    merge_bridge_table,
    merge_fact_table,
)
from utils.config_loader import ConfigError, load_config
from utils.logger import get_logger, get_run_id, setup_logging

logger = get_logger(__name__)

# dim_date is static and generated once over a wide range (Chapter 5, step 1).
DIM_DATE_START = date(2020, 1, 1)
DIM_DATE_END = date(2030, 12, 31)


@dataclass(frozen=True)
class LayerPaths:
    """
    Every Delta path one run touches, derived from a single prefix.

    Same reason the notebooks build their paths from a PREFIX: in Fabric these
    resolve inside the lakehouse ("Tables/"), locally they are directories
    under the repo ("data/delta/"). One prefix parameter beats nine path
    parameters that can drift out of agreement with each other.
    """

    prefix: str = "data/delta/"

    @property
    def bronze(self) -> str:
        return f"{self.prefix}bronze_job_postings"

    @property
    def silver(self) -> str:
        return f"{self.prefix}silver_job_postings"

    @property
    def quarantine(self) -> str:
        return f"{self.prefix}silver_job_postings_quarantine"

    @property
    def dim_date(self) -> str:
        return f"{self.prefix}dim_date"

    @property
    def dim_skill(self) -> str:
        return f"{self.prefix}dim_skill"

    @property
    def dim_location(self) -> str:
        return f"{self.prefix}dim_location"

    @property
    def dim_company(self) -> str:
        return f"{self.prefix}dim_company"

    @property
    def fact(self) -> str:
        return f"{self.prefix}fact_job_postings"

    @property
    def bridge(self) -> str:
        return f"{self.prefix}bridge_job_skill"


@dataclass
class PipelineRunResult:
    """
    What the Fabric pipeline tracks in its Set Variable activities: which
    sources made it, which did not, and how far the shared stages got.
    Returned rather than logged-and-discarded so tests can assert on it and
    __main__ can derive an exit code from it.
    """

    window_date: date
    run_id: str = field(default_factory=get_run_id)
    succeeded_sources: list[str] = field(default_factory=list)
    failed_sources: list[str] = field(default_factory=list)
    total_records: int = 0
    silver_ran: bool = False
    gold_ran: bool = False
    fatal_error: str | None = None

    @property
    def any_source_succeeded(self) -> bool:
        return len(self.succeeded_sources) > 0

    @property
    def is_full_success(self) -> bool:
        return (
            not self.failed_sources
            and self.silver_ran
            and self.gold_ran
            and not self.fatal_error
        )


def send_teams_alert(webhook_url: str | None, message: str) -> None:
    """
    Mirrors the Web Activity alert in master_pipeline.json. webhook_url is None
    by default in local/CI runs so a test run cannot page anyone; pass it
    explicitly (or set TEAMS_WEBHOOK_URL for a __main__ run) to really notify.

    A failed alert is logged, never raised: losing the notification about a
    failure must not become a second, louder failure that masks the first.
    """
    if not webhook_url:
        logger.warning("ALERT (no webhook configured, logging only): %s", message)
        return
    try:
        requests.post(webhook_url, json={"text": message}, timeout=10)
        logger.info("Sent Teams alert: %s", message)
    except requests.exceptions.RequestException as exc:
        logger.error("Failed to send Teams alert: %s", exc)


def _ingest_source(spark, connector, landing_dir: str, paths: LayerPaths) -> int:
    """
    One ForEach iteration: fetch every country configured for this source, land
    the raw JSON, then write Bronze. Returns the record count fetched.

    The Bronze write re-reads the whole landing directory, so running it per
    source repeats work already done for earlier sources in the same run. That
    is deliberate: the dynamic partition overwrite makes the repeat idempotent,
    and keeping the write inside the loop is what stops a source's failure at
    that source instead of aborting the others (matching the ForEach in
    master_pipeline.json). With a handful of sources, the wasted reads cost far
    less than losing per-source isolation.
    """
    records = 0
    for country in connector.source_config.get("countries", ["us"]):
        postings = connector.fetch(country=country)
        write_postings_json(postings, landing_dir, connector.source_name, country)
        records += len(postings)

    run_bronze_ingestion(spark, json_dir=landing_dir, table_path=paths.bronze)
    return records


def _run_gold(spark, paths: LayerPaths, run_date: date) -> None:
    """
    Gold in dependency order. The fact build resolves surrogate keys by joining
    against the dimensions, so every dimension has to be current *first* —
    building the fact against stale dimensions does not fail loudly, it
    silently produces NULL foreign keys that only surface in Power BI.
    """
    if DeltaTable.isDeltaTable(spark, paths.dim_date):
        logger.info("dim_date already exists — skipping generation")
    else:
        dim_date_df = generate_dim_date(spark, DIM_DATE_START, DIM_DATE_END)
        dim_date_df.write.format("delta").mode("overwrite").save(paths.dim_date)
        logger.info("Generated dim_date")

    upsert_dim_skill(spark, paths.dim_skill)

    silver_df = spark.read.format("delta").load(paths.silver)
    upsert_dim_location(spark, silver_df, paths.dim_location)
    upsert_dim_company_scd2(
        spark=spark,
        silver_df=silver_df,
        table_path=paths.dim_company,
        run_date=run_date,
    )

    fact_df, bridge_df = build_fact_and_bridge(
        spark=spark,
        silver_df=silver_df,
        dim_company_path=paths.dim_company,
        dim_location_path=paths.dim_location,
        dim_date_path=paths.dim_date,
        dim_skill_path=paths.dim_skill,
    )
    merge_fact_table(spark, fact_df, paths.fact)
    merge_bridge_table(spark, bridge_df, paths.bridge)


def run_pipeline(
    window_date: date,
    spark,
    teams_webhook_url: str | None = None,
    config_dir: str = "config",
    table_prefix: str = "data/delta/",
    landing_dir: str | None = None,
) -> PipelineRunResult:
    """
    One tumbling window. window_date is the parameter the trigger passes in —
    it drives both the Silver partition filter and the SCD2 effective dates, so
    backfilling a historical window produces the values that window should have
    had rather than today's.

    Paths are parameters with local defaults for the same reason they are in
    run_ingestion.run(): a Fabric run has neither the repo root as its working
    directory nor local relative paths that resolve.
    """
    setup_logging(config_path=f"{config_dir}/logging_config.yaml")
    paths = LayerPaths(prefix=table_prefix)
    result = PipelineRunResult(window_date=window_date)
    logger.info(
        "Starting local orchestration run_id=%s window=%s prefix=%s",
        result.run_id, window_date, table_prefix,
    )

    try:
        app_config = load_config(
            config_path=f"{config_dir}/config.yaml",
            sources_path=f"{config_dir}/sources.yaml",
        )
    except ConfigError as exc:
        # Nothing downstream is worth isolating: without config there are no
        # sources, no credentials and no paths. Fail the whole window.
        logger.critical("Configuration error, aborting run: %s", exc)
        result.fatal_error = str(exc)
        send_teams_alert(teams_webhook_url, f"Pipeline aborted — config error: {exc}")
        return result

    resolved_landing = landing_dir or app_config.get(
        "storage", "bronze_path", default="data/bronze"
    )
    reference_path = f"{config_dir}/reference/country_codes.csv"
    iso_reference_path = f"{config_dir}/reference/country_iso.csv"

    connectors = build_enabled_connectors(app_config)
    if not connectors:
        # Not a failure. sources.yaml is allowed to have everything disabled,
        # and that must not look the same as "every source blew up".
        logger.warning("No enabled sources in sources.yaml — nothing to do.")
        return result

    # --- Ingestion + Bronze, one isolated iteration per source (ForEach) ---
    for connector in connectors:
        try:
            result.total_records += _ingest_source(
                spark, connector, resolved_landing, paths
            )
            result.succeeded_sources.append(connector.source_name)
        except Exception as exc:
            logger.error(
                "[%s] failed during ingestion/bronze: %s",
                connector.source_name, exc, exc_info=True,
            )
            result.failed_sources.append(connector.source_name)

    # --- Gate: partial data beats no data, but zero sources means stop ---
    if not result.any_source_succeeded:
        send_teams_alert(
            teams_webhook_url,
            f"ALL sources failed for window {window_date}. "
            f"Pipeline halted before Silver/Gold.",
        )
        return result

    if result.failed_sources:
        send_teams_alert(
            teams_webhook_url,
            f"Partial failure for window {window_date}. "
            f"Failed: {result.failed_sources}. Silver/Gold will run on the "
            f"remaining data — this window's numbers are incomplete.",
        )

    # --- Silver (shared, not per-source: its failure is the window's) ---
    try:
        run_silver_transformation(
            spark=spark,
            bronze_table_path=paths.bronze,
            reference_path=reference_path,
            silver_table_path=paths.silver,
            quarantine_table_path=paths.quarantine,
            ingestion_date=str(window_date),
            iso_reference_path=iso_reference_path,
        )
        result.silver_ran = True
    except Exception as exc:
        logger.error("Silver transformation failed: %s", exc, exc_info=True)
        result.fatal_error = f"silver: {exc}"
        send_teams_alert(
            teams_webhook_url,
            f"Silver transformation failed for window {window_date}: {exc}",
        )
        return result

    # --- Gold ---
    try:
        _run_gold(spark, paths, run_date=window_date)
        result.gold_ran = True
    except Exception as exc:
        logger.error("Gold transformation failed: %s", exc, exc_info=True)
        result.fatal_error = f"gold: {exc}"
        send_teams_alert(
            teams_webhook_url,
            f"Gold transformation failed for window {window_date}: {exc}",
        )
        return result

    logger.info(
        "Pipeline run complete. window=%s records=%d succeeded=%s failed=%s "
        "silver=%s gold=%s",
        window_date, result.total_records, result.succeeded_sources,
        result.failed_sources, result.silver_ran, result.gold_ran,
    )
    return result


if __name__ == "__main__":
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    # A plain local getOrCreate() has no Delta catalog wired in, so every
    # DeltaTable call downstream fails with "Delta is not enabled". A Fabric
    # session already has it; this branch exists for local and CI runs.
    builder = (
        SparkSession.builder.appName("local_orchestrator")
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark_session = configure_spark_with_delta_pip(builder).getOrCreate()

    # Usage: python -m pipelines.local_orchestrator [YYYY-MM-DD]
    # The trigger passes the window explicitly; the default is only for a
    # manual "just run today" invocation.
    window = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today()

    run_result = run_pipeline(
        window_date=window,
        spark=spark_session,
        teams_webhook_url=os.environ.get("TEAMS_WEBHOOK_URL"),
    )

    # Non-zero on anything short of a clean run, so a scheduler or CI job
    # surfaces a partial window instead of reporting green on incomplete data.
    sys.exit(0 if run_result.is_full_success else 1)
