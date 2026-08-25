
"""
CLI entry point for a manual/local ingestion run.
Usage: python -m ingestion.run_ingestion
In Fabric, notebook 01_bronze_ingestion.ipynb calls the same functions —
this script and that notebook share this exact code path, they are not
duplicated implementations.
"""

from __future__ import annotations

import json
import sys
from dataclasses import asdict
from pathlib import Path

from ingestion.base_connector import JobPosting
from ingestion.connector_factory import build_enabled_connectors
from utils.config_loader import ConfigError, load_config
from utils.logger import get_logger, get_run_id, setup_logging

logger = get_logger(__name__)


def write_postings_json(
    postings: list[JobPosting],
    bronze_dir: str | Path,
    source_name: str,
    country: str,
) -> Path | None:
    """
    Serializes one connector fetch into a JSON array file in the raw landing
    directory. Extracted so pipelines/local_orchestrator.py lands files the
    exact same way this CLI does — Bronze globs this directory by filename
    convention, so a second implementation drifting on naming or record shape
    would produce files Bronze silently cannot read.

    Returns the path written, or None when the fetch produced nothing.
    """
    if not postings:
        return None

    resolved = Path(bronze_dir)
    resolved.mkdir(parents=True, exist_ok=True)
    out_file = resolved / f"{source_name}_{country}_{get_run_id()}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump([asdict(p) for p in postings], f, indent=2)
    logger.info("Wrote %d records to %s", len(postings), out_file)
    return out_file


def run(
    config_path: str = "config/config.yaml",
    sources_path: str = "config/sources.yaml",
    logging_config_path: str = "config/logging_config.yaml",
    bronze_path: str | None = None,
) -> int:
    """
    Every path is a parameter with the local default baked in, so the same
    function drives a local CLI run, a CI run, and a Fabric notebook without
    any of them having to chdir or edit files on disk. A Fabric notebook has
    no repo root as its working directory, so relative defaults cannot resolve
    there — that is the whole reason these are arguments rather than constants.

    bronze_path overrides storage.bronze_path from config when supplied.
    """
    setup_logging(config_path=logging_config_path)
    logger.info("Starting ingestion run_id=%s", get_run_id())

    try:
        app_config = load_config(config_path=config_path, sources_path=sources_path)
    except ConfigError as exc:
        logger.critical("Configuration error, aborting run: %s", exc)
        return 1

    connectors = build_enabled_connectors(app_config)
    if not connectors:
        logger.warning("No enabled sources found in sources.yaml — nothing to do.")
        return 0

    resolved_bronze = Path(
        bronze_path
        or app_config.get("storage", "bronze_path", default="data/bronze")
    )
    resolved_bronze.mkdir(parents=True, exist_ok=True)

    total_records = 0
    failures = []

    for connector in connectors:
        countries = connector.source_config.get("countries", ["us"])
        for country in countries:
            try:
                postings = connector.fetch(country=country)
            except Exception as exc:  # connector-level failure must not kill other sources
                logger.error(
                    "[%s] Unhandled failure for country=%s: %s",
                    connector.source_name, country, exc, exc_info=True,
                )
                failures.append((connector.source_name, country, str(exc)))
                continue

            if write_postings_json(
                postings, resolved_bronze, connector.source_name, country
            ):
                total_records += len(postings)

    logger.info(
        "Ingestion run complete. total_records=%d failures=%d",
        total_records, len(failures),
    )

    if failures:
        for source, country, err in failures:
            logger.error("FAILED: source=%s country=%s error=%s", source, country, err)
        return 1  # non-zero exit so CI/orchestration can detect partial failure

    return 0


if __name__ == "__main__":
    sys.exit(run())