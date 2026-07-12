
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

from ingestion.connector_factory import build_enabled_connectors
from utils.config_loader import ConfigError, load_config
from utils.logger import get_logger, get_run_id, setup_logging

logger = get_logger(__name__)


def run() -> int:
    setup_logging()
    logger.info("Starting ingestion run_id=%s", get_run_id())

    try:
        app_config = load_config()
    except ConfigError as exc:
        logger.critical("Configuration error, aborting run: %s", exc)
        return 1

    connectors = build_enabled_connectors(app_config)
    if not connectors:
        logger.warning("No enabled sources found in sources.yaml — nothing to do.")
        return 0

    bronze_path = Path(app_config.get("storage", "bronze_path", default="data/bronze"))
    bronze_path.mkdir(parents=True, exist_ok=True)

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

            if postings:
                out_file = bronze_path / f"{connector.source_name}_{country}_{get_run_id()}.json"
                with open(out_file, "w", encoding="utf-8") as f:
                    json.dump([asdict(p) for p in postings], f, indent=2)
                logger.info("Wrote %d records to %s", len(postings), out_file)
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