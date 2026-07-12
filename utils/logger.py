"""
Production logging setup for the Job Market Analytics Platform.

Design goals:
- Every log line is tagged with a run_id so a single pipeline execution's
  logs can be grepped out of a shared log file.
- Console gets INFO+, file gets DEBUG+ (verbose history without noisy console).
- Configuration lives in YAML, not hardcoded, so ops can change log levels
  without touching Python code.
"""

from __future__ import annotations

import logging
import logging.config
import os
import uuid
from pathlib import Path

import yaml

# One run_id per process. Every log line in a single ingestion run shares this ID.
_RUN_ID = str(uuid.uuid4())[:8]


class RunIdFilter(logging.Filter):
    """Injects the process-wide run_id into every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = _RUN_ID
        return True


def get_run_id() -> str:
    return _RUN_ID


def setup_logging(config_path: str = "config/logging_config.yaml") -> None:
    """
    Loads logging_config.yaml and configures the root logger.
    Creates the log directory if it doesn't exist (RotatingFileHandler
    does not create parent directories automatically).
    """
    config_file = Path(config_path)
    if not config_file.exists():
        # Fail safe: fall back to basic console logging rather than crashing
        # the whole pipeline just because a log config is missing.
        logging.basicConfig(level=logging.INFO)
        logging.getLogger(__name__).warning(
            "Logging config not found at %s — using basicConfig fallback.",
            config_path,
        )
        return

    with open(config_file, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Ensure the log directory exists before handlers try to open files in it.
    log_filename = config.get("handlers", {}).get("file", {}).get("filename")
    if log_filename:
        Path(log_filename).parent.mkdir(parents=True, exist_ok=True)

    logging.config.dictConfig(config)


def get_logger(name: str) -> logging.Logger:
    """Standard entry point every module should use: logger = get_logger(__name__)"""
    return logging.getLogger(name)