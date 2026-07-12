"""
Abstract base class every job-board connector must implement.

Design pattern: Template Method. fetch() defines the fixed algorithm
(paginate -> request -> parse -> normalize -> accumulate), while subclasses
only implement the API-specific pieces: _build_request(), _parse_response(),
and _has_more_pages().

This means retry logic, timeout handling, and logging are written ONCE here
and inherited by every connector — a new source cannot "forget" to implement
retries because it's not their responsibility to.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import requests

from utils.config_loader import AppConfig
from utils.logger import get_logger, get_run_id

logger = get_logger(__name__)


class ConnectorError(Exception):
    """Raised when a connector exhausts retries or receives an unrecoverable response."""


@dataclass
class JobPosting:
    """
    Common normalized schema every connector must map its source's response into.
    This is what gets written to Bronze — NOT the raw API response — because
    even Bronze needs a minimally consistent shape to be queryable, while still
    preserving the full raw payload for audit purposes.
    """
    source: str
    source_job_id: str
    title: str
    company: str | None
    location_raw: str | None
    country: str | None
    description: str | None
    salary_min: float | None
    salary_max: float | None
    currency: str | None
    remote: bool | None
    posted_date: str | None
    url: str | None
    ingestion_timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    run_id: str = field(default_factory=get_run_id)
    raw_payload: dict = field(default_factory=dict)


class BaseConnector(ABC):
    def __init__(self, source_config: dict, app_config: AppConfig):
        self.source_config = source_config
        self.app_config = app_config
        self.source_name = source_config["name"]
        self.max_retries = app_config.get("ingestion", "max_retries", default=3)
        self.backoff_seconds = app_config.get(
            "ingestion", "retry_backoff_seconds", default=2
        )
        self.timeout = app_config.get(
            "ingestion", "request_timeout_seconds", default=15
        )
        self.max_pages = app_config.get(
            "ingestion", "max_pages_per_run", default=5
        )

    # ---- Fixed algorithm (Template Method) -----------------------------

    def fetch(self, country: str) -> list[JobPosting]:
        """
        Paginated fetch with retry-on-failure. Returns normalized JobPosting
        records. Individual page failures after max_retries are logged and
        skipped rather than aborting the entire run — partial data beats no
        data for a daily ingestion job.
        """
        all_postings: list[JobPosting] = []
        page = 1

        while page <= self.max_pages:
            try:
                response_json = self._request_with_retry(country=country, page=page)
            except ConnectorError:
                logger.error(
                    "[%s] Giving up on page %d after %d retries — skipping remaining pages.",
                    self.source_name, page, self.max_retries,
                )
                break

            postings = self._parse_response(response_json, country=country)
            all_postings.extend(postings)
            logger.info(
                "[%s] country=%s page=%d fetched=%d records (running total=%d)",
                self.source_name, country, page, len(postings), len(all_postings),
            )

            if not self._has_more_pages(response_json, page):
                break
            page += 1

        return all_postings

    def _request_with_retry(self, country: str, page: int) -> dict:
        last_exception: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                url, params = self._build_request(country=country, page=page)
                response = requests.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as exc:
                last_exception = exc
                wait = self.backoff_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "[%s] Request failed (attempt %d/%d): %s — retrying in %ds",
                    self.source_name, attempt, self.max_retries, exc, wait,
                )
                if attempt < self.max_retries:
                    time.sleep(wait)

        raise ConnectorError(
            f"[{self.source_name}] Exhausted {self.max_retries} retries. "
            f"Last error: {last_exception}"
        )

    # ---- Subclass-specific pieces ---------------------------------------

    @abstractmethod
    def _build_request(self, country: str, page: int) -> tuple[str, dict[str, Any]]:
        """Returns (url, query_params) for this API/page."""
        raise NotImplementedError

    @abstractmethod
    def _parse_response(self, response_json: dict, country: str) -> list[JobPosting]:
        """Maps this API's raw response shape into a list of JobPosting."""
        raise NotImplementedError

    @abstractmethod
    def _has_more_pages(self, response_json: dict, current_page: int) -> bool:
        """Returns whether another page should be fetched."""
        raise NotImplementedError