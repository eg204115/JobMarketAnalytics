
"""
Jooble API connector. Docs: https://jooble.org/api/about

Jooble is POST-based (unusual vs. most REST job APIs) and paginates via a
'page' field in the JSON body rather than the URL, so _build_request returns
a POST-style params dict that run_ingestion.py's requester needs to send as
a JSON body, not query params. We handle that by overriding the request
mechanics minimally rather than forcing GET semantics onto a POST API.
"""

from __future__ import annotations

from typing import Any

import requests

from ingestion.base_connector import BaseConnector, ConnectorError, JobPosting
from utils.config_loader import get_source_credential
from utils.logger import get_logger

logger = get_logger(__name__)


class JoobleConnector(BaseConnector):
    """
    Overrides _request_with_retry because Jooble requires POST + JSON body,
    while BaseConnector's default implementation assumes GET + query params.
    This is a deliberate, minimal override — everything else (retry/backoff/
    logging) is reused from the parent via super().
    """

    def _build_request(self, country: str, page: int) -> tuple[str, dict[str, Any]]:
        api_key = get_source_credential(self.source_config, "api_key_env")
        base_url = self.source_config["base_url"]
        url = f"{base_url}/{api_key}"
        body = {
            "keywords": "",       # empty = all job categories
            "location": country,
            "page": str(page),
        }
        return url, body

    def _request_with_retry(self, country: str, page: int) -> dict:
        import time

        last_exception: Exception | None = None
        url, body = self._build_request(country=country, page=page)

        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.post(url, json=body, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as exc:
                last_exception = exc
                wait = self.backoff_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "[jooble] Request failed (attempt %d/%d): %s — retrying in %ds",
                    attempt, self.max_retries, exc, wait,
                )
                if attempt < self.max_retries:
                    time.sleep(wait)

        raise ConnectorError(
            f"[jooble] Exhausted {self.max_retries} retries. Last error: {last_exception}"
        )

    def _parse_response(self, response_json: dict, country: str) -> list[JobPosting]:
        results = response_json.get("jobs", [])
        postings: list[JobPosting] = []

        for item in results:
            try:
                postings.append(
                    JobPosting(
                        source="jooble",
                        source_job_id=str(item.get("id", item.get("link", ""))),
                        title=(item.get("title") or "").strip(),
                        company=item.get("company"),
                        location_raw=item.get("location"),
                        country=country,
                        description=item.get("snippet"),
                        salary_min=None,   # Jooble returns salary as free text, parsed in Silver
                        salary_max=None,
                        currency=None,
                        remote=None,
                        posted_date=item.get("updated"),
                        url=item.get("link"),
                        raw_payload=item,
                    )
                )
            except Exception as exc:
                logger.warning("[jooble] Skipping malformed record: %s", exc)

        return postings

    def _has_more_pages(self, response_json: dict, current_page: int) -> bool:
        total_available = response_json.get("totalCount", 0)
        results_per_page = self.app_config.get(
            "ingestion", "results_per_page", default=50
        )
        return current_page * results_per_page < total_available