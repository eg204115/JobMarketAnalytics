"""
Adzuna API connector. Docs: https://developer.adzuna.com/docs/search

Adzuna paginates via 'page' in the URL path itself and reports total result
count, so _has_more_pages compares cumulative fetched count against 'count'.
"""

from __future__ import annotations

from typing import Any

from ingestion.base_connector import BaseConnector, JobPosting
from utils.config_loader import get_source_credential
from utils.logger import get_logger

logger = get_logger(__name__)


def _as_float(value: Any) -> float | None:
    """
    Adzuna returns whole-number salaries as JSON ints ("salary_min": 0), but
    JobPosting declares these as float and Bronze applies DoubleType, whose
    verifier accepts float only — an int raises CANNOT_ACCEPT_OBJECT_IN_TYPE
    and aborts the whole batch. Normalizing here keeps the dataclass's declared
    contract honest, which is the connector's job per JobPosting's docstring.

    bool is checked first because it subclasses int, so float(True) would
    otherwise silently become 1.0.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class AdzunaConnector(BaseConnector):

    def _build_request(self, country: str, page: int) -> tuple[str, dict[str, Any]]:
        app_id = get_source_credential(self.source_config, "app_id_env")
        app_key = get_source_credential(self.source_config, "app_key_env")
        base_url = self.source_config["base_url"]

        # Adzuna's REST path includes country + page: /jobs/{country}/search/{page}
        url = f"{base_url}/{country}/search/{page}"
        params = {
            "app_id": app_id,
            "app_key": app_key,
            "results_per_page": self.app_config.get(
                "ingestion", "results_per_page", default=50
            ),
            "content-type": "application/json",
        }

        # Adzuna returns every job category when `what` is omitted. Sent only
        # when configured so a source with no query keeps the old behaviour.
        query = self.source_config.get("query")
        if query:
            params["what"] = query

        return url, params

    def _parse_response(self, response_json: dict, country: str) -> list[JobPosting]:
        results = response_json.get("results", [])
        postings: list[JobPosting] = []

        for item in results:
            try:
                postings.append(
                    JobPosting(
                        source="adzuna",
                        source_job_id=str(item.get("id")),
                        title=item.get("title", "").strip(),
                        company=(item.get("company") or {}).get("display_name"),
                        location_raw=(item.get("location") or {}).get("display_name"),
                        country=country,
                        description=item.get("description"),
                        salary_min=_as_float(item.get("salary_min")),
                        salary_max=_as_float(item.get("salary_max")),
                        currency=item.get("salary_currency") or None,
                        remote=None,  # Adzuna doesn't expose this directly; derived in Silver
                        posted_date=item.get("created"),
                        url=item.get("redirect_url"),
                        raw_payload=item,
                    )
                )
            except Exception as exc:
                # A single malformed record should not kill the whole page.
                logger.warning(
                    "[adzuna] Skipping malformed record id=%s: %s",
                    item.get("id", "unknown"), exc,
                )

        return postings

    def _has_more_pages(self, response_json: dict, current_page: int) -> bool:
        total_available = response_json.get("count", 0)
        results_per_page = self.app_config.get(
            "ingestion", "results_per_page", default=50
        )
        return current_page * results_per_page < total_available
