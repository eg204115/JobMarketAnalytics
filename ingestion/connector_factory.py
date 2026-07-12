"""
Instantiates the correct connector class based on sources.yaml, without the
caller needing to know concrete class names. This is what lets orchestration
code (Chapter 6) say "run all enabled sources" without an if/elif chain.
"""

from __future__ import annotations

from ingestion.adzuna_connector import AdzunaConnector
from ingestion.base_connector import BaseConnector
from ingestion.jooble_connector import JoobleConnector
from utils.config_loader import AppConfig, ConfigError

# Explicit registry (no dynamic getattr/importlib magic) — deliberately
# simple and greppable over "clever". Add a new connector = add one line here.
_CONNECTOR_REGISTRY: dict[str, type[BaseConnector]] = {
    "AdzunaConnector": AdzunaConnector,
    "JoobleConnector": JoobleConnector,
}


def build_enabled_connectors(app_config: AppConfig) -> list[BaseConnector]:
    """Returns instantiated connector objects for every source with enabled: true."""
    sources = app_config.get("sources", default=[])
    connectors: list[BaseConnector] = []

    for source_cfg in sources:
        if not source_cfg.get("enabled", False):
            continue

        class_name = source_cfg.get("class")
        connector_cls = _CONNECTOR_REGISTRY.get(class_name)
        if connector_cls is None:
            raise ConfigError(
                f"sources.yaml references unknown connector class '{class_name}' "
                f"for source '{source_cfg.get('name')}'. "
                f"Registered classes: {list(_CONNECTOR_REGISTRY.keys())}"
            )

        connectors.append(connector_cls(source_cfg, app_config))

    return connectors
