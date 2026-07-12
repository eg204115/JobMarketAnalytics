"""
Unit tests for connector_factory.py. These run without any network access —
BaseConnector's abstract methods mean we can't hit a real API in a unit test,
which is exactly right: integration tests (Chapter 5) hit real APIs; unit
tests validate wiring and logic only.
"""

import pytest

from ingestion.adzuna_connector import AdzunaConnector
from ingestion.connector_factory import build_enabled_connectors
from ingestion.jooble_connector import JoobleConnector
from utils.config_loader import AppConfig, ConfigError


def _make_config(sources: list[dict]) -> AppConfig:
    return AppConfig(raw={"sources": sources, "ingestion": {}})


def test_builds_only_enabled_connectors():
    config = _make_config([
        {"name": "adzuna", "enabled": True, "class": "AdzunaConnector",
         "base_url": "x", "auth": {}, "countries": ["us"]},
        {"name": "jooble", "enabled": False, "class": "JoobleConnector",
         "base_url": "x", "auth": {}, "countries": ["us"]},
    ])

    connectors = build_enabled_connectors(config)

    assert len(connectors) == 1
    assert isinstance(connectors[0], AdzunaConnector)


def test_unknown_connector_class_raises_config_error():
    config = _make_config([
        {"name": "mystery", "enabled": True, "class": "DoesNotExistConnector",
         "base_url": "x", "auth": {}, "countries": ["us"]},
    ])

    with pytest.raises(ConfigError):
        build_enabled_connectors(config)


def test_no_enabled_sources_returns_empty_list():
    config = _make_config([
        {"name": "adzuna", "enabled": False, "class": "AdzunaConnector",
         "base_url": "x", "auth": {}, "countries": ["us"]},
    ])

    assert build_enabled_connectors(config) == []