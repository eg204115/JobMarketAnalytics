"""
Merges non-secret YAML config with secret environment variables into a single,
read-only configuration object. This is the ONLY module allowed to read
os.environ directly for API credentials — every other module receives
credentials already resolved, so secrets are never scattered across the codebase.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

_ENV_VAR_PATTERN = re.compile(r"\$\{(\w+)(?::([^}]*))?\}")


class ConfigError(Exception):
    """Raised when required configuration or secrets are missing."""


def _resolve_env_placeholders(value: Any) -> Any:
    """
    Recursively resolves ${VAR_NAME} and ${VAR_NAME:default} placeholders
    found inside YAML string values using environment variables.
    """
    if isinstance(value, str):
        def replace(match: re.Match) -> str:
            var_name, default = match.group(1), match.group(2)
            resolved = os.environ.get(var_name, default)
            if resolved is None:
                raise ConfigError(
                    f"Environment variable '{var_name}' is required but not set, "
                    f"and no default was provided in config."
                )
            return resolved

        return _ENV_VAR_PATTERN.sub(replace, value)

    if isinstance(value, dict):
        return {k: _resolve_env_placeholders(v) for k, v in value.items()}

    if isinstance(value, list):
        return [_resolve_env_placeholders(v) for v in value]

    return value


@dataclass(frozen=True)
class AppConfig:
    """Immutable, fully-resolved application configuration."""

    raw: dict = field(repr=False)

    def get(self, *keys: str, default: Any = None) -> Any:
        """Safe nested lookup: config.get('ingestion', 'max_retries')"""
        node = self.raw
        for key in keys:
            if not isinstance(node, dict) or key not in node:
                return default
            node = node[key]
        return node


def load_config(
    config_path: str = "config/config.yaml",
    sources_path: str = "config/sources.yaml",
    env_file: str = ".env",
) -> AppConfig:
    """
    Loads .env into the process environment, then loads and merges
    config.yaml + sources.yaml, resolving ${ENV_VAR} placeholders.

    Raises ConfigError if a required file is missing or an env var
    referenced without a default is unset.
    """
    if Path(env_file).exists():
        load_dotenv(env_file)

    merged: dict = {}
    for path in (config_path, sources_path):
        p = Path(path)
        if not p.exists():
            raise ConfigError(f"Required config file not found: {path}")
        with open(p, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        merged.update(data)

    resolved = _resolve_env_placeholders(merged)
    return AppConfig(raw=resolved)


def get_source_credential(source_config: dict, key_name: str) -> str:
    """
    Resolves a specific credential for a source entry in sources.yaml,
    e.g. get_source_credential(source_cfg, 'app_key_env') -> actual API key value.
    Raises ConfigError with a clear message if the env var is missing —
    this is deliberately verbose because a silent None credential produces
    a confusing 401 error three layers downstream instead.
    """
    env_var_name = source_config.get("auth", {}).get(key_name)
    if not env_var_name:
        raise ConfigError(f"No '{key_name}' configured in sources.yaml auth block.")
    value = os.environ.get(env_var_name)
    if not value:
        raise ConfigError(
            f"Environment variable '{env_var_name}' is not set. "
            f"Did you copy .env.example to .env and fill in real credentials?"
        )
    return value