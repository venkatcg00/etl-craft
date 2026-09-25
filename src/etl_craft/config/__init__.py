"""``craft-connector.yml``: finding it, reading it, and what each connection target accepts.

The file is written by the team and only read by the engine; secrets are always variable
names, never values.
"""

from etl_craft.config.discovery import resolve_config_path
from etl_craft.config.loader import load_config, parse_config
from etl_craft.config.model import (
    CloningConfig,
    ConnectionProfile,
    ConnectionSection,
    ConnectorConfig,
    DagDefaults,
    DocsSiteConfig,
    EmailConfig,
    EmailProfile,
    ExecutionLimits,
    SettingSource,
    SourceConfig,
)
from etl_craft.config.resolve import profile_needs_secret, profile_secret, resolve_secret

__all__ = [
    "CloningConfig",
    "ConnectionProfile",
    "ConnectionSection",
    "ConnectorConfig",
    "DagDefaults",
    "DocsSiteConfig",
    "EmailConfig",
    "EmailProfile",
    "ExecutionLimits",
    "SettingSource",
    "SourceConfig",
    "load_config",
    "parse_config",
    "profile_needs_secret",
    "profile_secret",
    "resolve_config_path",
    "resolve_secret",
]
