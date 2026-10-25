"""Retention is explicit, bounded and backwards compatible with existing project files."""

from pathlib import Path

import pytest

from etl_craft.config import parse_config
from etl_craft.core.errors import ConfigurationError

pytestmark = pytest.mark.unit


def configuration(**settings):
    return parse_config(
        {
            "Secrets": {"Source_type": "ENVIRONMENT"},
            "Engine": {
                "dev": {"Jdbc_url": "jdbc:sqlite:engine.db", "Auth_mode": "NONE", "Schema": "main"}
            },
            "Retention": settings,
        },
        Path("craft-connector.yml"),
    )


def test_unset_retention_keeps_all_history():
    policy = configuration().retention
    assert policy.run_history_days is None
    assert policy.attempt_log_days is None
    assert policy.daily_at is None


@pytest.mark.parametrize(
    "field",
    [
        "Run_history_days",
        "Attempt_log_days",
        "Consumption_days",
        "Interventions_days",
        "Config_history_days",
    ],
)
@pytest.mark.parametrize("value", [0, -1, True, 1.5, "forever"])
def test_invalid_retention_period_names_its_remedy(field, value):
    with pytest.raises(ConfigurationError, match=field):
        configuration(**{field: value})


@pytest.mark.parametrize("value", ["3:00", "24:00", "03:60", "03:00:00", "tomorrow"])
def test_invalid_daily_time_is_refused(value):
    with pytest.raises(ConfigurationError, match=r"Retention.Daily_at=.*expected HH:MM"):
        configuration(Daily_at=value)


def test_explicit_retention_is_loaded():
    config = configuration(Run_history_days=90, Attempt_log_days=30, Daily_at="03:00")
    assert config.retention.run_history_days == 90
    assert config.retention.attempt_log_days == 30
    assert config.retention.daily_at == "03:00"


def test_developer_publication_cannot_override_host_retention(tmp_path):
    import yaml

    from etl_craft.config.project import project_config

    host = configuration(Run_history_days=90)
    path = tmp_path / "craft-connector.yml"
    path.write_text(
        yaml.safe_dump(
            {
                "Secrets": {"Source_type": "ENVIRONMENT"},
                "Retention": {"Run_history_days": 1, "Daily_at": "00:00"},
            }
        )
    )
    pinned = project_config(host, path)
    assert pinned.retention == host.retention
    assert pinned.retention.daily_at is None


def test_direct_policy_cannot_accept_a_negative_period():
    from etl_craft.config.model import Retention

    with pytest.raises(ConfigurationError, match="Run_history_days"):
        Retention(run_history_days=-1)
