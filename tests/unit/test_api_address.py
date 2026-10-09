"""HTTP listener configuration validates before any connection is opened."""

from pathlib import Path

import pytest

from etl_craft.config import parse_config
from etl_craft.config.model import parse_api_address
from etl_craft.core.errors import ConfigurationError

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("value", ["localhost:8730", "127.0.0.1:0", "[::1]:8730"])
def test_api_address(value):
    host, port = parse_api_address(value)
    assert host and 0 <= port <= 65535
    config = parse_config(
        {
            "Secrets": {"Source_type": "environment"},
            "Orchestration": {"Mode": "local", "Api_address": value},
            "Engine": {"test": {"jdbc_url": "jdbc:sqlite:engine.db", "schema": "main"}},
        },
        Path("craft-connector.yml"),
    )
    assert config.api_address == value


@pytest.mark.parametrize(
    "value",
    [
        "localhost",
        "localhost:-1",
        "localhost:65536",
        "user@localhost:80",
        "localhost:80/path",
        "localhost:80?secret=x",
        "bad host:80",
    ],
)
def test_bad_api_address(value):
    with pytest.raises(ConfigurationError, match="Api_address"):
        parse_api_address(value)
