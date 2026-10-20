"""Spellings from before 1.0.0 are refused by name: no aliases, no case folding."""

import pytest
import yaml

from etl_craft.cli import build_parser
from etl_craft.config import load_config
from etl_craft.core.errors import ConfigurationError

pytestmark = pytest.mark.unit


OLD_PIPELINE, OLD_TASK = "--pipeline" + "_code", "--task" + "_code"
"""Built from parts, so a search for the old spellings finds only what still uses them."""


@pytest.mark.parametrize(
    "argv",
    [
        ["status", OLD_PIPELINE, "P"],
        ["explain", "--pipeline-code", "P", OLD_TASK, "T"],
        ["--server", "https://controller.example.com", "whoami"],
        ["list"],
    ],
)
def test_old_command_line_spellings_are_invalid(argv, capsys):
    with pytest.raises(SystemExit) as stopped:
        build_parser().parse_args(argv)
    assert stopped.value.code == 2
    assert "error:" in capsys.readouterr().err


def write(tmp_path, raw):
    path = tmp_path / "craft-connector.yml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    return path


def minimal(**sections):
    raw = {
        "Secrets": {"Source_type": "ENVIRONMENT"},
        "Orchestration": {},
        "Engine": {"dev": {"Jdbc_url": "jdbc:sqlite:e.db", "Schema": "main"}},
    }
    return {name: sections.get(name, value) for name, value in raw.items()}


@pytest.mark.parametrize(
    ("sections", "message"),
    [
        (
            {"Engine": {"dev": {"jdbc_url": "jdbc:sqlite:e.db", "Schema": "main"}}},
            r"unknown key\(s\) \['jdbc_url'\] in Engine.dev; .* Jdbc_url",
        ),
        (
            {
                "Orchestration": {
                    "Email": {"host": "smtp.example.com", "Port": 25, "From_address": "a@b"}
                }
            },
            r"unknown key\(s\) \['host'\] in Orchestration.Email; .* Host",
        ),
        (
            {"Orchestration": {"Dependency_gates": "warn"}},
            "Dependency_gates must be ENFORCE, WARN or OFF, got 'warn'",
        ),
        ({"Secrets": {"Source_type": "environment"}}, "Source_type must be one of ENVIRONMENT"),
        (
            {
                "Engine": {
                    "dev": {"Jdbc_url": "jdbc:sqlite:e.db", "Schema": "main", "Auth_mode": "none"}
                }
            },
            "Auth_mode resolved to 'none'",
        ),
    ],
)
def test_old_configuration_spellings_are_refused_by_name(tmp_path, sections, message):
    with pytest.raises(ConfigurationError, match=message):
        load_config(write(tmp_path, minimal(**sections)))
