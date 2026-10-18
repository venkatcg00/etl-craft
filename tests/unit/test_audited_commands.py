"""Every command that changes something records its request in AUD_ACTIONS; the rest only read."""

import pytest

from etl_craft.cli.commands import COMMANDS
from etl_craft.engine.audit import MUTATING_COMMANDS

pytestmark = pytest.mark.unit

READ_ONLY = {
    "audit",
    "doctor",
    "explain",
    "generate-yml",
    "graph",
    "history",
    "list",
    "status",
    "steps",
    "validate",
    "whoami",
    "logs",
}
"""The commands that change nothing in the Engine DB or the warehouse."""


def test_every_command_is_audited_or_only_reads():
    names = {command.name for command in COMMANDS}
    service_actions = {"data edit", "data unlock"}
    assert service_actions <= MUTATING_COMMANDS
    audited = {name.split()[0] for name in MUTATING_COMMANDS - service_actions}
    assert names - audited - READ_ONLY == set()
    assert audited & READ_ONLY == set()
    assert audited <= names
