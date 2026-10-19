"""The command variants and deployment role order shared by every transport."""

import pytest

from etl_craft.cli import COMMANDS
from etl_craft.core.enums import ApiRole
from etl_craft.core.errors import ConfigurationError
from etl_craft.services.operations.permissions import required_role

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "command,arguments,role",
    [
        ("run", {}, ApiRole.OPERATOR),
        ("account", {"account_verb": "list"}, ApiRole.ADMIN),
        ("token", {"token_verb": "create"}, ApiRole.ADMIN),
        ("config", {"config_verb": "apply"}, ApiRole.DEVELOPER),
        ("config", {"config_verb": "plan"}, ApiRole.VIEWER),
        ("config", {"config_verb": "export"}, ApiRole.VIEWER),
        ("migrate", {}, ApiRole.DEVELOPER),
        ("migrate engine", {}, ApiRole.ADMIN),
        ("lineage", {}, ApiRole.VIEWER),
        ("lineage", {"refresh": True}, ApiRole.DEVELOPER),
        ("generate-docs", {}, ApiRole.DEVELOPER),
        ("docs-version", {}, ApiRole.ADMIN),
        ("docs-version", {"check": True}, ApiRole.VIEWER),
        ("rehash", {}, ApiRole.ADMIN),
        ("rehash", {"dry_run": True}, ApiRole.VIEWER),
        ("setup", {}, ApiRole.ADMIN),
        ("setup", {"print_grants": True}, ApiRole.VIEWER),
        ("publish-docs", {}, ApiRole.ADMIN),
        ("clone", {}, ApiRole.ADMIN),
        ("controller", {}, ApiRole.ADMIN),
        ("login", {}, None),
        ("logout", {}, None),
    ],
)
def test_variants_use_the_approved_shared_policy(command, arguments, role):
    assert required_role(command, arguments) == role


def test_every_registered_command_has_a_policy_and_unknown_operations_fail_closed():
    for command in COMMANDS:
        if command.name in {"account", "token", "config"}:
            continue
        required_role(command.name, {})
    with pytest.raises(ConfigurationError, match="no role policy"):
        required_role("unregistered-command", {})
