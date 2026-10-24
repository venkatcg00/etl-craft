"""The action table: one row per action holds its role, audit, placement and route."""

import types
from dataclasses import fields, is_dataclass

import pytest

from etl_craft.cli.commands import COMMANDS
from etl_craft.core.enums import ActionName, ActionScope, ApiRole, Audit, HttpMethod, Placement
from etl_craft.services.operations.actions import ACTIONS

pytestmark = pytest.mark.unit


def test_every_action_has_one_row_and_every_command_names_real_actions():
    assert set(ACTIONS) == set(ActionName)
    for command in COMMANDS:
        assert command.actions, command.name
        assert set(command.actions) <= set(ACTIONS), command.name


def test_every_action_is_reachable_from_the_cli_or_a_route():
    from_cli = {name for command in COMMANDS for name in command.actions}
    routed = {name for name, action in ACTIONS.items() if action.path is not None}
    assert set(ACTIONS) - from_cli - routed == set()


@pytest.mark.parametrize(
    "name,role",
    [
        (ActionName.RUN, ApiRole.OPERATOR),
        (ActionName.CONTROL, ApiRole.OPERATOR),
        (ActionName.ACCOUNT_LIST, ApiRole.ADMIN),
        (ActionName.TOKEN_CREATE, ApiRole.ADMIN),
        (ActionName.CONFIG_APPLY, ApiRole.DEVELOPER),
        (ActionName.CONFIG_PLAN, ApiRole.VIEWER),
        (ActionName.CONFIG_EXPORT, ApiRole.VIEWER),
        (ActionName.LINEAGE, ApiRole.VIEWER),
        (ActionName.LINEAGE_REFRESH, ApiRole.DEVELOPER),
        (ActionName.GENERATE_DOCS, ApiRole.DEVELOPER),
        (ActionName.DOCS_VERSION, ApiRole.ADMIN),
        (ActionName.DOCS_VERSION_CHECK, ApiRole.VIEWER),
        (ActionName.REHASH, ApiRole.ADMIN),
        (ActionName.REHASH_PLAN, ApiRole.VIEWER),
        (ActionName.SETUP, ApiRole.ADMIN),
        (ActionName.SETUP_GRANTS, ApiRole.VIEWER),
        (ActionName.PUBLISH_DOCS, ApiRole.ADMIN),
        (ActionName.CLONE, ApiRole.ADMIN),
        (ActionName.CONTROLLER, ApiRole.ADMIN),
        (ActionName.UI, ApiRole.ADMIN),
        (ActionName.LOGIN, None),
        (ActionName.LOGOUT, None),
    ],
)
def test_roles_follow_the_approved_policy(name, role):
    assert ACTIONS[name].role == role


def test_reads_are_get_routes_that_record_nothing():
    for action in ACTIONS.values():
        if action.method == HttpMethod.GET:
            assert action.audit == Audit.NONE, action.name


def test_every_change_is_audited():
    unaudited = {
        name
        for name, action in ACTIONS.items()
        if action.method == HttpMethod.POST and action.audit == Audit.NONE
    }
    # The bulk request invokes the audited control once per object; a token refresh issues
    # no new authority; provenance only reads, with a structured key in its body. An agent's
    # claim, renewal and log upload repeat every few seconds, and each is recorded where it
    # takes effect: the attempt row, the agent registry and the log chunks.
    assert unaudited == {
        ActionName.CONTROL_BULK,
        ActionName.REFRESH,
        ActionName.DATA_PROVENANCE,
        ActionName.AGENT_CLAIM,
        ActionName.AGENT_RENEW,
        ActionName.AGENT_LOG,
    }


def test_host_actions_have_no_route_and_say_where_they_run():
    for action in ACTIONS.values():
        on_host = action.placement == Placement.CONTROLLER_HOST
        assert (action.path is None) == on_host, action.name
        assert (action.remedy is not None) == on_host, action.name


def test_routed_actions_answer_with_documents_and_take_keyword_requests():
    for action in ACTIONS.values():
        assert is_dataclass(action.request), action.name
        assert all(f.kw_only for f in fields(action.request)), action.name
        if action.path is None or action.streams or action.result is None:
            continue
        documents = (
            action.result.__args__
            if isinstance(action.result, types.UnionType)
            else (action.result,)
        )
        assert all(isinstance(getattr(d, "SCHEMA", None), str) for d in documents), action.name


def test_pipeline_selections_use_pipeline_scope_and_identity_administration_stays_deployment():
    for action in ACTIONS.values():
        names = {field.name for field in fields(action.request)}
        # An agent reaches only the attempt its own token claimed, fenced by owner and status.
        if action.role == ApiRole.AGENT:
            continue
        if names & {"pipeline_code", "run_id", "attempt_id", "control_id", "task_run_id"}:
            assert action.scope == ActionScope.PIPELINE, action.name
    for name in (ActionName.PIPELINES, ActionName.LINEAGE, ActionName.DOCS_VERSION_CHECK):
        assert ACTIONS[name].scope == ActionScope.PIPELINE
    for name in (
        ActionName.GROUP_ADD,
        ActionName.GROUP_LIST,
        ActionName.GROUP_DISABLE,
        ActionName.GROUP_ADD_MEMBER,
        ActionName.GROUP_REMOVE_MEMBER,
        ActionName.ACCOUNT_ADD,
        ActionName.TOKEN_CREATE,
    ):
        assert ACTIONS[name].scope == ActionScope.DEPLOYMENT


def test_config_apply_has_project_scope():
    assert ACTIONS[ActionName.CONFIG_APPLY].scope == ActionScope.PROJECT
