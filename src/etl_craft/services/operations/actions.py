"""One row per action: what it calls, who may, whether it is audited, where it runs, its route.

Every interface reaches an operation through ``invoke``, which checks the role, records the
request once and calls the operation. The CLI, the HTTP routes, the reference page and the
controller client read everything else about an action from this table.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace

from sqlalchemy.engine import Connection

from etl_craft.core.actor import acting_as, sso_groups
from etl_craft.core.enums import ActionName, ActionScope, ApiRole, Audit, HttpMethod, Placement
from etl_craft.core.errors import PermissionDeniedError, UsageError
from etl_craft.engine.controls import ControlView
from etl_craft.engine.migrations import pending_migrations
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.group_claims import token_groups
from etl_craft.engine.repository.tasks import fetch_task_parameters
from etl_craft.engine.schema import InitResult
from etl_craft.services.catalog.gaps import GapSnapshot
from etl_craft.services.config_files import ConfigExport, ConfigSync
from etl_craft.services.operations import (
    accounts,
    agent_claims,
    agent_logs,
    agent_models,
    agent_reports,
    agents,
    capabilities,
    catalog,
    config_files,
    controls,
    diagnostics,
    documents,
    groups,
    inspect,
    maintenance,
    models,
    processes,
    runs,
    sessions,
    status,
    tokens,
    triggers,
    values,
)
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.permissions import pipeline_access, require_child_groups
from etl_craft.services.operations.recording import record_refusal, recorded
from etl_craft.services.operations.requests import (
    AttemptId,
    AuditInput,
    BulkControlRequest,
    CapabilitiesInput,
    ControlId,
    ControlRequest,
    HistoryInput,
    LineageInput,
    LineageRefreshInput,
    LogInput,
    MarkRequest,
    NoInput,
    PipelineInput,
    ReconcileInput,
    Request,
    RunFilter,
    RunId,
    RunRequest,
    RunSelection,
    TagFilter,
    TaskSelection,
    ValidateInput,
    arguments,
)
from etl_craft.services.operations.visibility import visible_result
from etl_craft.services.rehash import RehashResult
from etl_craft.services.setup import SetupResult
from etl_craft.services.trigger_validation import trigger_definition
from etl_craft.services.validate import Report

ON_HOST = "run it on the controller's host"
CONFIG_ON_HOST = (
    "commit the config files to the project the controller deploys and run "
    "`etl-craft config apply` there, by CI or an admin"
)
MIGRATION_ON_HOST = (
    "commit the migration to the project the controller deploys and run `etl-craft migrate` "
    "there, by CI or an admin"
)
STARTED_ON_HOST = "start it on the host that serves it"


@dataclass(frozen=True)
class Action:
    """One action and every rule an interface needs about it.

    ``call`` performs the operation or validates a process startup. It is ``None`` when an
    interface owns the complete process inside ``admitted``. ``result`` is the document type,
    or ``None`` for no content; ``streams`` marks
    an action that returns bytes. ``escalate`` raises the role from what the request would
    change, as ``migrate`` does for packaged migrations. ``remedy`` says how to run a
    ``CONTROLLER_HOST`` action, for an interface that refuses it elsewhere.
    """

    name: ActionName
    call: Callable[..., object] | None
    request: type[Request]
    result: object
    role: ApiRole | None
    method: HttpMethod | None = None
    path: str | None = None
    audit: Audit = Audit.NONE
    placement: Placement = Placement.ANYWHERE
    remedy: str | None = None
    provisioning: bool = False
    streams: bool = False
    scope: ActionScope = ActionScope.DEPLOYMENT
    escalate: Callable[[OperationContext, object], ApiRole] | None = None

    def required_role(self, ctx: OperationContext, request: object) -> ApiRole | None:
        """Return the role this request needs."""
        return self.role if self.escalate is None else self.escalate(ctx, request)


def _migration_role(ctx: OperationContext, request: object) -> ApiRole:
    """Require an admin for packaged Engine DB migrations and a developer for project ones."""
    assert isinstance(request, maintenance.MigrateInput)
    pending = pending_migrations(
        ctx.engine, project_dir=request.migrations_dir, project_default=ctx.config.migrations_dir
    )
    return ApiRole.ADMIN if any(n.startswith("engine/") for n in pending) else ApiRole.DEVELOPER


VIEWER, OPERATOR = ApiRole.VIEWER, ApiRole.OPERATOR
DEVELOPER, ADMIN = ApiRole.DEVELOPER, ApiRole.ADMIN
GET, POST = HttpMethod.GET, HttpMethod.POST
REQUEST, BY_ACTION = Audit.REQUEST, Audit.BY_ACTION
API = "/api/v1"
HOST = Placement.CONTROLLER_HOST

_ROWS = (
    Action(ActionName.AGENTS, agents.list_agents, NoInput, agents.AgentsView, VIEWER, GET,
           f"{API}/agents"),
    Action(ActionName.AGENT_CONTROL, agents.command, agent_models.AgentControl,
           agent_models.AgentReply, OPERATOR, POST, f"{API}/agents/commands", REQUEST),
    Action(ActionName.AGENT_REGISTER, agents.register, agent_models.AgentRegister,
           agent_models.AgentReply, ApiRole.AGENT, POST, f"{API}/agent/register", REQUEST),
    Action(ActionName.AGENT_DEREGISTER, agents.deregister, agent_models.AgentRequest,
           agent_models.AgentReply, ApiRole.AGENT, POST, f"{API}/agent/deregister", REQUEST),
    Action(ActionName.AGENT_CLAIM, agent_claims.claim, agent_models.AgentClaim,
           agent_models.AgentReply, ApiRole.AGENT, POST, f"{API}/agent/claim"),
    Action(ActionName.AGENT_RENEW, agents.renew, agent_models.AgentRenew,
           agent_models.AgentReply, ApiRole.AGENT, POST, f"{API}/agent/renew"),
    Action(ActionName.AGENT_REPORT, agent_reports.report, agent_models.AgentTask,
           agent_models.AgentReply, ApiRole.AGENT, POST, f"{API}/agent/report", BY_ACTION),
    Action(ActionName.AGENT_LOG, agent_logs.upload, agent_models.AgentLog,
           agent_models.AgentReply, ApiRole.AGENT, POST, f"{API}/agent/log"),

    # Sign-in: no role; the sign-in actions record their own row once the account is proven.
    Action(ActionName.LOGIN_METHODS, sessions.sign_in_methods, NoInput, sessions.SignInMethods,
           None, GET, f"{API}/login/methods"),
    Action(ActionName.LOGIN, sessions.sign_in, sessions.SignIn, sessions.Session, None, POST,
           f"{API}/login/password", BY_ACTION),
    Action(ActionName.LOGIN_SSO, sessions.start_sso, NoInput, sessions.SsoStart, None, POST,
           f"{API}/login/sso", BY_ACTION),
    Action(ActionName.LOGIN_SSO_POLL, sessions.poll_sso, sessions.SsoPollInput, sessions.SsoPoll,
           None, POST, f"{API}/login/sso/poll", BY_ACTION),
    Action(ActionName.REFRESH, sessions.refresh, sessions.TokenInput, sessions.Session, None,
           POST, f"{API}/login/refresh"),
    Action(ActionName.LOGOUT, sessions.sign_out, sessions.TokenInput, None, None, POST,
           f"{API}/login/revoke", BY_ACTION),
    # Reads.
    Action(ActionName.WHOAMI, sessions.whoami, NoInput, sessions.Identity, VIEWER, GET,
           f"{API}/whoami"),
    Action(ActionName.HEALTH, lambda ctx, request: models.Health(), NoInput, models.Health, VIEWER,
           GET, f"{API}/health"),
    Action(ActionName.PIPELINES, inspect.list_pipelines, TagFilter, models.PipelineListView, VIEWER,
           GET, f"{API}/pipelines",
           scope=ActionScope.PIPELINE),
    Action(ActionName.PIPELINE, inspect.get_pipeline, PipelineInput, models.PipelineView, VIEWER,
           GET, f"{API}/pipelines/{{pipeline_code}}",
           scope=ActionScope.PIPELINE),
    Action(ActionName.GRAPH, inspect.pipeline_graph, PipelineInput, models.GraphView, VIEWER, GET,
           f"{API}/graph",
           scope=ActionScope.PIPELINE),
    Action(ActionName.STEPS, inspect.pipeline_steps, RunSelection, models.StepsView, VIEWER, GET,
           f"{API}/steps",
           scope=ActionScope.PIPELINE),
    Action(ActionName.STATUS, status.pipeline_status, RunFilter,
           status.StatusView | status.StatusListView, VIEWER, GET,
           f"{API}/status",
           scope=ActionScope.PIPELINE),
    Action(ActionName.RUN_DETAIL, inspect.get_run, RunId, models.RunView, VIEWER, GET,
           f"{API}/runs/{{run_id}}",
           scope=ActionScope.PIPELINE),
    Action(ActionName.CAPABILITIES, capabilities.control_capabilities, CapabilitiesInput,
           capabilities.CapabilitiesView, VIEWER, GET, f"{API}/capabilities",
           scope=ActionScope.PIPELINE),
    Action(ActionName.CONTROL_REQUEST, controls.get_control, ControlId, ControlView, VIEWER, GET,
           f"{API}/control-requests/{{control_id}}",
           scope=ActionScope.PIPELINE),
    Action(ActionName.EXPLAIN, status.explain_task, TaskSelection, status.Explanation, VIEWER, GET,
           f"{API}/explanations",
           scope=ActionScope.PIPELINE),
    Action(ActionName.HISTORY, inspect.run_history, HistoryInput, models.HistoryView, VIEWER, GET,
           f"{API}/history",
           scope=ActionScope.PIPELINE),
    Action(ActionName.AUDIT, inspect.audit, AuditInput, models.AuditView, VIEWER, GET,
           f"{API}/audit",
           scope=ActionScope.PIPELINE),
    Action(ActionName.ATTEMPT, inspect.get_attempt, AttemptId, models.AttemptView, VIEWER, GET,
           f"{API}/attempts/{{attempt_id}}",
           scope=ActionScope.PIPELINE),
    Action(ActionName.LOGS, inspect.attempt_log, LogInput, None, VIEWER, GET,
           f"{API}/attempts/{{attempt_id}}/log", streams=True,
           scope=ActionScope.PIPELINE),
    Action(ActionName.CATALOG_GAPS, catalog.get_gaps, catalog.GapsInput, GapSnapshot, VIEWER, GET,
           f"{API}/catalog/gaps",
           scope=ActionScope.PIPELINE),
    Action(ActionName.VALIDATE, diagnostics.validate_metadata, ValidateInput, Report, VIEWER, GET,
           f"{API}/validation",
           scope=ActionScope.PIPELINE),
    Action(ActionName.LINEAGE, diagnostics.trace_lineage, LineageInput, diagnostics.LineageView,
           VIEWER, GET, f"{API}/lineage",
           scope=ActionScope.PIPELINE),
    Action(ActionName.DATA_PROVENANCE, values.provenance, values.ValueKey, values.ValueResult,
           VIEWER, POST, f"{API}/values/provenance",
           scope=ActionScope.PIPELINE),
    Action(ActionName.DOCS_VERSION_CHECK, documents.check_versions, NoInput,
           documents.DocsVersions, VIEWER, GET, f"{API}/docs/versions",
           scope=ActionScope.PIPELINE),
    Action(ActionName.REHASH_PLAN, maintenance.plan_rehash, maintenance.RehashInput,
           RehashResult, VIEWER, GET, f"{API}/rehash"),
    # Runs and controls.
    Action(ActionName.TRIGGER_PIPELINE, triggers.initialize, triggers.TriggerRequest,
           triggers.TriggerView, OPERATOR, POST, f"{API}/triggers", REQUEST,
           scope=ActionScope.PIPELINE),
    Action(ActionName.RUN, runs.execute_run, RunRequest,
           models.OperationResult | models.BackfillView, OPERATOR, POST, f"{API}/runs", REQUEST,
           scope=ActionScope.PIPELINE),
    Action(ActionName.MARK, runs.mark, MarkRequest, models.OperationResult, OPERATOR, POST,
           f"{API}/marks", REQUEST,
           scope=ActionScope.PIPELINE),
    Action(ActionName.CONTROL, controls.control, ControlRequest,
           ControlView | models.OperationResult, OPERATOR, POST, f"{API}/controls", REQUEST,
           scope=ActionScope.PIPELINE),
    Action(ActionName.CONTROL_BULK, controls.request_bulk_controls, BulkControlRequest,
           controls.BulkControlView, OPERATOR, POST, f"{API}/controls/bulk",
           scope=ActionScope.PIPELINE),
    Action(ActionName.RECONCILE, runs.reconcile_runs, ReconcileInput, models.ReconciliationView,
           OPERATOR, POST, f"{API}/reconciliations", REQUEST,
           scope=ActionScope.PIPELINE),
    Action(ActionName.GROUP_LIST, groups.list_groups, NoInput, groups.Groups, ADMIN, GET,
           f"{API}/groups"),
    Action(ActionName.GROUP_ADD, groups.add_group, groups.GroupAdd, groups.Groups, ADMIN, POST,
           f"{API}/groups", REQUEST),
    Action(ActionName.GROUP_DISABLE, groups.disable_group, groups.GroupName, groups.Groups,
           ADMIN, POST, f"{API}/groups/disable", REQUEST),
    Action(ActionName.GROUP_ADD_MEMBER, groups.add_member, groups.GroupMember, groups.Groups,
           ADMIN, POST, f"{API}/groups/members", REQUEST),
    Action(ActionName.GROUP_REMOVE_MEMBER, groups.remove_member, groups.GroupMember, groups.Groups,
           ADMIN, POST, f"{API}/groups/members/remove", REQUEST),
    # Development and administration.
    Action(ActionName.LINEAGE_REFRESH, diagnostics.refresh_lineage, LineageRefreshInput,
           diagnostics.LineageView, DEVELOPER, POST, f"{API}/lineage/refresh", REQUEST,
           scope=ActionScope.PIPELINE),
    Action(ActionName.GENERATE_DOCS, documents.generate_docs, documents.DocsInput,
           documents.DocsBuild, DEVELOPER, POST, f"{API}/docs", REQUEST),
    Action(ActionName.DOCS_VERSION, documents.record_versions, NoInput, documents.DocsVersions,
           ADMIN, POST, f"{API}/docs/versions", REQUEST),
    Action(ActionName.DATA_EDIT, values.write_values, values.ValueEdit, values.ValueResult, ADMIN,
           POST, f"{API}/values", BY_ACTION,
           scope=ActionScope.PIPELINE),
    Action(ActionName.DATA_UNLOCK, values.unlock_values, values.ValueUnlock, values.ValueResult,
           ADMIN, POST, f"{API}/values/unlock", BY_ACTION,
           scope=ActionScope.PIPELINE),
    Action(ActionName.ACCOUNT_LIST, accounts.list_accounts, NoInput, accounts.Accounts, ADMIN, GET,
           f"{API}/accounts"),
    Action(ActionName.ACCOUNT_ADD, accounts.add_account, accounts.AccountAdd, accounts.Accounts,
           ADMIN, POST, f"{API}/accounts", REQUEST, provisioning=True),
    Action(ActionName.ACCOUNT_SET_ROLE, accounts.set_role, accounts.AccountRole,
           accounts.Accounts, ADMIN, POST, f"{API}/accounts/role", REQUEST),
    Action(ActionName.ACCOUNT_DISABLE, accounts.disable, accounts.AccountName, accounts.Accounts,
           ADMIN, POST, f"{API}/accounts/disable", REQUEST),
    Action(ActionName.ACCOUNT_RESET_PASSWORD, accounts.reset_password, accounts.AccountPassword,
           accounts.Accounts, ADMIN, POST, f"{API}/accounts/password", REQUEST),
    Action(ActionName.TOKEN_LIST, tokens.list_tokens, NoInput, tokens.TokensView, ADMIN, GET,
           f"{API}/tokens"),
    Action(ActionName.TOKEN_CREATE, tokens.create_token, tokens.TokenCreate, tokens.CreatedToken,
           ADMIN, POST, f"{API}/tokens", REQUEST),
    Action(ActionName.TOKEN_REVOKE, tokens.revoke_token, tokens.TokenRevoke, tokens.TokensView,
           ADMIN, POST, f"{API}/tokens/revoke", REQUEST),
    Action(ActionName.REHASH, maintenance.rehash_target, maintenance.RehashInput, RehashResult,
           ADMIN, POST, f"{API}/rehash", REQUEST),
    Action(ActionName.UPGRADE_TARGETS, maintenance.upgrade_audit_columns,
           maintenance.UpgradeInput, maintenance.TargetUpgrades, ADMIN, POST,
           f"{API}/targets/upgrade", REQUEST),
    Action(ActionName.CLONE, maintenance.clone_tables, NoInput, maintenance.Clones, ADMIN, POST,
           f"{API}/clones", REQUEST),
    # The controller's host only: local files, Engine DB setup and long-running processes.
    Action(ActionName.DOCTOR, diagnostics.check_configuration, NoInput, diagnostics.DoctorView,
           VIEWER, placement=HOST, remedy=ON_HOST, provisioning=True),
    Action(ActionName.CONFIG_PLAN, config_files.plan_config, config_files.ConfigDirectory,
           ConfigSync, VIEWER, placement=HOST, remedy=CONFIG_ON_HOST),
    Action(ActionName.CONFIG_EXPORT, config_files.export_config, config_files.ConfigExportInput,
           ConfigExport, VIEWER, placement=HOST, remedy=CONFIG_ON_HOST),
    Action(ActionName.CONFIG_APPLY, config_files.apply_config, config_files.ConfigDirectory,
           ConfigSync, DEVELOPER, audit=REQUEST, placement=HOST, remedy=CONFIG_ON_HOST),
    Action(ActionName.SETUP_GRANTS, maintenance.grants, NoInput, maintenance.Grants, VIEWER,
           placement=HOST, remedy=ON_HOST),
    Action(ActionName.SETUP, maintenance.setup_engine, NoInput, SetupResult, ADMIN, audit=REQUEST,
           placement=HOST, remedy=ON_HOST, provisioning=True),
    Action(ActionName.INIT_DB, maintenance.initialize, maintenance.InitInput, InitResult, ADMIN,
           audit=REQUEST, placement=HOST, remedy=ON_HOST, provisioning=True),
    Action(ActionName.MIGRATE, maintenance.migrate, maintenance.MigrateInput, list, DEVELOPER,
           audit=REQUEST, placement=HOST, remedy=MIGRATION_ON_HOST, provisioning=True,
           escalate=_migration_role),
    Action(ActionName.CONTROLLER, processes.controller, NoInput, None, ADMIN, audit=REQUEST,
           placement=HOST, remedy=STARTED_ON_HOST),
    Action(ActionName.UI, processes.ui, NoInput, None, ADMIN, audit=REQUEST, placement=HOST,
           remedy=STARTED_ON_HOST),
    Action(ActionName.PUBLISH_DOCS, None, documents.PublishInput, None, ADMIN, audit=REQUEST,
           placement=HOST, remedy=STARTED_ON_HOST),
)  # fmt: skip

ACTIONS: dict[ActionName, Action] = {row.name: row for row in _ROWS}
"""Every action, by name."""


def has_role(role: ApiRole | None, required: ApiRole | None) -> bool:
    """Whether ``role`` meets ``required``; an unproven role meets only no requirement."""
    if ApiRole.AGENT in {role, required}:
        return role == required
    return required is None or (
        role is not None and tuple(ApiRole).index(role) >= tuple(ApiRole).index(required)
    )


@contextmanager
def admitted(ctx: OperationContext, name: ActionName, request: object) -> Iterator[None]:
    """Check the role and record the request once, then run the action's work inside.

    A refusal is recorded as one ``REFUSED`` row. Local provisioning skips only the role check
    of the actions the table marks as provisioning.
    """
    action = ACTIONS[name]
    if not isinstance(request, action.request):
        raise UsageError(
            f"{name}: request is {type(request).__name__}; expected {action.request.__name__}"
        )
    given = arguments(request)
    required = action.required_role(ctx, request)
    try:
        _authorize(ctx, action, request, required)
    except PermissionDeniedError as error:
        record_refusal(ctx, name, given, error)
        raise
    if action.audit == Audit.REQUEST:
        with recorded(ctx, name, given):
            yield
    else:
        with acting_as(ctx.actor):
            yield


def _authorize(
    ctx: OperationContext, action: Action, request: object, required: ApiRole | None
) -> None:
    if ApiRole.AGENT in {ctx.role, required}:
        _require_role(ctx, action, required)
        return
    _require_trigger_groups(ctx, action, request)
    if ctx.role == ApiRole.ADMIN or (action.provisioning and ctx.provisioning):
        return
    if action.scope == ActionScope.PIPELINE and required is not None:
        with ctx.engine.connect() as conn:
            access = pipeline_access(conn, ctx)
            if isinstance(request, BulkControlRequest):
                if has_role(ctx.role, required) or any(
                    access.permits(selection.pipeline_code, required)
                    for selection in request.requests
                ):
                    return
                access.require(ctx, request.requests[0].pipeline_code, required)
            codes = _request_codes(conn, action, request)
            existing = [selected for selected in codes if selected in access.roles]
            for selected in existing:
                access.require(ctx, selected, required)
            if existing:
                return
            if action.name in (ActionName.RECONCILE, ActionName.LINEAGE_REFRESH):
                for selected in access.roles:
                    access.require(ctx, selected, required)
                if access.roles:
                    return
            # Aggregate reads filter their selections; bulk controls invoke CONTROL per object.
            if not codes and required == ApiRole.VIEWER and ctx.role is not None:
                return
    _require_role(ctx, action, required)


def _require_role(ctx: OperationContext, action: Action, required: ApiRole | None) -> None:
    """Refuse both insufficient account roles and a different credential protocol."""
    if not has_role(ctx.role, required):
        raise PermissionDeniedError(
            f"account {ctx.actor.name!r}: role={ctx.role or 'unproven'}; action '{action.name}': "
            f"{required} role is required; ask an administrator to grant {required} or a higher "
            "role"
        )


def _require_trigger_groups(ctx: OperationContext, action: Action, request: object) -> None:
    """Check retained groups even when the initiating account is currently an admin."""
    if isinstance(request, triggers.TriggerRequest) and ctx.sso_group_ids:
        with ctx.engine.connect() as conn:
            for selected in _request_codes(conn, action, request):
                require_child_groups(conn, ctx, selected)


def invoke(ctx: OperationContext, name: ActionName, request: object) -> object:
    """Run one action for ``ctx``'s caller: the only way an interface reaches an operation."""
    call = ACTIONS[name].call
    if call is None:
        raise UsageError(f"{name} is a long-running process; its interface starts it")
    ctx = _request_context(ctx, name, request)
    claims = sso_groups.set(ctx.sso_group_ids)
    try:
        with admitted(ctx, name, request):
            result = call(ctx, request)
        return (
            visible_result(ctx, result) if ACTIONS[name].scope == ActionScope.PIPELINE else result
        )
    finally:
        sso_groups.reset(claims)


def _request_context(ctx: OperationContext, name: ActionName, request: object) -> OperationContext:
    """Restore admitted claims for trigger tasks, and authenticated claims for other callers."""
    if ACTIONS[name].scope == ActionScope.PIPELINE and ctx.credential_token_id is not None:
        with ctx.engine.connect() as conn:
            ctx = replace(ctx, sso_group_ids=token_groups(conn, ctx.credential_token_id))
    if name == ActionName.TRIGGER_PIPELINE and isinstance(request, triggers.TriggerRequest):
        from etl_craft.services.operations.child_admission import initiating_context

        with ctx.engine.connect() as conn:
            row = conn.execute(
                statement(conn, "trigger_context"), {"task_run_id": request.task_run_id}
            ).one_or_none()
            if row is not None:
                initiator = initiating_context(conn, ctx, row.pipeline_run_id)
                if ctx.actor == initiator.actor:
                    ctx = initiator
    return ctx


def _request_codes(conn: Connection, action: Action, request: object) -> list[str]:
    """Resolve exact identities, and the requested child rather than an implicit parent run."""
    if isinstance(request, triggers.TriggerRequest):
        row = conn.execute(
            statement(conn, "trigger_context"), {"task_run_id": request.task_run_id}
        ).one_or_none()
        if row is None:
            return []
        child = trigger_definition(conn, row.pipeline_id, fetch_task_parameters(conn, row.task_id))
        return [child.pipeline_code]
    code = getattr(request, "pipeline_code", None)
    if code is not None:
        return [code]
    identities = {
        key: getattr(request, key, None)
        for key in ("run_id", "attempt_id", "control_id", "task_run_id")
    }
    if any(value is not None for value in identities.values()):
        return list(conn.execute(statement(conn, "permission_scope"), identities).scalars())
    return []
