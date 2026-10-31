"""One action table for calls, authorization, audit, placement and generated routes."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace

from sqlalchemy.engine import Connection

from etl_craft.core.actor import acting_as, sso_groups
from etl_craft.core.definitions import using
from etl_craft.core.enums import (
    ActionName,
    ActionScope,
    ApiRole,
    Audit,
    HttpMethod,
    Placement,
    Transport,
    YesNo,
)
from etl_craft.core.enums_access import Permission
from etl_craft.core.errors import (
    ConfigurationError,
    PermissionDeniedError,
    ResourceNotFoundError,
    UsageError,
)
from etl_craft.engine.controls import ControlView
from etl_craft.engine.migrations import pending_migrations
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.group_claims import token_groups
from etl_craft.engine.repository.pipelines import selected_project
from etl_craft.engine.repository.tasks import fetch_admitted_parameters
from etl_craft.engine.schema import InitResult
from etl_craft.services.catalog.gaps import GapSnapshot
from etl_craft.services.config_files import ConfigExport, ConfigSync
from etl_craft.services.insights import Insights
from etl_craft.services.operations import (
    about,
    access,
    accounts,
    admin_metadata,
    agent_claims,
    agent_doctor,
    agent_logs,
    agent_models,
    agent_reports,
    agents,
    announcements,
    bundles,
    capabilities,
    catalog,
    catalog_pages,
    config_files,
    controls,
    diagnostics,
    documents,
    groups,
    insights,
    inspect,
    items,
    maintenance,
    models,
    offsets,
    personal,
    pipeline_refresh,
    processes,
    project_health,
    renames,
    retention,
    run_log,
    run_zip,
    runs,
    search,
    sessions,
    status,
    tokens,
    triggers,
    values,
)
from etl_craft.services.operations.action_types import Action as Action
from etl_craft.services.operations.agent_uploads import attempt_request
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.permissions import (
    pipeline_access,
    project_not_found,
    require_child_groups,
    require_project,
)
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
    RunFilter,
    RunId,
    RunRequest,
    RunSelection,
    TagFilter,
    TaskSelection,
    ValidateInput,
    arguments,
)
from etl_craft.services.operations.upload_actions import UPLOAD_ACTIONS
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


def action_path(action: Action) -> str:
    """Return the public route, adding the selected project to project-owned actions."""
    assert action.path is not None
    if action.scope in (ActionScope.PROJECT, ActionScope.PIPELINE):
        return action.path.replace(API, f"{API}/projects/{{project}}", 1)
    return action.path


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
    Action(ActionName.AGENT_DOCTOR, agent_doctor.inspect, agent_doctor.AgentDoctor,
           agent_doctor.AgentImports, ApiRole.AGENT, POST, f"{API}/agent/doctor", REQUEST,
           scope=ActionScope.PROJECT),
    Action(ActionName.BUNDLE_PUBLISH, bundles.publish, bundles.PublishBundle, bundles.BundleView,
           DEVELOPER, POST, f"{API}/bundles", REQUEST, scope=ActionScope.PROJECT,
           escalate=bundles.required_role),
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
    *UPLOAD_ACTIONS,
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
    Action(ActionName.WHOAMI, sessions.whoami, NoInput, sessions.Identity, VIEWER, GET,
           f"{API}/whoami"),
    Action(ActionName.ABOUT, about.read_about, NoInput, about.About, VIEWER, GET, f"{API}/about"),
    Action(ActionName.CATALOG_PAGE, catalog_pages.read_catalog, catalog_pages.CatalogInput,
           None, VIEWER, GET, f"{API}/catalog/page", scope=ActionScope.PROJECT, streams=True),
    Action(ActionName.PROJECT_HEALTH, project_health.read_health, project_health.HealthInput,
           project_health.ProjectHealth, VIEWER, GET, f"{API}/project-health"),
    Action(ActionName.ACCOUNT_SETTINGS, personal.save_settings, personal.SettingsPatch,
           personal.Settings, VIEWER, POST, f"{API}/account/settings", REQUEST),
    Action(ActionName.PASSWORD_CHANGE, personal.change_password, personal.ChangePassword,
           personal.PasswordChanged, VIEWER, POST, f"{API}/account/password", REQUEST),
    Action(ActionName.ACCOUNT_UNLOCK, accounts.unlock, accounts.AccountName,
           accounts.Accounts, ADMIN, POST, f"{API}/accounts/unlock", REQUEST),
    Action(ActionName.HEALTH, lambda ctx, request: models.Health(), NoInput, models.Health, VIEWER,
           GET, f"{API}/health"),
    Action(ActionName.RENAME, renames.rename, renames.RenameInput, renames.RenameResult,
           DEVELOPER, POST, f"{API}/renames", REQUEST, scope=ActionScope.PROJECT),
    Action(ActionName.PIPELINE_REFRESH, pipeline_refresh.refresh, pipeline_refresh.RefreshInput,
           pipeline_refresh.RefreshResults, DEVELOPER, POST, f"{API}/pipelines/refresh",
           audit=REQUEST, scope=ActionScope.PIPELINE),
    Action(ActionName.PIPELINE_REFRESH_HISTORY, pipeline_refresh.history,
           pipeline_refresh.RefreshInput, pipeline_refresh.RefreshResults, VIEWER, GET,
           f"{API}/pipeline-refreshes", scope=ActionScope.PIPELINE),
    Action(ActionName.PIPELINES, inspect.list_pipelines, TagFilter, models.PipelineListView, VIEWER,
           GET, f"{API}/pipelines",
           scope=ActionScope.PIPELINE),
    Action(ActionName.OFFSET, offsets.read_offset, offsets.TaskInput, offsets.TaskOffset, VIEWER,
           GET, f"{API}/offset", scope=ActionScope.PIPELINE),
    Action(ActionName.SEARCH, search.search, search.SearchInput, search.SearchView, VIEWER, GET,
           f"{API}/search", scope=ActionScope.PIPELINE),
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
    Action(ActionName.RUN_LOG, run_log.run_log, RunSelection, None, VIEWER, GET,
           f"{API}/run-log", streams=True, scope=ActionScope.PIPELINE),
    Action(ActionName.RUN_ZIP, run_zip.run_zip, RunSelection, None, VIEWER, GET,
           f"{API}/run-zip", streams=True, scope=ActionScope.PIPELINE),
    Action(ActionName.INSIGHTS, insights.insights, insights.InsightsInput, Insights, VIEWER, GET,
           f"{API}/insights", scope=ActionScope.PIPELINE),
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
           RehashResult, VIEWER, GET, f"{API}/rehash", scope=ActionScope.PROJECT),
    Action(ActionName.TRIGGER_PIPELINE, triggers.initialize, triggers.TriggerRequest,
           triggers.TriggerView, OPERATOR, POST, f"{API}/triggers", REQUEST,
           scope=ActionScope.PIPELINE),
    Action(ActionName.RUN, runs.execute_run, RunRequest,
           models.OperationResult | models.BackfillView, OPERATOR, POST, f"{API}/runs", REQUEST,
           scope=ActionScope.PIPELINE),
    Action(ActionName.ITEMS, items.inspect, items.ItemInput, items.ItemView, VIEWER,
           GET, f"{API}/pipelines/{{pipeline_code}}/runs/{{run_id}}/tasks/{{task_code}}/items",
           scope=ActionScope.PIPELINE),
    Action(ActionName.ITEM_CONTROL, items.control, items.ItemControl, items.ItemView, OPERATOR,
           POST, f"{API}/pipelines/{{pipeline_code}}/runs/{{run_id}}/tasks/{{task_code}}/items"
           "/controls",
           REQUEST, scope=ActionScope.PIPELINE),
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
    Action(ActionName.PROJECT_ADD, admin_metadata.add_project, admin_metadata.ProjectFields,
           admin_metadata.Projects,
           ADMIN, POST, f"{API}/projects", REQUEST),
    Action(ActionName.PROJECT_SET, admin_metadata.set_project, admin_metadata.ProjectFields,
           admin_metadata.Projects,
           ADMIN, POST, f"{API}/projects/set", REQUEST),
    Action(ActionName.PROJECT_LIST, admin_metadata.list_projects, admin_metadata.ProjectIdentity,
           admin_metadata.Projects,
           VIEWER, GET, f"{API}/projects", REQUEST),
    Action(ActionName.PROJECT_DISABLE, admin_metadata.disable_project,
           admin_metadata.ProjectIdentity,
           admin_metadata.Projects,
           ADMIN, POST, f"{API}/projects/disable", REQUEST),
    Action(ActionName.POOL_ADD, admin_metadata.add_pool, admin_metadata.PoolFields,
           admin_metadata.Pools,
           ADMIN, POST, f"{API}/pools", REQUEST),
    Action(ActionName.POOL_SET, admin_metadata.set_pool, admin_metadata.PoolFields,
           admin_metadata.Pools,
           ADMIN, POST, f"{API}/pools/set", REQUEST),
    Action(ActionName.POOL_LIST, admin_metadata.list_pools, admin_metadata.PoolIdentity,
           admin_metadata.Pools,
           ADMIN, GET, f"{API}/pools", REQUEST),
    Action(ActionName.POOL_DISABLE, admin_metadata.disable_pool, admin_metadata.PoolIdentity,
           admin_metadata.Pools,
           ADMIN, POST, f"{API}/pools/disable", REQUEST),
    Action(ActionName.ACCESS_GRANT, access.grant_access, access.AccessWrite, access.AccessView,
           VIEWER, POST, f"{API}/access/grants", BY_ACTION, escalate=access.required_role),
    Action(ActionName.ACCESS_REVOKE, access.revoke_access, access.AccessWrite, access.AccessView,
           VIEWER, POST, f"{API}/access/revocations", BY_ACTION),
    Action(ActionName.ACCESS_LIST, access.list_access, access.AccessFilter, access.AccessView,
           VIEWER, GET, f"{API}/access/grants", REQUEST),
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
    Action(ActionName.LINEAGE_REFRESH, diagnostics.refresh_lineage, LineageRefreshInput,
           diagnostics.LineageView, DEVELOPER, POST, f"{API}/lineage/refresh", REQUEST,
           scope=ActionScope.PIPELINE),
    Action(ActionName.GENERATE_DOCS, documents.generate_docs, documents.DocsInput,
           documents.DocsBuild, DEVELOPER, POST, f"{API}/docs", REQUEST, scope=ActionScope.PROJECT),
    Action(ActionName.DOCS_VERSION, documents.record_versions, NoInput, documents.DocsVersions,
           ADMIN, POST, f"{API}/docs/versions", REQUEST, scope=ActionScope.PROJECT),
    Action(ActionName.DATA_EDIT, values.write_values, values.ValueEdit, values.ValueResult, ADMIN,
           POST, f"{API}/values", BY_ACTION,
           scope=ActionScope.PIPELINE),
    Action(ActionName.DATA_UNLOCK, values.unlock_values, values.ValueUnlock, values.ValueResult,
           ADMIN, POST, f"{API}/values/unlock", BY_ACTION,
           scope=ActionScope.PIPELINE),
    Action(ActionName.ANNOUNCE_LIST, announcements.read, announcements.AnnouncementFilter,
           announcements.Announcements, VIEWER, GET, f"{API}/announcements"),
    Action(ActionName.ANNOUNCE_ADD, announcements.add_announcement, announcements.AnnouncementAdd,
           announcements.Announcements, ADMIN, POST, f"{API}/announcements", REQUEST),
    Action(ActionName.ANNOUNCE_END, announcements.end_announcement, announcements.AnnouncementEnd,
           announcements.Announcements, ADMIN, POST, f"{API}/announcements/end", REQUEST),
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
           ADMIN, POST, f"{API}/rehash", REQUEST, scope=ActionScope.PROJECT),
    Action(ActionName.PURGE, retention.purge, retention.PurgeInput, retention.PurgeView,
           ADMIN, POST, f"{API}/purge", REQUEST, scope=ActionScope.PROJECT),
    Action(ActionName.UPGRADE_TARGETS, maintenance.upgrade_audit_columns,
           maintenance.UpgradeInput, maintenance.TargetUpgrades, ADMIN, POST,
           f"{API}/targets/upgrade", REQUEST, scope=ActionScope.PROJECT),
    Action(ActionName.CLONE, maintenance.clone_tables, NoInput, maintenance.Clones, ADMIN, POST,
           f"{API}/clones", REQUEST, scope=ActionScope.PROJECT),
    # The controller's host only: local files, Engine DB setup and long-running processes.
    Action(ActionName.DOCTOR, diagnostics.check_configuration, NoInput, diagnostics.DoctorView,
           VIEWER, placement=HOST, remedy=ON_HOST, provisioning=True),
    Action(ActionName.CONFIG_PLAN, config_files.plan_config, config_files.ConfigDirectory,
           ConfigSync, VIEWER, placement=HOST, remedy=CONFIG_ON_HOST,
           scope=ActionScope.PROJECT),
    Action(ActionName.CONFIG_EXPORT, config_files.export_config, config_files.ConfigExportInput,
           ConfigExport, VIEWER, placement=HOST, remedy=CONFIG_ON_HOST,
           scope=ActionScope.PROJECT),
    Action(ActionName.CONFIG_APPLY, config_files.apply_config, config_files.ConfigDirectory,
           ConfigSync, DEVELOPER, audit=REQUEST, placement=HOST, remedy=CONFIG_ON_HOST,
           scope=ActionScope.PROJECT),
    Action(ActionName.SETUP_GRANTS, maintenance.grants, NoInput, maintenance.Grants, VIEWER,
           placement=HOST, remedy=ON_HOST),
    Action(ActionName.SETUP, maintenance.setup_engine, NoInput, SetupResult, ADMIN, audit=REQUEST,
           placement=HOST, remedy=ON_HOST, provisioning=True),
    Action(ActionName.INIT_DB, maintenance.initialize, maintenance.InitInput, InitResult, ADMIN,
           audit=REQUEST, placement=HOST, remedy=ON_HOST, provisioning=True),
    Action(ActionName.MIGRATE, maintenance.migrate, maintenance.MigrateInput, list, DEVELOPER,
           audit=REQUEST, placement=HOST, remedy=MIGRATION_ON_HOST, provisioning=True,
           escalate=_migration_role, scope=ActionScope.PROJECT),
    Action(ActionName.CONTROLLER, processes.controller, NoInput, None, ADMIN, audit=REQUEST,
           placement=HOST, remedy=STARTED_ON_HOST),
    Action(ActionName.UI, processes.ui, NoInput, None, ADMIN, audit=REQUEST, placement=HOST,
           remedy=STARTED_ON_HOST),
    Action(ActionName.PUBLISH_DOCS, None, documents.PublishInput, None, ADMIN, audit=REQUEST,
           placement=HOST, remedy=STARTED_ON_HOST, scope=ActionScope.PROJECT),
)  # fmt: skip

PROJECT_MANAGE_ACTIONS = frozenset(
    {
        ActionName.RENAME,
        ActionName.CONFIG_PLAN,
        ActionName.PIPELINE_REFRESH,
        ActionName.CONFIG_APPLY,
        ActionName.MIGRATE,
        ActionName.BUNDLE_PUBLISH,
        ActionName.GENERATE_DOCS,
    }
)

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
    ctx = _project_context(ctx, action, request)
    if not isinstance(request, action.request):
        raise UsageError(
            f"{name}: request is {type(request).__name__}; expected {action.request.__name__}"
        )
    with selected_project(ctx.project_id):
        given = arguments(request)
        required = action.required_role(ctx, request)
        try:
            _authorize(ctx, action, request, required)
        except (PermissionDeniedError, ResourceNotFoundError) as error:
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
    personal.require_password_change(ctx, action.name)
    if ApiRole.AGENT in {ctx.role, required}:
        _require_role(ctx, action, required)
        return
    _require_trigger_groups(ctx, action, request)
    _require_project_identity(ctx, action, request)
    if ctx.role == ApiRole.ADMIN or (action.provisioning and ctx.provisioning):
        return
    scoped = action.scope != ActionScope.DEPLOYMENT or isinstance(request, access.AccessFilter)
    bulk = isinstance(request, access.AccessWrite) and request.content is not None
    if scoped and not bulk:
        with ctx.engine.connect() as conn:
            needed = (
                Permission.CAN_MANAGE
                if action.name in PROJECT_MANAGE_ACTIONS
                else Permission.CAN_VIEW
            )
            require_project(conn, ctx, needed)
        if action.name in PROJECT_MANAGE_ACTIONS and required != ApiRole.ADMIN:
            return
    if _authorize_pipeline(ctx, action, request, required):
        return
    _require_role(ctx, action, required)


def _authorize_pipeline(
    ctx: OperationContext, action: Action, request: object, required: ApiRole | None
) -> bool:
    """Apply pipeline grants without granting deployment-wide privileges."""
    if action.scope == ActionScope.PIPELINE and required is not None:
        if action.name == ActionName.LINEAGE_REFRESH:
            _require_role(ctx, action, required)
            required = ApiRole.VIEWER
        with ctx.engine.connect() as conn:
            policy = pipeline_access(conn, ctx)
            if isinstance(request, BulkControlRequest):
                if has_role(ctx.role, required) or any(
                    policy.permits(selection.pipeline_code, required)
                    for selection in request.requests
                ):
                    return True
                policy.require(ctx, request.requests[0].pipeline_code, required)
            codes = _request_codes(conn, action, request)
            existing = codes
            for selected in existing:
                policy.require(ctx, selected, required)
            if existing:
                return True
            if action.name in (ActionName.RECONCILE, ActionName.LINEAGE_REFRESH):
                if any(not policy.permits(code, required) for code in policy.roles):
                    raise PermissionDeniedError(
                        f"account {ctx.actor.name!r}: {action.name} requires {required} role "
                        f"on every pipeline in project {ctx.project!r}; ask its project manager "
                        "for the required permission or select an accessible pipeline"
                    )
                if policy.roles:
                    return True
            # Aggregate reads filter their selections; bulk controls invoke CONTROL per object.
            if not codes and required == ApiRole.VIEWER and ctx.role is not None:
                return True
    return False


def _require_project_identity(ctx: OperationContext, action: Action, request: object) -> None:
    """Require an exact run or attempt id to belong to the selected project."""
    if (
        action.scope == ActionScope.PIPELINE
        and any(
            getattr(request, key, None) is not None
            for key in ("run_id", "attempt_id", "control_id", "task_run_id")
        )
        and getattr(request, "pipeline_code", None) is None
    ):
        with ctx.engine.connect() as conn:
            if not _request_codes(conn, action, request):
                raise ResourceNotFoundError(
                    f"project {ctx.project!r}: requested execution identity is outside this "
                    "project or missing; expected an identity in the selected project; "
                    "select the owning project and its exact identity"
                )


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
    if ctx.credential_attempt_id is not None and not attempt_request(ctx, name, request):
        error = PermissionDeniedError(
            f"attempt {ctx.credential_attempt_id}: credential cannot perform {name}; "
            "use it only to renew, report or log its own live attempt; "
            "use an agent token for claims"
        )
        assert isinstance(request, ACTIONS[name].request)
        record_refusal(ctx, name, arguments(request), error)
        raise error
    ctx = _request_context(ctx, name, request)
    try:
        ctx = _project_context(ctx, ACTIONS[name], request)
    except (PermissionDeniedError, ResourceNotFoundError) as error:
        assert isinstance(request, ACTIONS[name].request)
        record_refusal(ctx, name, arguments(request), error)
        raise
    claims = sso_groups.set(ctx.sso_group_ids)
    try:
        with using(None), selected_project(ctx.project_id), admitted(ctx, name, request):
            from etl_craft.core.governance import using_policy
            from etl_craft.services.operations.governance import project_policy

            if name == ActionName.MIGRATE:
                result = call(project_settings(ctx, name, request), request)
            else:
                with using_policy(project_policy(ctx, request)):
                    result = call(project_settings(ctx, name, request), request)
            return (
                visible_result(ctx, result)
                if ACTIONS[name].scope == ActionScope.PIPELINE
                else result
            )
    finally:
        sso_groups.reset(claims)


def project_settings(ctx: OperationContext, name: ActionName, request: object) -> OperationContext:
    """Use approved project connections for operations that inspect warehouse resources."""
    if ctx.transport == Transport.LOCAL and (
        not ctx.config.artifacts.store or name in (ActionName.PIPELINE_REFRESH, ActionName.RENAME)
    ):
        return ctx
    if (
        name == ActionName.PURGE
        and isinstance(request, retention.PurgeInput)
        and request.archive_to is None
    ):
        with ctx.engine.connect() as conn:
            if conn.execute(statement(conn, "retention_warehouse_targets")).first() is None:
                return ctx
    if name not in {
        ActionName.PURGE,
        ActionName.UPLOAD,
        ActionName.RUN,
        ActionName.VALIDATE,
        ActionName.PIPELINE_REFRESH,
        ActionName.RENAME,
        ActionName.LINEAGE,
        ActionName.LINEAGE_REFRESH,
        ActionName.CATALOG_GAPS,
        ActionName.REHASH,
        ActionName.REHASH_PLAN,
        ActionName.UPGRADE_TARGETS,
        ActionName.CLONE,
        ActionName.GENERATE_DOCS,
        ActionName.DATA_EDIT,
        ActionName.DATA_UNLOCK,
        ActionName.DATA_PROVENANCE,
    }:
        return ctx
    from etl_craft.config.project import project_config
    from etl_craft.engine.bundles import Bundle

    with ctx.engine.connect() as conn:
        identity = conn.execute(statement(conn, "bundle_latest")).scalar_one_or_none()
        bundle = None if identity is None else Bundle.load(conn, identity)
    if bundle is None:
        if ctx.project != ctx.config.project:
            raise ConfigurationError(
                f"project {ctx.project!r}: no approved bundle; expected its own connection "
                "definitions; publish this project as an admin before using warehouse operations"
            )
        return ctx
    root = bundle.materialize(ctx.config)
    pinned = project_config(ctx.config, root / "craft-connector.yml")
    return replace(ctx, config=replace(pinned, project=ctx.project, timezone=ctx.config.timezone))


def _access_project(ctx: OperationContext, request: object) -> OperationContext:
    """Access uses its body/query project because its established route is deployment-scoped."""
    if (
        isinstance(request, access.AccessFilter)
        and request.project is not None
        and ctx.project != request.project
    ):
        return replace(ctx, project=request.project, project_id=None)
    return ctx


def _legacy_project(conn: Connection, ctx: OperationContext, action: Action) -> bool:
    """Only the explicitly selected legacy project can bootstrap project metadata."""
    from etl_craft.dialects.engine import for_engine

    if action.name != ActionName.MIGRATE or for_engine(conn).has_table(conn, "CFG_PROJECTS"):
        return False
    if ctx.project == "default":
        return True
    raise UsageError(
        "project metadata is absent; migrate --project default before selecting another project"
    )


def _project_context(ctx: OperationContext, action: Action, request: object) -> OperationContext:
    """Resolve one active project before a project-owned operation reads any object."""
    ctx = _access_project(ctx, request)
    if action.scope == ActionScope.DEPLOYMENT and not isinstance(request, access.AccessFilter):
        return ctx
    if isinstance(request, access.AccessWrite) and request.content is not None:
        return ctx
    if not ctx.project:
        ctx = replace(ctx, project=ctx.config.project)
    if not ctx.project:
        raise UsageError(
            f"{ctx.config.config_path or 'craft-connector.yml'}: missing Orchestration.Project; "
            "supply --project P or set Orchestration.Project in this file"
        )
    if ctx.project_id is not None:
        return ctx
    if ctx.transport == Transport.LOCAL:
        ctx = replace(ctx, config=replace(ctx.config, project=ctx.project))
    with ctx.engine.connect() as conn:
        if _legacy_project(conn, ctx, action):
            return ctx
        row = conn.execute(statement(conn, "project_by_code"), {"code": ctx.project}).one_or_none()
        if (
            row is not None
            and ctx.role not in (ApiRole.ADMIN, ApiRole.AGENT)
            and not (action.provisioning and ctx.provisioning)
        ):
            require_project(conn, replace(ctx, project_id=int(row.project_id)))
    if row is None or (
        row.active_flag == YesNo.NO
        and action.name
        not in {
            ActionName.CONFIG_PLAN,
            ActionName.CONFIG_APPLY,
            ActionName.CONFIG_EXPORT,
            ActionName.PURGE,
        }
    ):
        if ctx.role not in (ApiRole.ADMIN, ApiRole.AGENT):
            raise project_not_found(ctx.project)
        if action.name in (ActionName.CONFIG_PLAN, ActionName.CONFIG_APPLY):
            return ctx
        raise UsageError(
            f"project={ctx.project!r}; expected an active CFG_PROJECTS row; add it to "
            "`etl-craft project add --project " + ctx.project + " --reason REASON`"
        )
    return replace(
        ctx,
        project_id=int(row.project_id),
        project=str(row.project_code),
        config=replace(ctx.config, timezone=str(row.timezone)),
    )


def _request_context(ctx: OperationContext, name: ActionName, request: object) -> OperationContext:
    """Restore admitted claims for trigger tasks, and authenticated claims for other callers."""
    if ctx.credential_token_id is not None:
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
        child = trigger_definition(
            conn, row.pipeline_id, fetch_admitted_parameters(conn, row.task_id, row.pipeline_run_id)
        )
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
