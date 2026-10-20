"""One row per action: what it calls, who may, whether it is audited, where it runs, its route.

Every interface reaches an operation through ``invoke``, which checks the role, records the
request once and calls the operation. The CLI, the HTTP routes, the reference page and the
controller client read everything else about an action from this table.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass

from etl_craft.core.actor import acting_as
from etl_craft.core.enums import ActionName, ApiRole, Audit, HttpMethod, Placement
from etl_craft.core.errors import PermissionDeniedError, UsageError
from etl_craft.engine.controls import ControlView
from etl_craft.engine.migrations import pending_migrations
from etl_craft.engine.schema import InitResult
from etl_craft.services.catalog.gaps import GapSnapshot
from etl_craft.services.config_files import ConfigExport, ConfigSync
from etl_craft.services.operations import (
    accounts,
    capabilities,
    catalog,
    config_files,
    controls,
    diagnostics,
    documents,
    inspect,
    maintenance,
    models,
    runs,
    sessions,
    status,
    tokens,
    values,
)
from etl_craft.services.operations.context import OperationContext
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
from etl_craft.services.rehash import RehashResult
from etl_craft.services.setup import SetupResult
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

    ``call`` is ``None`` for a long-running process its interface starts itself inside
    ``admitted``. ``result`` is the document type, or ``None`` for no content; ``streams`` marks
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
           GET, f"{API}/pipelines"),
    Action(ActionName.PIPELINE, inspect.get_pipeline, PipelineInput, models.PipelineView, VIEWER,
           GET, f"{API}/pipelines/{{pipeline_code}}"),
    Action(ActionName.GRAPH, inspect.pipeline_graph, PipelineInput, models.GraphView, VIEWER, GET,
           f"{API}/graph"),
    Action(ActionName.STEPS, inspect.pipeline_steps, RunSelection, models.StepsView, VIEWER, GET,
           f"{API}/steps"),
    Action(ActionName.STATUS, status.pipeline_status, RunFilter,
           status.StatusView | status.StatusListView, VIEWER, GET,
           f"{API}/status"),
    Action(ActionName.RUN_DETAIL, inspect.get_run, RunId, models.RunView, VIEWER, GET,
           f"{API}/runs/{{run_id}}"),
    Action(ActionName.CAPABILITIES, capabilities.control_capabilities, CapabilitiesInput,
           capabilities.CapabilitiesView, VIEWER, GET, f"{API}/capabilities"),
    Action(ActionName.CONTROL_REQUEST, controls.get_control, ControlId, ControlView, VIEWER, GET,
           f"{API}/control-requests/{{control_id}}"),
    Action(ActionName.EXPLAIN, status.explain_task, TaskSelection, status.Explanation, VIEWER, GET,
           f"{API}/explanations"),
    Action(ActionName.HISTORY, inspect.run_history, HistoryInput, models.HistoryView, VIEWER, GET,
           f"{API}/history"),
    Action(ActionName.AUDIT, inspect.audit, AuditInput, models.AuditView, VIEWER, GET,
           f"{API}/audit"),
    Action(ActionName.ATTEMPT, inspect.get_attempt, AttemptId, models.AttemptView, VIEWER, GET,
           f"{API}/attempts/{{attempt_id}}"),
    Action(ActionName.LOGS, inspect.attempt_log, LogInput, None, VIEWER, GET,
           f"{API}/attempts/{{attempt_id}}/log", streams=True),
    Action(ActionName.CATALOG_GAPS, catalog.get_gaps, catalog.GapsInput, GapSnapshot, VIEWER, GET,
           f"{API}/catalog/gaps"),
    Action(ActionName.VALIDATE, diagnostics.validate_metadata, ValidateInput, Report, VIEWER, GET,
           f"{API}/validation"),
    Action(ActionName.LINEAGE, diagnostics.trace_lineage, LineageInput, diagnostics.LineageView,
           VIEWER, GET, f"{API}/lineage"),
    Action(ActionName.DATA_PROVENANCE, values.provenance, values.ValueKey, values.ValueResult,
           VIEWER, POST, f"{API}/values/provenance"),
    Action(ActionName.DOCS_VERSION_CHECK, documents.check_versions, NoInput,
           documents.DocsVersions, VIEWER, GET, f"{API}/docs/versions"),
    Action(ActionName.REHASH_PLAN, maintenance.plan_rehash, maintenance.RehashInput,
           RehashResult, VIEWER, GET, f"{API}/rehash"),
    # Runs and controls.
    Action(ActionName.RUN, runs.execute_run, RunRequest,
           models.OperationResult | models.BackfillView, OPERATOR, POST, f"{API}/runs", REQUEST),
    Action(ActionName.MARK, runs.mark, MarkRequest, models.OperationResult, OPERATOR, POST,
           f"{API}/marks", REQUEST),
    Action(ActionName.CONTROL, controls.control, ControlRequest,
           ControlView | models.OperationResult, OPERATOR, POST, f"{API}/controls", REQUEST),
    Action(ActionName.CONTROL_BULK, controls.request_bulk_controls, BulkControlRequest,
           controls.BulkControlView, OPERATOR, POST, f"{API}/controls/bulk"),
    Action(ActionName.RECONCILE, runs.reconcile_runs, ReconcileInput, models.ReconciliationView,
           OPERATOR, POST, f"{API}/reconciliations", REQUEST),
    # Development and administration.
    Action(ActionName.LINEAGE_REFRESH, diagnostics.refresh_lineage, LineageRefreshInput,
           diagnostics.LineageView, DEVELOPER, POST, f"{API}/lineage/refresh", REQUEST),
    Action(ActionName.GENERATE_DOCS, documents.generate_docs, documents.DocsInput,
           documents.DocsBuild, DEVELOPER, POST, f"{API}/docs", REQUEST),
    Action(ActionName.DOCS_VERSION, documents.record_versions, NoInput, documents.DocsVersions,
           ADMIN, POST, f"{API}/docs/versions", REQUEST),
    Action(ActionName.DATA_EDIT, values.write_values, values.ValueEdit, values.ValueResult, ADMIN,
           POST, f"{API}/values", BY_ACTION),
    Action(ActionName.DATA_UNLOCK, values.unlock_values, values.ValueUnlock, values.ValueResult,
           ADMIN, POST, f"{API}/values/unlock", BY_ACTION),
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
    Action(ActionName.CONTROLLER, None, NoInput, None, ADMIN, audit=REQUEST, placement=HOST,
           remedy=STARTED_ON_HOST),
    Action(ActionName.PUBLISH_DOCS, None, documents.PublishInput, None, ADMIN, audit=REQUEST,
           placement=HOST, remedy=STARTED_ON_HOST),
)  # fmt: skip

ACTIONS: dict[ActionName, Action] = {row.name: row for row in _ROWS}
"""Every action, by name."""


def has_role(role: ApiRole | None, required: ApiRole | None) -> bool:
    """Whether ``role`` meets ``required``; an unproven role meets only no requirement."""
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
    if not (action.provisioning and ctx.provisioning) and not has_role(ctx.role, required):
        error = PermissionDeniedError(
            f"account {ctx.actor.name!r}: role={ctx.role or 'unproven'}; action '{name}': "
            f"{required} role is required; ask an administrator to grant {required} or a higher "
            "role"
        )
        record_refusal(ctx, name, given, error)
        raise error
    if action.audit == Audit.REQUEST:
        with recorded(ctx, name, given):
            yield
    else:
        with acting_as(ctx.actor):
            yield


def invoke(ctx: OperationContext, name: ActionName, request: object) -> object:
    """Run one action for ``ctx``'s caller: the only way an interface reaches an operation."""
    call = ACTIONS[name].call
    if call is None:
        raise UsageError(f"{name} is a long-running process; its interface starts it")
    with admitted(ctx, name, request):
        return call(ctx, request)
