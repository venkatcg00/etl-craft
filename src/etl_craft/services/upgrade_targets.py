"""Upgrade configured targets' execution identities and legacy SCD active state."""

from __future__ import annotations

from dataclasses import dataclass, field

from sqlalchemy.engine import Connection, Engine

from etl_craft.config import ConnectorConfig
from etl_craft.config.targets import active_catalog, parse_warehouse_url
from etl_craft.core.enums import Handler, SqlAction
from etl_craft.core.errors import ConfigurationError, HandlerError
from etl_craft.core.text import qualify, split_object_ref
from etl_craft.dialects.warehouse import WarehouseDialect, resolve
from etl_craft.engine import locks
from etl_craft.engine.repository.tasks import fetch_task_parameters
from etl_craft.engine.repository.validation import fetch_active_tasks
from etl_craft.handlers.registry import TaskContext
from etl_craft.handlers.resources import LocalLocker
from etl_craft.handlers.sql import task_dialect
from etl_craft.handlers.sql.protection import refuse_replacement
from etl_craft.handlers.sql.session import Session
from etl_craft.handlers.sql.tables import IDENTITY_COLUMNS, check_identity_types, require_target
from etl_craft.warehouse.connection import open_warehouse


@dataclass(frozen=True)
class UpgradeResult:
    """One existing target, with its additions or already-upgraded outcome."""

    target: str
    changed: bool
    dry_run: bool
    sql: str | None
    columns: tuple[str, ...] = ()


def upgrade_targets(
    engine: Engine,
    config: ConnectorConfig,
    *,
    action: str | None = None,
    target: str | None = None,
    dry_run: bool = False,
) -> list[UpgradeResult]:
    """Upgrade configured targets under their mutation locks; preserve historical rows."""
    actions = {str(kind) for kind in SqlAction} - {SqlAction.DROP_TABLE, SqlAction.CREATE_VIEW}
    if action is not None and action not in actions:
        raise HandlerError(f"upgrade-targets does not support `action` {action}")
    if config.warehouse is None:
        raise ConfigurationError("upgrade-targets needs a Warehouse section in craft-connector.yml")
    catalog = active_catalog(config)
    selected = None
    if target is not None:
        split_object_ref(target)
        selected = qualify(target, catalog).lower()
    plan = _UpgradePlan()
    selection = _Selection(
        catalog,
        actions,
        action,
        selected,
        dry_run=dry_run,
        dialect_name=parse_warehouse_url(config.warehouse.jdbc_url).dialect,
    )
    with engine.connect() as conn:
        _plan_writers(conn, config, plan, selection)
    if selected is not None and selected not in plan.targets:
        raise HandlerError(f"{target}: no active {action or Handler.SQL} task writes this target")
    _check_writers(plan)
    return [_upgrade_target(engine, config, plan, key, selection) for key in sorted(plan.targets)]


@dataclass
class _UpgradePlan:
    """The writers of each target, their merge actions and the targets selected to upgrade."""

    contracts: dict[str, list[tuple[str, str, WarehouseDialect, bool]]] = field(
        default_factory=dict
    )
    merge_actions: dict[str, set[str]] = field(default_factory=dict)
    targets: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class _Selection:
    """Which tasks' targets are upgraded: their catalog, allowed actions and filters."""

    catalog: str
    actions: set[str]
    action: str | None
    selected: str | None
    dry_run: bool
    dialect_name: str


def _plan_writers(
    conn: Connection, config: ConnectorConfig, plan: _UpgradePlan, selection: _Selection
) -> None:
    """Record every active SQL or Python task that writes a selected target."""
    catalog, actions, action, selected = (
        selection.catalog,
        selection.actions,
        selection.action,
        selection.selected,
    )
    for task in fetch_active_tasks(conn):
        if task.handler not in {Handler.SQL, Handler.PYTHON}:
            continue
        params = fetch_task_parameters(conn, task.task_id)
        kind = (
            Handler.PYTHON
            if task.handler == Handler.PYTHON
            else (params.get("SQL_ACTION") or "").strip().upper()
        )
        if kind != Handler.PYTHON and kind not in actions:
            continue
        written = (params.get("TARGET_OBJECT") or "").strip()
        if not written:
            if task.handler == Handler.PYTHON:
                continue
            raise HandlerError(f"{task.label}: {kind} needs TARGET_OBJECT")
        split_object_ref(written)
        qualified = qualify(written, catalog)
        key = qualified.lower()
        if selected is not None and key != selected:
            continue
        merge = (
            params.get("SETUP_FOR", "").strip().upper() if kind == SqlAction.SETUP_TABLE else kind
        )
        if merge in {SqlAction.SCD1_MERGE, SqlAction.SCD2_MERGE}:
            plan.merge_actions.setdefault(key, set()).add(merge)
        context = TaskContext(
            config=config,
            pipeline_id=task.pipeline_id,
            pipeline_code=task.pipeline_code,
            task_id=task.task_id,
            task_code=task.task_code,
            pipeline_run_id=0,
            task_run_id=0,
            attempt=0,
            handler=task.handler,
            refresh_type=task.refresh_type,
            task_params=params,
        )
        dialect = task_dialect(context)
        declared = task.handler == Handler.SQL or bool(params.get("TABLE_FORMAT"))
        plan.contracts.setdefault(key, []).append((qualified, task.label, dialect, declared))
        if action is None or kind == action:
            plan.targets.add(key)


def _check_writers(plan: _UpgradePlan) -> None:
    """Refuse targets whose writers disagree on the merge lifecycle or the table format."""
    for key in sorted(plan.targets):
        if len(plan.merge_actions.get(key, set())) > 1:
            raise HandlerError(
                f"{key}: SCD1_MERGE and SCD2_MERGE have different row lifecycles; "
                "give the target one merge action before upgrading"
            )
        writers = [writer for writer in plan.contracts[key] if writer[3]]
        if len({dialect.spec.table_format for _, _, dialect, _ in writers}) > 1:
            names = ", ".join(f"{label} ({d.spec.table_format})" for _, label, d, _ in writers)
            raise HandlerError(
                f"{key}: active tasks resolve different table formats: {names}; "
                "fix TABLE_FORMAT before upgrading"
            )


def _upgrade_target(
    engine: Engine,
    config: ConnectorConfig,
    plan: _UpgradePlan,
    key: str,
    selection: _Selection,
) -> UpgradeResult:
    """Add the missing audit columns and lifecycle state to one target under its lock."""
    catalog, dry_run = selection.catalog, selection.dry_run
    writers = [writer for writer in plan.contracts[key] if writer[3]]
    qualified, _, dialect, declared = (writers or plan.contracts[key])[0]
    with (
        locks.target(qualified, "").hold(LocalLocker(engine, config)),
        open_warehouse(config, LocalLocker(engine, config)) as warehouse,
        warehouse.begin() as conn,
    ):
        if not declared:
            actual = dialect.existing_table_format(conn, qualified)
            if actual is not None:
                dialect = resolve(selection.dialect_name, actual)
        session = Session(
            conn,
            dialect,
            catalog=catalog,
            action="UPGRADE_TARGETS",
            target_object=qualified,
            task_run_id=0,
            params={},
        )
        columns = {name.lower() for name, _ in require_target(session)}
        session.check_target_format()
        check_identity_types(session, tuple(c for c in IDENTITY_COLUMNS if c.lower() in columns))
        additions = []
        for column in ("PIPELINE_ID", "PIPELINE_RUN_ID", "TASK_RUN_ID"):
            if column.lower() in columns:
                continue
            else:
                additions.append(column)
        statements = [
            f"{dialect.alter_table_keyword()} {qualified} ADD COLUMN {column} BIGINT"
            for column in additions
        ]
        backfill = False
        merge = plan.merge_actions.get(key)
        if merge == {SqlAction.SCD1_MERGE}:
            backfill = _scd1_statements(session, columns, additions, statements)
        elif merge == {SqlAction.SCD2_MERGE}:
            backfill = _scd2_statements(session, columns, statements)
        if not dry_run and statements:
            refuse_replacement(session)
        if not dry_run:
            for sql in statements:
                session.run(sql, step="upgrade target audit columns")
        return UpgradeResult(
            qualified,
            bool(additions) or backfill,
            dry_run,
            "\n".join(statements) or None,
            tuple(additions),
        )


def _scd1_statements(
    session: Session, columns: set[str], additions: list[str], statements: list[str]
) -> bool:
    """Add the SCD1 active-state upgrade; return whether rows need ACTIVE_FLAG backfilled."""
    qualified, dialect = session.target_object, session.dialect
    if "delete_flag" not in columns:
        raise HandlerError(
            f"{qualified}: SCD1 active-state upgrade needs DELETE_FLAG; "
            "migrate the legacy deletion state explicitly before upgrading"
        )
    invalid = session.count(
        f"SELECT COUNT(*) FROM {qualified} WHERE DELETE_FLAG IS NULL "
        "OR DELETE_FLAG NOT IN ('Y', 'N')",
        step="validate legacy SCD1 deletion state",
    )
    if invalid:
        raise HandlerError(
            f"{qualified}: {invalid} row(s) have NULL or invalid DELETE_FLAG; "
            "set each to Y or N from known deletion evidence before upgrading"
        )
    if "active_flag" not in columns:
        additions.append("ACTIVE_FLAG")
        statements.append(
            f"{dialect.alter_table_keyword()} {qualified} ADD COLUMN ACTIVE_FLAG "
            f"{dialect.evolution_column_type(dialect.audit_column_type('ACTIVE_FLAG'))}"
        )
        backfill = True
    else:
        backfill = bool(
            session.count(
                f"SELECT COUNT(*) FROM {qualified} WHERE ACTIVE_FLAG IS NULL",
                step="unfinished SCD1 active-state upgrade",
            )
        )
    if backfill:
        statements.append(
            f"UPDATE {qualified} SET ACTIVE_FLAG = CASE WHEN DELETE_FLAG = 'Y' "
            "THEN 'N' ELSE 'Y' END WHERE ACTIVE_FLAG IS NULL"
        )
    return backfill


def _scd2_statements(session: Session, columns: set[str], statements: list[str]) -> bool:
    """Add the SCD2 soft-delete repair; return whether any active version is soft-deleted."""
    qualified = session.target_object
    if not {"active_flag", "delete_flag"}.issubset(columns):
        raise HandlerError(
            f"{qualified}: SCD2 state needs ACTIVE_FLAG and DELETE_FLAG; "
            "migrate the legacy version state explicitly before upgrading"
        )
    backfill = bool(
        session.count(
            f"SELECT COUNT(*) FROM {qualified} WHERE ACTIVE_FLAG = 'Y' AND DELETE_FLAG = 'Y'",
            step="legacy active soft-deleted SCD2 versions",
        )
    )
    if backfill:
        statements.append(
            f"UPDATE {qualified} SET ACTIVE_FLAG = 'N' "
            "WHERE ACTIVE_FLAG = 'Y' AND DELETE_FLAG = 'Y'"
        )
    return backfill
