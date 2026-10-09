"""Add nullable pipeline and task-run identities to configured SQL and ingestion targets."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.engine import Engine

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
from etl_craft.handlers.sql import task_dialect
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
    actions = {str(kind) for kind in SqlAction} - {SqlAction.DROP_TABLE}
    if action is not None and action not in actions:
        raise HandlerError(f"upgrade-targets does not support --action {action}")
    if config.warehouse is None:
        raise ConfigurationError("upgrade-targets needs a Warehouse section in craft-connector.yml")
    catalog = active_catalog(config)
    selected = None
    if target is not None:
        split_object_ref(target)
        selected = qualify(target, catalog).lower()
    contracts: dict[str, list[tuple[str, str, WarehouseDialect, bool]]] = {}
    targets = set()
    with engine.connect() as conn:
        for task in fetch_active_tasks(conn):
            if task.handler not in {Handler.SQL, Handler.PYTHON}:
                continue
            params = fetch_task_parameters(conn, task.task_id)
            kind = (
                "PYTHON"
                if task.handler == Handler.PYTHON
                else (params.get("SQL_ACTION") or "").strip().upper()
            )
            if kind != "PYTHON" and kind not in actions:
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
            contracts.setdefault(key, []).append((qualified, task.label, dialect, declared))
            if action is None or kind == action:
                targets.add(key)
    if selected is not None and selected not in targets:
        raise HandlerError(f"{target}: no active {action or 'SQL'} task writes this target")
    for key in sorted(targets):
        writers = [writer for writer in contracts[key] if writer[3]]
        if len({dialect.spec.table_format for _, _, dialect, _ in writers}) > 1:
            names = ", ".join(f"{label} ({d.spec.table_format})" for _, label, d, _ in writers)
            raise HandlerError(
                f"{key}: active tasks resolve different table formats: {names}; "
                "fix TABLE_FORMAT before upgrading"
            )
    results = []
    for key in sorted(targets):
        writers = [writer for writer in contracts[key] if writer[3]]
        qualified, _, dialect, declared = (writers or contracts[key])[0]
        with (
            locks.target(qualified).hold(engine),
            open_warehouse(config, engine) as warehouse,
            warehouse.begin() as conn,
        ):
            if not declared:
                actual = dialect.existing_table_format(conn, qualified)
                if actual is not None:
                    dialect = resolve(
                        parse_warehouse_url(config.warehouse.jdbc_url).dialect, actual
                    )
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
            check_identity_types(
                session, tuple(c for c in IDENTITY_COLUMNS if c.lower() in columns)
            )
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
            if not dry_run:
                for column, sql in zip(additions, statements, strict=True):
                    session.run(sql, step=f"add nullable {column}")
            results.append(
                UpgradeResult(
                    qualified,
                    bool(additions),
                    dry_run,
                    "\n".join(statements) or None,
                    tuple(additions),
                )
            )
    return results
