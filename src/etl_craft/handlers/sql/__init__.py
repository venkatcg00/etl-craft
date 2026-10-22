"""``HANDLER=SQL``: wrap the task's read-only SELECT in one of nine actions on the warehouse.

The task supplies a SELECT, inline in ``SELECT_QUERY`` or as a file under the project's
``sql_scripts/`` in ``SCRIPT_FILE``, and names its ``SQL_ACTION`` and ``TARGET_OBJECT``; the
engine owns every write. The parameters are checked first (``spec``), then the action uses the
warehouse's transaction or publication strategy (``actions``), with every statement logged
(``session``).
"""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import UTC, datetime

from sqlalchemy.engine import Engine

from etl_craft.config.targets import active_catalog
from etl_craft.core.actor import Actor
from etl_craft.core.enums import ActorKind, SqlAction
from etl_craft.core.errors import ConfigurationError, SqlGuardError
from etl_craft.core.faults import fault_point
from etl_craft.core.text import qualify as qualify
from etl_craft.dialects.warehouse import WarehouseDialect
from etl_craft.engine import locks
from etl_craft.engine.task_store import TargetPublication, TaskStore
from etl_craft.handlers.registry import HandlerResult, TaskContext, task_store
from etl_craft.handlers.sql.actions import ACTIONS, ActionContext
from etl_craft.handlers.sql.protection import (
    ProtectionRequest,
    for_task,
    refuse_evidence_target,
)
from etl_craft.handlers.sql.session import Session
from etl_craft.handlers.sql.spec import read_sql_task
from etl_craft.warehouse.connection import open_warehouse, task_warehouse_dialect

logger = logging.getLogger(__name__)


def run(context: TaskContext, engine_db: Engine | TaskStore) -> HandlerResult:
    """Run the SQL task described by ``context`` and return its counts."""
    store = task_store(context, engine_db)
    task = read_sql_task(context)
    config = context.config
    if config.warehouse is None:
        raise ConfigurationError("a SQL task needs a Warehouse section in craft-connector.yml")
    dialect = task_dialect(context)
    problem = dialect.unsupported_storage_problem(
        context.task_params
    ) or dialect.task_storage_problem(context.task_params)
    if problem is not None:
        raise SqlGuardError(problem)
    catalog = active_catalog(config)
    logger.info(
        "%s %s from %s on %s",
        task.action,
        f"{catalog}.{task.target_object}",
        task.source,
        dialect.spec.display_name,
    )
    logger.debug("the SELECT:\n%s", task.select_sql)
    # Hold through warehouse commit: a computed ROW_ID base must include the previous insert.
    # Generated keys still share mutation ordering with replacement and hash upgrades.
    lock = locks.target(task.target_object, catalog)
    with store.hold(lock.key, lock.name, 30):
        state = store.state(task.action)
        if task.action in {SqlAction.SCD1_MERGE, SqlAction.SCD2_MERGE}:
            other = state.merge_conflict
            if other is not None:
                raise SqlGuardError(
                    f"{task.target_object}: {task.action} conflicts with an active {other} writer; "
                    "give the target one merge action before running"
                )
        if task.watermark is not None:
            stored = None if context.backfill else state.offset
            task = read_sql_task(context, stored)
        with (
            open_warehouse(config, store) as warehouse,
            warehouse.begin() as conn,
        ):
            session = Session(
                conn,
                dialect,
                catalog=catalog,
                action=task.action,
                target_object=task.target_object,
                task_run_id=context.task_run_id,
                params=context.task_params,
            )
            action = ActionContext(
                task=task,
                context=context,
                store=store,
                user=context.writer,
                now=datetime.now(UTC),
            )
            outcome = _apply(session, action)
        if task.action == SqlAction.SCD1_MERGE:
            fault_point("sql.scd1.after_commit")
        elif task.action == SqlAction.SCD2_MERGE:
            fault_point("sql.scd2.after_commit")
        if isinstance(outcome, SqlGuardError):
            raise outcome
        store.publish(
            TargetPublication(
                session.target,
                session.publish_hash_version,
                session.clear_hash_version,
                session.publish_view_statement,
            )
        )
    return replace(outcome, offset=session.watermark_to) if task.watermark is not None else outcome


def task_dialect(context: TaskContext) -> WarehouseDialect:
    """Choose the dialect: the warehouse connection and the task's table format.

    The format is the task's ``TABLE_FORMAT`` parameter, else ``Warehouse.Table_format``. Where
    the connection fixes the format (Trino, DuckDB over Iceberg), a task cannot choose another.
    """
    assert context.config.warehouse is not None
    return task_warehouse_dialect(context.config, context.task_params)


def _apply(session: Session, action: ActionContext) -> HandlerResult | SqlGuardError:
    """Apply the action and evidence, retaining quarantine-only failures until commit."""
    task = action.task
    refuse_evidence_target(session)
    try:
        if (
            task.action
            in {
                SqlAction.CREATE_TABLE,
                SqlAction.SETUP_TABLE,
                SqlAction.OVERWRITE_TABLE,
                SqlAction.SCD1_MERGE,
                SqlAction.SCD2_MERGE,
            }
            and session.target_columns()
        ):
            session.check_target_format()
        session.protection = for_task(session, task)
        result = ACTIONS[task.action](session, action)
        if session.protection is not None:
            context = action.context
            session.protection.finish(
                ProtectionRequest(
                    Actor(action.user, ActorKind.SYSTEM),
                    f"ETL {task.action}",
                    f"task-run:{context.task_run_id}/attempt:{context.attempt}",
                    action.now,
                )
            )
            result = replace(
                result,
                variables={
                    **result.variables,
                    "PROTECTED_ROWS": session.protection.prevented_rows,
                    "PROTECTED_VALUES": session.protection.prevented_values,
                },
            )
        return result
    except SqlGuardError as error:
        if error is not session.failure_to_commit:
            raise
        # The target is untouched; commit what was set aside, then fail.
        return error
    finally:
        session.sweep()
