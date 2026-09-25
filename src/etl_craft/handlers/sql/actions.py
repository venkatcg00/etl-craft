"""The eight SQL actions. Each wraps the task's SELECT in the writes it stands for.

- ``CREATE_TABLE`` drops the target and creates it again from the SELECT's rows.
- ``SETUP_TABLE`` creates the target, empty, from the SELECT's shape plus the audit columns of
  the action that writes it, when it does not exist yet; an existing target is left alone.
- ``OVERWRITE_TABLE`` empties the target and inserts the SELECT's rows.
- ``APPEND_TABLE`` inserts the SELECT's rows, without comparing the shapes.
- ``SCD1_MERGE`` updates changed rows in place by merge key and inserts new keys.
- ``SCD2_MERGE`` closes the active version of a changed key (``ACTIVE_FLAG='N'``) and inserts a
  new one.
- ``DROP_TABLE`` drops the target if it exists, once this pipeline's ``CREATE_TABLE`` task for
  it has succeeded in the run.
- ``DELETE_ROWS`` deletes, or flags ``DELETE_FLAG='Y'``, the target rows whose merge key the
  SELECT returns.

Only ``CREATE_TABLE`` and ``SETUP_TABLE`` create tables; every other action fails, naming the
remedy, when its target does not exist.

Each reports the counts it can know: source rows, target rows after the write, and the rows
inserted, updated or deleted.

A row counts as changed when its ``HASH_KEY``, an MD5 over ``MERGE_COMPARE_COLUMNS``, differs.
Updates use correlated subqueries rather than ``MERGE`` or ``UPDATE ... FROM``, which not every
warehouse has.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.engine import Engine

from etl_craft.core.enums import RunStatus, SqlAction
from etl_craft.core.errors import HandlerError
from etl_craft.engine.repository.tasks import TargetTask, fetch_target_tasks
from etl_craft.engine.runlog import fetch_task_run_status
from etl_craft.handlers.registry import HandlerResult, TaskContext
from etl_craft.handlers.sql.session import Session
from etl_craft.handlers.sql.spec import SqlTask
from etl_craft.handlers.sql.tables import (
    AUDIT_COLUMNS,
    add_hash_key,
    add_row_id,
    build_stage,
    check_or_evolve,
    create_target_shape,
    dedupe,
    require_target,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ActionContext:
    """What every action needs besides the warehouse session."""

    task: SqlTask
    context: TaskContext
    engine_db: Engine
    user: str
    now: datetime

    @property
    def select_sql(self) -> str:
        """The task's SELECT; every action but ``DROP_TABLE`` has one."""
        assert self.task.select_sql is not None
        return self.task.select_sql

    @property
    def stamp(self) -> dict[str, object]:
        """The values the audit columns are stamped with."""
        return {
            "pipeline_run_id": self.context.pipeline_run_id,
            "now": self.now,
            "updated_by": self.user,
        }


Action = Callable[[Session, ActionContext], HandlerResult]


def create_table(session: Session, action: ActionContext) -> HandlerResult:
    """Replace the target with the SELECT's rows, stamped with the run id."""
    stage = build_stage(session, action.select_sql)
    source = session.count(f"SELECT COUNT(*) FROM {stage}", step="source rows")
    session.run(f"DROP TABLE IF EXISTS {session.target}", step="drop the old target")
    computed = session.dialect.surrogate_key == "computed"
    # Where ROW_ID cannot be added afterwards, the rows are numbered as the table is created.
    row_id = ", CAST(ROW_NUMBER() OVER (ORDER BY NULL) AS BIGINT) AS ROW_ID" if computed else ""
    session.create_table_as(
        session.target,
        f"SELECT s.*, CAST({int(action.context.pipeline_run_id)} AS BIGINT) AS PIPELINE_RUN_ID"
        f"{row_id} FROM {stage} s",
        step="create the target from the SELECT",
    )
    if not computed:
        add_row_id(session)
    session.drop(stage)
    return HandlerResult(source_count=source, target_count=source, insert_count=source)


def setup_table(session: Session, action: ActionContext) -> HandlerResult:
    """Create the target, empty, when it does not exist; leave an existing one alone.

    Its columns are the SELECT's, then ``PIPELINE_RUN_ID``, the audit columns of the action
    that writes it and ``ROW_ID``. That action is ``SETUP_FOR`` when set, else the one the
    pipeline's other tasks on the target write with.
    """
    if session.target_columns():
        logger.info("%s exists; SETUP_TABLE leaves it as it is", session.target)
        return HandlerResult(source_count=0, target_count=0, insert_count=0)
    with action.engine_db.connect() as conn:
        others = fetch_target_tasks(
            conn, action.context.pipeline_id, action.context.task_id, action.task.target_object
        )
    writer = writer_action(action.task.setup_for, others, action.task.target_object)
    stage = build_stage(session, action.select_sql, empty=True)
    create_target_shape(session, stage, AUDIT_COLUMNS[writer])
    session.drop(stage)
    logger.info("created %s with the audit columns of %s", session.target, writer)
    return HandlerResult(source_count=0, target_count=0, insert_count=0)


def writer_action(setup_for: str | None, others: list[TargetTask], target: str) -> str:
    """Return the action whose audit columns a SETUP_TABLE target gets; ``HandlerError`` if unclear.

    ``others`` are the pipeline's other tasks on the target.
    """
    writers = [task for task in others if task.sql_action in AUDIT_COLUMNS]
    found = sorted({task.sql_action for task in writers})
    listed = ", ".join(f"{task.task_code} ({task.sql_action})" for task in writers)
    if setup_for is not None:
        clashing = [a for a in found if AUDIT_COLUMNS[a] != AUDIT_COLUMNS[setup_for]]
        if clashing:
            raise HandlerError(
                f"SETUP_FOR={setup_for}, but tasks in this pipeline write {target} as {listed}, "
                "which need other audit columns"
            )
        return setup_for
    audit_sets = {AUDIT_COLUMNS[a] for a in found}
    if len(audit_sets) > 1:
        raise HandlerError(
            f"tasks in this pipeline write {target} with actions that need different audit "
            f"columns ({listed}); a table has one set, so give it one writing action, or set "
            "SETUP_FOR to the one it is for"
        )
    if not found:
        raise HandlerError(
            f"no task in this pipeline writes {target}, so SETUP_TABLE cannot tell which audit "
            "columns it needs; set SETUP_FOR to the action that writes it: "
            f"{', '.join(AUDIT_COLUMNS)}"
        )
    return found[0]


def overwrite_table(session: Session, action: ActionContext) -> HandlerResult:
    """Empty the target and insert the SELECT's rows."""
    stage = build_stage(session, action.select_sql)
    source = session.count(f"SELECT COUNT(*) FROM {stage}", step="source rows")
    check_or_evolve(
        session, stage, SqlAction.OVERWRITE_TABLE, schema_evolution=action.task.schema_evolution
    )
    columns = ", ".join(name for name, _ in session.columns(stage))
    session.run(f"TRUNCATE TABLE {session.target}", step="empty the target")
    row_id_columns, row_id_values = session.row_id_insert_parts()
    session.run(
        f"INSERT INTO {session.target} ({columns}, PIPELINE_RUN_ID, UPDATE_DATE{row_id_columns}) "
        f"SELECT {columns}, :pipeline_run_id, :now{row_id_values} FROM {stage}",
        action.stamp,
        step="insert the SELECT's rows",
    )
    session.drop(stage)
    return HandlerResult(source_count=source, target_count=source, insert_count=source)


def append_table(session: Session, action: ActionContext) -> HandlerResult:
    """Insert the SELECT's rows, stamped with the run and CREATE_DATE, without shape checks.

    Columns are matched by name; a column the target lacks fails the insert, with the database's
    own message.
    """
    stage = build_stage(session, action.select_sql)
    source = session.count(f"SELECT COUNT(*) FROM {stage}", step="source rows")
    require_target(session)
    columns = ", ".join(name for name, _ in session.columns(stage))
    row_id_columns, row_id_values = session.row_id_insert_parts()
    session.run(
        f"INSERT INTO {session.target} ({columns}, PIPELINE_RUN_ID, CREATE_DATE{row_id_columns}) "
        f"SELECT {columns}, :pipeline_run_id, :now{row_id_values} FROM {stage}",
        action.stamp,
        step="append the SELECT's rows",
    )
    session.drop(stage)
    target_count = session.count(f"SELECT COUNT(*) FROM {session.target}", step="target rows")
    return HandlerResult(source_count=source, target_count=target_count, insert_count=source)


def _merge_stage(session: Session, action: ActionContext, kind: SqlAction) -> tuple[str, int]:
    """Stage, de-duplicate, check and hash the SELECT for a merge; return (stage, source rows)."""
    task = action.task
    stage = build_stage(session, action.select_sql)
    source = session.count(f"SELECT COUNT(*) FROM {stage}", step="source rows")
    stage = dedupe(session, stage, task.merge_key, task.dedupe_order)
    check_or_evolve(session, stage, kind, schema_evolution=task.schema_evolution)
    add_hash_key(session, stage, task.merge_compare_columns)
    return stage, source


def _changed_keys(session: Session, stage: str, merge_key: tuple[str, ...], condition: str) -> str:
    """Materialize the merge keys whose target row meets ``condition``; return the table.

    A plain join, so later statements correlate on key equality alone: Snowflake cannot
    evaluate a correlated subquery whose correlation is not an equality.
    """
    changed = session.scratch("changed_keys")
    key_match = " AND ".join(f"t.{k} = s.{k}" for k in merge_key)
    keys = ", ".join(f"s.{k}" for k in merge_key)
    session.create_scratch(
        changed,
        f"SELECT DISTINCT {keys} FROM {stage} s JOIN {session.target} t ON {key_match} "
        f"WHERE {condition}",
        step="find changed keys",
    )
    return changed


def scd1_merge(session: Session, action: ActionContext) -> HandlerResult:
    """Update changed rows in place by merge key and insert new keys.

    With ``PRESERVE_TARGET``, a NULL in the SELECT keeps the target's value, and the hash is
    taken over the values actually kept.
    """
    task = action.task
    stage, source = _merge_stage(session, action, SqlAction.SCD1_MERGE)
    stage_columns = [name for name, _ in session.columns(stage)]
    keys = {key.lower() for key in task.merge_key}
    non_key = [column for column in stage_columns if column.lower() not in keys]
    target = session.target
    dialect = session.dialect
    key_match = " AND ".join(f"t.{k} = s.{k}" for k in task.merge_key)

    def kept_hash(alias: str) -> str:
        return dialect.hash_expression(
            [f"COALESCE(s.{c}, {alias}.{c})" for c in task.merge_compare_columns]
        )

    compared = kept_hash("t") if task.preserve_target else "s.HASH_KEY"
    changed = _changed_keys(
        session, stage, task.merge_key, f"t.HASH_KEY IS DISTINCT FROM {compared}"
    )
    update_count = session.count(f"SELECT COUNT(*) FROM {changed}", step="changed rows")

    update_target, q = session.mutation_target()
    update_key_match = " AND ".join(f"{q}.{k} = s.{k}" for k in task.merge_key)
    changed_match = " AND ".join(f"{q}.{k} = ck.{k}" for k in task.merge_key)

    def source_value(column: str) -> str:
        value = dialect.scalar_source_value(f"s.{column}")
        return f"(SELECT {value} FROM {stage} s WHERE {update_key_match})"

    assignments = []
    for column in non_key:
        value = source_value(column)
        if task.preserve_target:
            if column.lower() == "hash_key":
                value = dialect.hash_expression(
                    [f"COALESCE({source_value(c)}, {q}.{c})" for c in task.merge_compare_columns]
                )
            else:
                value = f"COALESCE({value}, {q}.{column})"
        assignments.append(f"{column} = {value}")
    assignments += [
        "PIPELINE_RUN_ID = :pipeline_run_id",
        "UPDATE_DATE = :now",
        "UPDATED_BY = :updated_by",
    ]
    session.run(
        f"UPDATE {update_target} SET {', '.join(assignments)} "
        f"WHERE EXISTS (SELECT 1 FROM {changed} ck WHERE {changed_match})",
        action.stamp,
        step="update changed rows",
    )

    new_keys = f"NOT EXISTS (SELECT 1 FROM {target} t WHERE {key_match})"
    insert_count = session.count(
        f"SELECT COUNT(*) FROM {stage} s WHERE {new_keys}", step="new rows"
    )
    columns = ", ".join(stage_columns)
    row_id_columns, row_id_values = session.row_id_insert_parts()
    session.run(
        f"INSERT INTO {target} ({columns}, PIPELINE_RUN_ID, CREATE_DATE, CREATED_BY, "
        f"UPDATE_DATE, UPDATED_BY, DELETE_FLAG{row_id_columns}) "
        f"SELECT {columns}, :pipeline_run_id, :now, :updated_by, :now, :updated_by, "
        f"'N'{row_id_values} FROM {stage} s WHERE {new_keys}",
        action.stamp,
        step="insert new rows",
    )
    session.drop(changed)
    session.drop(stage)
    target_count = session.count(f"SELECT COUNT(*) FROM {target}", step="target rows")
    return HandlerResult(
        source_count=source,
        target_count=target_count,
        insert_count=insert_count,
        update_count=update_count,
    )


def scd2_merge(session: Session, action: ActionContext) -> HandlerResult:
    """Close the active version of each changed key and insert a new active version.

    A key with no active version, new or left without one by an interrupted earlier run, gets
    one inserted, so a rerun converges.
    """
    task = action.task
    stage, source = _merge_stage(session, action, SqlAction.SCD2_MERGE)
    stage_columns = [name for name, _ in session.columns(stage)]
    target = session.target
    key_match = " AND ".join(f"t.{k} = s.{k}" for k in task.merge_key)

    changed = _changed_keys(
        session,
        stage,
        task.merge_key,
        "t.ACTIVE_FLAG = 'Y' AND t.HASH_KEY IS DISTINCT FROM s.HASH_KEY",
    )
    closed = session.count(f"SELECT COUNT(*) FROM {changed}", step="changed keys")
    update_target, q = session.mutation_target()
    changed_match = " AND ".join(f"{q}.{k} = ck.{k}" for k in task.merge_key)
    session.run(
        f"UPDATE {update_target} SET ACTIVE_FLAG = 'N', UPDATE_DATE = :now, "
        f"UPDATED_BY = :updated_by WHERE {q}.ACTIVE_FLAG = 'Y' AND EXISTS "
        f"(SELECT 1 FROM {changed} ck WHERE {changed_match})",
        action.stamp,
        step="close the active version of changed keys",
    )

    columns = ", ".join(stage_columns)
    insert_head = (
        f"INSERT INTO {target} ({columns}, PIPELINE_RUN_ID, CREATE_DATE, CREATED_BY, "
        "UPDATE_DATE, UPDATED_BY, DELETE_FLAG, ACTIVE_FLAG{row_id_columns}) "
        f"SELECT {columns}, :pipeline_run_id, :now, :updated_by, :now, :updated_by, 'N', "
        "'Y'{row_id_values} "
    )
    row_id_columns, row_id_values = session.row_id_insert_parts()
    stage_changed = " AND ".join(f"s.{k} = ck.{k}" for k in task.merge_key)
    session.run(
        insert_head.format(row_id_columns=row_id_columns, row_id_values=row_id_values)
        + f"FROM {stage} s WHERE EXISTS (SELECT 1 FROM {changed} ck WHERE {stage_changed})",
        action.stamp,
        step="insert new versions of changed keys",
    )

    no_active = f"NOT EXISTS (SELECT 1 FROM {target} t WHERE {key_match} AND t.ACTIVE_FLAG = 'Y')"
    new = session.count(f"SELECT COUNT(*) FROM {stage} s WHERE {no_active}", step="new keys")
    row_id_columns, row_id_values = session.row_id_insert_parts()
    session.run(
        insert_head.format(row_id_columns=row_id_columns, row_id_values=row_id_values)
        + f"FROM {stage} s WHERE {no_active}",
        action.stamp,
        step="insert new keys",
    )
    session.drop(changed)
    session.drop(stage)
    target_count = session.count(f"SELECT COUNT(*) FROM {target}", step="target rows")
    return HandlerResult(
        source_count=source,
        target_count=target_count,
        insert_count=closed + new,
        update_count=closed,
    )


def drop_table(session: Session, action: ActionContext) -> HandlerResult:
    """Drop the target if it exists, only once this pipeline's CREATE_TABLE task ran this run.

    A target already gone is not an error: the drop has nothing left to do.
    """
    context = action.context
    target_object = action.task.target_object
    with action.engine_db.connect() as conn:
        others = fetch_target_tasks(conn, context.pipeline_id, context.task_id, target_object)
        creator = next((task for task in others if task.sql_action == SqlAction.CREATE_TABLE), None)
        status = (
            fetch_task_run_status(conn, creator.task_id, context.pipeline_run_id)
            if creator is not None
            else None
        )
    if creator is None:
        raise HandlerError(
            f"DROP_TABLE refused for {target_object}: no other active task in this pipeline "
            "creates it with SQL_ACTION=CREATE_TABLE, and DROP_TABLE removes only tables its "
            "own pipeline creates"
        )
    if status != RunStatus.SUCCESS:
        raise HandlerError(
            f"DROP_TABLE refused for {target_object}: the task that creates it "
            f"({creator.task_code}) is {status or 'not run'} under pipeline_run_id="
            f"{context.pipeline_run_id}, not SUCCESS; make the drop depend on it"
        )
    if not session.target_columns():
        logger.info("%s does not exist; nothing to drop", session.target)
        return HandlerResult()
    session.run(f"DROP TABLE {session.target}", step="drop the target")
    return HandlerResult()


def delete_rows(session: Session, action: ActionContext) -> HandlerResult:
    """Delete, or flag ``DELETE_FLAG='Y'``, the target rows whose merge key the SELECT returns."""
    task = action.task
    stage = build_stage(session, action.select_sql)
    source = session.count(f"SELECT COUNT(*) FROM {stage}", step="source rows")
    target = session.target
    require_target(session)
    key_match = " AND ".join(f"t.{k} = s.{k}" for k in task.merge_key)
    delete_count = session.count(
        f"SELECT COUNT(*) FROM {target} t WHERE EXISTS (SELECT 1 FROM {stage} s WHERE {key_match})",
        step="rows to delete",
    )
    mutation, q = session.mutation_target()
    match = " AND ".join(f"{q}.{k} = s.{k}" for k in task.merge_key)
    if task.hard_delete:
        session.run(
            f"DELETE FROM {mutation} WHERE EXISTS (SELECT 1 FROM {stage} s WHERE {match})",
            step="delete the rows",
        )
    else:
        have = {name.lower() for name, _ in session.target_columns()}
        missing = [c for c in ("DELETE_FLAG", "UPDATE_DATE", "UPDATED_BY") if c.lower() not in have]
        if missing:
            raise HandlerError(
                f"{target} lacks {', '.join(missing)}, which a soft DELETE_ROWS sets; run a "
                "SETUP_TABLE task for it, add the columns, or set HARD_DELETE=true"
            )
        session.run(
            f"UPDATE {mutation} SET DELETE_FLAG = 'Y', UPDATE_DATE = :now, "
            f"UPDATED_BY = :updated_by WHERE EXISTS (SELECT 1 FROM {stage} s WHERE {match})",
            action.stamp,
            step="flag the rows deleted",
        )
    session.drop(stage)
    return HandlerResult(source_count=source, delete_count=delete_count)


ACTIONS: dict[SqlAction, Action] = {
    SqlAction.CREATE_TABLE: create_table,
    SqlAction.SETUP_TABLE: setup_table,
    SqlAction.OVERWRITE_TABLE: overwrite_table,
    SqlAction.APPEND_TABLE: append_table,
    SqlAction.SCD1_MERGE: scd1_merge,
    SqlAction.SCD2_MERGE: scd2_merge,
    SqlAction.DROP_TABLE: drop_table,
    SqlAction.DELETE_ROWS: delete_rows,
}


def utc_now() -> datetime:
    """Return the time audit columns are stamped with."""
    return datetime.now(UTC)
