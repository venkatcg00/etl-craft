"""The eight SQL actions. Each wraps the task's SELECT in the writes it stands for.

- ``CREATE_TABLE`` replaces the target from the staged SELECT with rollback or recovery.
- ``SETUP_TABLE`` creates the target, empty, from the SELECT's shape plus the audit columns of
  the action that writes it, when it does not exist yet; an existing target is left alone.
- ``OVERWRITE_TABLE`` replaces the rows with atomic publication or row recovery.
- ``APPEND_TABLE`` inserts the SELECT's rows, without comparing the shapes.
- ``SCD1_MERGE`` updates changed rows in place by merge key and inserts new keys.
- ``SCD2_MERGE`` closes the active version of a changed key (``ACTIVE_FLAG='N'``) and inserts a
  new one.

  In both merges a key soft-deleted by ``DELETE_ROWS`` that the SELECT returns again comes back
  (``DELETE_FLAG='N'``), changed or not. A NULL in a merge key column fails the task.
- ``DROP_TABLE`` drops the target if it exists, once this pipeline's ``CREATE_TABLE`` task for
  it has succeeded in the run.
- ``DELETE_ROWS`` deletes, or flags ``DELETE_FLAG='Y'``, the target rows whose merge key the
  SELECT returns; rows already flagged are left as they are.

Only ``CREATE_TABLE`` and ``SETUP_TABLE`` create tables; every other action fails, naming the
remedy, when its target does not exist.

Each reports the counts it can know: source rows, target rows after the write, and the rows
inserted, updated or deleted.

A row counts as changed when its ``HASH_KEY``, an MD5 over ``MERGE_COMPARE_COLUMNS``, differs.
Merge updates join the deduplicated stage using the warehouse's UPDATE FROM or matched MERGE
strategy. SCD2 materializes changed keys before closing active versions so its insert phase can
still find those keys afterwards.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy.engine import Engine

from etl_craft.core.enums import RunStatus, SqlAction
from etl_craft.core.errors import SqlGuardError
from etl_craft.core.faults import fault_point
from etl_craft.engine.repository.hash_versions import fetch_hash_version
from etl_craft.engine.repository.tasks import TargetTask, fetch_target_tasks
from etl_craft.engine.runlog import fetch_task_run_status
from etl_craft.handlers.registry import HandlerResult, TaskContext
from etl_craft.handlers.sql.replacement import cleanup, promote, recover_overwrite
from etl_craft.handlers.sql.session import Session
from etl_craft.handlers.sql.spec import SqlTask
from etl_craft.handlers.sql.tables import (
    AUDIT_COLUMNS,
    IDENTITY_COLUMNS,
    add_hash_key,
    add_row_id,
    build_stage,
    check_identity_types,
    check_or_evolve,
    check_target_audit,
    create_target_shape,
    dedupe,
    refuse_null_keys,
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
            "pipeline_id": self.context.pipeline_id,
            "task_run_id": self.context.task_run_id,
            "now": self.now,
            "updated_by": self.user,
        }


Action = Callable[[Session, ActionContext], HandlerResult]


def create_table(session: Session, action: ActionContext) -> HandlerResult:
    """Replace the target with the SELECT's rows, stamped with the run id."""
    stage = build_stage(session, action.select_sql)
    source = session.count(f"SELECT COUNT(*) FROM {stage}", step="source rows")
    existing = bool(session.target_columns())
    strategy = session.dialect.replace_strategy
    computed = session.dialect.surrogate_key == "computed"
    row_id = ", CAST(ROW_NUMBER() OVER (ORDER BY NULL) AS BIGINT) AS ROW_ID" if computed else ""
    select_sql = (
        f"SELECT s.*, CAST({int(action.context.pipeline_run_id)} AS BIGINT) AS PIPELINE_RUN_ID"
        f", CAST({int(action.context.pipeline_id)} AS BIGINT) AS PIPELINE_ID"
        f", CAST({int(action.context.task_run_id)} AS BIGINT) AS TASK_RUN_ID"
        f"{row_id} FROM {stage} s"
    )
    if session.dialect.identity_in_create:
        candidate = session.scratch("replace", persistent=True)
        types = session.column_types(stage)
        names = [name for name, _ in session.columns(stage)]
        columns = ", ".join(
            [f"{name} {types[name.lower()]}" for name in names]
            + ["PIPELINE_RUN_ID BIGINT", "PIPELINE_ID BIGINT", "TASK_RUN_ID BIGINT"]
        )
        create_ddl, publish_ddl = session.dialect.identity_replacement_ddl(
            session.conn,
            session.target,
            candidate,
            columns,
            session.params,
            existing=existing,
        )
        session.run(create_ddl, step="create the identity replacement")
        session.run(
            f"INSERT INTO {candidate} "
            f"({', '.join(names)}, PIPELINE_RUN_ID, PIPELINE_ID, TASK_RUN_ID) {select_sql}",
            step="populate the identity replacement",
        )
        fault_point("sql.replace.before_publish")
        session.run(publish_ddl, step="atomically replace the target")
        cleanup(session, candidate)
    elif strategy == "create_or_replace":
        ddl = session.dialect.replacement_ddl(
            session.conn, session.target, select_sql, session.params, existing=existing
        )
        fault_point("sql.replace.before_publish")
        session.run(ddl, step="atomically replace the target")
    elif strategy == "copy_and_restore":
        if existing and any(session.params.get(k) for k in ("EXTERNAL_LOCATION", "BASE_LOCATION")):
            raise SqlGuardError(
                f"{session.target}: cannot safely stage replacement at its existing storage path; "
                "use OVERWRITE_TABLE to keep the table definition"
            )
        candidate = session.scratch("replace", persistent=True)
        session.create_table_as(candidate, select_sql, step="prepare the complete replacement")
        if not computed:
            raise SqlGuardError(
                f"{session.target}: safe replacement requires a declared ROW_ID strategy"
            )
        if existing:
            session.dialect.preserve_replacement_properties(session.conn, session.target, candidate)
        promote(session, candidate, existing=existing)
    else:
        comment = (
            session.dialect.replacement_comment(session.conn, session.target) if existing else None
        )
        fault_point("sql.replace.before_publish")
        session.run(f"DROP TABLE IF EXISTS {session.target}", step="drop the old target")
        fault_point("sql.replace.after_clear")
        session.create_table_as(
            session.target, select_sql, step="create the target from the SELECT"
        )
        if not computed:
            add_row_id(session)
        if comment is not None:
            escaped = comment.replace("'", "''")
            session.run(
                f"COMMENT ON TABLE {session.target} IS '{escaped}'",
                step="preserve the table comment",
            )
        fault_point("sql.replace.after_publish")
    session.clear_hash_version = True
    cleanup(session, stage)
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
    if writer in {SqlAction.SCD1_MERGE, SqlAction.SCD2_MERGE}:
        session.publish_hash_version = 2
    logger.info("created %s with the audit columns of %s", session.target, writer)
    return HandlerResult(source_count=0, target_count=0, insert_count=0)


def writer_action(setup_for: str | None, others: list[TargetTask], target: str) -> str:
    """Return the writer whose audit columns a SETUP_TABLE target gets.

    ``others`` are the pipeline's other tasks on the target.
    """
    writers = [task for task in others if task.sql_action in AUDIT_COLUMNS]
    found = sorted({task.sql_action for task in writers})
    listed = ", ".join(f"{task.task_code} ({task.sql_action})" for task in writers)
    if setup_for is not None:
        clashing = [a for a in found if AUDIT_COLUMNS[a] != AUDIT_COLUMNS[setup_for]]
        if clashing:
            raise SqlGuardError(
                f"SETUP_FOR={setup_for}, but tasks in this pipeline write {target} as {listed}, "
                "which need other audit columns"
            )
        return setup_for
    audit_sets = {AUDIT_COLUMNS[a] for a in found}
    if len(audit_sets) > 1:
        raise SqlGuardError(
            f"tasks in this pipeline write {target} with actions that need different audit "
            f"columns ({listed}); a table has one set, so give it one writing action, or set "
            "SETUP_FOR to the one it is for"
        )
    if not found:
        raise SqlGuardError(
            f"no task in this pipeline writes {target}, so SETUP_TABLE cannot tell which audit "
            "columns it needs; set SETUP_FOR to the action that writes it: "
            f"{', '.join(AUDIT_COLUMNS)}"
        )
    return found[0]


def overwrite_table(session: Session, action: ActionContext) -> HandlerResult:
    """Empty the target and insert the SELECT's rows."""
    stage = build_stage(session, action.select_sql)
    source = session.count(f"SELECT COUNT(*) FROM {stage}", step="source rows")
    strategy = session.dialect.replace_strategy
    check_or_evolve(
        session, stage, SqlAction.OVERWRITE_TABLE, schema_evolution=action.task.schema_evolution
    )
    columns = ", ".join(name for name, _ in session.columns(stage))
    computed = not session.row_id_generated()
    row_id_columns = ", ROW_ID" if computed else ""
    row_id_values = ", CAST(ROW_NUMBER() OVER (ORDER BY NULL) AS BIGINT)" if computed else ""
    select_sql = (
        f"SELECT {columns}, :pipeline_run_id, :pipeline_id, :task_run_id, :now{row_id_values} "
        f"FROM {stage}"
    )
    written_columns = (
        f"{columns}, PIPELINE_RUN_ID, PIPELINE_ID, TASK_RUN_ID, UPDATE_DATE{row_id_columns}"
    )
    if strategy == "create_or_replace":
        if session.dialect.overwrite_uses_ctas:
            names = [name for name, _ in session.target_columns()]
            types = session.column_types(session.target)
            managed = {
                "pipeline_run_id": "CAST(:pipeline_run_id AS BIGINT) AS PIPELINE_RUN_ID",
                "task_run_id": "CAST(:task_run_id AS BIGINT) AS TASK_RUN_ID",
                "pipeline_id": "CAST(:pipeline_id AS BIGINT) AS PIPELINE_ID",
                "update_date": f"CAST(:now AS {types['update_date']}) AS UPDATE_DATE",
                "row_id": "CAST(ROW_NUMBER() OVER (ORDER BY NULL) AS BIGINT) AS ROW_ID",
            }
            expressions = [
                managed.get(name.lower(), f"CAST({name} AS {types[name.lower()]}) AS {name}")
                for name in names
            ]
            ddl = session.dialect.replacement_ddl(
                session.conn,
                session.target,
                f"SELECT {', '.join(expressions)} FROM {stage}",
                {},
                existing=True,
            )
        else:
            ddl = session.dialect.overwrite_statement(session.target, written_columns, select_sql)
        fault_point("sql.replace.before_publish")
        session.run(ddl, action.stamp, step="atomically overwrite the target")
    else:
        guard = (
            recover_overwrite(session)
            if strategy == "copy_and_restore"
            else contextlib.nullcontext()
        )
        with guard:
            fault_point("sql.replace.before_publish")
            session.run(f"TRUNCATE TABLE {session.target}", step="empty the target")
            fault_point("sql.replace.after_clear")
            session.run(
                f"INSERT INTO {session.target} ({written_columns}) {select_sql}",
                action.stamp,
                step="insert the SELECT's rows",
            )
            fault_point("sql.replace.after_publish")
    cleanup(session, stage)
    return HandlerResult(source_count=source, target_count=source, insert_count=source)


def append_table(session: Session, action: ActionContext) -> HandlerResult:
    """Replace this task run's appended batch, retaining rows from other loads.

    TASK_RUN_ID makes retries converge after an insert committed without its task outcome, so a
    target without it is refused before anything is staged, naming the command that adds it.
    """
    present = {name.lower() for name, _ in require_target(session)}
    if "task_run_id" not in present:
        raise SqlGuardError(
            f"{session.target} has no TASK_RUN_ID column, so retrying an append could duplicate "
            "rows; add it with `etl-craft upgrade-targets --action APPEND_TABLE --target "
            f"{session.target_object}`"
        )
    check_identity_types(session, tuple(c for c in IDENTITY_COLUMNS if c.lower() in present))
    stage = build_stage(session, action.select_sql)
    source = session.count(f"SELECT COUNT(*) FROM {stage}", step="source rows")
    session.run(
        f"DELETE FROM {session.target} WHERE TASK_RUN_ID = :task_run_id",
        {"task_run_id": action.context.task_run_id},
        step="clear this task run's previous append",
    )
    fault_point("sql.append.after_delete")
    columns = ", ".join(name for name, _ in session.columns(stage))
    row_id_columns, row_id_values = session.row_id_insert_parts()
    pipeline_column = ", PIPELINE_ID" if "pipeline_id" in present else ""
    pipeline_value = ", :pipeline_id" if pipeline_column else ""
    session.run(
        f"INSERT INTO {session.target} "
        f"({columns}, PIPELINE_RUN_ID, CREATE_DATE, TASK_RUN_ID{pipeline_column}{row_id_columns}) "
        f"SELECT {columns}, :pipeline_run_id, :now, :task_run_id{pipeline_value}{row_id_values} "
        f"FROM {stage}",
        action.stamp,
        step="append the SELECT's rows",
    )
    fault_point("sql.append.after_insert")
    session.drop(stage)
    target_count = session.count(f"SELECT COUNT(*) FROM {session.target}", step="target rows")
    return HandlerResult(source_count=source, target_count=target_count, insert_count=source)


def _merge_stage(session: Session, action: ActionContext, kind: SqlAction) -> tuple[str, int]:
    """Stage, de-duplicate, check and hash the SELECT for a merge; return (stage, source rows)."""
    task = action.task
    check_target_audit(session, kind)
    with action.engine_db.connect() as conn:
        version = fetch_hash_version(conn, session.target)
    if version != 2:
        raise SqlGuardError(
            f"{session.target} has hash version {version or 'unknown'}; "
            f"run `etl-craft rehash --target {session.target_object}` before merging"
        )
    stage = build_stage(session, action.select_sql)
    source = session.count(f"SELECT COUNT(*) FROM {stage}", step="source rows")
    refuse_null_keys(session, stage, task.merge_key)
    stage = dedupe(session, stage, task.merge_key, task.dedupe_order)
    check_or_evolve(session, stage, kind, schema_evolution=task.schema_evolution)
    add_hash_key(session, stage, task.merge_compare_columns)
    session.prepare_update_stage(stage, task.merge_key)
    return stage, source


def _changed_keys(session: Session, stage: str, merge_key: tuple[str, ...], condition: str) -> str:
    """Materialize the merge keys whose target row meets ``condition``; return the table.

    Keep changed keys available after SCD2 closes their old active versions, and count each
    key once even when its target has history.
    """
    changed = session.scratch("changed_keys")
    key_match = _key_match(merge_key)
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
    key_match = _key_match(task.merge_key)

    def kept_hash(alias: str) -> str:
        return session.hash(
            [f"COALESCE(s.{c}, {alias}.{c})" for c in task.merge_compare_columns],
            task.merge_compare_columns,
        )

    compared = kept_hash("t") if task.preserve_target else "s.HASH_KEY"
    condition = f"(t.HASH_KEY IS DISTINCT FROM {compared} OR t.DELETE_FLAG = 'Y')"
    # A soft-deleted key the SELECT returns again comes back, changed or not.
    changed = _changed_keys(
        session,
        stage,
        task.merge_key,
        condition,
    )
    update_count = session.count(f"SELECT COUNT(*) FROM {changed}", step="changed rows")

    assignments = {}
    for column in non_key:
        value = f"s.{column}"
        if task.preserve_target:
            value = (
                kept_hash("t") if column.lower() == "hash_key" else f"COALESCE({value}, t.{column})"
            )
        assignments[column] = value
    assignments.update(
        PIPELINE_RUN_ID=":pipeline_run_id",
        TASK_RUN_ID=":task_run_id",
        PIPELINE_ID=":pipeline_id",
        UPDATE_DATE=":now",
        UPDATED_BY=":updated_by",
        DELETE_FLAG="'N'",
    )
    session.run(
        session.dialect.update_from_stage(target, stage, task.merge_key, assignments, condition),
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
        f"INSERT INTO {target} ({columns}, PIPELINE_RUN_ID, PIPELINE_ID, TASK_RUN_ID, "
        f"CREATE_DATE, CREATED_BY, "
        f"UPDATE_DATE, UPDATED_BY, DELETE_FLAG{row_id_columns}) "
        f"SELECT {columns}, :pipeline_run_id, :pipeline_id, :task_run_id, "
        f":now, :updated_by, :now, :updated_by, "
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
    key_match = _key_match(task.merge_key)

    changed = _changed_keys(
        session,
        stage,
        task.merge_key,
        # A soft-deleted active version is closed and followed by a live one, as a change is.
        "t.ACTIVE_FLAG = 'Y' AND (t.HASH_KEY IS DISTINCT FROM s.HASH_KEY OR t.DELETE_FLAG = 'Y')",
    )
    closed = session.count(f"SELECT COUNT(*) FROM {changed}", step="changed keys")
    session.prepare_update_stage(changed, task.merge_key)
    session.run(
        session.dialect.update_from_stage(
            target,
            changed,
            task.merge_key,
            {
                "ACTIVE_FLAG": "'N'",
                "UPDATE_DATE": ":now",
                "UPDATED_BY": ":updated_by",
                "PIPELINE_RUN_ID": ":pipeline_run_id",
                "TASK_RUN_ID": ":task_run_id",
                "PIPELINE_ID": ":pipeline_id",
            },
            "t.ACTIVE_FLAG = 'Y'",
        ),
        action.stamp,
        step="close the active version of changed keys",
    )

    columns = ", ".join(stage_columns)
    insert_head = (
        f"INSERT INTO {target} ({columns}, PIPELINE_RUN_ID, PIPELINE_ID, TASK_RUN_ID, "
        f"CREATE_DATE, CREATED_BY, "
        "UPDATE_DATE, UPDATED_BY, DELETE_FLAG, ACTIVE_FLAG{row_id_columns}) "
        f"SELECT {columns}, :pipeline_run_id, :pipeline_id, :task_run_id, "
        f":now, :updated_by, :now, :updated_by, 'N', "
        "'Y'{row_id_values} "
    )
    row_id_columns, row_id_values = session.row_id_insert_parts()
    stage_changed = _key_match(task.merge_key, "s", "ck")
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
        raise SqlGuardError(
            f"DROP_TABLE refused for {target_object}: no other active task in this pipeline "
            "creates it with SQL_ACTION=CREATE_TABLE, and DROP_TABLE removes only tables its "
            "own pipeline creates"
        )
    if status != RunStatus.SUCCESS:
        raise SqlGuardError(
            f"DROP_TABLE refused for {target_object}: the task that creates it "
            f"({creator.task_code}) is {status or 'not run'} under pipeline_run_id="
            f"{context.pipeline_run_id}, not SUCCESS; make the drop depend on it"
        )
    if not session.target_columns():
        logger.info("%s does not exist; nothing to drop", session.target)
        return HandlerResult()
    session.clear_hash_version = True
    session.run(f"DROP TABLE {session.target}", step="drop the target")
    return HandlerResult()


def delete_rows(session: Session, action: ActionContext) -> HandlerResult:
    """Delete, or flag ``DELETE_FLAG='Y'``, the target rows whose merge key the SELECT returns."""
    task = action.task
    stage = build_stage(session, action.select_sql)
    source = session.count(f"SELECT COUNT(*) FROM {stage}", step="source rows")
    target = session.target
    require_target(session)
    refuse_null_keys(session, stage, task.merge_key)
    if not task.hard_delete:
        have = {name.lower() for name, _ in session.target_columns()}
        missing = [
            c
            for c in (
                "PIPELINE_RUN_ID",
                "PIPELINE_ID",
                "TASK_RUN_ID",
                "DELETE_FLAG",
                "UPDATE_DATE",
                "UPDATED_BY",
            )
            if c.lower() not in have
        ]
        if missing:
            raise SqlGuardError(
                f"{target} lacks {', '.join(missing)}, which a soft DELETE_ROWS sets; run a "
                "SETUP_TABLE task for it, add the columns, or set HARD_DELETE=true"
            )
        check_identity_types(session)
    key_match = _key_match(task.merge_key)
    # A soft delete leaves rows already flagged as they were, with their first UPDATE_DATE.
    live = "" if task.hard_delete else " AND (t.DELETE_FLAG IS NULL OR t.DELETE_FLAG <> 'Y')"
    delete_count = session.count(
        f"SELECT COUNT(*) FROM {target} t WHERE EXISTS (SELECT 1 FROM {stage} s WHERE {key_match})"
        f"{live}",
        step="rows to delete",
    )
    mutation, q = session.mutation_target()
    match = _key_match(task.merge_key, q)
    if task.hard_delete:
        session.run(
            f"DELETE FROM {mutation} WHERE EXISTS (SELECT 1 FROM {stage} s WHERE {match})",
            step="delete the rows",
        )
    else:
        session.run(
            f"UPDATE {mutation} SET DELETE_FLAG = 'Y', UPDATE_DATE = :now, "
            f"PIPELINE_RUN_ID = :pipeline_run_id, PIPELINE_ID = :pipeline_id, "
            f"TASK_RUN_ID = :task_run_id, "
            f"UPDATED_BY = :updated_by WHERE EXISTS (SELECT 1 FROM {stage} s WHERE {match}) "
            f"AND ({q}.DELETE_FLAG IS NULL OR {q}.DELETE_FLAG <> 'Y')",
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


def _key_match(keys: tuple[str, ...], target: str = "t", source: str = "s") -> str:
    return " AND ".join(f"{target}.{key} = {source}.{key}" for key in keys)
