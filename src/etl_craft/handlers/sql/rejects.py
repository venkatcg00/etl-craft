"""Rows a SQL task sets aside rather than fail on: ``REJECT_LIMIT``.

With ``REJECT_LIMIT`` set, ``APPEND_TABLE``, ``OVERWRITE_TABLE`` and the merges check every
staged row against the existing target before anything is written. A row is set aside, for the
first of these it meets and the column that failed:

1. a NULL in a merge key column;
2. a value that does not fit its target column's declared type (``reject_checks``);
3. a merge key that another row passing 1 and 2 also has, unless ``MERGE_DEDUPE_ORDER`` chooses
   between them: every row of that key is set aside, since which one wins is the author's call.

Rows set aside go to ``<target>__rejects``, created as the engine's recovery tables are: the
SELECT's columns as text, then ``TASK_RUN_ID``, ``REJECT_REASON`` and ``REJECT_COLUMN``. A task
run's rejects replace those of its earlier attempts. The other rows load, cast to the target's
types. More rows than the limit allows fail the task before anything is written.
"""

from __future__ import annotations

from etl_craft.core.errors import SqlGuardError
from etl_craft.handlers.sql.session import Session
from etl_craft.handlers.sql.spec import SqlTask
from etl_craft.handlers.sql.tables import require_target

Check = tuple[str, str, str]
"""A condition that sets a row aside, the column it tests and the reason."""


def screen(session: Session, stage: str, task: SqlTask, source: int) -> tuple[str, int | None]:
    """Set aside the staged rows the target cannot take.

    Return the stage of the rows that fit and how many rows were set aside; without
    ``REJECT_LIMIT``, the stage as it is and ``None``.
    """
    if task.reject_limit is None:
        return stage, None
    require_target(session)
    text_type = session.dialect.string_type
    types = session.column_types(session.target)
    names = [name for name, _ in session.columns(stage)]
    named = {name.lower(): name for name in names}
    keys = [named.get(key.lower(), key) for key in task.merge_key]
    checks: list[Check] = [(f"{key} IS NULL", key, "NULL in MERGE_KEY") for key in keys]
    for name in names:
        kind = types.get(name.lower())
        if kind is not None:
            checks.extend(
                (f"{name} IS NOT NULL AND {condition}", name, reason)
                for condition, reason in session.dialect.reject_checks(name, kind)
            )
    body = (
        f"SELECT s.*, {_case(checks, 2, text_type)} AS etl_reason, "
        f"{_case(checks, 1, text_type)} AS etl_column FROM {stage} s"
    )
    if keys and task.dedupe_order is None:
        listed = ", ".join(keys)
        twice = (
            "etl_reason IS NULL AND COUNT(CASE WHEN etl_reason IS NULL THEN 1 END) "
            f"OVER (PARTITION BY {listed}) > 1"
        )
        body = (
            f"SELECT {', '.join(names)}, CAST(CASE WHEN {twice} THEN 'MERGE_KEY returned more "
            f"than once' ELSE etl_reason END AS {text_type}) AS etl_reason, CAST(CASE WHEN "
            f"{twice} THEN '{listed}' ELSE etl_column END AS {text_type}) AS etl_column "
            f"FROM ({body}) c"
        )
    checked = session.scratch("checked")
    session.create_scratch(
        checked,
        body,
        step=f"check each row against the target's types with {session.dialect.safe_cast}",
    )
    rejected = session.count(
        f"SELECT COUNT(*) FROM {checked} WHERE etl_reason IS NOT NULL", step="rows set aside"
    )
    allowed = task.reject_limit.rows(source)
    if rejected > allowed:
        reasons = session.run(
            f"SELECT etl_column, etl_reason, COUNT(*) FROM {checked} WHERE etl_reason IS NOT NULL "
            "GROUP BY etl_column, etl_reason ORDER BY COUNT(*) DESC, etl_column, etl_reason "
            "LIMIT 5",
            step="the most common reasons",
        ).all()
        common = "; ".join(f"{column} {reason} ({count} rows)" for column, reason, count in reasons)
        raise SqlGuardError(
            f"{session.action} {session.target}: {rejected} of {source} row(s) do not fit, more "
            f"than REJECT_LIMIT={task.reject_limit} allows ({allowed}), so nothing was written. "
            f"Most common: {common}. Fix the source or the SELECT, or raise REJECT_LIMIT"
        )
    _save(session, checked, names, rejected)
    fits = [f"CAST({n} AS {types[n.lower()]}) AS {n}" if n.lower() in types else n for n in names]
    kept = session.scratch("kept")
    session.create_scratch(
        kept,
        f"SELECT {', '.join(fits)} FROM {checked} WHERE etl_reason IS NULL",
        step="keep the rows that fit, cast to the target's types",
    )
    session.drop(checked)
    session.drop(stage)
    return kept, rejected


def _case(checks: list[Check], part: int, text_type: str) -> str:
    """Return the first matching check's column (``part`` 1) or reason (2), else NULL."""
    if not checks:
        return f"CAST(NULL AS {text_type})"
    whens = " ".join(
        "WHEN {} THEN '{}'".format(check[0], check[part].replace("'", "''")) for check in checks
    )
    return f"CAST(CASE {whens} END AS {text_type})"


def _save(session: Session, checked: str, names: list[str], rejected: int) -> None:
    """Replace this task run's rows in ``<target>__rejects`` with the ones just set aside."""
    rejects = f"{session.target}__rejects"
    text_type = session.dialect.string_type
    present = {name.lower() for name, _ in session.columns(rejects)}
    if not present and not rejected:
        return
    if not present:
        shape = [f"CAST(NULL AS {text_type}) AS {name}" for name in names]
        shape += [
            "CAST(NULL AS BIGINT) AS TASK_RUN_ID",
            f"CAST(NULL AS {text_type}) AS REJECT_REASON",
            f"CAST(NULL AS {text_type}) AS REJECT_COLUMN",
        ]
        session.create_side_table(
            rejects,
            f"SELECT {', '.join(shape)} FROM {checked} WHERE 1 = 0",
            step=f"create {rejects}",
        )
    for name in names:
        if present and name.lower() not in present:
            session.run(
                f"ALTER TABLE {rejects} ADD COLUMN {name} {text_type}",
                step=f"add column {name} to {rejects}",
            )
    run = {"task_run_id": session.task_run_id}
    session.run(
        f"DELETE FROM {rejects} WHERE TASK_RUN_ID = :task_run_id",
        run,
        step="clear this task run's earlier rejects",
    )
    if rejected:
        values = ", ".join(f"CAST({name} AS {text_type})" for name in names)
        session.run(
            f"INSERT INTO {rejects} ({', '.join(names)}, TASK_RUN_ID, REJECT_REASON, "
            f"REJECT_COLUMN) SELECT {values}, :task_run_id, etl_reason, etl_column FROM {checked} "
            "WHERE etl_reason IS NOT NULL",
            run,
            step=f"set the rows that do not fit aside in {rejects}",
        )
