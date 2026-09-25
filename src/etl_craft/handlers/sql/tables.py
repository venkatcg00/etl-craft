"""Target tables: creating their shape, their ROW_ID key, and keeping them in step with the SELECT.

Every table the engine creates carries, after the SELECT's own columns, ``PIPELINE_RUN_ID``,
the audit columns of its action (``AUDIT_COLUMNS``) and ``ROW_ID``, a generated key that business
rules reference. Only ``CREATE_TABLE`` and ``SETUP_TABLE`` create tables; every other action
needs its target to exist already, created by a ``SETUP_TABLE`` task or by hand.

Before ``OVERWRITE_TABLE`` and the merges write a row, the target is checked against the SELECT:

- a target missing one of the action's engine-managed columns fails, naming them;
- a SELECT missing a column the target has fails: columns are never dropped;
- a SELECT with a new column fails unless ``SCHEMA_EVOLUTION`` is true, when the target is
  rebuilt with the column in the SELECT's position, existing rows NULL in it.

Where the warehouse has transactional DDL (PostgreSQL, DuckDB) an action's statements commit or
roll back together. Trino and the cloud warehouses commit each statement, so an action that
fails part-way can leave its work half done; every action re-derives its effect from the
target's current state, so running the task again converges.
"""

from __future__ import annotations

from etl_craft.core.enums import SqlAction
from etl_craft.core.errors import HandlerError
from etl_craft.handlers.sql.session import ROW_ID_COLUMN, Session

AUDIT_COLUMNS: dict[str, tuple[str, ...]] = {
    SqlAction.CREATE_TABLE: (),
    SqlAction.OVERWRITE_TABLE: ("UPDATE_DATE",),
    SqlAction.APPEND_TABLE: ("CREATE_DATE",),
    SqlAction.SCD1_MERGE: (
        "HASH_KEY",
        "CREATE_DATE",
        "CREATED_BY",
        "UPDATE_DATE",
        "UPDATED_BY",
        "DELETE_FLAG",
    ),
    SqlAction.SCD2_MERGE: (
        "HASH_KEY",
        "CREATE_DATE",
        "CREATED_BY",
        "UPDATE_DATE",
        "UPDATED_BY",
        "DELETE_FLAG",
        "ACTIVE_FLAG",
    ),
}
"""The audit columns each action adds after ``PIPELINE_RUN_ID``, in order."""


def build_stage(session: Session, select_sql: str, *, empty: bool = False) -> str:
    """Materialize the SELECT once into this task run's stage table; return its name.

    Every later statement reads the stage, so the SELECT runs exactly once. ``empty`` keeps
    only its shape.
    """
    stage = session.scratch("stage")
    body = f"SELECT * FROM ({select_sql}) etl_src WHERE 1 = 0" if empty else select_sql
    session.create_scratch(stage, body, step="stage the SELECT")
    return stage


def create_target_shape(session: Session, stage: str, audit_columns: tuple[str, ...]) -> None:
    """Create the target, empty: the stage's columns, ``PIPELINE_RUN_ID``, ``audit_columns``."""
    parts = [f"s.{name}" for name, _ in session.columns(stage)]
    parts.append("CAST(NULL AS BIGINT) AS PIPELINE_RUN_ID")
    parts.extend(
        f"CAST(NULL AS {session.dialect.audit_column_type(column)}) AS {column}"
        for column in audit_columns
    )
    session.create_table_as(
        session.target,
        f"SELECT {', '.join(parts)} FROM {stage} s WHERE 1 = 0",
        step="create the target",
    )
    add_row_id(session)


def sequence_name(session: Session) -> str:
    """Return the target's own ROW_ID sequence (DuckDB), in the target's schema."""
    return session.qualify(f"{session.schema}.etl_seq_{session.table}_row_id")


def add_row_id(session: Session) -> None:
    """Give a new target its ROW_ID key: identity, sequence default, or computed (Iceberg)."""
    strategy = session.dialect.surrogate_key
    target = session.target
    if strategy == "computed":
        # Iceberg has no identity, sequences or enforced keys: number the rows in a rebuild.
        rebuild = f"{target}__etl_rowid"
        session.drop(rebuild)
        session.create_table_as(
            rebuild,
            f"SELECT *, CAST(ROW_NUMBER() OVER (ORDER BY NULL) AS BIGINT) AS {ROW_ID_COLUMN} "
            f"FROM {target}",
            step="number the rows",
        )
        session.run(f"DROP TABLE {target}", step="replace the target")
        session.rename(rebuild, target)
        return
    if strategy == "sequence":
        sequence = sequence_name(session)
        session.run(f"DROP SEQUENCE IF EXISTS {sequence}", step="clear the ROW_ID sequence")
        session.run(f"CREATE SEQUENCE {sequence} START 1", step="create the ROW_ID sequence")
        session.run(
            f"ALTER TABLE {target} ADD COLUMN {ROW_ID_COLUMN} BIGINT DEFAULT nextval('{sequence}')",
            step="add ROW_ID",
        )
    else:
        session.run(
            f"ALTER TABLE {target} ADD COLUMN {ROW_ID_COLUMN} BIGINT GENERATED ALWAYS AS IDENTITY",
            step="add ROW_ID",
        )
    session.run(f"ALTER TABLE {target} ADD PRIMARY KEY ({ROW_ID_COLUMN})", step="key on ROW_ID")


def restore_row_id(session: Session) -> None:
    """Make a rebuilt target's carried-over ROW_ID its key again, continuing after its values."""
    has_row_id = any(name.lower() == ROW_ID_COLUMN.lower() for name, _ in session.target_columns())
    if not has_row_id:
        add_row_id(session)
        return
    strategy = session.dialect.surrogate_key
    if strategy == "computed":
        return
    target = session.target
    next_value = session.count(
        f"SELECT COALESCE(MAX({ROW_ID_COLUMN}), 0) + 1 FROM {target}", step="next ROW_ID"
    )
    if strategy == "sequence":
        sequence = sequence_name(session)
        session.run(f"DROP SEQUENCE IF EXISTS {sequence}", step="clear the ROW_ID sequence")
        session.run(
            f"CREATE SEQUENCE {sequence} START {next_value}", step="create the ROW_ID sequence"
        )
        session.run(
            f"ALTER TABLE {target} ALTER COLUMN {ROW_ID_COLUMN} SET DEFAULT nextval('{sequence}')",
            step="default ROW_ID to the sequence",
        )
    else:
        session.run(
            f"ALTER TABLE {target} ALTER COLUMN {ROW_ID_COLUMN} SET NOT NULL",
            step="ROW_ID not null",
        )
        session.run(
            f"ALTER TABLE {target} ALTER COLUMN {ROW_ID_COLUMN} ADD GENERATED ALWAYS AS IDENTITY",
            step="make ROW_ID an identity",
        )
        session.run(
            f"ALTER TABLE {target} ALTER COLUMN {ROW_ID_COLUMN} RESTART WITH {next_value}",
            step="continue ROW_ID",
        )
    session.run(f"ALTER TABLE {target} ADD PRIMARY KEY ({ROW_ID_COLUMN})", step="key on ROW_ID")


def require_target(session: Session) -> list[tuple[str, str]]:
    """Return the target's columns; ``HandlerError`` naming the remedy when it does not exist."""
    columns = session.target_columns()
    if not columns:
        raise HandlerError(
            f"{session.action}: the target {session.target} does not exist. Only CREATE_TABLE "
            "and SETUP_TABLE create tables: add a SETUP_TABLE task for it that runs first, or "
            "create it with the columns and audit columns this action writes"
        )
    return columns


def check_or_evolve(session: Session, stage: str, action: str, *, schema_evolution: bool) -> None:
    """Check the existing target against the stage, adding new columns when allowed."""
    audit = AUDIT_COLUMNS[action]
    target_columns = require_target(session)
    required = ("PIPELINE_RUN_ID", *audit)
    have = {name.lower() for name, _ in target_columns}
    missing_audit = [column for column in required if column.lower() not in have]
    if missing_audit:
        raise HandlerError(
            f"{session.target} exists but lacks {', '.join(missing_audit)}, which "
            f"SQL_ACTION={action} maintains; run a SETUP_TABLE task for it, or add the columns. "
            "SCHEMA_EVOLUTION adds only the SELECT's own columns"
        )
    managed = {column.lower() for column in required} | {ROW_ID_COLUMN.lower()}
    stage_columns = session.columns(stage)
    stage_names = [name for name, _ in stage_columns]
    stage_set = {name.lower() for name in stage_names}
    business = [name for name, _ in target_columns if name.lower() not in managed]
    business_set = {name.lower() for name in business}
    if stage_set == business_set:
        return
    missing = [name for name in business if name.lower() not in stage_set]
    if missing:
        raise HandlerError(
            f"{session.target} has column(s) {', '.join(missing)} that the SELECT no longer "
            "returns; columns are never dropped, so return them (NULL if need be)"
        )
    new = [name for name in stage_names if name.lower() not in business_set]
    if not schema_evolution:
        raise HandlerError(
            f"the SELECT returns new column(s) {', '.join(new)} that {session.target} does not "
            "have; set SCHEMA_EVOLUTION=true to add them"
        )
    evolve(session, managed, stage_columns, target_columns)


def evolve(
    session: Session,
    managed: set[str],
    stage_columns: list[tuple[str, str]],
    target_columns: list[tuple[str, str]],
) -> None:
    """Rebuild the target with the stage's column order, keeping its rows and ROW_ID values.

    A new column is NULL in existing rows. Indexes and grants on the old table are not
    carried over.
    """
    old = {name.lower() for name, _ in target_columns if name.lower() not in managed}
    parts = [
        f"t.{name}" if name.lower() in old else f"CAST(NULL AS {data_type}) AS {name}"
        for name, data_type in stage_columns
    ]
    parts.extend(f"t.{name}" for name, _ in target_columns if name.lower() in managed)
    added = [name for name, _ in stage_columns if name.lower() not in old]
    rebuild = session.qualify(f"{session.schema}.{session.table}__etl_evolve")
    session.drop(rebuild)
    session.create_table_as(
        rebuild,
        f"SELECT {', '.join(parts)} FROM {session.target} t",
        step=f"rebuild the target with new column(s) {', '.join(added)}",
    )
    session.run(f"DROP TABLE {session.target}", step="replace the target")
    session.rename(rebuild, session.target)
    restore_row_id(session)


def dedupe(session: Session, stage: str, merge_key: tuple[str, ...], order: str | None) -> str:
    """Return a stage with one row per merge key: this one, or a copy keeping ``order``'s first.

    Without ``MERGE_DEDUPE_ORDER``, duplicate keys fail before the target is touched: which row
    should win is the author's call, never the engine's.
    """
    keys = ", ".join(merge_key)
    duplicates = session.run(
        f"SELECT {keys}, COUNT(*) FROM {stage} GROUP BY {keys} HAVING COUNT(*) > 1",
        step="look for duplicate merge keys",
    ).all()
    if not duplicates:
        return stage
    if order is None:
        sample = "; ".join(
            ", ".join(f"{k}={v!r}" for k, v in zip(merge_key, row[:-1], strict=True))
            + f" ({row[-1]} rows)"
            for row in duplicates[:5]
        )
        raise HandlerError(
            f"the SELECT returns {len(duplicates)} MERGE_KEY value(s) ({keys}) more than once, "
            f"for example {sample}; a merge needs one row per key. Return one, or set "
            "MERGE_DEDUPE_ORDER (for example 'updated_at DESC') to choose which row is kept"
        )
    columns = ", ".join(name for name, _ in session.columns(stage))
    deduped = session.scratch("dedup")
    session.create_scratch(
        deduped,
        f"SELECT {columns} FROM (SELECT {columns}, ROW_NUMBER() OVER (PARTITION BY {keys} "
        f"ORDER BY {order}) AS etl_rn FROM {stage}) ranked WHERE etl_rn = 1",
        step=f"keep one row per merge key by {order} ({len(duplicates)} key(s) had several)",
    )
    session.drop(stage)
    return deduped


def add_hash_key(session: Session, stage: str, compare_columns: tuple[str, ...]) -> None:
    """Add ``HASH_KEY`` to the stage, over ``MERGE_COMPARE_COLUMNS``.

    Added after the schema check, which compares the SELECT's own columns only.
    """
    session.run(f"ALTER TABLE {stage} ADD COLUMN HASH_KEY VARCHAR(32)", step="add HASH_KEY")
    hashed = session.dialect.hash_expression([f"{stage}.{column}" for column in compare_columns])
    session.run(f"UPDATE {stage} SET HASH_KEY = {hashed}", step="hash the compare columns")
