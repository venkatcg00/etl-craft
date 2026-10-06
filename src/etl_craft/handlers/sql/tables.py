"""Target tables: creating their shape, their ROW_ID key, and keeping them in step with the SELECT.

Every table the engine creates carries, after the SELECT's own columns, ``PIPELINE_RUN_ID``,
the audit columns of its action (``AUDIT_COLUMNS``) and ``ROW_ID``, a generated key that business
rules reference. Only ``CREATE_TABLE`` and ``SETUP_TABLE`` create tables; every other action
needs its target to exist already, created by a ``SETUP_TABLE`` task or by hand.

Before ``OVERWRITE_TABLE`` and the merges write a row, the target is checked against the SELECT:

- a target missing one of the action's engine-managed columns fails, naming them;
- a SELECT missing a column the target has fails: columns are never dropped;
- a SELECT with a new column fails unless ``SCHEMA_EVOLUTION`` is true, when the target is
  extended with nullable columns at the end, preserving existing rows and properties.

Where the warehouse has transactional DDL (PostgreSQL, DuckDB) an action's statements commit or
roll back together. Trino and the cloud warehouses commit each statement, so an action that
fails part-way can leave its work half done; every action re-derives its effect from the
target's current state, so running the task again converges.
"""

from __future__ import annotations

import re
from collections.abc import Sequence

from etl_craft.core.enums import SqlAction
from etl_craft.core.errors import HandlerError
from etl_craft.core.text import as_subquery
from etl_craft.handlers.sql.session import ROW_ID_COLUMN, Session

IDENTITY_COLUMNS = ("PIPELINE_ID", "PIPELINE_RUN_ID", "TASK_RUN_ID")

AUDIT_COLUMNS: dict[str, tuple[str, ...]] = {
    SqlAction.CREATE_TABLE: ("PIPELINE_ID", "TASK_RUN_ID"),
    SqlAction.OVERWRITE_TABLE: ("PIPELINE_ID", "TASK_RUN_ID", "UPDATE_DATE"),
    SqlAction.APPEND_TABLE: ("PIPELINE_ID", "TASK_RUN_ID", "CREATE_DATE"),
    SqlAction.SCD1_MERGE: (
        "PIPELINE_ID",
        "TASK_RUN_ID",
        "HASH_KEY",
        "CREATE_DATE",
        "CREATED_BY",
        "UPDATE_DATE",
        "UPDATED_BY",
        "DELETE_FLAG",
    ),
    SqlAction.SCD2_MERGE: (
        "PIPELINE_ID",
        "TASK_RUN_ID",
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


ENGINE_COLUMNS = frozenset(
    name.lower()
    for name in (
        "PIPELINE_RUN_ID",
        ROW_ID_COLUMN,
        *(column for columns in AUDIT_COLUMNS.values() for column in columns),
    )
)
"""The columns etl-craft writes itself, lower case; a SELECT may not return them."""

_PLAIN_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def build_stage(session: Session, select_sql: str, *, empty: bool = False) -> str:
    """Materialize the SELECT once into this task run's stage table; return its name.

    Every later statement reads the stage, so the SELECT runs exactly once. ``empty`` keeps
    only its shape. The stage's columns are checked before anything reads it (see
    ``check_stage_columns``).
    """
    stage = session.scratch("stage")
    body = f"SELECT * FROM {as_subquery(select_sql)} etl_src WHERE 1 = 0" if empty else select_sql
    session.create_scratch(stage, body, step="stage the SELECT")
    check_stage_columns(session, stage)
    return stage


def check_stage_columns(session: Session, stage: str) -> None:
    """Refuse a SELECT whose columns the engine cannot write; ``HandlerError`` naming them.

    A column etl-craft writes itself (``ENGINE_COLUMNS``), as a ``SELECT *`` over a table the
    engine wrote returns, would clash with the engine's own value. A column whose name needs
    quoting could not be named in the statements the engine writes: one that is not a plain
    identifier, or, where the warehouse folds unquoted names (``identifier_case``), one that
    is not in that case, such as ``"CustomerId"`` on PostgreSQL.
    """
    names = [name for name, _ in session.columns(stage)]
    reserved = [name for name in names if name.lower() in ENGINE_COLUMNS]
    if reserved:
        raise HandlerError(
            f"{session.action} {session.target}: the SELECT returns {', '.join(reserved)}, which "
            "etl-craft writes itself; list the columns you need instead of `*`, or alias them"
        )
    case = session.dialect.identifier_case
    folded = {"lower": str.lower, "upper": str.upper}.get(case or "")
    quoted = [
        name
        for name in names
        if not _PLAIN_IDENTIFIER.fullmatch(name) or (folded is not None and name != folded(name))
    ]
    if quoted:
        raise HandlerError(
            f"{session.action} {session.target}: the SELECT returns "
            f"{', '.join(repr(name) for name in quoted)}, which need(s) quoting; alias each to a "
            f"plain name, for example `AS {_plain(quoted[0])}`"
        )


def _plain(name: str) -> str:
    """Suggest a plain, lower-case alias for ``name``."""
    snake = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)
    cleaned = re.sub(r"[^A-Za-z0-9_]+", "_", snake).strip("_").lower()
    return cleaned if cleaned and not cleaned[0].isdigit() else f"c_{cleaned}"


def create_target_shape(session: Session, stage: str, audit_columns: tuple[str, ...]) -> None:
    """Create the target, empty: the stage's columns, ``PIPELINE_RUN_ID``, ``audit_columns``."""
    if session.dialect.identity_in_create:
        types = session.column_types(stage)
        columns = [f"{name} {types[name.lower()]}" for name, _ in session.columns(stage)]
        columns.append("PIPELINE_RUN_ID BIGINT")
        columns.extend(
            f"{name} {session.dialect.audit_column_type(name)}" for name in audit_columns
        )
        session.run(
            session.dialect.identity_table_ddl(session.target, ", ".join(columns), session.params),
            step="create the target with an identity ROW_ID",
        )
        return
    parts = [f"s.{name}" for name, _ in session.columns(stage)]
    parts.append("CAST(NULL AS BIGINT) AS PIPELINE_RUN_ID")
    parts.extend(
        f"CAST(NULL AS {session.dialect.audit_column_type(column)}) AS {column}"
        for column in audit_columns
    )
    computed = session.dialect.surrogate_key == "computed"
    if computed:
        # Each insert fills a computed ROW_ID, so the empty table only needs the column.
        parts.append(f"CAST(NULL AS BIGINT) AS {ROW_ID_COLUMN}")
    session.create_table_as(
        session.target,
        f"SELECT {', '.join(parts)} FROM {stage} s WHERE 1 = 0",
        step="create the target",
    )
    if not computed:
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


def check_target_audit(session: Session, action: str) -> list[tuple[str, str]]:
    """Refuse an absent target or missing action-managed columns before any write."""
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
    check_identity_types(session)
    return target_columns


def check_or_evolve(session: Session, stage: str, action: str, *, schema_evolution: bool) -> None:
    """Check the existing target against the stage, adding new columns when allowed."""
    target_columns = check_target_audit(session, action)
    required = ("PIPELINE_RUN_ID", *AUDIT_COLUMNS[action])
    managed = {column.lower() for column in required} | {ROW_ID_COLUMN.lower()}
    stage_columns = session.columns(stage)
    stage_names = [name for name, _ in stage_columns]
    stage_set = {name.lower() for name in stage_names}
    business = [name for name, _ in target_columns if name.lower() not in managed]
    business_set = {name.lower() for name in business}
    missing = [name for name in business if name.lower() not in stage_set]
    if missing:
        raise HandlerError(
            f"{session.target} has column(s) {', '.join(missing)} that the SELECT no longer "
            "returns; columns are never dropped, so return them (NULL if need be)"
        )
    if schema_evolution:
        stage_types = session.column_types(stage)
        target_types = session.column_types(session.target)
        for name in business:
            source_type = stage_types[name.lower()]
            target_type = target_types[name.lower()]
            if not session.dialect.same_column_type(source_type, target_type):
                raise HandlerError(
                    f"{session.target}.{name}: SCHEMA_EVOLUTION cannot change the existing type "
                    f"{target_type} to the SELECT's type {source_type}; cast the SELECT to "
                    "the target type or migrate the column explicitly"
                )
    else:
        stage_types = {}
    new = [name for name in stage_names if name.lower() not in business_set]
    if not new:
        return
    if not schema_evolution:
        raise HandlerError(
            f"the SELECT returns new column(s) {', '.join(new)} that {session.target} does not "
            "have; set SCHEMA_EVOLUTION=true to add them"
        )
    evolve(session, [(name, stage_types[name.lower()]) for name in new])


def evolve(session: Session, columns: list[tuple[str, str]]) -> None:
    """Append nullable columns without rebuilding rows, keys, properties or dependencies."""
    columns = [
        (name, session.dialect.evolution_column_type(data_type)) for name, data_type in columns
    ]
    for name, data_type in columns:
        problem = session.dialect.column_addition_problem(data_type)
        if problem:
            raise HandlerError(f"{session.target}.{name}: {problem}")
    for name, data_type in columns:
        session.run(
            f"{session.dialect.alter_table_keyword()} {session.target} "
            f"ADD COLUMN {name} {data_type}",
            step=f"add column {name} ({data_type})",
        )


def refuse_null_keys(session: Session, stage: str, merge_key: tuple[str, ...]) -> None:
    """Fail when a staged row has a NULL in a merge key column, before the target is touched.

    A NULL key matches no target row, so a merge would insert it again on every run and a
    delete would never find it.
    """
    nulls = " OR ".join(f"{key} IS NULL" for key in merge_key)
    count = session.count(f"SELECT COUNT(*) FROM {stage} WHERE {nulls}", step="NULL merge keys")
    if count:
        raise HandlerError(
            f"{session.action} {session.target}: the SELECT returns {count} row(s) with a NULL "
            f"in MERGE_KEY ({', '.join(merge_key)}); every key column must be set: filter those "
            "rows out or COALESCE the key"
        )


def dedupe(session: Session, stage: str, merge_key: tuple[str, ...], order: str | None) -> str:
    """Return a stage with one row per merge key: this one, or a copy keeping ``order``'s first.

    Without ``MERGE_DEDUPE_ORDER``, duplicate keys fail before the target is touched: which row
    should win is the author's call, never the engine's. For the same reason an order that
    ties between rows that differ fails too; rows that are identical may tie.
    """
    keys = ", ".join(merge_key)
    duplicated = f"SELECT {keys} FROM {stage} GROUP BY {keys} HAVING COUNT(*) > 1"
    count = session.count(f"SELECT COUNT(*) FROM ({duplicated}) d", step="duplicate merge keys")
    if not count:
        return stage
    if order is None:
        sample = session.run(
            f"SELECT {keys}, COUNT(*) FROM {stage} GROUP BY {keys} HAVING COUNT(*) > 1 "
            f"ORDER BY {keys} LIMIT 5",
            step="examples of duplicate merge keys",
        ).all()
        examples = "; ".join(
            _describe_key(merge_key, row[:-1]) + f" ({row[-1]} rows)" for row in sample
        )
        raise HandlerError(
            f"the SELECT returns {count} MERGE_KEY value(s) ({keys}) more than once, "
            f"for example {examples}; a merge needs one row per key. Return one, or set "
            "MERGE_DEDUPE_ORDER (for example 'updated_at DESC') to choose which row is kept"
        )
    columns = [name for name, _ in session.columns(stage)]
    listed = ", ".join(columns)
    row_hash = session.dialect.hash_expression(list(columns))
    tied = (
        f"SELECT {keys} FROM (SELECT {keys}, {row_hash} AS etl_h, RANK() OVER (PARTITION BY "
        f"{keys} ORDER BY {order}) AS etl_r FROM {stage}) ranked WHERE etl_r = 1 "
        f"GROUP BY {keys} HAVING COUNT(DISTINCT etl_h) > 1"
    )
    ties = session.count(f"SELECT COUNT(*) FROM ({tied}) t", step="ties in MERGE_DEDUPE_ORDER")
    if ties:
        sample = session.run(f"{tied} ORDER BY {keys} LIMIT 5", step="examples of ties").all()
        examples = "; ".join(_describe_key(merge_key, row) for row in sample)
        raise HandlerError(
            f"MERGE_DEDUPE_ORDER ({order}) leaves ties between different rows for {ties} "
            f"key(s), for example {examples}; add a column to the order that breaks the tie"
        )
    deduped = session.scratch("dedup")
    session.create_scratch(
        deduped,
        f"SELECT {listed} FROM (SELECT {listed}, ROW_NUMBER() OVER (PARTITION BY {keys} "
        f"ORDER BY {order}) AS etl_rn FROM {stage}) ranked WHERE etl_rn = 1",
        step=f"keep one row per merge key by {order} ({count} key(s) had several)",
    )
    session.drop(stage)
    return deduped


def _describe_key(merge_key: tuple[str, ...], values: Sequence[object]) -> str:
    return ", ".join(f"{k}={v!r}" for k, v in zip(merge_key, values, strict=True))


def add_hash_key(session: Session, stage: str, compare_columns: tuple[str, ...]) -> None:
    """Add ``HASH_KEY`` to the stage, over ``MERGE_COMPARE_COLUMNS``.

    Added after the schema check, which compares the SELECT's own columns only.
    """
    session.run(f"ALTER TABLE {stage} ADD COLUMN HASH_KEY VARCHAR(32)", step="add HASH_KEY")
    hashed = session.hash([f"{stage}.{column}" for column in compare_columns], compare_columns)
    session.run(f"UPDATE {stage} SET HASH_KEY = {hashed}", step="hash the compare columns")


def check_identity_types(session: Session, columns: tuple[str, ...] = IDENTITY_COLUMNS) -> None:
    """Refuse identity columns that cannot hold the engine's integer ids."""
    types = session.column_types(session.target)
    for column in columns:
        kind = types[column.lower()]
        if not session.dialect.is_bigint_type(kind):
            raise HandlerError(
                f"{session.target}.{column} has type {kind}; expected BIGINT. "
                "Migrate this column explicitly before writing or upgrading"
            )
