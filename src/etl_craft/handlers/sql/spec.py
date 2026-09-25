"""A SQL task's parameters, read and checked before anything touches the warehouse.

Every mistake in a task's definition fails the task here with the parameter, its value and the
remedy, so no action ever starts on a half-valid definition.

- ``SQL_ACTION``: one of the eight actions.
- ``TARGET_OBJECT``: ``schema.table``, in the active warehouse profile's database, or
  ``database.schema.table``.
- ``SOURCE_SQL`` or ``SOURCE_SQL_FILE`` (every action but ``DROP_TABLE``): the read-only
  SELECT, inline or as a file under ``sql_files/``; exactly one of them.
- ``PIPELINE_ID_SUBSTITUTION``: ``true`` replaces ``$$pipeline_id`` with the run id.
- ``PIPELINE_ID_FILTER``: ``true`` replaces ``$$pipeline_id_filter`` with
  ``pipeline_run_id = <id>``, or ``1=1`` on a FULL refresh.
- ``MERGE_KEY`` (``SCD1_MERGE``, ``SCD2_MERGE``, ``DELETE_ROWS``): ``|``-separated key columns.
- ``MERGE_COMPARE_COLUMNS`` (the merges): ``|``-separated columns hashed into ``HASH_KEY``.
- ``MERGE_DEDUPE_ORDER`` (the merges): ``ORDER BY`` terms choosing the row kept per key.
- ``SCHEMA_EVOLUTION`` (``OVERWRITE_TABLE`` and the merges): ``true`` adds new SELECT columns
  to the target.
- ``PRESERVE_TARGET`` (``SCD1_MERGE``): ``true`` keeps a target value where the source is NULL.
- ``HARD_DELETE`` (``DELETE_ROWS``): ``true`` deletes rows; otherwise they get
  ``DELETE_FLAG='Y'``.
- ``SETUP_FOR`` (``SETUP_TABLE``): the action that writes the table, whose audit columns it
  gets; needed when no other task in the pipeline writes it.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from etl_craft.config.project import sql_file
from etl_craft.core.enums import SqlAction
from etl_craft.core.errors import HandlerError, MetadataError
from etl_craft.core.text import (
    is_safe_identifier,
    is_safe_object_ref,
    is_safe_order_term,
    read_only_problem,
    split_statements,
    substitute_pipeline_id,
    suggest,
)
from etl_craft.handlers.registry import TaskContext

MERGES = frozenset({SqlAction.SCD1_MERGE, SqlAction.SCD2_MERGE})
WRITERS = frozenset(
    {
        SqlAction.CREATE_TABLE,
        SqlAction.OVERWRITE_TABLE,
        SqlAction.APPEND_TABLE,
        SqlAction.SCD1_MERGE,
        SqlAction.SCD2_MERGE,
    }
)
"""The actions that write rows into their target, and so have audit columns of their own."""
KEYED = MERGES | {SqlAction.DELETE_ROWS}

# Each yes/no parameter, and the actions it applies to.
FLAGS: dict[str, frozenset[SqlAction]] = {
    "SCHEMA_EVOLUTION": frozenset({SqlAction.OVERWRITE_TABLE, *MERGES}),
    "PRESERVE_TARGET": frozenset({SqlAction.SCD1_MERGE}),
    "HARD_DELETE": frozenset({SqlAction.DELETE_ROWS}),
}


@dataclass(frozen=True)
class SqlTask:
    """A SQL task's checked definition.

    ``select_sql`` is the SELECT with its tokens replaced, or ``None`` for ``DROP_TABLE``;
    ``source`` names where it came from, for messages.
    """

    action: SqlAction
    target_object: str
    select_sql: str | None
    source: str
    merge_key: tuple[str, ...] = ()
    merge_compare_columns: tuple[str, ...] = ()
    dedupe_order: str | None = None
    schema_evolution: bool = False
    preserve_target: bool = False
    hard_delete: bool = False
    setup_for: SqlAction | None = None


def read_sql_task(context: TaskContext) -> SqlTask:
    """Read and check the SQL task's parameters; ``HandlerError`` or ``MetadataError`` if wrong."""
    params = context.task_params
    action = _action(params)
    target = (params.get("TARGET_OBJECT") or "").strip()
    if not target:
        raise HandlerError(f"TARGET_OBJECT is required for SQL_ACTION={action}")
    if not is_safe_object_ref(target):
        raise HandlerError(
            f"TARGET_OBJECT={target!r} must be 'schema.table' or 'database.schema.table', "
            "letters, digits and underscores only; without a database, the active Warehouse "
            "profile's is used"
        )
    flags = {name: _flag(params, name, action, applies) for name, applies in FLAGS.items()}

    select_sql, source = None, "none"
    if action == SqlAction.DROP_TABLE:
        given = [name for name in ("SOURCE_SQL", "SOURCE_SQL_FILE") if params.get(name)]
        if given:
            raise HandlerError(f"SQL_ACTION=DROP_TABLE takes no SELECT, but {given[0]} is set")
    else:
        select_sql, source = _select(context)

    merge_key: tuple[str, ...] = ()
    if action in KEYED:
        merge_key = _columns(params, "MERGE_KEY", action)
    compare: tuple[str, ...] = ()
    dedupe_order = None
    if action in MERGES:
        compare = _columns(params, "MERGE_COMPARE_COLUMNS", action)
        dedupe_order = _dedupe_order(params.get("MERGE_DEDUPE_ORDER"))
    else:
        for name in ("MERGE_COMPARE_COLUMNS", "MERGE_DEDUPE_ORDER"):
            if params.get(name):
                raise HandlerError(
                    f"{name} applies only to SCD1_MERGE and SCD2_MERGE, not {action}"
                )
    if action not in KEYED and params.get("MERGE_KEY"):
        raise HandlerError(f"MERGE_KEY does not apply to SQL_ACTION={action}")
    setup_for = _setup_for(params.get("SETUP_FOR"), action)

    return SqlTask(
        action=action,
        target_object=target,
        select_sql=select_sql,
        source=source,
        merge_key=merge_key,
        merge_compare_columns=compare,
        dedupe_order=dedupe_order,
        schema_evolution=flags["SCHEMA_EVOLUTION"],
        preserve_target=flags["PRESERVE_TARGET"],
        hard_delete=flags["HARD_DELETE"],
        setup_for=setup_for,
    )


def _action(params: Mapping[str, str]) -> SqlAction:
    written = (params.get("SQL_ACTION") or "").strip()
    actions = [member.value for member in SqlAction]
    if written.upper() in actions:
        return SqlAction(written.upper())
    hints = suggest(written.upper(), actions)
    hint = f" — did you mean: {', '.join(hints)}" if hints else ""
    raise HandlerError(
        f"SQL_ACTION={written!r} is not one of {', '.join(actions)}{hint}"
        if written
        else f"SQL_ACTION is required; one of {', '.join(actions)}"
    )


def parse_flag(params: Mapping[str, str], name: str) -> bool:
    """Read a yes/no parameter: ``true`` or ``false`` in any case; absent means false."""
    value = params.get(name)
    if value is None or not value.strip():
        return False
    lowered = value.strip().lower()
    if lowered not in {"true", "false"}:
        raise HandlerError(f"{name}={value!r} must be true or false")
    return lowered == "true"


def _flag(
    params: Mapping[str, str], name: str, action: SqlAction, applies: frozenset[SqlAction]
) -> bool:
    enabled = parse_flag(params, name)
    if name in params and action not in applies:
        names = ", ".join(sorted(applies))
        raise HandlerError(f"{name} applies only to {names}, not SQL_ACTION={action}")
    return enabled


def _select(context: TaskContext) -> tuple[str, str]:
    """Return the task's SELECT with its tokens replaced, and where it came from."""
    params = context.task_params
    inline = params.get("SOURCE_SQL")
    file_name = params.get("SOURCE_SQL_FILE")
    if inline and file_name:
        raise HandlerError(
            "set SOURCE_SQL or SOURCE_SQL_FILE, not both: the task would have two SELECTs"
        )
    if file_name:
        path = sql_file(context.config, file_name)
        try:
            raw = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as error:
            raise MetadataError(
                f"SOURCE_SQL_FILE={file_name!r}: cannot read {path}: {error}"
            ) from error
        source = f"SOURCE_SQL_FILE={file_name!r}"
    elif inline:
        raw, source = inline, "SOURCE_SQL"
    else:
        raise HandlerError(
            f"SQL_ACTION={params.get('SQL_ACTION')} needs a SELECT: set SOURCE_SQL, or "
            "SOURCE_SQL_FILE naming a file under sql_files/"
        )
    # Tokens first: a ``$$`` would otherwise read as a dollar-quoted string when splitting.
    substituted = substitute_pipeline_id(
        raw,
        pipeline_run_id=context.pipeline_run_id,
        refresh_type=context.refresh_type,
        substitution=parse_flag(params, "PIPELINE_ID_SUBSTITUTION"),
        filter_enabled=parse_flag(params, "PIPELINE_ID_FILTER"),
        source=source,
    )
    statements = split_statements(substituted)
    if len(statements) != 1:
        raise HandlerError(
            f"{source} must hold exactly one SELECT; it holds {len(statements)} statements"
        )
    select_sql = statements[0]
    problem = read_only_problem(select_sql)
    if problem is not None:
        raise HandlerError(
            f"{source} must be a read-only SELECT (the engine writes the target itself); it "
            f"{problem}"
        )
    return select_sql, source


def _columns(params: Mapping[str, str], name: str, action: SqlAction) -> tuple[str, ...]:
    value = params.get(name) or ""
    columns = tuple(part.strip() for part in value.split("|") if part.strip())
    if not columns:
        raise HandlerError(f"{name} is required for SQL_ACTION={action}: '|'-separated columns")
    bad = [column for column in columns if not is_safe_identifier(column)]
    if bad:
        raise HandlerError(
            f"{name}={value!r}: {', '.join(repr(b) for b in bad)} is not a plain column name"
        )
    return columns


def _setup_for(value: str | None, action: SqlAction) -> SqlAction | None:
    if value is None or not value.strip():
        return None
    if action != SqlAction.SETUP_TABLE:
        raise HandlerError(f"SETUP_FOR applies only to SETUP_TABLE, not SQL_ACTION={action}")
    written = value.strip().upper()
    if written not in WRITERS:
        raise HandlerError(
            f"SETUP_FOR={value!r} must name the action that writes the table: "
            f"{', '.join(sorted(WRITERS))}"
        )
    return SqlAction(written)


def _dedupe_order(value: str | None) -> str | None:
    if value is None or not value.strip():
        return None
    terms = [term.strip() for term in value.split(",")]
    bad = [term for term in terms if not is_safe_order_term(term)]
    if bad:
        raise HandlerError(
            f"MERGE_DEDUPE_ORDER={value!r}: {', '.join(repr(b) for b in bad)} is not "
            "'column [ASC|DESC] [NULLS FIRST|LAST]'"
        )
    return ", ".join(terms)
