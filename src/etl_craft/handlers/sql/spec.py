"""A SQL task's parameters, read and checked before anything touches the warehouse.

Every mistake in a task's definition fails the task here with the parameter, its value and the
remedy, so no action ever starts on a half-valid definition.

- ``SQL_ACTION``: one of the nine actions.
- ``TARGET_OBJECT``: ``schema.table``, in the active warehouse profile's database, or
  ``database.schema.table``.
- ``SOURCE_SQL`` or ``SOURCE_SQL_FILE`` (every action but ``DROP_TABLE``): the read-only
  SELECT, inline or as a file under ``sql_files/``; exactly one of them.
- ``PIPELINE_RUN_ID_SUBSTITUTION``: ``true`` replaces ``$$pipeline_run_id`` with the run id.
- ``PIPELINE_RUN_ID_FILTER``: ``true`` replaces ``$$pipeline_run_id_filter`` with
  ``pipeline_run_id = <id>``, or ``1=1`` on a FULL refresh.
- ``RUN_DATE_SUBSTITUTION``: ``true`` replaces ``$$run_date`` with the date the run runs as of,
  ``DATE 'YYYY-MM-DD'``: the day it started, or the date of a backfill run.
- ``MERGE_KEY`` (``SCD1_MERGE``, ``SCD2_MERGE``, ``DELETE_ROWS``): ``|``-separated key columns.
- ``MERGE_COMPARE_COLUMNS`` (the merges): ordered ``|``-separated scalar columns hashed into
  version-2 ``HASH_KEY``; floating point is refused, and decimals require a declared scale.
- ``MERGE_DEDUPE_ORDER`` (the merges): ``ORDER BY`` terms choosing the row kept per key.
- ``SCHEMA_EVOLUTION`` (``OVERWRITE_TABLE`` and the merges): ``true`` adds new SELECT columns
  to the target.
- ``PRESERVE_TARGET`` (``SCD1_MERGE``): ``true`` keeps a target value where the source is NULL.
- ``HARD_DELETE`` (``DELETE_ROWS``): ``true`` deletes rows; otherwise they get
  ``DELETE_FLAG='Y'``.
- ``SETUP_FOR`` (``SETUP_TABLE``): the action that writes the table, whose audit columns it
  gets; needed when no other task in the pipeline writes it.
- ``SECURE_VIEW`` (``CREATE_VIEW``): ``true`` creates a secure view where the warehouse has
  them (Snowflake's ``SECURE``, PostgreSQL's ``security_barrier``). A view stores no data, so
  it takes no ``TABLE_FORMAT`` or storage parameter.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date

from etl_craft.config import ConnectorConfig
from etl_craft.config.project import sql_file
from etl_craft.core.enums import SqlAction
from etl_craft.core.errors import MetadataError, SqlGuardError
from etl_craft.core.text import (
    LINEAGE_RUN_DATE,
    is_safe_identifier,
    is_safe_object_ref,
    is_safe_order_term,
    read_only_problem,
    split_statements,
    substitute_task_tokens,
    suggest,
)
from etl_craft.dialects.warehouse.base import STORAGE_PARAMETERS
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
    "SECURE_VIEW": frozenset({SqlAction.CREATE_VIEW}),
}

PARAMETERS = frozenset(
    {
        "SQL_ACTION",
        "TARGET_OBJECT",
        "SOURCE_SQL",
        "SOURCE_SQL_FILE",
        "PIPELINE_RUN_ID_SUBSTITUTION",
        "PIPELINE_ID_SUBSTITUTION",
        "TASK_RUN_ID_SUBSTITUTION",
        "PIPELINE_RUN_ID_FILTER",
        "RUN_DATE_SUBSTITUTION",
        "MERGE_KEY",
        "MERGE_COMPARE_COLUMNS",
        "MERGE_DEDUPE_ORDER",
        "SETUP_FOR",
        "TABLE_FORMAT",
        *FLAGS,
        *STORAGE_PARAMETERS,
    }
)
"""The task parameters a SQL task reads."""


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
    secure_view: bool = False


def read_sql_task(context: TaskContext) -> SqlTask:
    """Read and check the SQL task's parameters; ``SqlGuardError`` or ``MetadataError`` if wrong."""
    params = context.task_params
    action = _action(params)
    target = (params.get("TARGET_OBJECT") or "").strip()
    if not target:
        raise SqlGuardError(f"TARGET_OBJECT is required for SQL_ACTION={action}")
    if not is_safe_object_ref(target):
        raise SqlGuardError(
            f"TARGET_OBJECT={target!r} must be 'schema.table' or 'database.schema.table', "
            "letters, digits and underscores only; without a database, the active Warehouse "
            "profile's is used"
        )
    flags = {name: _flag(params, name, action, applies) for name, applies in FLAGS.items()}
    if action == SqlAction.CREATE_VIEW:
        stored = [name for name in ("TABLE_FORMAT", *STORAGE_PARAMETERS) if params.get(name)]
        if stored:
            raise SqlGuardError(
                f"SQL_ACTION=CREATE_VIEW takes no {stored[0]}: a view stores no data; "
                "remove the parameter"
            )

    select_sql, source = None, "none"
    if action == SqlAction.DROP_TABLE:
        given = [name for name in ("SOURCE_SQL", "SOURCE_SQL_FILE") if params.get(name)]
        if given:
            raise SqlGuardError(f"SQL_ACTION=DROP_TABLE takes no SELECT, but {given[0]} is set")
    else:
        select_sql, source = resolve_select(
            context.config,
            context.task_params,
            pipeline_run_id=context.pipeline_run_id,
            pipeline_id=context.pipeline_id,
            task_run_id=context.task_run_id,
            refresh_type=context.refresh_type,
            run_date=context.run_date,
        )

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
                raise SqlGuardError(
                    f"{name} applies only to SCD1_MERGE and SCD2_MERGE, not {action}"
                )
    if action not in KEYED and params.get("MERGE_KEY"):
        raise SqlGuardError(f"MERGE_KEY does not apply to SQL_ACTION={action}")
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
        secure_view=flags["SECURE_VIEW"],
    )


def _action(params: Mapping[str, str]) -> SqlAction:
    written = (params.get("SQL_ACTION") or "").strip()
    actions = [member.value for member in SqlAction]
    if written.upper() in actions:
        return SqlAction(written.upper())
    hints = suggest(written.upper(), actions)
    hint = f" — did you mean: {', '.join(hints)}" if hints else ""
    raise SqlGuardError(
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
        raise SqlGuardError(f"{name}={value!r} must be true or false")
    return lowered == "true"


def _flag(
    params: Mapping[str, str], name: str, action: SqlAction, applies: frozenset[SqlAction]
) -> bool:
    enabled = parse_flag(params, name)
    if name in params and action not in applies:
        names = ", ".join(sorted(applies))
        raise SqlGuardError(f"{name} applies only to {names}, not SQL_ACTION={action}")
    return enabled


def resolve_select(
    config: ConnectorConfig,
    params: Mapping[str, str],
    *,
    pipeline_run_id: int,
    refresh_type: str,
    run_date: date = LINEAGE_RUN_DATE,
    pipeline_id: int = 0,
    task_run_id: int = 0,
) -> tuple[str, str]:
    """Return a SQL task's SELECT, inline or from its file, with the tokens replaced.

    Also returns where it came from, for messages. ``SqlGuardError`` or ``MetadataError`` when
    the task has no usable single read-only SELECT.
    """
    inline = params.get("SOURCE_SQL")
    file_name = params.get("SOURCE_SQL_FILE")
    if inline and file_name:
        raise SqlGuardError(
            "set SOURCE_SQL or SOURCE_SQL_FILE, not both: the task would have two SELECTs"
        )
    if file_name:
        path = sql_file(config, file_name)
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
        raise SqlGuardError(
            f"SQL_ACTION={params.get('SQL_ACTION')} needs a SELECT: set SOURCE_SQL, or "
            "SOURCE_SQL_FILE naming a file under sql_files/"
        )
    # Tokens first: a ``$$`` would otherwise read as a dollar-quoted string when splitting.
    substituted = substitute_task_tokens(
        raw,
        pipeline_run_id=pipeline_run_id,
        refresh_type=refresh_type,
        pipeline_run_id_substitution=parse_flag(params, "PIPELINE_RUN_ID_SUBSTITUTION"),
        pipeline_id_substitution=parse_flag(params, "PIPELINE_ID_SUBSTITUTION"),
        task_run_id_substitution=parse_flag(params, "TASK_RUN_ID_SUBSTITUTION"),
        pipeline_id=pipeline_id,
        task_run_id=task_run_id,
        filter_enabled=parse_flag(params, "PIPELINE_RUN_ID_FILTER"),
        source=source,
        run_date=run_date,
        run_date_substitution=parse_flag(params, "RUN_DATE_SUBSTITUTION"),
    )
    statements = split_statements(substituted)
    if len(statements) != 1:
        raise SqlGuardError(
            f"{source} must hold exactly one SELECT; it holds {len(statements)} statements"
        )
    select_sql = statements[0]
    problem = read_only_problem(select_sql)
    if problem is not None:
        raise SqlGuardError(
            f"{source} must be a read-only SELECT (the engine writes the target itself); it "
            f"{problem}"
        )
    return select_sql, source


def _columns(params: Mapping[str, str], name: str, action: SqlAction) -> tuple[str, ...]:
    value = params.get(name) or ""
    columns = tuple(part.strip() for part in value.split("|") if part.strip())
    if not columns:
        raise SqlGuardError(f"{name} is required for SQL_ACTION={action}: '|'-separated columns")
    bad = [column for column in columns if not is_safe_identifier(column)]
    if bad:
        raise SqlGuardError(
            f"{name}={value!r}: {', '.join(repr(b) for b in bad)} is not a plain column name"
        )
    return columns


def _setup_for(value: str | None, action: SqlAction) -> SqlAction | None:
    if value is None or not value.strip():
        return None
    if action != SqlAction.SETUP_TABLE:
        raise SqlGuardError(f"SETUP_FOR applies only to SETUP_TABLE, not SQL_ACTION={action}")
    written = value.strip().upper()
    if written not in WRITERS:
        raise SqlGuardError(
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
        raise SqlGuardError(
            f"MERGE_DEDUPE_ORDER={value!r}: {', '.join(repr(b) for b in bad)} is not "
            "'column [ASC|DESC] [NULLS FIRST|LAST]'"
        )
    return ", ".join(terms)
