"""A SQL task's parameters, read and checked before anything touches the warehouse.

Every mistake in a task's definition fails the task here with the parameter, its value and the
remedy, so no action ever starts on a half-valid definition.

- ``SQL_ACTION``: one of the nine actions.
- ``TARGET_OBJECT``: ``schema.table``, in the active warehouse profile's database, or
  ``database.schema.table``.
- ``SELECT_QUERY`` or ``SCRIPT_FILE`` (every action but ``DROP_TABLE``): the read-only
  SELECT, inline or as a file under ``sql_scripts/``; exactly one of them.
- ``PIPELINE_RUN_ID_SUBSTITUTION``: ``true`` replaces ``$$pipeline_run_id`` with the run id.
- ``PIPELINE_RUN_ID_FILTER``: ``true`` replaces ``$$pipeline_run_id_filter`` with
  ``pipeline_run_id = <id>``, or ``1=1`` on a FULL refresh.
- ``RUN_DATE_SUBSTITUTION``: ``true`` replaces ``$$run_date`` with the date the run runs as of,
  ``DATE 'YYYY-MM-DD'``: the day it started, or the date of a backfill run.
- ``MERGE_KEY`` (``SCD1_MERGE``, ``SCD2_MERGE``, ``DELETE_ROWS``): ``|``-separated key columns.
- ``MERGE_COMPARE_COLUMNS`` (the merges): ordered ``|``-separated scalar columns hashed into
  version-2 ``HASH_KEY``; floating point is refused, and decimals require a declared scale.
- ``MERGE_DEDUPE_ORDER`` (the merges): ``ORDER BY`` terms choosing the row kept per key.
- ``ALLOW_EMPTY`` (``CREATE_TABLE``, ``OVERWRITE_TABLE``): ``false`` fails the task when the
  SELECT returns no rows, before the target changes; absent or ``true`` replaces it with none.
- ``SCHEMA_EVOLUTION`` (``OVERWRITE_TABLE`` and the merges): ``true`` adds new SELECT columns
  to the target.
- ``PRESERVE_TARGET`` (``SCD1_MERGE``): ``true`` keeps a target value where the source is NULL.
- ``HARD_DELETE`` (``DELETE_ROWS``): ``true`` deletes rows; otherwise they get
  ``DELETE_FLAG='Y'`` and ``ACTIVE_FLAG='N'``; closed versions stay unchanged.
- ``SETUP_FOR`` (``SETUP_TABLE``): the action that writes the table, whose audit columns it
  gets; needed when no other task in the pipeline writes it.
- ``SECURE_VIEW`` (``CREATE_VIEW``): ``true`` creates a secure view where the warehouse has
  them (Snowflake's ``SECURE``, PostgreSQL's ``security_barrier``). A view stores no data, so
  it takes no ``TABLE_FORMAT`` or storage parameter.
- ``REJECT_LIMIT`` (``APPEND_TABLE``, ``OVERWRITE_TABLE`` and the merges): the most rows, or
  ``N%`` of the rows the SELECT returns, set aside in ``<target>__rejects`` rather than failing
  the task (see ``rejects``).
- ``SKIP_REJECTS_OVER_LIMIT`` (with ``REJECT_LIMIT``): ``true`` writes nothing at all when more
  rows do not fit than the limit allows; otherwise those rows are still set aside. The task fails
  either way.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date

from sqlglot import exp

from etl_craft.config import ConnectorConfig
from etl_craft.config.project import sql_script
from etl_craft.core.enums import OffsetType, SqlAction
from etl_craft.core.errors import MetadataError, SqlGuardError
from etl_craft.core.text import (
    LINEAGE_RUN_DATE,
    TOKEN,
    is_safe_identifier,
    is_safe_object_ref,
    is_safe_order_term,
    pipe_list,
    split_statements,
    sql_parts,
    substitute_task_tokens,
    suggest,
)
from etl_craft.dialects.query_analysis import read_only_problem
from etl_craft.dialects.warehouse.base import STORAGE_PARAMETERS
from etl_craft.engine.repository.offsets import StoredOffset
from etl_craft.handlers.registry import TaskContext
from etl_craft.handlers.sql.watermarks import Watermark, read_watermark, source_pipelines
from etl_craft.warehouse.connection import warehouse_dialect

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
SCREENED = MERGES | {SqlAction.APPEND_TABLE, SqlAction.OVERWRITE_TABLE}
"""The actions that take ``REJECT_LIMIT``: they write into a target whose types already exist."""

# Each yes/no parameter, and the actions it applies to.
FLAGS: dict[str, frozenset[SqlAction]] = {
    "ALLOW_EMPTY": frozenset({SqlAction.CREATE_TABLE, SqlAction.OVERWRITE_TABLE}),
    "SCHEMA_EVOLUTION": frozenset({SqlAction.OVERWRITE_TABLE, *MERGES}),
    "PRESERVE_TARGET": frozenset({SqlAction.SCD1_MERGE}),
    "HARD_DELETE": frozenset({SqlAction.DELETE_ROWS}),
    "SECURE_VIEW": frozenset({SqlAction.CREATE_VIEW}),
    "SKIP_REJECTS_OVER_LIMIT": SCREENED,
}

DEFAULT_TRUE = frozenset({"ALLOW_EMPTY"})
"""The yes/no parameters that mean true when absent."""

PARAMETERS = frozenset(
    {
        "SQL_ACTION",
        "TARGET_OBJECT",
        "SELECT_QUERY",
        "SCRIPT_FILE",
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
        "REJECT_LIMIT",
        "WATERMARK_COLUMN",
        "WATERMARK_TYPE",
        "WATERMARK_START",
        "WATERMARK_SOURCE_PIPELINES",
        *FLAGS,
        *STORAGE_PARAMETERS,
    }
)
"""The task parameters a SQL task reads."""


@dataclass(frozen=True)
class RejectLimit:
    """``REJECT_LIMIT``: at most ``value`` rows, or ``value`` percent of them."""

    value: float
    percent: bool

    def rows(self, source: int) -> int:
        """Return how many of ``source`` rows may be set aside."""
        return int(source * self.value / 100) if self.percent else int(self.value)

    def __str__(self) -> str:
        """Return the limit as written: ``100`` or ``0.5%``."""
        return f"{self.value:g}%" if self.percent else f"{self.value:g}"


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
    allow_empty: bool = True
    preserve_target: bool = False
    hard_delete: bool = False
    setup_for: SqlAction | None = None
    secure_view: bool = False
    reject_limit: RejectLimit | None = None
    skip_rejects_over_limit: bool = False
    watermark: Watermark | None = None
    bounded: bool = False


def read_sql_task(context: TaskContext, stored: StoredOffset | None = None) -> SqlTask:
    """Read and check the SQL task's parameters; ``SqlGuardError`` or ``MetadataError`` if wrong."""
    params = context.task_params
    action = read_action(params)
    watermark = read_watermark(params, stored)
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
        _refuse_view_storage(params)

    select_sql, source = None, "none"
    if action == SqlAction.DROP_TABLE:
        names = ("SELECT_QUERY", "SCRIPT_FILE", "WATERMARK_SOURCE_PIPELINES")
        if given := [name for name in names if params.get(name)]:
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
            stored=stored,
            event_values={
                "event_files": context.event_files,
                "event_keys": context.event_keys,
                "upload_ids": tuple(str(upload.upload_id) for upload in context.uploads),
            },
        )

    merge_key = _columns(params, "MERGE_KEY", action, context) if action in KEYED else ()
    compare: tuple[str, ...] = ()
    dedupe_order = None
    if action in MERGES:
        compare = _columns(params, "MERGE_COMPARE_COLUMNS", action, context)
        dedupe_order = _dedupe_order(params.get("MERGE_DEDUPE_ORDER"))
    else:
        for name in ("MERGE_COMPARE_COLUMNS", "MERGE_DEDUPE_ORDER"):
            if params.get(name):
                raise SqlGuardError(
                    f"{name} applies only to SCD1_MERGE and SCD2_MERGE, not {action}"
                )
    if action not in KEYED and params.get("MERGE_KEY"):
        raise SqlGuardError(f"MERGE_KEY does not apply to SQL_ACTION={action}")
    setup_for = read_setup_for(params.get("SETUP_FOR"), action)
    reject_limit = _reject_limit(params.get("REJECT_LIMIT"), action)
    if flags["SKIP_REJECTS_OVER_LIMIT"] and reject_limit is None:
        raise SqlGuardError(
            "SKIP_REJECTS_OVER_LIMIT applies only with REJECT_LIMIT: set the limit, or remove it"
        )

    return SqlTask(
        action=action,
        target_object=target,
        select_sql=select_sql,
        source=source,
        merge_key=merge_key,
        merge_compare_columns=compare,
        dedupe_order=dedupe_order,
        schema_evolution=flags["SCHEMA_EVOLUTION"],
        allow_empty=flags["ALLOW_EMPTY"],
        preserve_target=flags["PRESERVE_TARGET"],
        hard_delete=flags["HARD_DELETE"],
        setup_for=setup_for,
        secure_view=flags["SECURE_VIEW"],
        reject_limit=reject_limit,
        skip_rejects_over_limit=flags["SKIP_REJECTS_OVER_LIMIT"],
        watermark=watermark,
        bounded=action != SqlAction.DROP_TABLE and bounded_definition(context.config, params),
    )


def read_action(params: Mapping[str, str]) -> SqlAction:
    """Parse action names with execution's case and whitespace rules."""
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
    if name in DEFAULT_TRUE and not (params.get(name) or "").strip():
        return True
    return enabled


def _refuse_view_storage(params: Mapping[str, str]) -> None:
    storage = [name for name in ("TABLE_FORMAT", *STORAGE_PARAMETERS) if params.get(name)]
    if storage:
        raise SqlGuardError(
            f"SQL_ACTION=CREATE_VIEW takes no {storage[0]}: a view stores no data; "
            "remove the parameter"
        )


def resolve_select(
    config: ConnectorConfig,
    params: Mapping[str, str],
    *,
    pipeline_run_id: int,
    refresh_type: str,
    run_date: date = LINEAGE_RUN_DATE,
    pipeline_id: int = 0,
    task_run_id: int = 0,
    stored: StoredOffset | None = None,
    event_values: Mapping[str, tuple[str, ...]] | None = None,
) -> tuple[str, str]:
    """Return a SQL task's SELECT, inline or from its file, with the tokens replaced.

    Also returns where it came from, for messages. ``SqlGuardError`` or ``MetadataError`` when
    the task has no usable single read-only SELECT.
    """
    raw, source = read_select(config, params)
    from etl_craft.dialects.warehouse.uploads import upload_select

    raw = upload_select(
        raw, len((event_values or {}).get("upload_ids", ())), validating=pipeline_run_id == 0
    )
    # Tokens first: a ``$$`` would otherwise read as a dollar-quoted string when splitting.
    watermark = read_watermark(params, stored)
    bounded = bounded_definition(config, params)
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
        watermark_literals=watermark.literals(config, bounded) if watermark is not None else None,
        event_literals={
            name: event_literals(config, (event_values or {}).get(name, ()))
            for name in ("event_files", "event_keys")
        }
        if pipeline_run_id
        else None,
    )
    statements = split_statements(substituted)
    if len(statements) != 1:
        raise SqlGuardError(
            f"{source} must hold exactly one SELECT; it holds {len(statements)} statements"
        )
    select_sql = statements[0]
    dialect = warehouse_dialect(config).sqlglot_dialect if config.warehouse is not None else None
    problem = read_only_problem(select_sql, dialect)
    if problem is not None:
        raise SqlGuardError(
            f"{source} must be a read-only SELECT (the engine writes the target itself); it "
            f"{problem}"
        )
    return select_sql, source


def _columns(
    params: Mapping[str, str], name: str, action: SqlAction, context: TaskContext
) -> tuple[str, ...]:
    value = params.get(name) or ""
    columns = pipe_list(value)
    if not columns:
        raise SqlGuardError(f"{name} is required for SQL_ACTION={action}: '|'-separated columns")
    bad = [column for column in columns if not is_safe_identifier(column)]
    if bad:
        raise SqlGuardError(
            f"{name}={value!r}: {', '.join(repr(b) for b in bad)} is not a plain column name"
        )
    seen = set()
    for column in columns:
        if column.lower() in seen:
            raise SqlGuardError(
                f"{context.pipeline_code}.{context.task_code}: {name}={value!r} repeats column "
                f"{column!r}; keep each column once, preserving the intended order"
            )
        seen.add(column.lower())
    return columns


def read_setup_for(value: str | None, action: SqlAction) -> SqlAction | None:
    """Parse the optional creation contract and refuse non-writing actions."""
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


def _reject_limit(value: str | None, action: SqlAction) -> RejectLimit | None:
    if value is None or not value.strip():
        return None
    if action not in SCREENED:
        raise SqlGuardError(
            f"REJECT_LIMIT applies only to {', '.join(sorted(SCREENED))}, not SQL_ACTION={action}"
        )
    written = value.strip()
    percent = written.endswith("%")
    number = written.removesuffix("%").strip()
    try:
        parsed = float(number) if percent else int(number)
    except ValueError:
        parsed = -1
    if not 0 <= parsed <= (100 if percent else math.inf):
        raise SqlGuardError(
            f"REJECT_LIMIT={value!r} must be a whole number of rows, such as 100, or a percentage "
            "of the rows the SELECT returns from 0% to 100%, such as 0.5%"
        )
    return RejectLimit(parsed, percent)


def event_literals(config: ConnectorConfig, values: tuple[str, ...]) -> str:
    """Render paths and signal ids with the same dialect-aware literal escaping."""
    dialect = warehouse_dialect(config).sqlglot_dialect if config.warehouse is not None else None
    return ", ".join(
        exp.Literal.string(value).sql(dialect=dialect).replace(":", "\\:") for value in values
    )


def read_select(config: ConnectorConfig, params: Mapping[str, str]) -> tuple[str, str]:
    """Read the one configured SELECT without substituting its tokens."""
    inline = params.get("SELECT_QUERY")
    file_name = params.get("SCRIPT_FILE")
    if inline and file_name:
        raise SqlGuardError(
            "set SELECT_QUERY or SCRIPT_FILE, not both: the task would have two SELECTs"
        )
    if file_name:
        path = sql_script(config, file_name)
        try:
            raw = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as error:
            raise MetadataError(
                f"SCRIPT_FILE={file_name!r}: cannot read {path}: {error}"
            ) from error
        source = f"SCRIPT_FILE={file_name!r}"
    elif inline:
        raw, source = inline, "SELECT_QUERY"
    else:
        raise SqlGuardError(
            f"SQL_ACTION={params.get('SQL_ACTION')} needs a SELECT: set SELECT_QUERY, or "
            "SCRIPT_FILE naming a project path such as sql_scripts/orders.sql"
        )
    return raw, source


def bounded_definition(config: ConnectorConfig, params: Mapping[str, str]) -> bool:
    """Require an explicit max token before enabling a bounded timestamp reader."""
    raw, _ = read_select(config, params)
    bounded = any(
        match[1] == "watermark_max"
        for part, protected in sql_parts(raw, placeholders=True)
        if not protected
        for match in TOKEN.finditer(part)
    )
    if bounded and params.get("WATERMARK_TYPE", "").strip().upper() != OffsetType.TIMESTAMP:
        raise SqlGuardError(
            f"WATERMARK_TYPE={params.get('WATERMARK_TYPE')!r} with $$watermark_max; "
            "bounded windows require TIMESTAMP and both $$watermark and $$watermark_max"
        )
    compared = {name.strip().lower() for name in params.get("MERGE_COMPARE_COLUMNS", "").split("|")}
    if bounded and params.get("WATERMARK_COLUMN", "").strip().lower() in compared:
        raise SqlGuardError(
            f"WATERMARK_COLUMN={params.get('WATERMARK_COLUMN')!r} is in MERGE_COMPARE_COLUMNS; "
            "a bounded source position is not a business change; remove it from compare columns"
        )
    if "WATERMARK_SOURCE_PIPELINES" in params:
        source_pipelines(params)
        if not bounded:
            raise SqlGuardError(
                "WATERMARK_SOURCE_PIPELINES requires a bounded TIMESTAMP definition; "
                "use both $$watermark and $$watermark_max"
            )
    return bounded
