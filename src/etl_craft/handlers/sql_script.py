"""Unmanaged SQL files, executed in order using the warehouse's native autocommit."""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.interfaces import DBAPICursor

from etl_craft.config.project import sql_script
from etl_craft.core.errors import HandlerError, SqlGuardError
from etl_craft.core.faults import fault_point
from etl_craft.core.text import mask_arguments, split_statements, sql_parts
from etl_craft.handlers.python_scripts import parse_input_params
from etl_craft.handlers.registry import HandlerResult, TaskContext
from etl_craft.warehouse.connection import open_warehouse, warehouse_dialect

logger = logging.getLogger(__name__)
# Statement audit must survive the caller's verbosity setting.
logger.setLevel(logging.INFO)
PARAMETERS = frozenset({"SCRIPT_FILE", "INPUT_PARAMS"})
CONTEXT_INPUTS = frozenset({"pipeline_id", "pipeline_run_id", "task_run_id", "run_date"})


@dataclass(frozen=True)
class Script:
    """A checked script and its bound inputs; values never enter the statement audit."""

    name: str
    statements: tuple[str, ...]
    inputs: Mapping[str, object]


def read_script(context: TaskContext) -> Script:
    """Read and check every statement's binds before executing any of the file."""
    if context.config.warehouse is None:
        raise SqlGuardError("SQL_SCRIPT needs a Warehouse section; configure craft-connector.yml")
    name = (context.task_params.get("SCRIPT_FILE") or "").strip()
    path = sql_script(context.config, name, setting="SCRIPT_FILE")
    try:
        statements = tuple(split_statements(path.read_text(encoding="utf-8")))
    except (OSError, UnicodeError) as error:
        raise SqlGuardError(f"SCRIPT_FILE={name!r}: {error}; save readable UTF-8 SQL") from error
    if not statements:
        raise SqlGuardError(f"SCRIPT_FILE={name!r} contains no statements; add SQL to the file")
    try:
        inputs = parse_input_params(context.task_params.get("INPUT_PARAMS"))
    except HandlerError as error:
        raise SqlGuardError(f"SCRIPT_FILE={name!r}: {error}") from error
    reserved = CONTEXT_INPUTS.intersection(inputs)
    if reserved:
        raise SqlGuardError(
            f"SCRIPT_FILE={name!r}: INPUT_PARAMS overrides {', '.join(sorted(reserved))}; "
            "remove these engine-provided keys"
        )
    inputs.update(
        pipeline_id=context.pipeline_id,
        pipeline_run_id=context.pipeline_run_id,
        task_run_id=context.task_run_id,
        run_date=context.run_date,
    )
    for index, sql in enumerate(statements, 1):
        needed = set(text(bindable_sql(sql)).compile().params)
        missing = needed - inputs.keys()
        if missing:
            raise SqlGuardError(
                f"SCRIPT_FILE={name!r}, statement {index}: missing binds "
                f"{', '.join(sorted(missing))}; supply them in INPUT_PARAMS"
            )
    return Script(name, statements, inputs)


def bindable_sql(sql: str) -> str:
    """Keep colons inside quotes and comments literal when SQLAlchemy compiles binds."""
    return "".join(
        part.replace(":", r"\:") if protected else part for part, protected in sql_parts(sql)
    )


def statement_text(sql: str) -> str:
    """Short audit text with string literals, bodies and comments omitted."""
    safe = "".join(
        "[REDACTED]" if protected and not part.startswith(('"', "`")) else part
        for part, protected in sql_parts(sql)
    )
    return " ".join(safe.split())[:240]


def run(context: TaskContext, engine_db: Engine) -> HandlerResult:
    """Execute statements on one native session; failure stops the file immediately."""
    script = read_script(context)
    dialect = warehouse_dialect(context.config)
    with open_warehouse(context.config, engine_db) as warehouse:
        connection = warehouse.raw_connection()
        try:
            driver = connection.driver_connection
            assert driver is not None
            dialect.script_autocommit(warehouse, driver)
            cursor = connection.cursor()
            try:
                for index, sql in enumerate(script.statements, 1):
                    _execute(cursor, warehouse, script, index, sql)
            finally:
                cursor.close()
        finally:
            connection.close()
    return HandlerResult(variables={"STATEMENTS": len(script.statements)})


def _execute(cursor: DBAPICursor, warehouse: Engine, script: Script, index: int, sql: str) -> None:
    """Record each native driver outcome without logging its bound values."""
    compiled = text(bindable_sql(sql)).compile(dialect=warehouse.dialect)
    values = compiled.construct_params(dict(script.inputs))
    params = (
        tuple(values[key] for key in (compiled.positiontup or ()))
        if compiled.positional
        else values
    )
    short = statement_text(sql)
    started = time.monotonic()
    logger.info("SQL_SCRIPT %s statement %d starting: %s", script.name, index, short)
    try:
        cursor.execute(str(compiled), params) if params else cursor.execute(sql)
        if cursor.description is not None:
            while cursor.fetchmany(1000):
                pass
    except Exception as error:
        # Native-driver boundary: persist the failed step before the child records its outcome.
        detail = str(mask_arguments({"error": str(error)})["error"])
        for value in script.inputs.values():
            if isinstance(value, str) and value:
                detail = detail.replace(value, "[REDACTED]")
        for part, protected in sql_parts(sql):
            if protected and part.startswith("'") and len(part) > 2:
                detail = detail.replace(part[1:-1].replace("''", "'"), "[REDACTED]")
        message = (
            f"SQL_SCRIPT {script.name} statement {index} failed: {short}: {detail}; "
            "correct the file before retrying from statement 1"
        )
        logger.error("%s (%.6fs)", message, time.monotonic() - started)
        raise HandlerError(message) from error
    rows = cursor.rowcount
    logger.info(
        "SQL_SCRIPT %s statement %d: %s; rows affected=%s; duration=%.6fs",
        script.name,
        index,
        short,
        rows if rows >= 0 else "unknown",
        time.monotonic() - started,
    )
    fault_point("sql_script.after_statement")
