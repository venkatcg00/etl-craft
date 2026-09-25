"""``HANDLER=PYTHON``: run an ingestion script from the project's ``ingestion_scripts/``.

The task's ``SCRIPT_NAME`` names the script, a path inside ``ingestion_scripts/``; the script
defines ``run(task)`` (see ``etl_craft.scripting``). It runs in the task's own process, so what
it prints and logs goes to the attempt's log, and the task's time limit applies to it.

Its ``INPUT_PARAMS`` task parameter, when set, must be a JSON array; the script gets it as a
list. The script gets the offset its last successful run stored, and returns its counts and,
optionally, a new offset, which is stored once it has succeeded. A stored offset keeps its type:
a script that returns another type fails. Everything that can be wrong with the script or what
it returns fails the task with a message naming the script and the problem.
"""

from __future__ import annotations

import json
import logging
import sys
import time
import types
from collections.abc import Callable
from pathlib import Path
from typing import Any

from sqlalchemy.engine import Engine

from etl_craft.config.project import ingestion_script
from etl_craft.core.errors import HandlerError
from etl_craft.core.log import capture_all_loggers
from etl_craft.engine.repository.offsets import StoredOffset, fetch_task_offset, save_task_offset
from etl_craft.handlers.registry import HandlerResult, TaskContext
from etl_craft.scripting import Offset, ScriptResult, ScriptTask

logger = logging.getLogger(__name__)

SCRIPT_LOGGER = "etl_craft_script"
"""The logger a script is given as ``task.logger``, named after its task."""


def run(context: TaskContext, engine_db: Engine) -> HandlerResult:
    """Run the task's ingestion script and return the counts it reports."""
    params = context.task_params
    name = (params.get("SCRIPT_NAME") or "").strip()
    if not name:
        raise HandlerError(
            "SCRIPT_NAME is required for HANDLER=PYTHON: a .py file under ingestion_scripts/"
        )
    path = ingestion_script(context.config, name)
    input_params = parse_input_params(params.get("INPUT_PARAMS"))
    with engine_db.connect() as conn:
        stored = fetch_task_offset(conn, context.task_id)
    offset = (
        Offset.from_stored(stored.offset_type, stored.offset_value)
        if stored is not None and stored.offset_value is not None
        else None
    )
    entry = load_script(path, name)
    task = ScriptTask(
        pipeline_code=context.pipeline_code,
        task_code=context.task_code,
        pipeline_run_id=context.pipeline_run_id,
        refresh_type=context.refresh_type,
        offset=offset,
        input_params=input_params,
        task_params=params,
        force=context.force,
        config=context.config,
        engine_db=engine_db,
        logger=logging.getLogger(f"{SCRIPT_LOGGER}.{context.task_code}"),
    )
    logger.info(
        "running %s from offset %s with %d input param(s)",
        name,
        "none (first run)" if offset is None else f"{offset.stored()} ({offset.type})",
        len(input_params),
    )
    started = time.monotonic()
    with capture_all_loggers():
        result = _call(entry, task, name)
    checked = check_result(result, name, stored)
    elapsed = time.monotonic() - started
    variables: dict[str, object] = {}
    if checked.offset is not None:
        with engine_db.begin() as conn:
            save_task_offset(
                conn,
                context.task_id,
                StoredOffset(checked.offset.type, checked.offset.stored()),
            )
        variables["OFFSET"] = f"{checked.offset.stored()} ({checked.offset.type})"
    variables.update(checked.variables)
    logger.info(
        "%s read %d, wrote %d, target holds %d (%.2fs); offset %s",
        name,
        checked.source_count,
        checked.insert_count,
        checked.target_count,
        elapsed,
        "unchanged" if checked.offset is None else f"now {checked.offset.stored()}",
    )
    return HandlerResult(
        source_count=checked.source_count,
        target_count=checked.target_count,
        insert_count=checked.insert_count,
        variables=variables,
    )


def parse_input_params(value: str | None) -> list[Any]:
    """Return ``INPUT_PARAMS`` as a list; it must be a JSON array. Absent means empty."""
    if value is None or not value.strip():
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as error:
        raise HandlerError(f"INPUT_PARAMS is not valid JSON ({error}): {value!r}") from error
    if not isinstance(parsed, list):
        raise HandlerError(
            f'INPUT_PARAMS must be a JSON array, such as ["eu", 30]; got {type(parsed).__name__}'
        )
    return parsed


def load_script(path: Path, name: str) -> Callable[[ScriptTask], Any]:
    """Import the script and return its ``run``; ``HandlerError`` saying what is wrong."""
    # Scripts may import helper modules kept beside them.
    folder = str(path.parent)
    if folder not in sys.path:
        sys.path.insert(0, folder)
    # Compiled from its source every time, never from cached bytecode, so an edited script is
    # always the one that runs.
    module = types.ModuleType(f"etl_craft_script_{path.stem}")
    module.__file__ = str(path)
    try:
        source = path.read_text(encoding="utf-8")
        exec(compile(source, str(path), "exec"), module.__dict__)
    except Exception as error:
        logger.exception("importing %s failed", path)
        raise HandlerError(
            f"SCRIPT_NAME={name!r}: importing it failed: {type(error).__name__}: {error}"
        ) from error
    entry = getattr(module, "run", None)
    if not callable(entry):
        raise HandlerError(
            f"SCRIPT_NAME={name!r} defines no run(task) function; see etl_craft.scripting"
        )
    return entry  # type: ignore[no-any-return]


def _call(entry: Callable[[ScriptTask], Any], task: ScriptTask, name: str) -> Any:
    try:
        return entry(task)
    except HandlerError:
        raise
    except SystemExit as error:
        raise HandlerError(
            f"{name} called sys.exit({error.code!r}); a script reports failure by raising an "
            "exception and success by returning a ScriptResult"
        ) from error
    except Exception as error:
        logger.exception("%s raised", name)
        raise HandlerError(
            f"{name} failed: {type(error).__name__}: {error} (the traceback is in the attempt's "
            "log)"
        ) from error


def check_result(result: Any, name: str, stored: StoredOffset | None) -> ScriptResult:
    """Check what the script returned; ``HandlerError`` naming each problem."""
    if not isinstance(result, ScriptResult):
        raise HandlerError(
            f"{name} returned {type(result).__name__}, not a ScriptResult with its counts"
        )
    problems = [
        f"{field}={value!r}"
        for field in ("source_count", "target_count", "insert_count")
        if isinstance(value := getattr(result, field), bool)
        or not isinstance(value, int)
        or value < 0
    ]
    if problems:
        raise HandlerError(
            f"{name} returned {', '.join(problems)}; each count must be a whole number, 0 or more"
        )
    if result.offset is not None and not isinstance(result.offset, Offset):
        raise HandlerError(
            f"{name} returned offset {result.offset!r}; build one with Offset.number, "
            "Offset.text or Offset.timestamp"
        )
    if (
        result.offset is not None
        and stored is not None
        and result.offset.type != stored.offset_type
    ):
        raise HandlerError(
            f"{name} returned a {result.offset.type} offset, but the stored one is "
            f"{stored.offset_type}; an offset keeps its type"
        )
    return result
