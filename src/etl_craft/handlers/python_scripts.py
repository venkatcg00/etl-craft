"""``HANDLER=PYTHON``: run a Python script from the project's ``python_scripts/``.

The task's ``SCRIPT_FILE`` names the script by its project path, inside ``python_scripts/``;
the script defines ``run(task)``, or ``run()`` when it needs nothing from the task (see
``etl_craft.scripting``). It runs in the task's own process, so what it prints and logs goes to
the attempt's log, and the task's time limit applies to it.

Its ``INPUT_PARAMS`` task parameter, when set, must be a JSON object; the script gets it as a
dictionary. The script gets the offset its last successful run stored, and returns the rows it
wrote, recorded as the source, target and insert counts, and optionally a new offset, returned
to the child for storage with the successful attempt outcome. A stored offset keeps its type:
a script that returns another type fails. A backfill run gets no offset and stores none:
the script reads its source for ``task.run_date``.
Everything that can be wrong with the script or what it returns fails the task with a message
naming the script and the problem.
"""

from __future__ import annotations

import ast
import importlib.util
import inspect
import json
import logging
import sys
import time
import types
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from sqlalchemy.engine import Engine

from etl_craft.config.model import PYTHON_SCRIPTS_DIRNAME
from etl_craft.config.project import python_script
from etl_craft.core.errors import HandlerError, PermanentError, TransientError
from etl_craft.core.log import capture_all_loggers
from etl_craft.engine.repository.offsets import StoredOffset
from etl_craft.engine.task_store import TaskStore
from etl_craft.handlers.registry import HandlerResult, TaskContext, task_store
from etl_craft.scripting import Offset, ScriptResult, ScriptTask

logger = logging.getLogger(__name__)

PARAMETERS = frozenset({"SCRIPT_FILE", "INPUT_PARAMS"})
"""The task parameters a Python-script task reads."""

SCRIPT_LOGGER = "etl_craft_script"
"""The logger a script is given as ``task.logger``, named after its task."""


def run(context: TaskContext, engine_db: Engine | TaskStore) -> HandlerResult:
    """Run the task's Python script and return the counts it reports."""
    params = context.task_params
    name = (params.get("SCRIPT_FILE") or "").strip()
    if not name:
        raise HandlerError(
            "SCRIPT_FILE is required for HANDLER=PYTHON, such as python_scripts/load.py"
        )
    path = python_script(context.config, name)
    input_params = event_input_params(context)
    store = task_store(context, engine_db)
    stored = store.state().offset
    offset = (
        Offset.from_stored(stored.offset_type, stored.offset_value)
        if stored is not None and stored.offset_value is not None and not context.backfill
        else None
    )
    entry = load_script(path, name)
    task = ScriptTask(
        pipeline_code=context.pipeline_code,
        task_code=context.task_code,
        pipeline_run_id=context.pipeline_run_id,
        pipeline_id=context.pipeline_id,
        task_run_id=context.task_run_id,
        refresh_type=context.refresh_type,
        offset=offset,
        input_params=input_params,
        task_params=params,
        force=context.force,
        config=context.config,
        _locks=store,
        logger=logging.getLogger(f"{SCRIPT_LOGGER}.{context.task_code}"),
        run_date=context.run_date,
        backfill=context.backfill,
        event_files=context.event_files,
        event_keys=context.event_keys,
        event_rows=context.event_rows,
    )
    logger.info(
        "running %s from offset %s with input params %s",
        name,
        "none (first run)" if offset is None else f"{offset.stored()} ({offset.datatype})",
        ", ".join(sorted(input_params)) or "none",
    )
    started = time.monotonic()
    with capture_all_loggers():
        result = _call(entry, task, name)
    checked = check_result(result, name, stored)
    result_variables = dict(checked.variables)
    elapsed = time.monotonic() - started
    variables: dict[str, object] = {}
    if checked.offset is not None and context.backfill:
        logger.warning(
            "%s returned offset %s in a backfill run for %s; a backfill does not store offsets, "
            "so the next scheduled run starts where the last one left off",
            name,
            checked.offset.stored(),
            context.run_date.isoformat(),
        )
    elif checked.offset is not None:
        variables["OFFSET"] = f"{checked.offset.stored()} ({checked.offset.datatype})"
    variables.update(result_variables)
    logger.info(
        "%s wrote %d row(s) (%.2fs); offset %s",
        name,
        checked.row_count,
        elapsed,
        "unchanged"
        if checked.offset is None or context.backfill
        else f"returned {checked.offset.stored()}",
    )
    return HandlerResult(
        source_count=checked.row_count,
        target_count=checked.row_count,
        insert_count=checked.row_count,
        variables=variables,
        offset=(
            StoredOffset(checked.offset.datatype, checked.offset.stored())
            if checked.offset is not None and not context.backfill
            else None
        ),
    )


_JSON_KINDS = {list: "array", str: "string", int: "number", float: "number", bool: "boolean"}


def parse_input_params(value: str | None) -> dict[str, Any]:
    """Return ``INPUT_PARAMS`` as a dictionary; it must be a JSON object. Absent means empty."""
    if value is None or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as error:
        raise HandlerError(f"INPUT_PARAMS is not valid JSON ({error}): {value!r}") from error
    if not isinstance(parsed, dict):
        raise HandlerError(
            'INPUT_PARAMS must be a JSON object, such as {"region": "eu", "days": 30}; '
            f"got a JSON {_JSON_KINDS.get(type(parsed), type(parsed).__name__)}: {value!r}"
        )
    return parsed


def load_script(path: Path, name: str) -> Callable[..., Any]:
    """Import the script and return its ``run``; ``HandlerError`` saying what is wrong."""
    # Scripts may import helper modules kept beside them.
    folder = str(path.parent)
    if folder not in sys.path:
        sys.path.insert(0, folder)
    # Compiled from its source every time, never from cached bytecode, so an edited script is
    # always the one that runs.
    relative = name.removeprefix(f"{PYTHON_SCRIPTS_DIRNAME}/")
    module_name = f"etl_craft_script_{relative.replace('/', '_').replace('.', '_')}"
    module = types.ModuleType(module_name)
    module.__file__ = str(path)
    module.__spec__ = importlib.util.spec_from_file_location(module_name, path)
    sys.modules[module_name] = module
    try:
        source = path.read_text(encoding="utf-8")
        exec(compile(source, str(path), "exec", dont_inherit=True), module.__dict__)
    except BaseException as error:
        sys.modules.pop(module_name, None)
        if not isinstance(error, Exception):
            raise
        logger.exception("importing %s failed", path)
        raise HandlerError(
            f"SCRIPT_FILE={name!r}: importing it failed: {type(error).__name__}: {error}"
        ) from error
    entry = getattr(module, "run", None)
    if not callable(entry):
        sys.modules.pop(module_name, None)
        raise HandlerError(
            f"SCRIPT_FILE={name!r} defines no run(task) function; see etl_craft.scripting"
        )
    return entry  # type: ignore[no-any-return]


def script_definition_problem(path: Path, name: str) -> str | None:
    """Read the script without running it; return why its ``run`` cannot be called, or ``None``.

    The script is parsed, not imported, so nothing in it runs. A ``run`` that is not a plain
    top-level function (imported, or assigned) is taken on trust.
    """
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError as error:
        return f"SCRIPT_FILE={name!r} has a syntax error at line {error.lineno}: {error.msg}"
    except (OSError, UnicodeDecodeError) as error:
        return f"SCRIPT_FILE={name!r} cannot be read: {error}"
    functions = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name == "run"
    ]
    if not functions:
        bound = {
            target.id
            for node in tree.body
            if isinstance(node, ast.Assign)
            for target in node.targets
            if isinstance(target, ast.Name)
        } | {
            alias.asname or alias.name
            for node in tree.body
            if isinstance(node, ast.Import | ast.ImportFrom)
            for alias in node.names
        }
        if "run" in bound:
            return None
        return f"SCRIPT_FILE={name!r} defines no run(task) function; see etl_craft.scripting"
    if isinstance(functions[-1], ast.AsyncFunctionDef):
        return f"{name}: run is async; it must be a plain function that returns a ScriptResult"
    args = functions[-1].args
    positional = len(args.posonlyargs) + len(args.args) - len(args.defaults)
    if positional > 1:
        return f"{name}: run takes {positional} arguments; it takes the task, or nothing"
    return None


def _takes_the_task(entry: Callable[..., Any], name: str) -> bool:
    """Whether ``run`` takes the task: ``run(task)`` does, ``run()`` does not."""
    try:
        parameters = inspect.signature(entry).parameters.values()
    except (TypeError, ValueError):
        return True
    positional = [
        p
        for p in parameters
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD) and p.default is p.empty
    ]
    if len(positional) > 1:
        raise HandlerError(
            f"{name}: run takes {len(positional)} arguments; it takes the task, or nothing"
        )
    return bool(positional) or any(p.kind == p.VAR_POSITIONAL for p in parameters)


def _call(entry: Callable[..., Any], task: ScriptTask, name: str) -> Any:
    with_task = _takes_the_task(entry, name)
    try:
        return entry(task) if with_task else entry()
    except (HandlerError, TransientError, PermanentError):
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
            f"{name} returned {type(result).__name__}, not a ScriptResult with its row count"
        )
    count = result.row_count
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise HandlerError(
            f"{name} returned row_count={count!r}; it must be a whole number, 0 or more"
        )
    if result.offset is not None and not isinstance(result.offset, Offset):
        raise HandlerError(
            f"{name} returned offset {result.offset!r}; build one with Offset.number, "
            "Offset.text or Offset.timestamp"
        )
    if (
        result.offset is not None
        and stored is not None
        and result.offset.datatype != stored.offset_type
    ):
        raise HandlerError(
            f"{name} returned a {result.offset.datatype} offset, but the stored one is "
            f"{stored.offset_type}; an offset keeps its datatype"
        )
    if not isinstance(result.variables, Mapping):
        raise HandlerError(
            f"{name} returned variables={result.variables!r}; it must be a mapping, "
            "such as {'REGION': 'eu'}"
        )
    return result


def event_input_params(context: TaskContext) -> dict[str, Any]:
    """Expand exact top-level event tokens through the same path for files and keys."""
    tokens = {"$$event_files": context.event_files, "$$event_keys": context.event_keys}
    return {
        name: list(tokens[value]) if isinstance(value, str) and value in tokens else value
        for name, value in parse_input_params(context.task_params.get("INPUT_PARAMS")).items()
    }
