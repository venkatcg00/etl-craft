"""HANDLER=PYTHON execution — invokes a team's own ingestion script as a subprocess.

CLAUDE.md's one hard rule: "the engine does not auto-inject the run id into
them; the team's own script is responsible for fetching and including
pipeline_run_id in whatever it inserts." Everything else in this module's
contract came from later, explicit instruction:

Contract: the script named by CFG_TASKS.SCRIPT_NAME is run as
`<python> <script>`, inheriting this process's environment plus
ETL_CRAFT_PIPELINE_CODE / ETL_CRAFT_TASK_CODE (so the script can resolve its
own pipeline_run_id — e.g. via `etl-craft`'s own craft-connector.yml, in the
same working directory) and running in the current working directory, same
as every other invocation of `etl-craft` itself. A nonzero exit is a
HandlerError, its message built from the process's own stderr (or stdout, if
stderr is empty).

[ADDITION] Return-variable contract, per explicit instruction: the script
must report at least two variables (INGESTION_COUNT — a plain number —
and LATEST_OFFSET_UPDATE — a `value|datatype` pair, e.g.
"2023-01-01 00:00:00|timestamp") and may report more; every name it's
expected to report must be declared up front, in CFG_TASKS.RETURN_VALUES
(pipe-separated — "any column that has a need to store more than one value
must use | as separator" — `ck_tasks_return_values` in schema.sql; see that
constraint's own post-signoff comment for why it moved from a closed
two-token list to an open one). The script reports back by printing one
JSON object as the *last* line of stdout (everything before it is free-form
log output, ignored — not captured into TASK_LOG, unlike an earlier version
of this module; see "log all the variables... as rows with variable = value
semantics" below), keyed by those same declared names:
    {"INGESTION_COUNT": 42, "LATEST_OFFSET_UPDATE": "2023-01-01 00:00:00|timestamp",
     "SOME_CUSTOM_VAR": "..."}
INGESTION_COUNT and LATEST_OFFSET_UPDATE must both be declared *and*
actually present in the output — HandlerError otherwise, since "at least
two variables" is not optional. Any other declared name is logged if
present, silently skipped if not (a script may only sometimes have
something extra to report). An undeclared key the script prints anyway is
ignored — RETURN_VALUES is the source of truth for what this module reads
back, not whatever a script happens to emit.

[ADDITION] "log all the variables in task log table's task log column as
rows with variable = value semantics" — every declared-and-present
variable (in RETURN_VALUES' own order, not the JSON's) becomes one
"NAME = value" line via HandlerResult.variables / execution.format_task_log,
the same shared formatter sql_actions.py/business_rules.py fall back to for
their own counts.

LATEST_OFFSET_UPDATE's `value|datatype` pair upserts AUD_TASK_OFFSET_TRACKER
for this task — CLAUDE.md's watermark table. Reading that watermark back
(to know where to resume from) is deliberately not this module's job: "for
ingestion scripts, it is the responsibility of the team to maintain that."
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.engine import Connection

from etl_craft.execution import HandlerError, HandlerResult, TaskExecutionContext

_VALID_OFFSET_TYPES = frozenset({"NUMBER", "TEXT", "TIMESTAMP"})
INGESTION_COUNT_VAR = "INGESTION_COUNT"
LATEST_OFFSET_UPDATE_VAR = "LATEST_OFFSET_UPDATE"
MANDATORY_RETURN_VARS = (INGESTION_COUNT_VAR, LATEST_OFFSET_UPDATE_VAR)


def _parse_trailing_json(stdout: str) -> dict:
    lines = [line for line in stdout.splitlines() if line.strip()]
    if not lines:
        return {}
    try:
        parsed = json.loads(lines[-1])
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_return_values(return_values: str | None) -> list[str]:
    # Pipe-separated, not comma — "any column that has a need to store more
    # than one value must use | as separator," per explicit instruction,
    # matching CFG_TASK_PARAMETERS' own MERGE_KEY/SOURCE_OBJECT/etc.
    declared = [name.strip() for name in (return_values or "").split("|") if name.strip()]
    missing = [name for name in MANDATORY_RETURN_VARS if name not in declared]
    if missing:
        raise HandlerError(
            f"CFG_TASKS.RETURN_VALUES must declare {list(MANDATORY_RETURN_VARS)} for "
            f"HANDLER=PYTHON (a script always reports at least these two) — missing {missing}"
        )
    return declared


def _upsert_offset_tracker(
    conn: Connection, task_id: int, offset_type: str, offset_value: str
) -> None:
    offset_type = offset_type.strip().upper()
    if offset_type not in _VALID_OFFSET_TYPES:
        raise HandlerError(
            f"script reported {LATEST_OFFSET_UPDATE_VAR} datatype {offset_type!r}, must be "
            f"one of {sorted(_VALID_OFFSET_TYPES)} (case-insensitive)"
        )
    now = datetime.now(UTC)
    existing = conn.execute(
        text("SELECT 1 FROM AUD_TASK_OFFSET_TRACKER WHERE TASK_ID = :task_id"), {"task_id": task_id}
    ).scalar_one_or_none()
    params = {
        "task_id": task_id,
        "offset_type": offset_type,
        "offset_value": offset_value,
        "now": now,
    }
    if existing is None:
        conn.execute(
            text(
                "INSERT INTO AUD_TASK_OFFSET_TRACKER (TASK_ID, OFFSET_TYPE, OFFSET_VALUE, "
                "LAST_UPDATED_TIMESTAMP) VALUES (:task_id, :offset_type, :offset_value, :now)"
            ),
            params,
        )
    else:
        conn.execute(
            text(
                "UPDATE AUD_TASK_OFFSET_TRACKER SET OFFSET_TYPE = :offset_type, "
                "OFFSET_VALUE = :offset_value, LAST_UPDATED_TIMESTAMP = :now "
                "WHERE TASK_ID = :task_id"
            ),
            params,
        )


def execute(cfg_conn: Connection, ctx: TaskExecutionContext) -> HandlerResult:
    """Run this task's SCRIPT_NAME as a subprocess; return its reported variables."""
    if not ctx.script_name:
        raise HandlerError("CFG_TASKS.SCRIPT_NAME is required for HANDLER=PYTHON")
    declared = _parse_return_values(ctx.return_values)

    env = dict(os.environ)
    env["ETL_CRAFT_PIPELINE_CODE"] = ctx.pipeline_code
    env["ETL_CRAFT_TASK_CODE"] = ctx.task_code

    process = subprocess.run(
        [sys.executable, ctx.script_name], env=env, capture_output=True, text=True
    )
    if process.returncode != 0:
        detail = (process.stderr or process.stdout or "").strip()[-2000:]
        raise HandlerError(
            f"script {ctx.script_name!r} exited {process.returncode}: {detail or '(no output)'}"
        )

    reported = _parse_trailing_json(process.stdout)
    missing_mandatory = [name for name in MANDATORY_RETURN_VARS if name not in reported]
    if missing_mandatory:
        raise HandlerError(
            f"script {ctx.script_name!r} did not report {missing_mandatory} — "
            f"HANDLER=PYTHON always requires both {list(MANDATORY_RETURN_VARS)}"
        )

    variables: dict[str, object] = {name: reported[name] for name in declared if name in reported}

    offset_raw = str(reported[LATEST_OFFSET_UPDATE_VAR])
    if "|" not in offset_raw:
        raise HandlerError(
            f"script {ctx.script_name!r} reported {LATEST_OFFSET_UPDATE_VAR}={offset_raw!r}, "
            "expected 'value|datatype' (e.g. '2023-01-01 00:00:00|timestamp')"
        )
    offset_value, offset_type = offset_raw.split("|", 1)
    _upsert_offset_tracker(cfg_conn, ctx.task_id, offset_type, offset_value.strip())

    try:
        ingestion_count = int(reported[INGESTION_COUNT_VAR])
    except (TypeError, ValueError) as exc:
        raise HandlerError(
            f"script {ctx.script_name!r} reported {INGESTION_COUNT_VAR}="
            f"{reported[INGESTION_COUNT_VAR]!r}, expected a number"
        ) from exc

    return HandlerResult(source_count=ingestion_count, variables=variables)
