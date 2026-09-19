"""HANDLER=PYTHON execution — invokes a team's own ingestion script as a subprocess.

[ADDITION] CLAUDE.md establishes the one hard rule here and nothing else:
"the engine does not auto-inject the run id into them; the team's own
script is responsible for fetching and including pipeline_run_id in
whatever it inserts." Everything below this line is this module's own
invention (no other part of this feature's instructions covered the PYTHON
handler in the level of detail given to the SQL/BUSINESS_RULES ones) —
flagged for confirmation before a real script gets written against it.

Contract: the script named by CFG_TASKS.SCRIPT_NAME is run as
`<python> <script>`, inheriting this process's environment plus
ETL_CRAFT_PIPELINE_CODE / ETL_CRAFT_TASK_CODE (so the script can resolve its
own pipeline_run_id — e.g. via `etl-craft`'s own craft-connector.yml, in the
same working directory) and running in the current working directory, same
as every other invocation of `etl-craft` itself. A nonzero exit is a
HandlerError, its message built from the process's own stderr (or stdout, if
stderr is empty).

The script may optionally report counts back by printing one JSON object as
the *last* line of stdout (everything before it is free-form log output,
captured verbatim into AUD_TASK_RUN_LOG.TASK_LOG):
  {"ingestion_count": <int>, "latest_offset_value": <str>,
   "latest_offset_type": "NUMBER"|"TEXT"|"TIMESTAMP"}
All three keys are optional and independent. `ingestion_count` maps to
HandlerResult.source_count (CFG_TASKS.RETURN_VALUES' INGESTION_COUNT token —
this module doesn't itself enforce that a script's declared RETURN_VALUES
matches what it actually printed; that's a validate.py-shaped check for
later, not built here). `latest_offset_value`/`latest_offset_type` together
upsert AUD_TASK_OFFSET_TRACKER for this task (LATEST_OFFSET_UPDATE) — both
must be present together, or neither is applied.
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

_MAX_TASK_LOG_CHARS = 10_000
_VALID_OFFSET_TYPES = frozenset({"NUMBER", "TEXT", "TIMESTAMP"})


def _parse_trailing_json(stdout: str) -> dict:
    lines = [line for line in stdout.splitlines() if line.strip()]
    if not lines:
        return {}
    try:
        parsed = json.loads(lines[-1])
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _upsert_offset_tracker(
    conn: Connection, task_id: int, offset_type: str, offset_value: str
) -> None:
    if offset_type not in _VALID_OFFSET_TYPES:
        raise HandlerError(
            f"script reported latest_offset_type={offset_type!r}, must be one of "
            f"{sorted(_VALID_OFFSET_TYPES)}"
        )
    now = datetime.now(UTC)
    existing = conn.execute(
        text("SELECT 1 FROM AUD_TASK_OFFSET_TRACKER WHERE TASK_ID = :task_id"), {"task_id": task_id}
    ).scalar_one_or_none()
    if existing is None:
        conn.execute(
            text(
                "INSERT INTO AUD_TASK_OFFSET_TRACKER (TASK_ID, OFFSET_TYPE, OFFSET_VALUE, "
                "LAST_UPDATED_TIMESTAMP) VALUES (:task_id, :offset_type, :offset_value, :now)"
            ),
            {
                "task_id": task_id,
                "offset_type": offset_type,
                "offset_value": offset_value,
                "now": now,
            },
        )
    else:
        conn.execute(
            text(
                "UPDATE AUD_TASK_OFFSET_TRACKER SET OFFSET_TYPE = :offset_type, "
                "OFFSET_VALUE = :offset_value, LAST_UPDATED_TIMESTAMP = :now "
                "WHERE TASK_ID = :task_id"
            ),
            {
                "task_id": task_id,
                "offset_type": offset_type,
                "offset_value": offset_value,
                "now": now,
            },
        )


def execute(cfg_conn: Connection, ctx: TaskExecutionContext) -> HandlerResult:
    """Run this task's SCRIPT_NAME as a subprocess; return whatever counts/log it reports."""
    if not ctx.script_name:
        raise HandlerError("CFG_TASKS.SCRIPT_NAME is required for HANDLER=PYTHON")

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
    ingestion_count = reported.get("ingestion_count")
    offset_value = reported.get("latest_offset_value")
    offset_type = reported.get("latest_offset_type")
    if offset_value is not None and offset_type is not None:
        _upsert_offset_tracker(cfg_conn, ctx.task_id, str(offset_type), str(offset_value))

    task_log = (process.stdout or "")[:_MAX_TASK_LOG_CHARS]
    return HandlerResult(
        source_count=int(ingestion_count) if ingestion_count is not None else None,
        task_log=task_log or None,
    )
