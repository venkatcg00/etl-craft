"""Land the agent list, whole, each run: it changes from one version to the next.

Version 1 has four agents. From version 2, Bo moves from the east team to the west and the
feed has no email for Bo (the SCD1 merge keeps the one it had), and Di has left. The offset is
the version loaded, so each run loads the next one.
"""

from sqlalchemy import text

from etl_craft.scripting import Offset, ScriptResult, ScriptTask


def agents(version: int) -> list[dict[str, str | None]]:
    """Return version ``version`` of the agent list."""
    rows: list[dict[str, str | None]] = [
        {"code": "A01", "name": "Ann", "team": "east", "email": "ann@example.com", "left": "N"},
        {"code": "A02", "name": "Bo", "team": "east", "email": "bo@example.com", "left": "N"},
        {"code": "A03", "name": "Cy", "team": "west", "email": "cy@example.com", "left": "N"},
        {"code": "A04", "name": "Di", "team": "west", "email": "di@example.com", "left": "N"},
    ]
    if version >= 2:
        rows[1] |= {"team": "west", "email": None}
        rows[3] |= {"left": "Y"}
    return rows


def run(task: ScriptTask) -> ScriptResult:
    """Replace the landed agent list with the next version."""
    version = (int(task.offset.value) if task.offset else 0) + 1
    table = task.table("lnd.agents")
    rows = agents(version)
    with task.warehouse() as engine, engine.begin() as conn:
        conn.execute(
            text(
                f"CREATE TABLE IF NOT EXISTS {table} (agent_code VARCHAR(100), "
                "agent_name VARCHAR(100), team VARCHAR(100), email VARCHAR(100), "
                "left_company VARCHAR(100))"
            )
        )
        conn.execute(text(f"DELETE FROM {table}"))
        for row in rows:
            conn.execute(
                text(f"INSERT INTO {table} VALUES (:code, :name, :team, :email, :left)"), row
            )
    return ScriptResult(row_count=len(rows), offset=Offset.number(version))
