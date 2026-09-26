"""Land Client Alpha's support interactions: the next ones after the last loaded, each run.

Client Alpha's desk exports every field as text, the way a document store hands it over; the
parse step casts them. The feed is synthetic and repeatable: interaction ``n`` is handled by
agent ``A0{n % 4 + 1}``, except every ninth, a test call (agent ``TEST``), and every eleventh,
an agent no one knows (``A99``). Ratings run 0 to 6, so some are out of range, and some calls
run long. The offset is the last interaction id loaded, a number.
"""

from datetime import date, timedelta

from sqlalchemy import text

from etl_craft.scripting import Offset, ScriptResult, ScriptTask

AREAS = ("billing", "technical", "account")


def interaction(n: int) -> dict[str, str | int]:
    """Return Client Alpha's interaction ``n``, every field as its desk exports it."""
    agent = "TEST" if n % 9 == 0 else "A99" if n % 11 == 0 else f"A0{n % 4 + 1}"
    return {
        "interaction_id": n,
        "agent_code": agent,
        "support_area": AREAS[n % 3],
        "contact_date": (date(2026, 9, 1) + timedelta(days=n % 28)).isoformat(),
        "status": "resolved" if n % 4 else "pending",
        "duration_seconds": str(60 * n % 900 + 30),
        "rating": str(n % 7),
    }


def run(task: ScriptTask) -> ScriptResult:
    """Land the next interactions after the stored offset."""
    last = int(task.offset.value) if task.offset else 0
    count = int(task.input_params.get("rows", 12))
    table = task.table("lnd.client_alpha")
    rows = [interaction(n) for n in range(last + 1, last + count + 1)]
    with task.warehouse() as engine, engine.begin() as conn:
        conn.execute(
            text(
                f"CREATE TABLE IF NOT EXISTS {table} (interaction_id BIGINT, agent_code VARCHAR, "
                "support_area VARCHAR, contact_date VARCHAR, status VARCHAR, "
                "duration_seconds VARCHAR, rating VARCHAR, pipeline_run_id BIGINT)"
            )
        )
        for row in rows:
            conn.execute(
                text(
                    f"INSERT INTO {table} VALUES (:interaction_id, :agent_code, :support_area, "
                    ":contact_date, :status, :duration_seconds, :rating, :run)"
                ),
                {**row, "run": task.pipeline_run_id},
            )
    task.logger.info("landed Client Alpha interactions %d to %d", last + 1, last + count)
    return ScriptResult(row_count=len(rows), offset=Offset.number(last + count))
