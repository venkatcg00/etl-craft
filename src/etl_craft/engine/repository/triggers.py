"""Explicit parent/child audit links bounded to the runs being displayed."""

from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy import bindparam
from sqlalchemy.engine import Connection

from etl_craft.engine.queries import statement


@dataclass(frozen=True)
class TriggerLink:
    """One immutable link from a trigger task run to its child run."""

    child_run_id: int
    child_code: str
    task_run_id: int
    parent_run_id: int
    parent_code: str
    task_code: str


def fetch_trigger_links(conn: Connection, run_ids: Sequence[int]) -> list[TriggerLink]:
    """Read only links touching the requested run detail or bounded catalog history."""
    if not run_ids:
        return []
    query = statement(conn, "trigger_links").bindparams(bindparam("run_ids", expanding=True))
    links = {}
    for start in range(0, len(run_ids), 400):
        for row in conn.execute(query, {"run_ids": list(run_ids[start : start + 400])}):
            link = TriggerLink(**row._mapping)
            links[link.child_run_id] = link
    return [links[key] for key in sorted(links)]
