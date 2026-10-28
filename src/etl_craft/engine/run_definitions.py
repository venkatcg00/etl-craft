"""Bind execution reads to a stored run definition, independent of live controls."""

from collections.abc import Iterator
from contextlib import contextmanager, nullcontext

from sqlalchemy.engine import Connection, Engine

from etl_craft.core.definitions import Definition, current, using
from etl_craft.core.errors import RunStateError
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import selected_project
from etl_craft.engine.runlog import RunSelector, select_run


@contextmanager
def for_run(bind: Engine | Connection, run_id: int | None) -> Iterator[None]:
    """Read once per execution scope; nested metadata readers share the admitted image."""
    old = current.get()
    if run_id is None or (old is not None and old.run_id == run_id):
        yield
        return
    with bind.connect() if isinstance(bind, Engine) else nullcontext(bind) as conn:
        definition, project_id = _read(conn, run_id)
    with using(definition), selected_project(project_id):
        yield


@contextmanager
def for_selection(engine: Engine, pipeline_id: int, selector: RunSelector) -> Iterator[None]:
    """Select an exact existing run before resolving its admitted task codes."""
    with engine.connect() as conn:
        selected = select_run(conn, pipeline_id, selector)
        definition, project_id = _read(conn, selected.pipeline_run_id)
    with using(definition), selected_project(project_id):
        yield


def _read(conn: Connection, run_id: int) -> tuple[Definition, int]:
    row = conn.execute(statement(conn, "run_definition"), {"run_id": run_id}).one_or_none()
    if row is None or row.snapshot is None:
        raise RunStateError(
            f"run {run_id}: no admitted metadata snapshot; expected a pinned definition; "
            "start a new run instead of reopening a pre-bundle completed run"
        )
    return Definition.read(row.snapshot, run_id, row.bundle_id), int(row.project_id)


@contextmanager
def for_admission(conn: Connection, pipeline_id: int) -> Iterator[Definition | None]:
    """Read the last passing definition before a new run's connection preflight."""
    if not conn.engine.get_execution_options().get("refresh_required"):
        yield None
        return
    from etl_craft.engine.refreshes import admitted

    snapshot, _, bundle_id = admitted(conn, pipeline_id)
    definition = Definition.read(snapshot, bundle_id=bundle_id)
    with using(definition):
        yield definition
