"""Dirty keys survive out-of-order commits and acknowledgement races on both Engine DBs."""

import json
from datetime import UTC, datetime

import pytest
from sqlalchemy import text

from etl_craft.core.actor import purpose
from etl_craft.engine.cloning import CloneState
from fixtures.metadata import add_pipeline
from fixtures.races import paused_after


@pytest.mark.chaos
@pytest.mark.engine_postgres
def test_pending_versions_survive_an_engine_database_reconnect(postgres_database):
    from fixtures.engine_db import apply_schema

    apply_schema(postgres_database.engine)
    change, pipeline = state(postgres_database)
    with postgres_database.engine.begin() as conn:
        pause = add_pause(conn, pipeline, "restart")
    postgres_database.engine.dispose()
    pending = change.pending(postgres_database.engine, "", 10)
    assert json.loads(pending[0][0]) == [pause]


def state(engine_db):
    engine = engine_db.engine
    with engine.begin() as conn:
        run = add_pipeline(conn, "P")
    change = CloneState("test-mirror", "AUD_PIPELINE_PAUSES")
    change.register(engine)
    return change, run


def add_pause(conn, pipeline, reason):
    return conn.execute(
        text(
            "INSERT INTO AUD_PIPELINE_PAUSES "
            "(PIPELINE_ID, REASON, PAUSED_BY, PAUSED_BY_KIND, PAUSED_AT) "
            "VALUES (:p,:r,'tester','SYSTEM',:now) RETURNING PIPELINE_PAUSE_ID AS pause_id"
        ),
        {"p": pipeline, "r": reason, "now": datetime.now(UTC)},
    ).scalar_one()


def test_exact_acknowledgement_preserves_a_newer_update_and_a_purge(engine_db):
    engine = engine_db.engine
    change, pipeline = state(engine_db)
    with engine.begin() as conn:
        pause = add_pause(conn, pipeline, "initial")
    first = change.pending(engine, "", 10)
    assert json.loads(first[0][0]) == [pause]
    with engine.begin() as conn:
        before = conn.execute(
            text("SELECT UPDATED_AT AS updated_at FROM AUD_PIPELINE_PAUSES")
        ).scalar_one()
        conn.execute(
            text("UPDATE AUD_PIPELINE_PAUSES SET REASON='corrected' WHERE PIPELINE_PAUSE_ID=:p"),
            {"p": pause},
        )
        after = conn.execute(
            text("SELECT UPDATED_AT AS updated_at FROM AUD_PIPELINE_PAUSES")
        ).scalar_one()
    assert after >= before
    change.acknowledge(engine, first)
    second = change.pending(engine, "", 10)
    assert len(second) == 1 and second[0][1] != first[0][1]
    token = purpose.set("retention")
    try:
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM AUD_PIPELINE_PAUSES WHERE PIPELINE_PAUSE_ID=:p"), {"p": pause}
            )
    finally:
        purpose.reset(token)
    change.acknowledge(engine, second)
    deleted = change.pending(engine, "", 10)
    assert len(deleted) == 1 and deleted[0][1] != second[0][1]
    change.acknowledge(engine, deleted)
    assert change.pending(engine, "", 10) == []


def test_old_key_committed_after_a_checkpoint_remains_pending(engine_db):
    engine = engine_db.engine
    change, pipeline = state(engine_db)
    if engine_db.dialect.name == "sqlite":
        # SQLite serializes writers; an earlier application timestamp is still permitted.
        with engine.begin() as conn:
            pause = add_pause(conn, pipeline, "late")
        change.complete(engine)
        assert json.loads(change.pending(engine, "", 10)[0][0]) == [pause]
        return
    from sqlalchemy.engine import Connection

    def writer():
        with engine.begin() as conn:
            return add_pause(conn, pipeline, "late commit")

    with engine.begin() as conn:
        other = add_pipeline(conn, "Q")
    with paused_after(
        writer,
        owner=Connection,
        name="execute",
        when=lambda conn, query, *args, **kwargs: (
            "INSERT INTO AUD_PIPELINE_PAUSES" in str(query) and args[0].get("r") == "late commit"
        ),
    ):
        assert change.pending(engine, "", 10) == []
        with engine.begin() as conn:
            later = add_pause(conn, other, "earlier commit")
        observed = change.pending(engine, "", 10)
        assert json.loads(observed[0][0]) == [later]
        change.acknowledge(engine, observed)
        change.complete(engine)
    retained = change.pending(engine, "", 10)
    assert len(retained) == 1
    assert json.loads(retained[0][0])[0] < later
