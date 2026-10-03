"""A transaction on the SQLite Engine DB is all or nothing, savepoints and DDL included."""

import threading

import pytest
from sqlalchemy import text

from etl_craft.engine import runlog
from fixtures.engine_db import apply_schema, sqlite_engine_db

pytestmark = pytest.mark.engine_sqlite


@pytest.fixture
def engine(tmp_path):
    db = sqlite_engine_db(tmp_path)
    apply_schema(db.engine)
    with db.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
                "VALUES ('P', 'P', 'FULL')"
            )
        )
    yield db.engine
    db.engine.dispose()


def pipeline_id(engine):
    with engine.connect() as conn:
        return conn.execute(text("SELECT PIPELINE_ID FROM CFG_PIPELINES")).scalar_one()


def run_count(engine):
    with engine.connect() as conn:
        return conn.execute(text("SELECT COUNT(*) FROM AUD_PIPELINES_RUN_LOG")).scalar_one()


class BoomError(Exception):
    pass


def test_a_savepoint_that_is_the_first_write_rolls_back_with_its_transaction(engine):
    pid = pipeline_id(engine)
    with pytest.raises(BoomError), engine.begin() as conn:
        with conn.begin_nested():
            conn.execute(
                text(
                    "INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS) "
                    "VALUES (:p, 'IN-PROGRESS')"
                ),
                {"p": pid},
            )
        raise BoomError
    assert run_count(engine) == 0


def test_a_run_started_in_a_transaction_that_fails_is_not_left_in_progress(engine):
    # The window between starting a run and marking it SKIPPED, when the gate refused it.
    pid = pipeline_id(engine)
    with pytest.raises(BoomError), engine.begin() as conn:
        runlog.find_or_create_active_run(conn, pid)
        raise BoomError
    assert run_count(engine) == 0


def test_ddl_rolls_back_with_its_transaction(engine):
    with pytest.raises(BoomError), engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE T_ROLLED_BACK (ID INTEGER)")
        raise BoomError
    with engine.connect() as conn:
        found = conn.execute(
            text("SELECT COUNT(*) FROM sqlite_master WHERE name = 'T_ROLLED_BACK'")
        ).scalar_one()
    assert found == 0


def test_transactions_that_read_then_write_wait_for_each_other(engine):
    pid = pipeline_id(engine)
    barrier = threading.Barrier(4)
    errors = []

    def read_then_write():
        try:
            barrier.wait(timeout=10)
            with engine.begin() as conn:
                seen = conn.execute(text("SELECT COUNT(*) FROM AUD_PIPELINES_RUN_LOG")).scalar_one()
                conn.execute(
                    text("INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS) VALUES (:p, :s)"),
                    {"p": pid, "s": "SUCCESS" if seen >= 0 else "FAILED"},
                )
        except Exception as error:
            errors.append(error)

    threads = [threading.Thread(target=read_then_write) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    assert errors == []
    assert run_count(engine) == 4
