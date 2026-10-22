"""Budget counters exclude unrelated work without hiding an attempt's own threads."""

import threading

import pytest
from sqlalchemy import create_engine

from fixtures.round_trips import count_round_trips

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("include_threads", [False, True])
def test_query_counter_isolates_thread_and_engine(tmp_path, include_threads):
    engine = create_engine(f"sqlite:///{tmp_path / 'counts.db'}")
    unrelated = create_engine(engine.url)
    errors = []

    def background():
        try:
            for selected in (engine, unrelated):
                with selected.connect() as conn:
                    conn.exec_driver_sql("SELECT 2")
        except Exception as error:
            errors.append(error)

    try:
        counted = (
            count_round_trips(engine, include_threads=True)
            if include_threads
            else count_round_trips(engine)
        )
        with counted as counter:
            with engine.connect() as conn:
                conn.exec_driver_sql("SELECT 1")
            worker = threading.Thread(target=background)
            worker.start()
            worker.join(5)
            assert not worker.is_alive()
        assert not errors
        expected = 2 if include_threads else 1
        assert counter.result()["engine_statements"] == expected
        assert counter.result()["engine_transactions"] == expected
    finally:
        engine.dispose()
        unrelated.dispose()
