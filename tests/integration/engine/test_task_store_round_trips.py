"""Both Engine DBs batch task metadata without changing exact-run peer selection."""

from etl_craft.core.enums import RunStatus, SqlAction
from etl_craft.dialects.engine import for_engine
from etl_craft.engine import locks, transitions
from etl_craft.engine.task_store import LocalTaskStore, TargetState, TaskIdentity
from etl_craft.handlers.resources import LocalLocker
from fixtures.metadata import add_pipeline, add_task, start_run
from fixtures.round_trips import count_round_trips


def test_task_state_batches_peer_statuses_and_target_contracts(engine_db):
    db = engine_db.engine
    with db.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        peers = [
            add_task(
                conn,
                pipeline,
                code,
                "SQL",
                SQL_ACTION="CREATE_TABLE",
                TARGET_OBJECT="catalog.schema.target",
                SELECT_QUERY="SELECT 1 AS id",
            )
            for code in ("first", "second")
        ]
        selected = add_task(
            conn,
            pipeline,
            "drop",
            "SQL",
            SQL_ACTION="DROP_TABLE",
            TARGET_OBJECT="catalog.schema.target",
        )
        run = start_run(conn, pipeline)
        for task in peers:
            binding = transitions.find_or_create_task_run(conn, task, run)
            transitions.finish_task_run(conn, binding.task_run_id, status=RunStatus.SUCCESS)
    store = LocalTaskStore(
        db,
        engine_db.config,
        TaskIdentity(pipeline, selected, run, 0, None, None),
        LocalLocker(db, engine_db.config),
    )
    lock = locks.target("catalog.schema.target", "catalog")
    with store.hold(lock.key, lock.name, 1), count_round_trips(db) as counter:
        state = store.state(SqlAction.DROP_TABLE)
        assert {peer.task_id for peer in state.peers} == set(peers)
        assert state.peer_statuses == {str(task): RunStatus.SUCCESS for task in peers}
        assert store.target("catalog.schema.target") == TargetState(None, None)
    overhead = 1 if for_engine(db).supports_api_replicas else 0
    assert counter.result()["engine_statements"] == 1 + overhead
    assert counter.result()["engine_transactions"] == 1
