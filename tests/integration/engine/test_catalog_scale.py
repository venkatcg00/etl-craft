"""Catalog statement counts and bounded, classified history on both Engine DBs."""

from sqlalchemy import event, inspect, text

from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.core.enums import InterventionAction
from etl_craft.engine.repository.intervention_rows import record_intervention
from etl_craft.services.catalog.collect import build_catalog
from etl_craft.services.catalog.site import write_site
from fixtures.catalog_scale import seed_catalog
from fixtures.metadata import add_pipeline_dependency


def test_cached_sql_catalog_statement_count_does_not_grow_with_task_count(engine_db):
    engine, config = engine_db.engine, engine_db.config
    counts = []

    def record(*args):
        counts.append(1)

    measured = []
    with acting_as(SYSTEM_ACTOR):
        for prefix, pipelines, total in (("SMALL", 10, 100), ("LARGE", 90, 1000)):
            with engine.begin() as conn:
                seed_catalog(conn, prefix=prefix, pipelines=pipelines, tasks=10, runs=2)
            build_catalog(engine, config)
            counts.clear()
            event.listen(engine, "before_cursor_execute", record)
            try:
                catalog = build_catalog(engine, config)
            finally:
                event.remove(engine, "before_cursor_execute", record)
            assert len(catalog.tasks) == total
            assert len(catalog.column_edges) == total
            measured.append(len(counts))
    assert measured[0] > 0
    assert measured[0] == measured[1], measured


def test_catalog_bounds_history_and_separates_scheduled_backfill_and_repair(engine_db, tmp_path):
    engine, config = engine_db.engine, engine_db.config
    with acting_as(SYSTEM_ACTOR):
        with engine.begin() as conn:
            seed_catalog(conn, prefix="METRIC", pipelines=1, tasks=1, runs=50)
            recent = (
                conn.execute(
                    text("""SELECT PIPELINE_RUN_ID AS id
                FROM AUD_PIPELINES_RUN_LOG ORDER BY PIPELINE_RUN_ID DESC LIMIT 3""")
                )
                .mappings()
                .all()
            )
            conn.execute(
                text(
                    "UPDATE AUD_PIPELINES_RUN_LOG SET BACKFILL='Y', TRIGGER_KIND='BACKFILL' "
                    "WHERE PIPELINE_RUN_ID=:id"
                ),
                {"id": recent[0]["id"]},
            )
            conn.execute(
                text(
                    "UPDATE AUD_PIPELINES_RUN_LOG SET OUTPUT_REVISION=2 WHERE PIPELINE_RUN_ID=:id"
                ),
                {"id": recent[1]["id"]},
            )
            conn.execute(
                text(
                    "INSERT INTO AUD_RUN_INTERVENTIONS (PIPELINE_ID,PIPELINE_RUN_ID,ACTION,"
                    "FROM_STATUS,TO_STATUS,REASON,REQUESTED_BY) SELECT PIPELINE_ID,PIPELINE_RUN_ID,"
                    "'REOPEN','FAILED','IN-PROGRESS','repair','owner' FROM AUD_PIPELINES_RUN_LOG "
                    "WHERE PIPELINE_RUN_ID=:id"
                ),
                {"id": recent[1]["id"]},
            )
        catalog = build_catalog(engine, config)
    runs = catalog.pipelines["METRIC_0"].runs
    assert len(runs) == 30
    assert len(catalog.tasks["METRIC_0.task_0"].runs) == 30
    assert [run.pipeline_run_id for run in runs] == sorted(
        (run.pipeline_run_id for run in runs), reverse=True
    )
    kinds = {run.pipeline_run_id: run.run_kind.value for run in runs}
    assert {
        run.pipeline_run_id: run.run_kind.value for run in catalog.tasks["METRIC_0.task_0"].runs
    } == kinds
    assert kinds[recent[0]["id"]] == "BACKFILL"
    assert kinds[recent[1]["id"]] == "SCHEDULED"
    assert kinds[recent[2]["id"]] == "SCHEDULED"
    site = write_site(catalog, config, tmp_path / "catalog")
    page = (site.folder / "pipelines/METRIC_0.html").read_text()
    assert "<td>Scheduled</td><td>29</td><td>97% of 29</td>" in page
    assert "<td>Backfill</td><td>1</td><td>100% of 1</td>" in page
    assert "<th>Repaired</th>" in page
    assert "<td>Repair</td>" not in page
    for table, index_name, columns in (
        ("AUD_PIPELINES_RUN_LOG", "ix_catalog_pipeline_runs", ["pipeline_id", "pipeline_run_id"]),
        ("AUD_TASK_RUN_LOG", "ix_catalog_task_runs", ["task_id", "task_run_id"]),
    ):
        stored_table = table.lower() if engine.dialect.name == "postgresql" else table
        assert any(
            index["name"] == index_name
            and [name.lower() for name in index["column_names"]] == columns
            for index in inspect(engine).get_indexes(stored_table)
        )


def test_catalog_keeps_only_consumption_and_interventions_touching_shown_runs(engine_db):
    engine, config = engine_db.engine, engine_db.config
    with acting_as(SYSTEM_ACTOR):
        with engine.begin() as conn:
            seed_catalog(conn, prefix="BOUNDED", pipelines=2, tasks=1, runs=50)
            pipelines = dict(
                conn.execute(
                    text("SELECT PIPELINE_CODE AS code, PIPELINE_ID AS id FROM CFG_PIPELINES")
                )
                .tuples()
                .all()
            )
            downstream, upstream = pipelines["BOUNDED_0"], pipelines["BOUNDED_1"]
            dependency = add_pipeline_dependency(conn, downstream, upstream)
            runs = {}
            for pipeline in (downstream, upstream):
                rows = (
                    conn.execute(
                        text(
                            "SELECT PIPELINE_RUN_ID AS id FROM AUD_PIPELINES_RUN_LOG "
                            "WHERE PIPELINE_ID=:id ORDER BY PIPELINE_RUN_ID"
                        ),
                        {"id": pipeline},
                    )
                    .mappings()
                    .all()
                )
                runs[pipeline] = (rows[0]["id"], rows[-1]["id"])
            old_down, new_down = runs[downstream]
            old_up, new_up = runs[upstream]
            for run_id, consumed in ((old_down, old_up), (new_down, old_up), (old_down, new_up)):
                conn.execute(
                    text(
                        "INSERT INTO AUD_DEPENDENCY_CONSUMPTION "
                        "(PIPELINE_DEPENDENCY_ID, PIPELINE_ID, PIPELINE_RUN_ID, "
                        "DEPENDS_ON_PIPELINE_ID, CONSUMED_PIPELINE_RUN_ID) "
                        "VALUES (:dependency, :downstream, :run, :upstream, :consumed)"
                    ),
                    {
                        "dependency": dependency,
                        "downstream": downstream,
                        "run": run_id,
                        "upstream": upstream,
                        "consumed": consumed,
                    },
                )
            for run_id in (old_down, new_down):
                record_intervention(
                    conn,
                    pipeline_id=downstream,
                    pipeline_run_id=run_id,
                    action=InterventionAction.REOPEN,
                    reason="catalog fixture",
                    requested_by="system",
                )
        catalog = build_catalog(engine, config)
    pairs = {
        (c.pipeline_run_id, c.upstream_run_id)
        for records in catalog.built_from.values()
        for c in records
    }
    assert pairs == {(new_down, old_up), (old_down, new_up)}
    assert set(catalog.pipelines["BOUNDED_0"].run_changes) == {new_down}
    assert old_down not in {r.pipeline_run_id for r in catalog.pipelines["BOUNDED_0"].runs}
