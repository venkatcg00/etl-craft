"""Project identities stay isolated when teams reuse pipeline and warehouse names."""

from dataclasses import replace

from sqlalchemy import text

from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.engine.repository.hash_versions import fetch_hash_version, save_hash_version
from etl_craft.engine.repository.pipelines import resolve_pipeline_id, selected_project
from etl_craft.engine.run_definitions import for_run
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.requests import TagFilter
from fixtures.metadata import add_pipeline, add_task, start_run


def project(conn, code):
    return conn.execute(
        text(
            "INSERT INTO CFG_PROJECTS (PROJECT_CODE,PROJECT_NAME,DEFAULT_POOL) "
            "VALUES (:code,:code,'default') RETURNING PROJECT_ID AS project_id"
        ),
        {"code": code},
    ).scalar_one()


def pipeline(conn, project_id, code):
    return conn.execute(
        text(
            "INSERT INTO CFG_PIPELINES (PROJECT_ID,PIPELINE_CODE,PIPELINE_NAME,REFRESH_TYPE) "
            "VALUES (:project,:code,:code,'FULL') RETURNING PIPELINE_ID AS pipeline_id"
        ),
        {"project": project_id, "code": code},
    ).scalar_one()


def test_selected_project_resolves_and_lists_only_its_same_code_pipeline(engine_db):
    with engine_db.engine.begin() as conn:
        first = add_pipeline(conn, "SHARED")
        second_project = project(conn, "second")
        second = pipeline(conn, second_project, "SHARED")
        with selected_project(second_project):
            assert resolve_pipeline_id(conn, "SHARED") == second
        assert resolve_pipeline_id(conn, "SHARED") == first
    ctx = OperationContext(engine_db.engine, engine_db.config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    for code, identity in (("default", first), ("second", second)):
        result = invoke(replace(ctx, project=code), ActionName.PIPELINES, TagFilter())
        assert [row.pipeline_id for row in result.pipelines] == [identity]


def test_run_restores_its_project_for_target_contracts(engine_db):
    with engine_db.engine.begin() as conn:
        second_project = project(conn, "second")
        own = pipeline(conn, second_project, "SHARED")
        add_task(conn, own, "T", "PYTHON")
        run = start_run(conn, own)
        save_hash_version(conn, "catalog.main.target", 1)
    with for_run(engine_db.engine, run), engine_db.engine.begin() as conn:
        assert fetch_hash_version(conn, "catalog.main.target") is None
        save_hash_version(conn, "catalog.main.target", 2)
    with engine_db.engine.connect() as conn:
        assert fetch_hash_version(conn, "catalog.main.target") == 1
        with selected_project(second_project):
            assert fetch_hash_version(conn, "catalog.main.target") == 2


def test_a_full_project_does_not_block_another_project(engine_db):
    from datetime import UTC, datetime, timedelta

    from etl_craft.engine import agent_transitions, transitions

    with engine_db.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_PROJECTS SET MAX_RUNNING_TASKS=1"))
        other = project(conn, "other")
        attempts = []
        for scope, code in ((1, "A"), (1, "B"), (other, "A")):
            own = pipeline(conn, scope, code)
            task = add_task(conn, own, "T")
            run = start_run(conn, own)
            summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
            attempts.append(transitions.queue_attempt(conn, summary, SYSTEM_ACTOR))
    capacity = {
        "host": "shared-host",
        "owner": "owner",
        "warehouse_limit": 1,
        "local_cpu": 100,
        "local_memory_mb": 10000,
        "local_slots": 100,
        "python_slots": 100,
        "warehouse_slots": 100,
        "lease": datetime.now(UTC) + timedelta(seconds=60),
    }
    for attempt, expected in zip(attempts, (True, False, True), strict=True):
        with engine_db.engine.begin() as conn:
            assert (
                agent_transitions.claim_with_capacity(
                    conn, attempt, SYSTEM_ACTOR, capacity, wait=True
                )
                is expected
            )


def test_config_apply_keeps_other_projects_and_links_same_code_upstream(engine_db, tmp_path):
    import csv

    from etl_craft.services.config_files import export_config, read_config_files, sync_config

    (tmp_path / "python_scripts").mkdir()
    (tmp_path / "python_scripts/task.py").write_text("def run(task):\n    pass\n")
    with engine_db.engine.begin() as conn:
        upstream = add_pipeline(conn, "SHARED")
        upstream_task = add_task(
            conn, upstream, "T", "PYTHON", SCRIPT_FILE="python_scripts/task.py"
        )
    with engine_db.engine.begin() as conn:
        project(conn, "second")
    folder = tmp_path / "config"
    export_config(engine_db.engine, folder)
    for name, changes in (
        (
            "pipeline_dependencies.csv",
            {
                "PIPELINE_CODE": "SHARED",
                "DEPENDS_ON_PROJECT_CODE": "default",
                "DEPENDS_ON_PIPELINE_CODE": "SHARED",
                "DEPENDENCY_TYPE": "SUCCESS",
                "CONSUME_REPAIRS": "Y",
                "ACTIVE_FLAG": "Y",
            },
        ),
        (
            "task_dependencies.csv",
            {
                "PIPELINE_CODE": "SHARED",
                "TASK_CODE": "T",
                "DEPENDS_ON_PROJECT_CODE": "default",
                "DEPENDS_ON_PIPELINE_CODE": "SHARED",
                "DEPENDS_ON_TASK_CODE": "T",
                "DEPENDENCY_TYPE": "SUCCESS",
                "CONSUME_REPAIRS": "Y",
                "ACTIVE_FLAG": "Y",
            },
        ),
    ):
        path = folder / name
        with path.open() as stream:
            reader = csv.DictReader(stream)
            fields, rows = reader.fieldnames, list(reader)
        with path.open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
            writer.writeheader()
            writer.writerows([{**rows[0], **changes}] if rows else [changes])
    config = replace(
        engine_db.config, project="second", config_path=tmp_path / "craft-connector.yml"
    )
    result = sync_config(engine_db.engine, config, read_config_files(folder), apply=True)
    assert not result.failed, result.findings
    with engine_db.engine.connect() as conn:
        assert resolve_pipeline_id(conn, "SHARED") == upstream
        row = conn.execute(
            text(
                "SELECT d.PIPELINE_ID AS child,d.DEPENDS_ON_PIPELINE_ID AS parent "
                "FROM CFG_PIPELINE_DEPENDENCY d"
            )
        ).one()
        assert row.parent == upstream and row.child != upstream
        task_edge = conn.execute(
            text("SELECT TASK_ID AS child,DEPENDS_ON_TASK_ID AS parent FROM CFG_TASK_DEPENDENCY")
        ).one()
        assert task_edge.parent == upstream_task and task_edge.child != upstream_task
    assert (
        sync_config(engine_db.engine, config, read_config_files(folder), apply=True).changes == ()
    )


def test_developer_cannot_change_a_projects_approved_connections(engine_db, tmp_path):
    import base64

    import pytest

    from etl_craft.config.object_models import Artifacts
    from etl_craft.core.errors import PermissionDeniedError
    from etl_craft.services.operations.bundles import PublishBundle

    config = replace(engine_db.config, artifacts=Artifacts((tmp_path / "store").as_uri()))
    ctx = OperationContext(engine_db.engine, config, SYSTEM_ACTOR, role=ApiRole.ADMIN)

    def upload(database):
        data = (
            "Secrets:\n  Source_type: ENVIRONMENT\nWarehouse:\n  dev:\n"
            f"    Jdbc_url: jdbc:duckdb:{database}\n    Schema: main\n"
        ).encode()
        return PublishBundle(files={"craft-connector.yml": base64.b64encode(data).decode()})

    original = upload("approved.db")
    invoke(ctx, ActionName.BUNDLE_PUBLISH, original)
    developer = replace(ctx, role=ApiRole.DEVELOPER)
    invoke(developer, ActionName.BUNDLE_PUBLISH, original)
    with pytest.raises(PermissionDeniedError, match="admin role is required"):
        invoke(developer, ActionName.BUNDLE_PUBLISH, upload("redirected.db"))
    with engine_db.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT COUNT(*) AS count FROM AUD_PROJECT_BUNDLES")).scalar_one()
            == 2
        )
        refusal = conn.execute(
            text(
                "SELECT OUTCOME AS outcome,PROJECT_ID AS project_id FROM AUD_ACTIONS "
                "ORDER BY ACTION_ID DESC LIMIT 1"
            )
        ).one()
        assert refusal.outcome == "REFUSED" and refusal.project_id == 1


def test_lineage_versions_and_closing_history_are_project_scoped(engine_db):
    from datetime import UTC, datetime

    from etl_craft.core.enums import YesNo
    from etl_craft.engine.queries import statement
    from etl_craft.engine.repository.lineage import fetch_lineage_versions, record_lineage_versions

    with engine_db.engine.begin() as conn:
        other = project(conn, "other")
        identities = []
        for scope in (1, other):
            with selected_project(scope):
                key = {"catalog_name": "catalog", "schema_name": "main", "table_name": "same"}
                number = conn.execute(statement(conn, "lineage_version_next"), key).scalar_one()
                assert number == 1
                identity = conn.execute(
                    statement(conn, "lineage_version_insert"),
                    key
                    | {
                        "version_number": number,
                        "edges_hash": "a" * 64,
                        "now": datetime(2026, 1, 1, tzinfo=UTC),
                        "forever": datetime(9999, 12, 31, tzinfo=UTC),
                    },
                ).scalar_one()
                identities.append(identity)
                assert [v.lineage_version_id for v in fetch_lineage_versions(conn)] == [identity]
        with selected_project(other):
            record_lineage_versions(conn, None, {})
            assert fetch_lineage_versions(conn)[0].current_flag == YesNo.NO
        assert fetch_lineage_versions(conn)[0].lineage_version_id == identities[0]
        assert fetch_lineage_versions(conn)[0].current_flag == YesNo.YES


def test_project_migration_filenames_and_checksums_are_independent(engine_db, tmp_path):
    from etl_craft.engine.migrations import (
        apply_pending_migrations,
        mark_packaged_migrations_applied,
        pending_migrations,
    )

    with engine_db.engine.begin() as conn:
        mark_packaged_migrations_applied(conn)
        other = project(conn, "other")
    for scope, code in ((1, "FIRST"), (other, "SECOND")):
        folder = tmp_path / str(scope)
        folder.mkdir()
        (folder / "0001_team.sql").write_text(f"CREATE TABLE {code} (ID INTEGER);")
        with selected_project(scope):
            assert "0001_team.sql" in apply_pending_migrations(engine_db.engine, folder)
            assert pending_migrations(engine_db.engine, folder) == []
    with engine_db.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT PROJECT_ID AS project_id,CHECKSUM AS checksum FROM SCHEMA_MIGRATIONS "
                "WHERE SOURCE='PROJECT' AND VERSION='0001_team.sql'"
            )
        ).all()
        assert {row.project_id for row in rows} == {1, other}
        assert len({row.checksum for row in rows}) == 2


def test_docs_publications_are_selected_and_updated_within_their_project(engine_db):
    from etl_craft.engine.repository.docs_site import fetch_publication, record_publication

    with engine_db.engine.begin() as conn:
        other = project(conn, "other")
        record_publication(conn, "https://first.example.com")
        original = fetch_publication(conn)
        with selected_project(other):
            assert fetch_publication(conn) is None
            record_publication(conn, "https://second.example.com")
            second = fetch_publication(conn)
            record_publication(conn, second.url)
            assert fetch_publication(conn).docs_publication_id == second.docs_publication_id
        assert fetch_publication(conn) == original


def test_project_default_pool_is_recorded_on_queued_attempts(engine_db):
    from etl_craft.engine import transitions
    from etl_craft.engine.repository.tasks import fetch_task_execution_detail

    with engine_db.engine.begin() as conn:
        conn.execute(text("INSERT INTO CFG_POOLS (POOL_NAME) VALUES ('team')"))
        other = project(conn, "other")
        conn.execute(
            text("UPDATE CFG_PROJECTS SET DEFAULT_POOL='team' WHERE PROJECT_ID=:id"), {"id": other}
        )
        own = pipeline(conn, other, "P")
        task = add_task(conn, own, "T")
        assert fetch_task_execution_detail(conn, task).pool == "team"
        run = start_run(conn, own)
        summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
        attempt = transitions.queue_attempt(conn, summary, SYSTEM_ACTOR)
        assert (
            conn.execute(
                text("SELECT POOL AS pool FROM AUD_TASK_ATTEMPTS WHERE ATTEMPT_ID=:id"),
                {"id": attempt},
            ).scalar_one()
            == "team"
        )


def test_pre_project_admitted_images_keep_their_default_project(engine_db):
    import json

    from etl_craft.core.definitions import Definition, using

    with engine_db.engine.begin() as conn:
        own = add_pipeline(conn, "P")
        add_task(conn, own, "T", "PYTHON")
        run = start_run(conn, own)
        image = json.loads(
            conn.execute(
                text(
                    "SELECT METADATA_SNAPSHOT AS snapshot FROM AUD_PIPELINES_RUN_LOG "
                    "WHERE PIPELINE_RUN_ID=:id"
                ),
                {"id": run},
            ).scalar_one()
        )
        for name in ("CFG_PIPELINES", "CFG_TABLES"):
            image[name]["columns"].remove("project_id")
            for row in image[name]["rows"]:
                row.pop("project_id")
        with using(Definition.read(json.dumps(image), run)):
            assert resolve_pipeline_id(conn, "P") == own


def test_a_grant_for_a_code_cannot_restrict_the_same_code_in_another_project(engine_db):
    with engine_db.engine.begin() as conn:
        add_pipeline(conn, "P")
        other = project(conn, "other")
        other_pipeline = pipeline(conn, other, "P")
        group = conn.execute(
            text(
                "INSERT INTO CFG_GROUPS (GROUP_NAME) VALUES ('restricted') "
                "RETURNING GROUP_ID AS group_id"
            )
        ).scalar_one()
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINE_GRANTS (PROJECT_ID,GROUP_ID,PIPELINE_CODE,PERMISSION) "
                "VALUES (1,:group,'P','CAN_VIEW')"
            ),
            {"group": group},
        )
    ctx = OperationContext(engine_db.engine, engine_db.config, SYSTEM_ACTOR, role=ApiRole.VIEWER)
    assert invoke(ctx, ActionName.PIPELINES, TagFilter()).pipelines == ()
    other_view = invoke(replace(ctx, project="other"), ActionName.PIPELINES, TagFilter())
    assert [row.pipeline_id for row in other_view.pipelines] == [other_pipeline]


def test_seeded_project_history_has_a_globally_unique_change_identity(engine_db):
    with engine_db.engine.connect() as conn:
        identities = list(
            conn.execute(text("SELECT CHANGE_ID AS change_id FROM AUD_METADATA_CHANGES")).scalars()
        )
    assert len(identities) >= 2
    assert len(set(identities)) == len(identities)


def test_cloning_copies_only_the_selected_projects_metadata_and_history(engine_db, tmp_path):
    from datetime import UTC, datetime

    import duckdb

    from etl_craft.config import CloningConfig, ConnectionProfile
    from etl_craft.core.enums import CloningScope
    from etl_craft.services.cloning import clone

    with engine_db.engine.begin() as conn:
        add_pipeline(conn, "PRIVATE")
        other = project(conn, "other")
        own = pipeline(conn, other, "VISIBLE")
        add_task(conn, own, "T", "PYTHON")
        own_run = start_run(conn, own)
        conn.execute(
            text(
                "INSERT INTO AUD_GATE_WAITS (PIPELINE_RUN_ID,FIRST_CHECK_AT,LOOKS,WAIT_KIND) "
                "VALUES (:run,:now,0,'PIPELINE_RUN')"
            ),
            {"run": own_run, "now": datetime.now(UTC)},
        )
    target = tmp_path / "mirror.duckdb"
    config = replace(
        engine_db.config,
        project="other",
        cloning=CloningConfig(True, CloningScope.ALL),
        warehouse=ConnectionProfile(
            "WAREHOUSE", "dev", f"jdbc:duckdb:{target}", "", "NONE", schema="main"
        ),
    )
    with selected_project(other):
        clone(engine_db.engine, config)
    with duckdb.connect(str(target)) as conn:
        assert conn.execute("SELECT PIPELINE_RUN_ID FROM AUD_GATE_WAITS").fetchall() == [(own_run,)]
        assert conn.execute("SELECT PIPELINE_CODE FROM CFG_PIPELINES").fetchall() == [("VISIBLE",)]
        assert conn.execute("SELECT PROJECT_ID FROM CFG_PROJECTS").fetchall() == [(other,)]
        assert conn.execute("SELECT PIPELINE_ID FROM CFG_TASKS").fetchall() == [(own,)]
        assert conn.execute(
            "SELECT DISTINCT PIPELINE_CODE FROM AUD_CFG_PIPELINES_HISTORY"
        ).fetchall() == [("VISIBLE",)]


def test_delayed_developer_publication_cannot_restore_a_superseded_contract(
    engine_db, tmp_path, monkeypatch
):
    import base64

    import pytest

    from etl_craft.config.object_models import Artifacts
    from etl_craft.core.errors import PermissionDeniedError
    from etl_craft.engine.artifacts import ArtifactStore
    from etl_craft.services.operations.bundles import PublishBundle

    config = replace(engine_db.config, artifacts=Artifacts((tmp_path / "store").as_uri()))
    admin = OperationContext(engine_db.engine, config, SYSTEM_ACTOR, role=ApiRole.ADMIN)

    def upload(database):
        content = (
            "Secrets:\n  Source_type: ENVIRONMENT\nWarehouse:\n  dev:\n"
            f"    Jdbc_url: jdbc:duckdb:{database}\n    Schema: main\n"
        ).encode()
        return PublishBundle(files={"craft-connector.yml": base64.b64encode(content).decode()})

    original, replacement = upload("first.db"), upload("replacement.db")
    invoke(admin, ActionName.BUNDLE_PUBLISH, original)
    write = ArtifactStore.write
    replaced = []

    def admin_replaces_during_upload(store, digest, data):
        if not replaced:
            replaced.append(invoke(admin, ActionName.BUNDLE_PUBLISH, replacement).bundle_id)
        return write(store, digest, data)

    with monkeypatch.context() as patch:
        # This interleaving occurs after role admission and before the developer's insert.
        def interleave(store, digest, data):
            patch.setattr(ArtifactStore, "write", write)
            return admin_replaces_during_upload(store, digest, data)

        patch.setattr(ArtifactStore, "write", interleave)
        with pytest.raises(PermissionDeniedError, match="changed since admission"):
            invoke(replace(admin, role=ApiRole.DEVELOPER), ActionName.BUNDLE_PUBLISH, original)
    with engine_db.engine.connect() as conn:
        from etl_craft.engine.queries import statement

        assert conn.execute(statement(conn, "bundle_latest")).scalar_one() == replaced[0]
        assert (
            conn.execute(text("SELECT COUNT(*) AS count FROM AUD_PROJECT_BUNDLES")).scalar_one()
            == 2
        )


def test_export_refuses_an_unknown_project_with_a_remedy(engine_db, tmp_path):
    import pytest

    from etl_craft.core.errors import MetadataFileError
    from etl_craft.services.config_files import export_config

    directory = tmp_path / "missing"
    with pytest.raises(MetadataFileError, match=r"project 'missing'.*select another project"):
        export_config(engine_db.engine, directory, project_code="missing")
    assert not directory.exists()


def test_sql_task_state_reads_only_its_projects_target_contract(engine_db):
    from etl_craft.engine.queries import statement
    from etl_craft.engine.repository.hash_versions import save_view_statement

    with engine_db.engine.begin() as conn:
        first = add_pipeline(conn, "P")
        task = add_task(conn, first, "merge", TARGET_OBJECT="warehouse.main.shared")
        run = start_run(conn, first)
        other = project(conn, "other")
        save_hash_version(conn, "warehouse.main.shared", 1)
        save_view_statement(conn, "warehouse.main.shared", "a" * 64)
        with selected_project(other):
            save_hash_version(conn, "warehouse.main.shared", 2)
            save_view_statement(conn, "warehouse.main.shared", "b" * 64)
        rows = conn.execute(
            statement(conn, "sql_task_state"),
            {
                "task_id": task,
                "pipeline_id": first,
                "pipeline_run_id": run,
                "sql_action": "SCD1_MERGE",
                "catalog": "warehouse",
            },
        ).all()
    assert len(rows) == 1
    assert rows[0].hash_version == 1
    assert rows[0].statement_sha256 == "a" * 64


def test_item_lookup_refuses_a_run_from_another_project(engine_db):
    from etl_craft.engine import transitions
    from etl_craft.engine.queries import statement

    with engine_db.engine.begin() as conn:
        first = add_pipeline(conn, "P")
        task = add_task(conn, first, "T", "PYTHON")
        run = start_run(conn, first)
        transitions.find_or_create_task_run(conn, task, run)
        other = project(conn, "other")
        with selected_project(other):
            assert (
                conn.execute(
                    statement(conn, "item_task"),
                    {"run_id": run, "pipeline_code": "P", "task_code": "T"},
                ).one_or_none()
                is None
            )
