"""Project migrations write only their own project's rows and team tables."""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from etl_craft.core.actor import Actor, ActorKind, acting_as, migration_project
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.core.errors import MigrationError
from etl_craft.dialects.engine import for_engine
from etl_craft.engine import migrations
from etl_craft.engine.repository.pipelines import selected_project
from etl_craft.services.operations import OperationContext
from fixtures.actions import act
from fixtures.metadata import add_pipeline

OWNER = Actor("owner", ActorKind.USER)


@pytest.fixture
def projects(engine_db, tmp_path):
    """Two projects that both have a pipeline called ``SALES``."""
    admin = OperationContext(engine_db.engine, engine_db.config, OWNER, role=ApiRole.ADMIN)
    identities = {}
    for code in ("east", "west"):
        act(admin, ActionName.PROJECT_ADD, project=code, reason="Scope fixture")
        with acting_as(OWNER), engine_db.engine.begin() as conn:
            identities[code] = conn.execute(
                text("SELECT PROJECT_ID AS project_id FROM CFG_PROJECTS WHERE PROJECT_CODE=:c"),
                {"c": code},
            ).scalar_one()
            add_pipeline(conn, "SALES", project_id=identities[code])
    with acting_as(OWNER), engine_db.engine.begin() as conn:
        migrations.mark_packaged_migrations_applied(conn)
    return identities


def migrate(engine_db, tmp_path, project_id, name, sql):
    """Apply one new file in ``project_id``'s migrations folder, removing it when refused."""
    folder = tmp_path / f"migrations-{project_id}"
    folder.mkdir(exist_ok=True)
    path = folder / name
    path.write_text(sql)
    try:
        with acting_as(OWNER), selected_project(project_id):
            return migrations.apply_pending_migrations(engine_db.engine, folder)
    except MigrationError:
        path.unlink()
        raise


def schedules(engine_db):
    with engine_db.engine.connect() as conn:
        return dict(
            conn.execute(
                text(
                    "SELECT PROJECT_ID AS project_id, RUN_SCHEDULE AS schedule "
                    "FROM CFG_PIPELINES WHERE PIPELINE_CODE='SALES'"
                )
            ).all()
        )


def test_project_migrations_are_scoped(engine_db, projects, tmp_path):
    east, west = projects["east"], projects["west"]
    by_code = "UPDATE CFG_PIPELINES SET RUN_SCHEDULE='0 5 * * *' WHERE PIPELINE_CODE='SALES';\n"
    with pytest.raises(MigrationError) as refused:
        migrate(engine_db, tmp_path, west, "0001_by_code.sql", by_code)
    message = str(refused.value)
    assert "0001_by_code.sql statement 1" in message
    assert f"in project {west}" in message
    assert "CFG_PIPELINES" in message
    assert "ETL_CRAFT_PROJECT" in message
    assert schedules(engine_db) == {east: None, west: None}

    scoped = by_code.replace(";", " AND PROJECT_ID=(SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT);")
    migrate(engine_db, tmp_path, west, "0001_scoped.sql", scoped)
    assert schedules(engine_db) == {east: None, west: "0 5 * * *"}

    unscoped_insert = (
        "INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) "
        "VALUES ('NEW', 'New', 'FULL');\n"
    )
    with pytest.raises(MigrationError, match="CFG_PIPELINES"):
        migrate(engine_db, tmp_path, west, "0002_default_project.sql", unscoped_insert)


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE CFG_ACCOUNTS SET ACTIVE_FLAG='N';",
        "DELETE FROM AUD_ACTIONS;",
        "INSERT INTO SCHEMA_MIGRATIONS (SOURCE, VERSION, CHECKSUM) VALUES ('PROJECT','x','y');",
        "ALTER TABLE CFG_TASKS ADD COLUMN TEAM_NOTE VARCHAR;",
        "DROP TABLE CFG_TASKS;",
        "CREATE INDEX ix_team ON CFG_PIPELINES (PIPELINE_NAME);",
    ],
)
def test_project_migrations_never_change_admin_audit_or_package_tables(
    engine_db, projects, tmp_path, sql
):
    with pytest.raises(MigrationError) as refused:
        migrate(engine_db, tmp_path, projects["east"], "0001_reach.sql", sql)
    assert "is etl-craft's" in str(refused.value)
    assert "0001_reach.sql statement 1" in str(refused.value)


def test_unclassifiable_statements_are_refused(engine_db, projects, tmp_path):
    sql = (
        "DO $$ BEGIN UPDATE CFG_PIPELINES SET RUN_SCHEDULE='x'; END $$;"
        if engine_db.engine.dialect.name == "postgresql"
        else "PRAGMA foreign_keys=OFF;"
    )
    with pytest.raises(MigrationError, match="cannot tell which tables it changes"):
        migrate(engine_db, tmp_path, projects["east"], "0001_opaque.sql", sql)


def test_engine_db_guards_refuse_other_projects_rows_without_the_preflight(engine_db, projects):
    """The guards hold even for a write the statement check did not see."""
    east, west = projects["east"], projects["west"]
    dialect = for_engine(engine_db.engine)
    for sql in (
        f"UPDATE CFG_PIPELINES SET RUN_SCHEDULE='x' WHERE PROJECT_ID={east}",
        "UPDATE CFG_PROJECTS SET PROJECT_NAME='renamed'",
    ):
        token = migration_project.set(west)
        try:
            with (
                pytest.raises(DBAPIError),
                acting_as(OWNER),
                engine_db.engine.begin() as conn,
            ):
                dialect.scope_migration_statement(conn, west)
                conn.execute(text(sql))
        finally:
            migration_project.reset(token)
    assert schedules(engine_db) == {east: None, west: None}


def test_team_table_project_ownership(engine_db, projects, tmp_path):
    east, west = projects["east"], projects["west"]
    migrate(
        engine_db,
        tmp_path,
        east,
        "0001_channels.sql",
        "CREATE TABLE CFG_CHANNELS (CODE VARCHAR(20));\nINSERT INTO CFG_CHANNELS VALUES ('EMAIL');",
    )
    with engine_db.engine.connect() as conn:
        owner = conn.execute(
            text(
                "SELECT PROJECT_ID AS project_id, MIGRATION AS migration, "
                "CLAIMED_BY AS claimed_by FROM AUD_TEAM_TABLES WHERE TABLE_NAME='CFG_CHANNELS'"
            )
        ).one()
    assert (owner.project_id, owner.migration, owner.claimed_by) == (
        east,
        "0001_channels.sql",
        "owner",
    )

    for name, sql in (
        ("0001_write.sql", "INSERT INTO CFG_CHANNELS VALUES ('SMS');"),
        ("0001_alter.sql", "ALTER TABLE CFG_CHANNELS ADD COLUMN NOTE VARCHAR(20);"),
        ("0001_drop.sql", "DROP TABLE CFG_CHANNELS;"),
    ):
        with pytest.raises(MigrationError) as refused:
            migrate(engine_db, tmp_path, west, name, sql)
        assert f"CFG_CHANNELS belongs to project {east}" in str(refused.value)
        assert "0001_channels.sql" in str(refused.value)

    reads = "CREATE TEMPORARY TABLE copied AS SELECT CODE FROM CFG_CHANNELS;\nDROP TABLE copied;"
    assert migrate(engine_db, tmp_path, west, "0001_reads.sql", reads) == ["0001_reads.sql"]


def test_first_project_migration_claims_an_unowned_team_table(engine_db, projects, tmp_path):
    east, west = projects["east"], projects["west"]
    with engine_db.engine.begin() as conn:
        conn.execute(text("CREATE TABLE CFG_REGIONS (CODE VARCHAR(20))"))
    migrate(engine_db, tmp_path, west, "0001_regions.sql", "INSERT INTO CFG_REGIONS VALUES ('EU');")
    with engine_db.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT PROJECT_ID FROM AUD_TEAM_TABLES WHERE TABLE_NAME='CFG_REGIONS'")
            ).scalar_one()
            == west
        )
    with pytest.raises(MigrationError, match=f"belongs to project {west}"):
        migrate(
            engine_db,
            tmp_path,
            east,
            "0001_regions.sql",
            "INSERT INTO CFG_REGIONS VALUES ('US');",
        )


def test_a_refused_statement_rolls_back_its_claims(engine_db, projects, tmp_path):
    sql = "CREATE TABLE CFG_LATER (CODE VARCHAR(20));\nUPDATE CFG_ACCOUNTS SET ACTIVE_FLAG='N';"
    with pytest.raises(MigrationError):
        migrate(engine_db, tmp_path, projects["east"], "0001_later.sql", sql)
    with engine_db.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_TEAM_TABLES")).scalar_one() == 0


def test_the_documented_examples_apply_in_two_projects_of_one_engine_db(
    engine_db, projects, tmp_path
):
    from fixtures.metadata_project import EXAMPLES

    for project_id in projects.values():
        folder = tmp_path / f"examples-{project_id}"
        folder.mkdir()
        for example in sorted(EXAMPLES.glob("*.sql")):
            (folder / example.name).write_text(example.read_text())
        with acting_as(OWNER), selected_project(project_id):
            assert len(migrations.apply_pending_migrations(engine_db.engine, folder)) == 6
    with engine_db.engine.connect() as conn:
        counts = dict(
            conn.execute(
                text(
                    "SELECT p.PROJECT_ID AS project_id, COUNT(*) AS tasks FROM CFG_TASKS t "
                    "JOIN CFG_PIPELINES p ON p.PIPELINE_ID=t.PIPELINE_ID "
                    "WHERE t.ACTIVE_FLAG='Y' AND p.ACTIVE_FLAG='Y' GROUP BY p.PROJECT_ID"
                )
            ).all()
        )
    assert counts == {projects["east"]: 17, projects["west"]: 17}
