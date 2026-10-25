"""Literal old/new/foreign reject rows shared by local and cloud maintenance checks."""

from datetime import UTC, datetime

from sqlalchemy import text

from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.retention import PurgeInput
from etl_craft.warehouse.connection import warehouse_dialect


def prepare(world, table="retained"):
    """Register a real managed target, then populate the package reject audit shape."""
    world.setup(table, "SELECT CAST(1 AS BIGINT) AS id, 'kept' AS val", "SCD1_MERGE")
    world.run(
        "seed",
        SQL_ACTION="SCD1_MERGE",
        TARGET_OBJECT=table,
        SELECT_QUERY="SELECT 1 AS id, 'kept' AS val",
        MERGE_KEY="id",
        MERGE_COMPARE_COLUMNS="val",
    )
    dialect = warehouse_dialect(world.config)
    stamp = dialect.audit_column_type("CREATE_DATE")
    string = dialect.string_type
    world.execute(
        f"CREATE TABLE {world.name(table + '__rejects')} (id {string}, business_date DATE, "
        "PIPELINE_ID BIGINT, PIPELINE_RUN_ID BIGINT, TASK_RUN_ID BIGINT, "
        f"CREATE_DATE {stamp}, CREATED_BY {string}, REJECT_REASON {string}, REJECT_COLUMN {string})"
    )
    for key, pipeline, created, business in (
        ("old", world.pipeline_id, datetime(2000, 1, 1, tzinfo=UTC), "2026-01-01"),
        ("recent", world.pipeline_id, datetime(2100, 1, 1, tzinfo=UTC), "1900-01-01"),
        ("foreign", 999999, datetime(2000, 1, 1, tzinfo=UTC), "2026-01-01"),
    ):
        world.execute(
            f"INSERT INTO {world.name(table + '__rejects')} VALUES "
            "(:key,CAST(:business AS DATE),:pipeline,1,1,:created,'test','invalid','id')",
            key=key,
            business=business,
            pipeline=pipeline,
            created=created,
        )
    return OperationContext(world.engine_db, world.config, SYSTEM_ACTOR, role=ApiRole.ADMIN)


def configure(world, *, table="retained", **periods):
    """Apply explicit table policy values using the actor-enabled fixture connection."""
    with world.engine_db.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_TABLES SET "
                + ",".join(f"{key}=:{key}" for key in periods)
                + " WHERE lower(TABLE_NAME)=lower(:table)"
            ),
            {**periods, "table": table},
        )


def rows(world, table="retained"):
    """Return named content fields so age never gets confused with business dates."""
    string = warehouse_dialect(world.config).string_type
    with world.warehouse.connect() as conn:
        return [
            dict(row)
            for row in conn.execute(
                text(
                    f"SELECT id AS id,CAST(business_date AS {string}) "
                    "AS business_date "
                    f"FROM {world.name(table + '__rejects')} ORDER BY id"
                )
            ).mappings()
        ]


def check_retention(world, table="retained"):
    """Keep unset, recent and foreign rows; the target itself survives every purge."""
    ctx = prepare(world, table)
    invoke(ctx, ActionName.PURGE, PurgeInput())
    assert len(rows(world, table)) == 3
    configure(world, table=table, REJECT_RETENTION_DAYS=30)
    snapshot = invoke(ctx, ActionName.PURGE, PurgeInput(dry_run=True))
    key = next(
        key
        for key in snapshot.counts
        if key.casefold() == world.name(table + "__rejects").casefold()
    )
    assert snapshot.counts[key] == 1
    assert len(rows(world, table)) == 3
    result = invoke(ctx, ActionName.PURGE, PurgeInput())
    assert result.counts[key] == 1
    assert rows(world, table) == [
        {"id": "foreign", "business_date": "2026-01-01"},
        {"id": "recent", "business_date": "1900-01-01"},
    ]
    repeated = invoke(ctx, ActionName.PURGE, PurgeInput())
    assert repeated.counts[key] == 0
    assert len(rows(world, table)) == 2
    with world.warehouse.connect() as conn:
        assert conn.execute(text(f"SELECT id AS id FROM {world.name(table)}")).scalar_one() == 1
