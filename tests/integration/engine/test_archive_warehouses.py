"""Archive compatibility on each supported local warehouse."""

import multiprocessing
from uuid import uuid4

import pytest
from sqlalchemy import text

from etl_craft.core.archive import ArchiveBatch, ArchiveColumn, ArchivePart
from etl_craft.core.enums import ArchiveKind, ArchiveStep
from etl_craft.warehouse.archive import ArchiveWriter
from etl_craft.warehouse.connection import warehouse_dialect
from fixtures.archive import deliver_in_process, exercise_archive


def test_archive_retry_and_added_columns_preserve_values(sql_world):
    world = sql_world
    exercise_archive(
        world.warehouse,
        warehouse_dialect(world.config),
        world.catalog,
        world.schema,
        world.config.cloning,
        "AUD_SAMPLE",
    )


@pytest.mark.parametrize("point", ["during_write", "after_warehouse_commit"])
def test_warehouse_delivery_recovers_after_process_loss(sql_world, point):
    world = sql_world
    part = ArchivePart(
        "AUD_DELIVERY",
        (
            ArchiveColumn("ID", ArchiveKind.INTEGER),
            ArchiveColumn("CONTENT", ArchiveKind.TEXT),
        ),
        ("ID",),
        ((1, "original"), (2, "second")),
    )
    batch = ArchiveBatch(
        uuid4().hex,
        uuid4().hex,
        "0057",
        "destination",
        world.schema,
        ArchiveStep.DELETE,
        "sample",
        None,
        (part,),
    )
    world.warehouse.dispose()
    process = multiprocessing.get_context("spawn").Process(
        target=deliver_in_process,
        args=(world.config, batch.dump(), f"archive.{point}:kill"),
    )
    process.start()
    try:
        process.join(timeout=60)
        assert process.exitcode == 137
    finally:
        if process.is_alive():
            process.kill()
            process.join(timeout=10)
    writer = ArchiveWriter(
        world.warehouse, warehouse_dialect(world.config), world.catalog, world.config.cloning
    )
    writer.copy(batch)
    writer.copy(batch)
    with world.warehouse.connect() as conn:
        assert (
            conn.execute(
                text(f"SELECT COUNT(*) AS count FROM {world.name('AUD_DELIVERY')}")
            ).scalar_one()
            == 2
        )
