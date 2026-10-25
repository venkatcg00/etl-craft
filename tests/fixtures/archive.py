"""Immutable snapshots use compatible types on every supported local warehouse."""

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import text

from etl_craft.core.archive import ArchiveBatch, ArchiveColumn, ArchivePart
from etl_craft.core.enums import ArchiveKind, ArchiveStep
from etl_craft.warehouse.archive import ArchiveWriter


def exercise_archive(warehouse, dialect, catalog, schema, cloning, table):
    """Compare exact values, idempotent delivery and added columns on a real warehouse."""
    columns = (
        ArchiveColumn("ID", ArchiveKind.INTEGER),
        ArchiveColumn("PAYLOAD", ArchiveKind.TEXT),
        ArchiveColumn("EXACT_VALUE", ArchiveKind.DECIMAL),
        ArchiveColumn("MOMENT", ArchiveKind.TIMESTAMP),
        ArchiveColumn("FLAG", ArchiveKind.BOOLEAN),
    )
    values = (
        1,
        "Unicode λ and 'quotes'",
        "12345678901234567890.12345678901234567890",
        datetime(2000, 1, 1, tzinfo=UTC).isoformat(timespec="microseconds"),
        True,
    )
    part = ArchivePart(table, columns, ("ID",), (values,))
    batch = ArchiveBatch(
        uuid4().hex,
        uuid4().hex,
        "0057",
        "target",
        schema,
        ArchiveStep.DELETE,
        "sample",
        None,
        (part,),
    )
    writer = ArchiveWriter(warehouse, dialect, catalog, cloning)
    writer.copy(batch)
    writer.copy(batch)
    from dataclasses import replace

    newer = replace(
        part,
        columns=(*columns, ArchiveColumn("ADDED", ArchiveKind.TEXT)),
        rows=((*values, "new column"),),
    )
    writer.copy(replace(batch, version="0058", parts=(newer,)))
    writer.copy(batch)
    with warehouse.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) AS count FROM {catalog}.{schema}.{table}"))
        assert result.scalar_one() == 2


def deliver_in_process(config, body, fault):
    """Open independent warehouse connections so a forced exit skips all cleanup."""
    import os

    from etl_craft.config.targets import active_catalog
    from etl_craft.warehouse.connection import build_warehouse_engine, warehouse_dialect

    os.environ["ETL_CRAFT_FAULT"] = fault
    warehouse = build_warehouse_engine(config)
    try:
        ArchiveWriter(
            warehouse, warehouse_dialect(config), active_catalog(config), config.cloning
        ).copy(ArchiveBatch.load(body))
    finally:
        warehouse.dispose()
