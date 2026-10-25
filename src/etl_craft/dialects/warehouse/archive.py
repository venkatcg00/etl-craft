"""Append-only archive SQL, independent of warehouse driver imports."""

from sqlalchemy import TextClause, text

from etl_craft.config.model import CloningConfig
from etl_craft.core.archive import ArchiveColumn
from etl_craft.core.enums import ArchiveKind
from etl_craft.dialects.warehouse.base import WarehouseDialect

META_COLUMNS = ("_ETL_ARCHIVE_ENGINE_ID", "_ETL_ARCHIVE_ROW_SHA256", "_ETL_ARCHIVE_SCHEMA_VERSION")


def column_type(dialect: WarehouseDialect, column: ArchiveColumn) -> str:
    """Use exact text for unconstrained numerics; native temporal and integral types otherwise."""
    return {
        ArchiveKind.INTEGER: "BIGINT",
        ArchiveKind.BOOLEAN: "BOOLEAN",
        ArchiveKind.DATE: "DATE",
        ArchiveKind.TIMESTAMP: dialect.audit_column_type("CREATE_DATE"),
    }.get(column.kind, dialect.string_type)


def create_table(
    dialect: WarehouseDialect,
    table: str,
    columns: tuple[ArchiveColumn, ...],
    cloning: CloningConfig,
) -> TextClause:
    """Create the source shape plus archive provenance columns."""
    definitions = [f"{c.name} {column_type(dialect, c)}" for c in columns]
    definitions.extend(f"{name} {dialect.string_type}" for name in META_COLUMNS)
    ddl = ", ".join(definitions)
    return text(dialect.mirror_table_ddl(table, ddl, cloning) or f"CREATE TABLE {table} ({ddl})")


def add_column(dialect: WarehouseDialect, table: str, column: ArchiveColumn) -> TextClause:
    """Extend an archive without replacing existing rows."""
    return text(
        f"{dialect.alter_table_keyword()} {table} ADD COLUMN "
        f"{column.name} {column_type(dialect, column)}"
    )


def read_rows(table: str, names: list[str], hashes: list[str], origin: str) -> TextClause:
    """Read a bounded set of content identities for exact verification."""
    fields = ", ".join(f"{name} AS {name.lower()}" for name in names)
    parameters = {f"h{n}": value for n, value in enumerate(hashes)}
    marks = ", ".join(f":{name}" for name in parameters)
    return text(
        f"SELECT {fields} FROM {table} WHERE _ETL_ARCHIVE_ENGINE_ID=:origin "
        f"AND _ETL_ARCHIVE_ROW_SHA256 IN ({marks}) LIMIT {len(hashes) + 1}"
    ).bindparams(origin=origin, **parameters)
