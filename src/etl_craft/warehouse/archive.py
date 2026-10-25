"""Create/evolve immutable archives and verify every stored value before source deletion."""

from dataclasses import dataclass, field

from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config.model import CloningConfig
from etl_craft.core.archive import (
    ArchiveBatch,
    ArchiveColumn,
    ArchivePart,
    bound,
    encoded,
)
from etl_craft.core.enums import ArchiveKind
from etl_craft.core.errors import ArchiveError
from etl_craft.core.faults import fault_point
from etl_craft.core.text import qualify
from etl_craft.dialects.warehouse import WarehouseDialect
from etl_craft.dialects.warehouse.archive import (
    META_COLUMNS,
    add_column,
    column_type,
    create_table,
    read_rows,
)
from etl_craft.dialects.warehouse.copying import MAX_BOUND_VALUES, insert_rows


@dataclass
class ArchiveWriter:
    """One admitted batch on an already locked warehouse destination."""

    engine: Engine
    dialect: WarehouseDialect
    catalog: str
    cloning: CloningConfig = field(default_factory=CloningConfig)

    def copy(self, batch: ArchiveBatch) -> None:
        """Retry partial or unacknowledged writes by verifying existing content first."""
        for part in batch.parts:
            try:
                self.write(batch, part)
            except SQLAlchemyError as error:
                failure = type(getattr(error, "orig", error)).__name__
                table = qualify(f"{batch.schema}.{part.table}", self.catalog)
                raise ArchiveError(
                    f"archive {table}: warehouse refused the batch ({failure}); "
                    "expected a writable compatible archive table; check warehouse permissions "
                    "and availability, then retry the same destination; source rows retained"
                ) from error
        fault_point("archive.after_verify")

    def write(self, batch: ArchiveBatch, part: ArchivePart) -> None:
        """Append missing content and read back committed physical values."""
        table = qualify(f"{batch.schema}.{part.table}", self.catalog)
        with self.engine.begin() as conn:
            self.evolve(conn, table, part.columns)
            existing = self.verified(conn, table, part, batch)
            rows = [
                (
                    *tuple(bound(c, v) for c, v in zip(part.columns, row, strict=True)),
                    batch.origin,
                    part.digest(row, batch.version),
                    batch.version,
                )
                for row in part.rows
                if part.digest(row, batch.version) not in existing
            ]
            names = [c.name for c in part.columns] + list(META_COLUMNS)
            per_statement = max(1, MAX_BOUND_VALUES // len(names))
            for offset in range(0, len(rows), per_statement):
                conn.execute(*insert_rows(table, names, rows[offset : offset + per_statement]))
                fault_point("archive.during_write")
        fault_point("archive.after_warehouse_commit")
        with self.engine.connect() as conn:
            found = self.verified(conn, table, part, batch)
        if len(found) != len(part.rows):
            raise ArchiveError(
                f"{table}: verified {len(found)} rows, expected {len(part.rows)}; "
                "restore the archive and retry; source rows were retained"
            )

    def evolve(self, conn: Connection, table: str, columns: tuple[ArchiveColumn, ...]) -> None:
        """Add compatible columns; refuse pre-existing ordinary mirrors and narrowed types."""
        metadata = self.dialect.read_metadata(conn, table)
        existing = self.dialect.full_column_types(conn, table) if metadata.columns else {}
        if not existing:
            conn.execute(create_table(self.dialect, table, columns, self.cloning))
            return
        for name in META_COLUMNS:
            if name.lower() not in existing:
                raise ArchiveError(
                    f"{table}.{name}: archive identity missing; expected a dedicated "
                    "archive table; choose an empty archive schema"
                )
        for c in columns + tuple(ArchiveColumn(n, ArchiveKind.TEXT) for n in META_COLUMNS):
            actual = existing.get(c.name.lower())
            expected = column_type(self.dialect, c)
            if actual is None:
                conn.execute(add_column(self.dialect, table, c))
            elif not self.dialect.archive_type_matches(actual, expected):
                raise ArchiveError(
                    f"{table}.{c.name}: type={actual!r}, expected {expected!r}; "
                    "restore a compatible archive type before retrying purge"
                )

    def verified(
        self,
        conn: Connection,
        table: str,
        part: ArchivePart,
        batch: ArchiveBatch,
    ) -> set[str]:
        """Compare physical values, not just a copied digest or an affected-row count."""
        expected = {part.digest(row, batch.version): row for row in part.rows}
        names = [c.name for c in part.columns] + list(META_COLUMNS)
        found: set[str] = set()
        for record in conn.execute(
            read_rows(table, names, list(expected), batch.origin)
        ).mappings():
            digest = str(record["_etl_archive_row_sha256"])
            try:
                actual = tuple(
                    encoded(c, record[c.name.lower()], warehouse=True) for c in part.columns
                )
            except (ValueError, TypeError) as error:
                raise ArchiveError(
                    f"{table}: invalid stored scalar for archive row {digest}; expected "
                    "the admitted source types; restore the archive; source retained"
                ) from error
            if (
                digest in found
                or digest not in expected
                or actual != expected[digest]
                or record["_etl_archive_schema_version"] != batch.version
            ):
                raise ArchiveError(
                    f"{table}: duplicate or changed archive row {digest}; expected "
                    "the admitted source values; restore the archive; source retained"
                )
            found.add(digest)
        return found
