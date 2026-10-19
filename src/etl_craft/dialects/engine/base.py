"""What differs between Engine DBs, behind one interface.

Each dialect is a directory holding its module, its ``schema.sql`` (what ``init-db`` applies to
an empty database), its ``migrations/`` (what ``migrate`` applies to an existing one) and its
``queries/``. Callers never branch on the Engine DB themselves: they ask for the dialect and
call the method they need.

The query catalog holds every Engine DB query as a ``.sql`` file named after the query. A
dialect's own ``queries/<name>.sql`` overrides the shared ``dialects/engine/queries/<name>.sql``
where the SQL differs. Every selected column is aliased in lower case, because SQLite and
PostgreSQL disagree on the case of unquoted identifiers.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from datetime import UTC, datetime
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import bindparam, inspect, text
from sqlalchemy.engine import Connection, Engine

from etl_craft.config.auth import EngineSpec
from etl_craft.core.actor import Actor
from etl_craft.core.text import split_statements
from etl_craft.dialects.engine.metadata import CREDENTIAL_COLUMNS

if TYPE_CHECKING:
    from etl_craft.config import ConnectionProfile, ConnectorConfig

SHARED_QUERIES = Path(__file__).parent / "queries"


class EngineDialect(ABC):
    """One Engine DB: how to connect, lock, split scripts, and which SQL to run."""

    spec: EngineSpec
    directory: Path

    @property
    def name(self) -> str:
        """SQLAlchemy's name for this database, such as ``postgresql``."""
        return self.spec.name

    @property
    def auth_modes(self) -> frozenset[str]:
        """The auth modes this Engine DB accepts."""
        return self.spec.auth_modes

    @abstractmethod
    def build_engine(
        self,
        config: ConnectorConfig,
        profile: ConnectionProfile,
        *,
        supervised: bool = False,
        **engine_kwargs: Any,
    ) -> Engine:
        """Build a SQLAlchemy engine for ``profile``, with no secret in its URL.

        ``supervised`` marks a task process whose supervisor already reached the Engine DB, so
        an outage at its first connection is waited out like any later one.
        """

    @abstractmethod
    def lock(
        self, engine: Engine, key: int, name: str, wait_seconds: float = 0
    ) -> AbstractContextManager[None]:
        """Hold the cross-process lock ``key`` for the ``with`` body.

        ``name`` identifies it in messages. With ``wait_seconds`` 0 it waits indefinitely;
        otherwise it raises ``LockTimeoutError`` once that time has passed.
        """

    @abstractmethod
    def refresh_metadata_triggers(self, conn: Connection) -> None:
        """Install metadata capture and write guards for the current table definitions."""

    def controller_lock_path(self, engine: Engine) -> str | None:
        """Return the local leadership lock path, or None for session leadership."""
        return None

    def max_connections(self, conn: Connection) -> int | None:
        """Return the server's connection limit; ``None`` for a database without one."""
        return None

    def schema_problem(self, conn: Connection, schema: str) -> str | None:
        """Return why the Engine schema cannot be used, or ``None``.

        SQLite has one schema per file, and the file is created on first connect.
        """
        return None

    def schema_path(self) -> Path:
        """Return the full schema a fresh install applies."""
        return self.directory / "schema.sql"

    def migrations_dir(self) -> Path:
        """Return the directory of the packaged ``ENGINE`` migration stream."""
        return self.directory / "migrations"

    def split_statements(self, sql_text: str) -> list[str]:
        """Split a script into statements this database accepts one at a time."""
        return split_statements(sql_text)

    @contextmanager
    def migration_transaction(
        self, engine: Engine, *, rebuild_metadata: str | None = None
    ) -> Iterator[Connection]:
        """Apply a migration and its ledger row together, including any dialect's rebuild setup."""
        with engine.begin() as conn:
            yield conn

    @cached_property
    def _catalog(self) -> dict[str, Path]:
        files = {path.stem: path for path in sorted(SHARED_QUERIES.glob("*.sql"))}
        files.update({path.stem: path for path in sorted(self.queries_dir().glob("*.sql"))})
        return files

    def queries_dir(self) -> Path:
        """Return this dialect's own queries directory; its files override shared ones."""
        return self.directory / "queries"

    def query_names(self) -> frozenset[str]:
        """Every query this dialect can run."""
        return frozenset(self._catalog)

    def query_path(self, name: str) -> Path:
        """Return the file query ``name`` is read from; ``LookupError`` for an unknown name."""
        try:
            return self._catalog[name]
        except KeyError:
            raise LookupError(f"no Engine DB query named {name!r} for {self.name}") from None

    def query(self, name: str) -> str:
        """Return the SQL text of query ``name``.

        Whole-line ``--`` comments describe the query in its file and are left out, so a
        comment may name a ``:parameter`` without the driver seeing a second one.
        """
        lines = self.query_path(name).read_text(encoding="utf-8").splitlines()
        return "\n".join(line for line in lines if not line.lstrip().startswith("--")).strip()

    def existing_tables(self, engine: Engine, names: tuple[str, ...]) -> list[str]:
        """Return which of ``names`` already exist as tables, in lower case and sorted."""
        statement = text(self.query("existing_tables")).bindparams(
            bindparam("names", expanding=True)
        )
        with engine.connect() as conn:
            rows = conn.execute(statement, {"names": [name.lower() for name in names]}).all()
        return sorted(str(row.table_name).lower() for row in rows)

    def stored_name(self, table: str) -> str:
        """Return ``table`` as this Engine DB's catalog stores an unquoted name."""
        return table

    def table_triggers(self, conn: Connection, table: str) -> list[str]:
        """Return the names of the user triggers on ``table``."""
        rows = conn.execute(text(self.query("table_triggers")), {"name": self.stored_name(table)})
        return list(rows.scalars())

    def table_indexes(self, conn: Connection, table: str) -> list[tuple[str, str]]:
        """Return each named index on ``table`` with its definition."""
        rows = conn.execute(text(self.query("table_indexes")), {"name": self.stored_name(table)})
        return [(row.name, row.definition) for row in rows]

    def drop_trigger(self, conn: Connection, trigger: str, table: str) -> None:
        """Drop ``trigger`` from ``table``."""
        conn.exec_driver_sql(f"DROP TRIGGER {conn.dialect.identifier_preparer.quote(trigger)}")

    def index_names(self, conn: Connection, table: str) -> list[str]:
        """Return the names of the indexes on ``table``, its primary key's excluded."""
        return [str(index["name"]) for index in inspect(conn).get_indexes(self.stored_name(table))]

    @abstractmethod
    def legacy_source_id_high_water(self, conn: Connection) -> int:
        """Return the highest ``SOURCE_COLUMN_ID`` the legacy table's id generator handed out."""

    @abstractmethod
    def set_column_id_high_water(self, conn: Connection, high_water: int) -> None:
        """Make ``CFG_COLUMNS`` ids continue after ``high_water``."""

    def record_config_conversion(
        self, conn: Connection, table: str, primary: str, *, migration: str, actor: Actor
    ) -> None:
        """Close ``table``'s current versions and record its rows again, stamps unchanged."""
        now = datetime.now(UTC)
        conn.execute(
            text(
                f"UPDATE AUD_{table}_HISTORY SET VALID_TO=:now,CURRENT_FLAG='N' "
                f"WHERE CURRENT_FLAG='Y' AND {primary} IN "
                f"(SELECT {primary} FROM {table})"
            ),
            {"now": now},
        )
        self._insert_history(conn, table, primary, ("UPDATE", migration, actor, now))
        conn.exec_driver_sql("DROP VIEW IF EXISTS AUD_METADATA_CHANGES")

    def record_config_baseline(
        self, conn: Connection, tables: tuple[str, ...], *, migration: str, actor: Actor
    ) -> None:
        """Record every row of ``tables`` as inserted by ``migration``, the start of history."""
        now = datetime.now(UTC)
        for table in tables:
            first = inspect(conn).get_columns(self.stored_name(table))[0]["name"]
            self._insert_history(conn, table, first, ("INSERT", migration, actor, now))

    def _insert_history(
        self,
        conn: Connection,
        table: str,
        order_by: str,
        change: tuple[str, str, Actor, datetime],
    ) -> None:
        """Copy ``table``'s rows into its history as current versions of one change."""
        operation, migration, actor, now = change
        quote = conn.dialect.identifier_preparer.quote
        names = [
            column["name"]
            for column in inspect(conn).get_columns(self.stored_name(table))
            if column["name"].upper() not in CREDENTIAL_COLUMNS.get(table.upper(), ())
        ]
        listed = ", ".join(quote(column) for column in names)
        conn.execute(
            text(
                f"INSERT INTO AUD_{table}_HISTORY (CHANGE_ID,VALID_FROM,VALID_TO,"
                "CURRENT_FLAG,OPERATION,ACTOR,ACTOR_KIND,MIGRATION,"
                f"{listed}) SELECT {self._last_change_id(conn)}+ROW_NUMBER() OVER "
                f"(ORDER BY {order_by}),:now,:end,'Y',:operation,:actor,:kind,:migration,"
                f"{listed} FROM {table}"
            ),
            {
                "operation": operation,
                "migration": migration,
                "now": now,
                "end": datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC),
                "actor": actor.name,
                "kind": actor.kind.value,
            },
        )

    def _last_change_id(self, conn: Connection) -> str:
        """Return an expression for the highest change id across the config histories."""
        histories = [
            table
            for table in inspect(conn).get_table_names()
            if table.upper().startswith("AUD_CFG_") and table.upper().endswith("_HISTORY")
        ]
        return (
            "(SELECT COALESCE(MAX(m),0) FROM ("
            + " UNION ALL ".join(f"SELECT MAX(CHANGE_ID) AS m FROM {table}" for table in histories)
            + ") changes)"
        )

    def table_rows(
        self, conn: Connection, table: str, *, order_by: str | None = None
    ) -> list[dict[str, Any]]:
        """Return every row of ``table``, keyed by lower-case column name."""
        order = f" ORDER BY {order_by}" if order_by else ""
        return [
            {str(name).lower(): value for name, value in row.items()}
            for row in conn.execute(text(f"SELECT * FROM {table}{order}")).mappings()
        ]

    def column_names(self, conn: Connection, table: str) -> list[str]:
        """Return ``table``'s column names in lower case, in table order."""
        names = conn.execute(text(f"SELECT * FROM {table} WHERE 1 = 0")).keys()
        return [str(name).lower() for name in names]

    def write_table(
        self, conn: Connection, table: str, columns: list[str], rows: list[dict[str, Any]]
    ) -> None:
        """Create ``table`` with untyped ``columns`` and insert ``rows`` into it."""
        conn.execute(text(f"CREATE TABLE {table} ({', '.join(columns)})"))
        if rows:
            conn.execute(
                text(
                    f"INSERT INTO {table} ({', '.join(columns)}) "
                    f"VALUES ({', '.join(f':{name}' for name in columns)})"
                ),
                rows,
            )
