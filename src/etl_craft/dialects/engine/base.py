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
from contextlib import AbstractContextManager
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Connection, Engine

from etl_craft.config.auth import EngineSpec
from etl_craft.core.text import split_statements

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
        self, config: ConnectorConfig, profile: ConnectionProfile, **engine_kwargs: Any
    ) -> Engine:
        """Build a SQLAlchemy engine for ``profile``, with no secret in its URL."""

    @abstractmethod
    def lock(
        self, engine: Engine, key: int, name: str, wait_seconds: float = 0
    ) -> AbstractContextManager[None]:
        """Hold the cross-process lock ``key`` for the ``with`` body.

        ``name`` identifies it in messages. With ``wait_seconds`` 0 it waits indefinitely;
        otherwise it raises ``LockTimeoutError`` once that time has passed.
        """

    def schema_problem(self, conn: Connection, schema: str) -> str | None:
        """Return why the Engine schema cannot be used, or ``None``.

        SQLite has one schema per file, and the file is created on first connect.
        """
        return None

    @abstractmethod
    def duration_seconds_sql(self) -> str:
        """Return the SQL expression for ``END_DATE - START_DATE`` in seconds."""

    def schema_path(self) -> Path:
        """Return the full schema a fresh install applies."""
        return self.directory / "schema.sql"

    def migrations_dir(self) -> Path:
        """Return the directory of the packaged ``ENGINE`` migration stream."""
        return self.directory / "migrations"

    def split_statements(self, sql_text: str) -> list[str]:
        """Split a script into statements this database accepts one at a time."""
        return split_statements(sql_text)

    def begin_ddl_transaction(self, conn: Connection) -> None:
        """Make DDL on ``conn`` part of its transaction; PostgreSQL's DDL already is."""
        return None

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
