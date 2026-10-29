"""Keep a project migration inside its own project.

Before each statement runs, its tables are read with sqlglot: it may write the project config
tables, whose rows the Engine DB's guards confine to the migrating project, and it may write and
change temporary tables and the team tables its project owns: every table that is not
etl-craft's. The first project migration that changes an unowned team table claims it for its
project in ``AUD_TEAM_TABLES``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

from sqlalchemy import inspect
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError

from etl_craft.core.actor import current_actor, migration_project
from etl_craft.core.errors import MigrationError
from etl_craft.dialects.engine import for_engine
from etl_craft.dialects.engine.guards import PACKAGE_TABLES
from etl_craft.dialects.engine.project_scope import (
    Touch,
    package_table,
    project_rows,
)
from etl_craft.engine.queries import run_script, statement

STATEMENT_KINDS = (
    "expected SELECT, INSERT, UPDATE, DELETE, MERGE, or CREATE, ALTER or DROP of a table, view "
    "or index"
)


@dataclass
class ProjectScope:
    """One project migration file's checks, statement by statement."""

    conn: Connection
    project_id: int
    migration: str
    temporary: set[str] = field(default_factory=set)

    def refusal(self, number: int, sql: str, reason: str) -> MigrationError:
        """Name the file, the statement and the project with ``reason``."""
        snippet = " ".join(sql.split())[:160]
        return MigrationError(
            f"project migration {self.migration} statement {number} ({snippet!r}) in project "
            f"{self.project_id}: {reason}"
        )

    def touches(self, number: int, sql: str) -> list[Touch]:
        """Return the tables the statement changes, refusing one sqlglot cannot classify."""
        try:
            return for_engine(self.conn).statement_touches(sql)
        except MigrationError as error:
            raise self.refusal(
                number,
                sql,
                f"cannot tell which tables it changes ({error}); {STATEMENT_KINDS}; split it into "
                "plain statements",
            ) from None

    def owner_problem(self, touch: Touch) -> str | None:
        """Why the project may not change ``touch``'s table, without claiming it."""
        table = self._index_table(touch) if touch.index else touch.table
        if table is None:
            return f"index {touch.table} was not found; drop only indexes that exist"
        if touch.temporary or table in self.temporary or project_rows(touch):
            return None
        if table.startswith("AUD_") and table not in PACKAGE_TABLES:
            return f"{table} is reserved for etl-craft (AUD_ names); rename this project's table"
        if package_table(table):
            return (
                f"{table} is etl-craft's; a project migration writes only its own project's rows "
                "in the project config tables and changes no etl-craft table"
            )
        owner = self.conn.execute(
            statement(self.conn, "team_table_owner"), {"table_name": table}
        ).first()
        if owner is not None and owner.project_id != self.project_id:
            return (
                f"{table} belongs to project {owner.project_id} (claimed by migration "
                f"{owner.migration}); change only this project's team tables, or rename this "
                "project's table"
            )
        return None

    def check(self, number: int, sql: str) -> None:
        """Refuse the statement before it runs, or claim the unowned team tables it changes."""
        for touch in self.touches(number, sql):
            if problem := self.owner_problem(touch):
                raise self.refusal(number, sql, problem)
            table = self._index_table(touch) if touch.index else touch.table
            assert table is not None
            if touch.temporary and touch.creates:
                self.temporary.add(table)
            elif not project_rows(touch):
                self._claim(table)

    def _claim(self, table: str) -> None:
        owner = self.conn.execute(
            statement(self.conn, "team_table_owner"), {"table_name": table}
        ).first()
        if owner is None and table not in self.temporary:
            self.conn.execute(
                statement(self.conn, "team_table_claim"),
                {
                    "table_name": table,
                    "owner_project_id": self.project_id,
                    "migration": self.migration,
                    "actor": current_actor().name,
                    "claimed_at": datetime.now(UTC),
                },
            )

    def _index_table(self, touch: Touch) -> str | None:
        row = self.conn.execute(
            statement(self.conn, "index_table"), {"index_name": touch.table}
        ).first()
        return None if row is None else str(row.table_name)

    def run(self, number: int, sql: str) -> None:
        """Run the team's statement with the Engine DB's guards scoped to this project."""
        dialect = for_engine(self.conn)
        token = migration_project.set(self.project_id)
        dialect.scope_migration_statement(self.conn, self.project_id)
        try:
            run_script(self.conn, [sql])
        except DBAPIError as error:
            raise self.refusal(number, sql, f"the Engine DB refused it: {error.orig}") from error
        finally:
            migration_project.reset(token)
        dialect.scope_migration_statement(self.conn, None)


def ownership_problems(
    conn: Connection, project_id: int, files: list[tuple[str, str]]
) -> list[str]:
    """Return the refusals pending ``(version, sql)`` files would meet, without claiming."""
    dialect = for_engine(conn)
    problems = []
    for version, text in files:
        scope = ProjectScope(conn, project_id, version)
        for number, sql in enumerate(dialect.split_statements(text), start=1):
            try:
                touches = scope.touches(number, sql)
            except MigrationError as error:
                problems.append(str(error))
                break
            for touch in touches:
                if problem := scope.owner_problem(touch):
                    problems.append(str(scope.refusal(number, sql, problem)))
                elif touch.temporary and touch.creates:
                    scope.temporary.add(touch.table)
    return problems


def unowned_team_tables(conn: Connection) -> list[str]:
    """Return the team tables no project migration has claimed yet."""
    owned = {str(row.table_name) for row in conn.execute(statement(conn, "team_tables"))}
    names = {name.upper() for name in inspect(conn).get_table_names()}
    return sorted(name for name in names if not package_table(name) and name not in owned)
