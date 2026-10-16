"""The Python each Engine DB migration marker runs, registered as data.

``STEPS`` maps every marker to its check and conversion. A new migration that uses an existing
marker changes no code; a new marker adds one entry here and leaves the migration runner
unchanged.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from sqlalchemy.engine import Connection

from etl_craft.core.errors import MigrationError
from etl_craft.core.text import is_metadata_code
from etl_craft.engine.queries import statement


@dataclass(frozen=True)
class Target:
    """Where converted metadata points: the warehouse catalog and SQL dialect."""

    catalog: str | None = None
    warehouse_dialect: str | None = None


NO_TARGET = Target()


LegacyRows = list[dict[str, Any]]
"""What a conversion marker's check reads before the migration's statements run."""


@dataclass(frozen=True)
class MarkerStep:
    """The Python a migration marker runs. A new migration using a marker needs no code.

    ``check`` runs before any pending upgrade and again inside the file's transaction; what it
    returns there is the state ``convert`` receives at the file's ``convert-at`` position.
    ``names_migration`` attributes the file's writes to the migration.
    """

    check: Callable[[Connection, Target], LegacyRows | None] | None = None
    convert: Callable[[Connection, LegacyRows, Target], None] | None = None
    names_migration: bool = False


def _legacy_sources(conn: Connection, target: Target) -> LegacyRows:
    from etl_craft.engine.repository.source_contracts import legacy_sources

    return legacy_sources(conn, target.catalog, target.warehouse_dialect)


def _convert_sources(conn: Connection, state: LegacyRows, target: Target) -> None:
    from etl_craft.engine.repository.source_contracts import convert_sources

    convert_sources(conn, state)


def _legacy_rules(conn: Connection, target: Target) -> LegacyRows:
    from etl_craft.engine.repository.rule_targets import legacy_rule_targets

    return legacy_rule_targets(conn, target.catalog, target.warehouse_dialect)


def _convert_rules(conn: Connection, state: LegacyRows, target: Target) -> None:
    from etl_craft.engine.repository.rule_targets import convert_rule_targets

    convert_rule_targets(conn, state, target.catalog, target.warehouse_dialect)


def _legacy_tasks(conn: Connection, target: Target) -> LegacyRows:
    from etl_craft.engine.repository.task_targets import legacy_task_targets

    return legacy_task_targets(conn, target.catalog, target.warehouse_dialect)


def _convert_tasks(conn: Connection, state: LegacyRows, target: Target) -> None:
    from etl_craft.engine.repository.task_targets import convert_task_targets

    convert_task_targets(conn, state, target.catalog, target.warehouse_dialect)


STEPS: dict[str, MarkerStep] = {
    "rebuild-metadata": MarkerStep(),
    "check-metadata-identities": MarkerStep(
        check=lambda conn, target: check_metadata_identities(conn)
    ),
    "check-metadata-codes": MarkerStep(check=lambda conn, target: check_metadata_codes(conn)),
    "convert-source-contracts": MarkerStep(check=_legacy_sources, convert=_convert_sources),
    "convert-rule-targets": MarkerStep(check=_legacy_rules, convert=_convert_rules),
    "convert-task-targets": MarkerStep(
        check=_legacy_tasks, convert=_convert_tasks, names_migration=True
    ),
}
"""Every marker, in the order its checks run."""
MARKERS = frozenset(STEPS)


def check_metadata_codes(conn: Connection) -> None:
    """Refuse the metadata-code migration while any code breaks the naming rule."""
    invalid = [
        f"{row.object} (id {row.object_id}) = {row.code!r}"
        for row in conn.execute(statement(conn, "metadata_codes"))
        if not is_metadata_code(row.code)
    ]
    if invalid:
        raise MigrationError(
            "metadata code migration cannot run: "
            + "; ".join(invalid)
            + "; rename these codes to start with an ASCII letter and contain only letters, "
            "digits and underscores, at most 128 characters, then run migrate again"
        )


def check_metadata_identities(conn: Connection) -> None:
    """Refuse the identity migration while config rows have conflicting identities."""
    from etl_craft.engine.repository.normalization import config_identity_problems

    problems = config_identity_problems(conn)
    if problems:
        raise MigrationError("config identity migration cannot run: " + "; ".join(problems))
