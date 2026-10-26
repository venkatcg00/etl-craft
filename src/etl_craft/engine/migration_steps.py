"""The Python each Engine DB migration marker runs, registered as data.

``STEPS`` maps every marker to its check and conversion. A new migration that uses an existing
marker changes no code; a new marker adds one entry here and leaves the migration runner
unchanged.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, TypedDict

from sqlalchemy import inspect
from sqlalchemy.engine import Connection

from etl_craft.core.actor import Actor, ActorKind, current_actor, migration
from etl_craft.core.errors import ConfigurationError, MigrationError, SqlGuardError
from etl_craft.core.text import is_metadata_code
from etl_craft.dialects.engine import for_engine
from etl_craft.engine.queries import project_statement, statement


@dataclass(frozen=True)
class Target:
    """Where converted metadata points: the warehouse catalog and SQL dialect."""

    catalog: str | None = None
    warehouse_dialect: str | None = None
    project_dialects: Mapping[str, str] | None = None


class TargetOptions(TypedDict, total=False):
    """Keyword inputs for the migration context used by setup and migrate."""

    catalog: str | None
    warehouse_dialect: str | None
    project_dialects: Mapping[str, str] | None


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
    from etl_craft.engine.repository import target_contracts as contracts

    return contracts.legacy_targets(
        conn, contracts.RULE_TARGETS, target.catalog, target.warehouse_dialect
    )


def _convert_rules(conn: Connection, state: LegacyRows, target: Target) -> None:
    from etl_craft.engine.repository import target_contracts as contracts

    contracts.convert_targets(
        conn, contracts.RULE_TARGETS, state, target.catalog, target.warehouse_dialect
    )


def _legacy_tasks(conn: Connection, target: Target) -> LegacyRows:
    from etl_craft.engine.repository import target_contracts as contracts

    return contracts.legacy_targets(
        conn, contracts.TASK_TARGETS, target.catalog, target.warehouse_dialect
    )


def _convert_tasks(conn: Connection, state: LegacyRows, target: Target) -> None:
    from etl_craft.engine.repository import target_contracts as contracts

    contracts.convert_targets(
        conn, contracts.TASK_TARGETS, state, target.catalog, target.warehouse_dialect
    )


def check_account_token_identities(conn: Connection) -> LegacyRows:
    """Refuse invalid or conflicting account identities before pending migration writes."""
    inspector = inspect(conn)
    if not inspector.has_table("cfg_api_tokens"):
        return []
    names: dict[str, list[str]] = defaultdict(list)
    invalid = []
    for row in conn.execute(statement(conn, "account_legacy_tokens")):
        try:
            Actor(row.name, ActorKind.SERVICE)
        except ConfigurationError:
            invalid.append(f"TOKEN_ID={row.token_id}, NAME={row.name!r}: invalid actor name")
        if row.name_count > 1:
            names[row.name].append(f"TOKEN_ID={row.token_id}, ROLE={row.role}")
    conflicts = invalid + [f"{name!r}: {', '.join(rows)}" for name, rows in names.items()]
    if conflicts:
        raise MigrationError(
            "CFG_API_TOKENS account migration found invalid identities: "
            + "; ".join(conflicts)
            + "; expected one legacy token per non-empty actor name of at most 128 characters "
            "without control characters, including inactive and revoked tokens. Nothing was "
            "changed; give these tokens valid, distinct NAME values through "
            "an explicit project migration before upgrading, then run etl-craft migrate again"
        )
    return []


def _record_accounts(conn: Connection, state: LegacyRows, target: Target) -> None:
    """Keep account creation and changed token ownership in existing typed histories."""
    dialect = for_engine(conn)
    dialect.refresh_metadata_triggers(conn)
    dialect.record_config_baseline(
        conn, ("CFG_ACCOUNTS",), migration=migration.get(), actor=current_actor()
    )
    dialect.record_config_conversion(
        conn, "CFG_API_TOKENS", "TOKEN_ID", migration=migration.get(), actor=current_actor()
    )


def _check_business_rule_flags(conn: Connection, target: Target) -> LegacyRows:
    from etl_craft.dialects.business_rules import converted_rule

    inspector = inspect(conn)
    if inspector.has_table("aud_business_rules_results"):
        conflicts = [
            f"rule={row.business_rule_id}, key={row.business_rule_key!r}, "
            f"result id={row.business_rule_result_id}"
            for row in conn.execute(statement(conn, "business_rule_duplicate_flags"))
        ]
        if conflicts:
            raise MigrationError(
                "AUD_BUSINESS_RULES_RESULTS has conflicting active flags: "
                + "; ".join(conflicts)
                + "; expected one ACTIVE_FLAG=Y result per (BUSINESS_RULE_ID, BUSINESS_RULE_KEY). "
                "Nothing was changed; use the previous package and an explicit project migration "
                "to clear the conflicting flags while retaining their history, "
                "then run migrate again"
            )
    if not inspector.has_table("cfg_business_rules"):
        return []
    rows = [
        dict(row._mapping)
        for row in conn.execute(project_statement(conn, "business_rule_legacy_sql"))
    ]
    dialects = target.project_dialects
    if dialects is None:
        dialects = {"default": target.warehouse_dialect} if target.warehouse_dialect else {}
    for row in rows:
        dialect = dialects.get(row["project_code"])
        if dialect is None:
            raise MigrationError(
                f"CFG_BUSINESS_RULES rule {row['business_rule_name']!r} "
                f"(BUSINESS_RULE_ID={row['business_rule_id']}), project={row['project_code']!r}: "
                "Warehouse dialect=None; expected the target project's Warehouse dialect; "
                "configure Warehouse and supply its connector file "
                "with migrate --project-config PATH"
            )
        try:
            row["converted_sql"] = converted_rule(
                row["business_rule_sql"], row["business_rule_key_column"], dialect
            )
        except (SqlGuardError, ValueError) as error:
            raise MigrationError(
                f"business rule {row['business_rule_name']!r} "
                f"(BUSINESS_RULE_ID={row['business_rule_id']}) "
                f"project={row['project_code']!r}, Warehouse dialect={dialect!r}, "
                f"BUSINESS_RULE_SQL={row['business_rule_sql']!r}: {error}; "
                "correct this rule with the previous package before upgrading"
            ) from error
    return rows


def _convert_business_rule_sql(conn: Connection, state: LegacyRows, target: Target) -> None:
    from etl_craft.engine.repository.target_contracts import (
        record_conversion,
        suspend_config_capture,
    )

    if not state:
        return
    suspend_config_capture(conn, "CFG_BUSINESS_RULES")
    for row in state:
        conn.execute(
            statement(conn, "business_rule_convert_sql"),
            {
                "business_rule_id": row["business_rule_id"],
                "rule_sql": row["converted_sql"],
            },
        )
    record_conversion(conn, "CFG_BUSINESS_RULES", "BUSINESS_RULE_ID", migration.get())


STEPS: dict[str, MarkerStep] = {
    "rebuild-metadata": MarkerStep(),
    "check-account-token-identities": MarkerStep(
        check=lambda conn, target: check_account_token_identities(conn),
        convert=_record_accounts,
        names_migration=True,
    ),
    "check-metadata-identities": MarkerStep(
        check=lambda conn, target: check_metadata_identities(conn)
    ),
    "check-metadata-codes": MarkerStep(check=lambda conn, target: check_metadata_codes(conn)),
    "convert-source-contracts": MarkerStep(check=_legacy_sources, convert=_convert_sources),
    "convert-rule-targets": MarkerStep(check=_legacy_rules, convert=_convert_rules),
    "convert-task-targets": MarkerStep(
        check=_legacy_tasks, convert=_convert_tasks, names_migration=True
    ),
    "convert-target-state": MarkerStep(check=lambda conn, target: check_target_names(conn)),
    "check-business-rule-flags": MarkerStep(
        check=_check_business_rule_flags, convert=_convert_business_rule_sql, names_migration=True
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


_TARGET_NAME = {
    3: re.compile(r"[^.\s]+\.[^.\s]+\.[^.\s]+"),
    2: re.compile(r"[^.\s]+(?:\.[^.\s]+){1,2}"),
}
_RECORDED_TARGET_NAMES = (
    ("AUD_TARGET_HASH_VERSION", "TARGET_OBJECT", "recorded_hash_version_names", 3),
    ("AUD_TARGET_VIEW_STATEMENT", "TARGET_OBJECT", "recorded_view_statement_names", 3),
    ("AUD_BUSINESS_RULES_RESULTS", "TARGET_TABLE", "recorded_rule_result_names", 2),
)


def check_target_names(conn: Connection) -> None:
    """Refuse the target-state migration while a recorded name cannot be split as recorded.

    Hash versions and view statements were recorded as ``catalog.schema.table``; rule results
    as ``schema.table`` or ``catalog.schema.table``. Each converts from its own spelling, never
    from today's configured catalog. A table an earlier pending migration creates has no rows.
    """
    inspector = inspect(conn)
    bad = [
        f"{table} {row.name!r}"
        for table, column, query, parts in _RECORDED_TARGET_NAMES
        if inspector.has_table(table.lower())
        and column.lower() in {c["name"].lower() for c in inspector.get_columns(table.lower())}
        for row in conn.execute(statement(conn, query))
        if not _TARGET_NAME[parts].fullmatch(str(row.name))
    ]
    if bad:
        raise MigrationError(
            "target state migration cannot run: "
            + "; ".join(bad)
            + " cannot be split into catalog, schema and table; nothing was changed. Correct or "
            "delete each row with a project migration and run etl-craft migrate again; a deleted "
            "hash version is rebuilt with etl-craft rehash"
        )
