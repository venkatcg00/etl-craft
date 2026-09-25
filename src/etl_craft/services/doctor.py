"""``doctor``: check a configuration end to end, reporting every problem rather than the first.

Each check says what it looked at and what it found: ``OK``, ``WARN`` (it works, but look at
this) or ``FAIL`` (this will stop runs). The checks cover the settings used as written that look
like variables nobody set, each secret, auth modes not verified against a live service here, the
Engine DB (its connection, schema, tables and pending migrations), the warehouse (its connection,
schema and, on Trino, that its catalog is Iceberg), how email is sent, and the project folders.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config import ConnectorConfig, profile_needs_secret, resolve_secret
from etl_craft.config.auth import EMAIL_VERIFIED_AUTH_MODES, engine_for_jdbc_url
from etl_craft.config.model import ConnectionProfile, EmailProfile
from etl_craft.core.enums import Mode
from etl_craft.core.errors import EtlCraftError
from etl_craft.engine.connection import check_reachable, engine_db
from etl_craft.engine.migrations import pending_migrations
from etl_craft.engine.schema import existing_engine_tables
from etl_craft.execution.connections import probe_email_relay, probe_warehouse
from etl_craft.warehouse.connection import (
    build_warehouse_engine,
    is_in_memory,
    is_single_writer,
    verify_iceberg_catalog,
    warehouse_dialect,
)


class Status(StrEnum):
    """What a check found."""

    OK = "OK"
    WARN = "WARN"
    FAIL = "FAIL"


@dataclass(frozen=True)
class Check:
    """One thing ``doctor`` looked at, and what it found."""

    name: str
    status: Status
    detail: str


def ok(name: str, detail: str) -> Check:
    """Return a passing check."""
    return Check(name, Status.OK, detail)


def warn(name: str, detail: str) -> Check:
    """Return a check that passes but deserves a look."""
    return Check(name, Status.WARN, detail)


def fail(name: str, detail: str) -> Check:
    """Return a failed check."""
    return Check(name, Status.FAIL, detail)


def run_checks(config: ConnectorConfig, *, engine_state: bool = True) -> list[Check]:
    """Run every check and return them all, failures included.

    ``engine_state`` adds the Engine DB's tables and pending migrations; ``setup``, which creates
    them, leaves it out.
    """
    engine_checks = _engine(config, engine_state=engine_state)
    engine_reachable = not any(
        check.name == "Engine DB connection" and check.status is Status.FAIL
        for check in engine_checks
    )
    return [
        ok(
            "Configuration",
            f"{config.config_path} (mode {config.mode}, project {config.project_dir})",
        ),
        *_settings(config),
        *engine_checks,
        *_warehouse(config, queue_on_engine_db=engine_reachable),
        *_email(config),
        *_project(config),
    ]


def _settings(config: ConnectorConfig) -> list[Check]:
    from_variables = sum(1 for source in config.settings if source.variable)
    checks = [
        ok(
            "Settings",
            f"{from_variables} from variables, {len(config.settings) - from_variables} as written",
        )
    ]
    checks.extend(
        warn(
            "Setting used as written",
            f"{source.where} is {source.written!r}: no variable of that name is set in the "
            f"{config.source.type} source, so the text itself is the value",
        )
        for source in config.settings
        if source.looks_like_a_missing_variable
    )
    return checks


def _secret(
    config: ConnectorConfig, label: str, profile: ConnectionProfile | EmailProfile
) -> list[Check]:
    if not profile_needs_secret(profile):
        return []
    try:
        resolve_secret(config, profile)
    except EtlCraftError as error:
        return [fail(f"{label} secret", str(error))]
    return [ok(f"{label} secret", f"resolved from {profile.secret_var}")]


def _auth(label: str, auth_mode: str, verified: frozenset[str], target: str) -> list[Check]:
    if auth_mode in verified:
        return []
    return [
        warn(
            f"{label} auth",
            f"auth_mode {auth_mode} on {target} follows the vendor's documentation but has not "
            "been run against a live service here: it can be used, but success is not guaranteed",
        )
    ]


def _engine(config: ConnectorConfig, *, engine_state: bool) -> list[Check]:
    profile = config.engine.active
    spec = engine_for_jdbc_url(profile.jdbc_url)
    checks = _secret(config, "Engine DB", profile)
    checks += _auth("Engine DB", profile.auth_mode, spec.verified_auth_modes, spec.display_name)
    try:
        engine = engine_db(config)
    except EtlCraftError as error:
        return [*checks, fail("Engine DB connection", str(error))]
    try:
        try:
            check_reachable(engine, profile.schema)
        except EtlCraftError as error:
            return [*checks, fail("Engine DB connection", str(error))]
        where = f"schema {profile.schema}" if profile.schema else "its default schema"
        checks.append(ok("Engine DB connection", f"{spec.display_name}, {where}"))
        if spec.name == "sqlite":
            detail = (
                "a SQLite Engine DB suits one machine; its writes are serialized. Use "
                "PostgreSQL in production"
            )
            if config.mode == Mode.REMOTE:
                checks.append(
                    warn(
                        "Engine DB kind",
                        f"{detail}: in remote mode every orchestrator worker must run on this "
                        "machine, because a worker elsewhere cannot open the file",
                    )
                )
            else:
                checks.append(ok("Engine DB kind", detail))
        if engine_state:
            checks += _engine_state(config, engine)
    finally:
        engine.dispose()
    return checks


def _engine_state(config: ConnectorConfig, engine: Engine) -> list[Check]:
    try:
        if not existing_engine_tables(engine):
            return [
                fail(
                    "Engine DB tables",
                    "the Engine DB has no etl-craft tables yet: run `etl-craft setup`",
                )
            ]
        pending = pending_migrations(engine, project_default=config.migrations_dir)
    except (EtlCraftError, SQLAlchemyError) as error:
        return [fail("Engine DB migrations", str(error))]
    if pending:
        return [
            fail(
                "Engine DB migrations",
                f"{len(pending)} pending: {', '.join(pending)}; run `etl-craft migrate`",
            )
        ]
    return [ok("Engine DB migrations", "up to date")]


def _warehouse(config: ConnectorConfig, *, queue_on_engine_db: bool) -> list[Check]:
    """Check the warehouse; a single-writer one is opened in the writers' queue when it can be.

    The queue is kept in the Engine DB, so when that cannot be reached the warehouse is opened
    outside it rather than reported as failing for the Engine DB's reason.
    """
    if config.warehouse is None:
        return [ok("Warehouse", "no Warehouse section: only SQL and BUSINESS_RULES tasks need one")]
    profile = config.warehouse.active
    checks = _secret(config, "Warehouse", profile)
    try:
        dialect = warehouse_dialect(config)
    except EtlCraftError as error:
        return [*checks, fail("Warehouse", str(error))]
    checks += _auth(
        "Warehouse", profile.auth_mode, dialect.spec.verified_auth_modes, dialect.display_name
    )
    if is_in_memory(config):
        return [
            *checks,
            fail(
                "Warehouse",
                "an in-memory DuckDB warehouse (jdbc:duckdb: with no file) starts empty in every "
                "task's process; name a file: jdbc:duckdb:<path>",
            ),
        ]
    if is_single_writer(config):
        checks.append(
            ok(
                "Warehouse writers",
                "one writer at a time: tasks and ingestion scripts writing to it queue behind "
                "each other",
            )
        )
    engine = engine_db(config) if queue_on_engine_db else None
    try:
        problem = probe_warehouse(config, engine, schema=True)
    finally:
        if engine is not None:
            engine.dispose()
    if problem is not None:
        return [*checks, fail("Warehouse connection", problem)]
    checks.append(
        ok("Warehouse connection", f"{dialect.display_name}, schema {profile.schema} exists")
    )
    if dialect.key == "trino_iceberg":
        warehouse = build_warehouse_engine(config)
        try:
            catalog_problem = verify_iceberg_catalog(config, warehouse)
        finally:
            warehouse.dispose()
        checks.append(
            fail("Warehouse catalog", catalog_problem)
            if catalog_problem
            else ok("Warehouse catalog", "an Iceberg catalog")
        )
    return checks


def _email(config: ConnectorConfig) -> list[Check]:
    if config.email is None:
        return [ok("Email", "no Email settings: only EMAIL_ALERT tasks and SLA emails need them")]
    profile = config.email.active
    if profile.transport == "sendmail":
        problem = probe_email_relay(config)
        if problem:
            return [fail("Email", problem)]
        return [ok("Email", f"sendmail at {profile.sendmail_path}, from {profile.from_address}")]
    checks = _secret(config, "Email", profile)
    checks += _auth("Email", profile.auth_mode, EMAIL_VERIFIED_AUTH_MODES, "the SMTP relay")
    problem = probe_email_relay(config)
    if problem:
        return [*checks, fail("Email relay", problem)]
    checks.append(ok("Email relay", f"{profile.host}:{profile.port} answers"))
    return checks


def _project(config: ConnectorConfig) -> list[Check]:
    present = [
        folder.name
        for folder in (config.sql_files_dir, config.ingestion_scripts_dir, config.migrations_dir)
        if folder.is_dir()
    ]
    return [
        ok(
            "Project folders",
            f"{', '.join(present)} in {config.project_dir}"
            if present
            else f"no sql_files/, ingestion_scripts/ or migrations/ in {config.project_dir} yet",
        )
    ]
