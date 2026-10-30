"""``doctor``: check a configuration end to end, reporting every problem rather than the first.

Each check says what it looked at and what it found: ``OK``, ``WARN`` (it works, but look at
this) or ``FAIL`` (this will stop runs). The checks cover the settings used as written that look
like variables nobody set, each secret, auth modes not verified against a live service here, the
Engine DB (its connection, schema, tables and pending migrations), the warehouse (its connection,
schema and, on Trino, that its catalog is Iceberg), how email is sent, whether cloning can
write where it is set to, the project folders, and the measured CPU and memory with the
deployment profile's sizes and the Engine DB connection budget.
"""

from __future__ import annotations

import importlib.util
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from urllib.parse import parse_qsl

from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config import ConnectorConfig, profile_needs_secret, resolve_secret
from etl_craft.config.auth import EMAIL_VERIFIED_AUTH_MODES, engine_for_jdbc_url
from etl_craft.config.loader_helpers import shown
from etl_craft.config.model import ConnectionProfile, EmailProfile
from etl_craft.config.resolve import resolve_named_secret, source_values
from etl_craft.config.sizing import ceilings, engine_pool
from etl_craft.core.enums import (
    CloningScope,
    DeploymentProfile,
    EmailTransport,
    GatePolicy,
    Governance,
    IcebergMaintenance,
    ProcessRole,
    YesNo,
)
from etl_craft.core.errors import EtlCraftError
from etl_craft.core.resources import (
    MIB,
    Host,
    Measurement,
    describe,
    discover,
    size,
)
from etl_craft.core.text import quote_identifier
from etl_craft.dialects.engine import for_engine, for_jdbc_url
from etl_craft.dialects.engine.guards import missing_guards
from etl_craft.engine.connection import check_reachable, engine_db
from etl_craft.engine.connection_budget import footprint, reservations
from etl_craft.engine.migrations import PROJECT, MigrationFile, pending_files
from etl_craft.engine.notifications import check_notifications
from etl_craft.engine.project_migrations import ownership_problems, unowned_team_tables
from etl_craft.engine.queries import statement
from etl_craft.engine.schema import existing_engine_tables
from etl_craft.execution.connections import probe_email_relay, probe_warehouse
from etl_craft.handlers.mail import email_tls_problem
from etl_craft.services.calendar_checks import DOCTOR_DAYS, holiday_lists_ending
from etl_craft.services.cloning import cloning_problem
from etl_craft.services.operations.accounts import has_admin
from etl_craft.warehouse.connection import (
    build_warehouse_engine,
    is_in_memory,
    is_single_writer,
    verify_iceberg_catalog,
    warehouse_dialect,
)
from etl_craft.warehouse.fencing import cluster_problem


class Status(StrEnum):
    """What a check found."""

    INFO = "INFO"
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
            f"{config.config_path} (project {config.project_dir})",
        ),
        *_settings(config),
        *_files(config),
        *engine_checks,
        *_warehouse(config, queue_on_engine_db=engine_reachable),
        *_email(config),
        *_cloning(config),
        *_docs_site(config),
        *_project(config),
        *_artifacts(config),
        *resource_checks(config),
    ]


def resource_checks(config: ConnectorConfig, host: Host | None = None) -> list[Check]:
    """Report the measured allocation, the profile and every size it implies.

    Without a profile the sizes are what laptop, the recommended profile, would apply; nothing
    changes until the team declares ``Deployment.Profile``.
    """
    allocation = discover(host)
    checks = [
        _measured("Resources: CPU", allocation.cpus, "CPUs", "sized as one CPU"),
        _measured(
            "Resources: memory", allocation.memory, "bytes", "no automatic task-memory admission"
        ),
        ok(
            "Resources: host",
            f"{allocation.host_cpus or 'unknown'} CPUs, "
            f"{(allocation.host_memory or 0) // MIB or 'unknown'} MB in {allocation.scope}, "
            f"observed {allocation.observed_at:%Y-%m-%d %H:%M:%S} UTC; context only when a "
            "tighter limit applies",
        ),
    ]
    profile = config.deployment or DeploymentProfile.LAPTOP
    if config.deployment is None:
        checks.append(
            ok(
                "Deployment profile",
                "none: slots, threads and pools come from the Orchestration settings. "
                "Recommended: LAPTOP, because etl-craft cannot tell whether this host is shared; "
                "declare CONTROLLER for a dedicated host. Sizes below are what LAPTOP would apply",
            )
        )
    elif config.deployment == DeploymentProfile.CONTROLLER_AGENTS:
        problem = cluster_problem(config)
        checks.append(
            fail("Deployment profile", problem)
            if problem
            else ok(
                "Deployment profile",
                "CONTROLLER_AGENTS: PostgreSQL queue; the controller schedules and agents "
                "execute tasks. With no eligible agent, attempts stay QUEUED",
            )
        )
    else:
        checks.append(ok("Deployment profile", f"{profile}: applied when run and controller start"))
    sizing = size(allocation, profile, ceilings(config))
    reserve = f"{sizing.reserve_cpus:g} CPUs" + (
        "" if sizing.reserve_memory is None else f", {sizing.reserve_memory / MIB:.0f} MB"
    )
    checks += [
        ok("Sizing: reserve", f"{reserve} for the rest of the host ({profile})"),
        *(
            ok(f"Sizing: {name}", f"{item.value} ({item.reason})")
            for name, item in (
                ("task slots", sizing.task_slots),
                ("python slots", sizing.python_slots),
                ("warehouse slots", sizing.warehouse_slots),
                ("server threads", sizing.server_threads),
                ("Engine DB pool", engine_pool(config)),
            )
        ),
        *(warn("Sizing: fallback", fallback) for fallback in sizing.fallbacks),
    ]
    return checks


def _measured(name: str, measurement: Measurement, unit: str, fallback: str) -> Check:
    if measurement.value is None:
        return warn(name, f"{describe(measurement, unit)}; {fallback}")
    return ok(name, describe(measurement, unit))


def _connection_budget(config: ConnectorConfig, engine: Engine) -> list[Check]:
    """Report registered process reservations, or the proposed first controller footprint."""
    with engine.connect() as conn:
        limit = for_engine(engine).max_connections(conn)
        if limit is None:
            return []
        if for_engine(conn).has_table(conn, "AUD_CONNECTION_RESERVATIONS"):
            live = reservations(conn, datetime.now(UTC))
            agents = conn.execute(statement(conn, "connection_budget_agents")).scalar_one()
        else:
            live = None
            agents = 0
    planned = footprint(config, ProcessRole.CONTROLLER)
    budget = live if live is not None and live.total else planned
    detail = (
        f"{budget.total} of max_connections={limit}: "
        f"{budget.roles.count(ProcessRole.CONTROLLER)} controller(s), "
        f"{budget.roles.count(ProcessRole.UI)} ui copy/copies, "
        f"{budget.roles.count(ProcessRole.RUN)} local run(s); process pools {budget.pool} "
        f"+ dedicated sessions {budget.sessions} + one per task process ({budget.tasks}); "
        f"{agents} registered API-only agents add 0 connections; overflow 0; "
        + (
            "registered deployment total"
            if live is not None and live.total
            else "proposed controller"
        )
    )
    if budget.total * 5 > limit * 4:
        return [
            fail(
                "Engine DB connection budget",
                f"{detail}; expected at most 80%; "
                "lower Db_pool_size/local slots or raise max_connections",
            )
        ]
    return [ok("Engine DB connection budget", detail)]


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
    if config.dependency_gates != GatePolicy.ENFORCE:
        unchecked = config.dependency_gates == GatePolicy.OFF
        checks.append(
            warn(
                "Dependency gates",
                f"Orchestration.Dependency_gates is {config.dependency_gates}: runs go ahead "
                "when their dependencies on other pipelines are not satisfied"
                + (" (they are not checked at all)" if unchecked else "")
                + ", and each bypass is recorded in AUD_RUN_INTERVENTIONS. Keep it enforce "
                "wherever the data must be right",
            )
        )
    return checks


def _files(config: ConnectorConfig) -> list[Check]:
    """Check every configured certificate, key and mail executable at its absolute path."""
    paths: list[tuple[str, str]] = []
    for label, section in (("Engine DB", config.engine), ("Warehouse", config.warehouse)):
        if section is None:
            continue
        profile = section
        for key in ("key_file", "cert_file", "private_key_file"):
            if profile.extra.get(key):
                paths.append((f"{label} {shown(key)}", str(profile.extra[key])))
        for key, value in parse_qsl(profile.jdbc_url.partition("?")[2]):
            if key.lower() in {"sslrootcert", "sslcert", "sslkey", "private_key_file"}:
                paths.append((f"{label} {key}", value))
    if config.email is not None:
        email = config.email
        if email.ca_file is not None:
            paths.append(("Email Ca_file", str(email.ca_file)))
        if email.transport == EmailTransport.SENDMAIL:
            paths.append(("Email Sendmail_path", email.sendmail_path))
    checks = []
    for label, written in paths:
        path = Path(written)
        try:
            with path.open("rb") as stream:
                stream.read(1)
        except OSError as error:
            checks.append(fail(label, f"{path}: not a readable file ({error}); provide the file"))
        else:
            checks.append(ok(label, f"{path}: readable"))
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
    profile = config.engine
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
            checks.append(ok("Engine DB kind", detail))
        try:
            check_notifications(config, engine)
        except EtlCraftError as error:
            checks.append(fail("Engine DB notifications", str(error)))
        else:
            if for_engine(engine).supports_api_replicas:
                checks.append(ok("Engine DB notifications", "LISTEN/NOTIFY round trip delivered"))
        checks += _connection_budget(config, engine)
        if spec.name == "postgresql" and all(
            getattr(config.retention, key) is None
            for key in (
                "run_history_days",
                "attempt_log_days",
                "consumption_days",
                "interventions_days",
                "config_history_days",
            )
        ):
            checks.append(
                ok(
                    "Retention",
                    "history is kept forever; configure Retention periods "
                    "and review `etl-craft purge --dry-run` to bound PostgreSQL history",
                )
            )
        if engine_state:
            checks += _engine_state(config, engine)
    finally:
        engine.dispose()
    return checks


def _warehouse_retention(config: ConnectorConfig, engine: Engine) -> list[Check]:
    with engine.connect() as conn:
        project = conn.execute(
            statement(conn, "project_by_code"), {"code": config.project}
        ).one_or_none()
        if project is None:
            return []
        tables = conn.execute(
            statement(conn, "config_rows_tables"), {"project_id": project.project_id}
        ).all()
    checks = []
    for table in tables:
        if table.object_kind == "VIEW":
            continue
        name = f"{table.schema_name}.{table.table_name}"
        if table.reject_retention_days is None:
            checks.append(
                ok(
                    "Warehouse retention",
                    f"{name} rejects are kept forever; "
                    "set REJECT_RETENTION_DAYS in tables.csv to bound them",
                )
            )
        for action, days in (
            (IcebergMaintenance.EXPIRE_SNAPSHOTS, table.snapshot_retention_days),
            (IcebergMaintenance.REMOVE_ORPHAN_FILES, table.orphan_retention_days),
        ):
            if (
                days is None
                and config.warehouse is not None
                and (warehouse_dialect(config).retention_maintenance(name, action, 7) is not None)
            ):
                setting = (
                    "SNAPSHOT_RETENTION_DAYS"
                    if action == IcebergMaintenance.EXPIRE_SNAPSHOTS
                    else "ORPHAN_RETENTION_DAYS"
                )
                checks.append(
                    ok(
                        "Warehouse retention",
                        f"{name}: {setting} is unset; "
                        "files are kept forever by package maintenance; configure tables.csv",
                    )
                )
            if days is not None and (
                config.warehouse is None
                or warehouse_dialect(config).retention_maintenance(name, action, days) is None
            ):
                checks.append(
                    fail(
                        "Warehouse retention",
                        f"{name}: {action} unsupported; unset the policy or use Trino over Iceberg",
                    )
                )
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
        files = pending_files(engine, project_default=config.migrations_dir)
        pending = [f"{file.source.lower()}/{file.version}" for file in files]
        scope = _scope_problems(config, engine, [f for f in files if f.source == PROJECT])
    except (EtlCraftError, SQLAlchemyError) as error:
        return [fail("Engine DB migrations", str(error))]
    if pending:
        return [
            fail(
                "Engine DB migrations",
                f"{len(pending)} pending: {', '.join(pending)}; run `etl-craft migrate`",
            ),
            *(fail("Project migration scope", problem) for problem in scope),
        ]
    checks = [ok("Engine DB migrations", "up to date"), *_warehouse_retention(config, engine)]
    checks.append(
        ok("Active admin", "an active administrator exists")
        if has_admin(engine)
        else fail(
            "Active admin",
            "no active admin; run `etl-craft setup` interactively "
            "with the Engine DB writer credentials to provision or recover one",
        )
    )
    dialect = for_engine(engine)
    with engine.connect() as conn:
        grants = dialect.extra_write_grants(conn)
        checks.extend(_guard_checks(conn, config))
    for table, role, privilege in grants:
        quoted_role = quote_identifier(role) if role != "PUBLIC" else role
        schema = config.engine.schema or "public"
        qualified = f"{quote_identifier(schema)}.{quote_identifier(table)}"
        checks.append(
            fail(
                "Engine DB write grant",
                f"{role} holds {privilege} on {qualified}; "
                f"REVOKE {privilege} ON {qualified} FROM {quoted_role};",
            )
        )
    path = dialect.exposed_file(engine)
    if path is not None:
        checks.append(
            warn(
                "Engine DB file permissions",
                f"{path} is writable by group or others; restrict access with chmod go-w",
            )
        )
    return checks


QUEUE_TABLE = "AUD_RESOURCE_LOCKS"
"""The Engine DB table holding the single-writer warehouse queue."""


def _queue_engine(config: ConnectorConfig) -> Engine | None:
    """Return the Engine DB holding the writers' queue, or ``None`` before setup creates it."""
    engine = engine_db(config)
    if for_engine(engine).existing_tables(engine, (QUEUE_TABLE,)):
        return engine
    engine.dispose()
    return None


def _warehouse(config: ConnectorConfig, *, queue_on_engine_db: bool) -> list[Check]:
    """Check the warehouse; a single-writer one is opened in the writers' queue when it can be.

    The queue is kept in the Engine DB, so when that cannot be reached the warehouse is opened
    outside it rather than reported as failing for the Engine DB's reason.
    """
    if config.warehouse is None:
        return [ok("Warehouse", "no Warehouse section: only SQL and BUSINESS_RULES tasks need one")]
    profile = config.warehouse
    checks = _secret(config, "Warehouse", profile)
    if name := profile.extra.get("s3_secret"):
        try:
            resolve_named_secret(config, str(name))
        except EtlCraftError as error:
            checks.append(fail("Warehouse storage secret", str(error)))
        else:
            checks.append(ok("Warehouse storage secret", f"resolved from {name}"))
    try:
        dialect = warehouse_dialect(config)
    except EtlCraftError as error:
        return [*checks, fail("Warehouse", str(error))]
    checks += _auth(
        "Warehouse", profile.auth_mode, dialect.spec.verified_auth_modes, dialect.spec.display_name
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
                "one writer at a time: tasks and Python scripts writing to it queue behind "
                "each other",
            )
        )
    engine = _queue_engine(config) if queue_on_engine_db else None
    try:
        problem = probe_warehouse(config, engine, schema=True)
    finally:
        if engine is not None:
            engine.dispose()
    if problem is not None:
        return [*checks, fail("Warehouse connection", problem)]
    checks.append(
        ok("Warehouse connection", f"{dialect.spec.display_name}, schema {profile.schema} exists")
    )
    if dialect.spec.key == "trino_iceberg":
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
    profile = config.email
    if profile.transport == EmailTransport.SENDMAIL:
        problem = probe_email_relay(config)
        if problem:
            return [fail("Email", problem)]
        return [ok("Email", f"sendmail at {profile.sendmail_path}, from {profile.from_address}")]
    checks = _secret(config, "Email", profile)
    checks += _auth("Email", profile.auth_mode, EMAIL_VERIFIED_AUTH_MODES, "the SMTP relay")
    tls_problem = email_tls_problem(profile)
    if tls_problem:
        return [*checks, fail("Email TLS", tls_problem)]
    checks.append(
        ok(
            "Email TLS",
            f"Tls_mode={profile.tls_mode}, Ca_file={profile.ca_file or 'system trust'}",
        )
    )
    problem = probe_email_relay(config)
    if problem:
        return [*checks, fail("Email relay", problem)]
    checks.append(ok("Email relay", f"{profile.host}:{profile.port} answers"))
    return checks


def _cloning(config: ConnectorConfig) -> list[Check]:
    cloning = config.cloning
    if not cloning.enabled or cloning.scope == CloningScope.NONE:
        return []
    problem = cloning_problem(config)
    if problem is not None:
        return [fail("Cloning", problem)]
    assert config.warehouse is not None
    return [
        ok(
            "Cloning",
            f"scope {cloning.scope}: every {cloning.every_minutes} minutes, "
            f"into schema {config.warehouse.schema}",
        )
    ]


def _docs_site(config: ConnectorConfig) -> list[Check]:
    """Report organization hosting or the local requirements for ngrok publication."""
    if config.docs_site.external_url:
        return [
            ok(
                "Docs site publishing",
                f"organization URL {config.docs_site.external_url}; no ngrok required",
            )
        ]
    name = config.docs_site.authtoken_var
    if name is None:
        return []
    checks = []
    if source_values(config.source).get(name):
        checks.append(ok("Docs site authtoken", f"resolved from {name}"))
    else:
        checks.append(
            warn(
                "Docs site authtoken",
                f"{name} is not set here: only the machine that runs publish-docs needs it",
            )
        )
    if importlib.util.find_spec("ngrok") is None:
        checks.append(
            warn(
                "Docs site publishing",
                "the ngrok SDK is not installed here: pip install 'etl-craft[publish]' on the "
                "machine that runs publish-docs",
            )
        )
    return checks


def _project(config: ConnectorConfig) -> list[Check]:
    present = [
        folder.name
        for folder in (config.sql_scripts_dir, config.python_scripts_dir, config.migrations_dir)
        if folder.is_dir()
    ]
    return [
        ok(
            "Project folders",
            f"{', '.join(present)} in {config.project_dir}"
            if present
            else f"no sql_scripts/, python_scripts/ or migrations/ in {config.project_dir} yet",
        )
    ]


def _artifacts(config: ConnectorConfig) -> list[Check]:
    """Prove the durable bundle store and configured log prefix are writable."""
    from etl_craft.core.errors import EtlCraftError
    from etl_craft.engine.artifacts import ArtifactStore

    checks: list[Check] = []
    try:
        artifacts = for_jdbc_url(config.engine.jdbc_url).artifact_location(config)
    except EtlCraftError as error:
        checks.append(fail("Artifact store", str(error)))
        artifacts = None
    else:
        if config.artifacts.store is None:
            checks.append(Check("Artifact store", Status.INFO, f"SQLite default: {artifacts}"))
    for label, address, profile in (
        ("Artifact store", artifacts, config.artifacts.profile),
        ("Log store", config.logs.store, config.logs.profile),
    ):
        if address is None:
            continue
        store = ArtifactStore(config, address, profile)
        try:
            store.probe()
        except (EtlCraftError, OSError) as error:
            checks.append(fail(label, str(error)))
        else:
            checks.append(
                ok(
                    label,
                    f"profile={profile}, prefix={address}: list/write/read succeeded",
                )
            )
    return checks


def _guard_checks(conn: Connection, config: ConnectorConfig) -> list[Check]:
    """Report missing guards and the selected project's direct-edit capability."""
    checks: list[Check] = []
    dialect = for_engine(conn)
    for trigger in missing_guards(conn):
        checks.append(
            fail(
                "Engine DB guard",
                f"missing trigger {trigger}; expected every package guard; "
                "re-run `etl-craft setup` or restore",
            )
        )
    project = conn.execute(
        statement(conn, "project_by_code"), {"code": config.project}
    ).one_or_none()
    policy = (
        None
        if project is None
        else conn.execute(
            statement(conn, "project_governance"), {"project_id": project.project_id}
        ).one()
    )
    if (
        policy is not None
        and policy.governance == Governance.RELAXED
        and not dialect.supports_api_replicas
    ):
        checks.append(
            Check(
                "Direct config edits",
                Status.INFO,
                "RELAXED project: direct SQL edits need a PostgreSQL Engine DB; "
                "use config files or commands on SQLite",
            )
        )

    if dialect.has_table(conn, "AUD_TEAM_TABLES"):
        checks.extend(
            Check(
                "Team table owner",
                Status.INFO,
                f"{table} has no owning project; the first project migration that changes it "
                "claims it for its project",
            )
            for table in unowned_team_tables(conn)
        )
    if project is not None and dialect.has_table(conn, "AUD_PIPELINE_REFRESHES"):
        checks.extend(_refresh_checks(conn, int(project.project_id)))
    if project is not None and dialect.has_table(conn, "CFG_CALENDARS"):
        ending = holiday_lists_ending(conn, datetime.now(UTC), DOCTOR_DAYS)
        checks.extend(warn(f"Holiday list ({where})", problem) for where, problem in ending)

    return checks


def _scope_problems(
    config: ConnectorConfig, engine: Engine, files: list[MigrationFile]
) -> list[str]:
    """Name pending project statements that would change another project's tables."""
    with engine.connect() as conn:
        if not files or not for_engine(conn).has_table(conn, "AUD_TEAM_TABLES"):
            return []
        project = conn.execute(
            statement(conn, "project_by_code"), {"code": config.project}
        ).one_or_none()
        if project is None:
            return []
        pending = [(file.version, file.sql) for file in files]
        return ownership_problems(conn, int(project.project_id), pending)


def _refresh_checks(conn: Connection, project_id: int) -> list[Check]:
    """List invalid baselines without hiding pipelines that retain a runnable version."""
    checks = []
    for row in conn.execute(statement(conn, "refresh_doctor"), {"project_id": project_id}):
        findings = [] if row.findings is None else json.loads(row.findings)
        detail = "; ".join(str(f["message"]) for f in findings) or "no refresh recorded"
        blocked = not row.has_baseline or row.block_on_refresh_error == YesNo.YES
        state = (
            "no version has passed yet" if not row.has_baseline else "last good version retained"
        )
        checks.append(
            Check(
                f"Pipeline refresh {row.pipeline_code}",
                Status.FAIL if blocked else Status.WARN,
                f"{state}; {detail}; correct the findings and run `etl-craft pipelines refresh`",
            )
        )
    return checks
