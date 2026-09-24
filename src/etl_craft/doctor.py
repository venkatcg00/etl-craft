"""`etl-craft doctor` — check a configuration end to end before anything depends on it.

[ADDITION, 2026-09-20, E2-16] `configure` writes a profile whose secret is
looked up as `ETL_CRAFT_{SECTION}_{PROFILE}_SECRET` and never mentions that
name anywhere, so the flow was: run `configure`, answer every prompt, then
have the next command fail with `secret 'ETL_CRAFT_POSTGRES_DEV_SECRET' not
found`. There was also no way to verify a configuration at all short of
running a real pipeline.

This is deliberately a read-only report built entirely from code that already
exists — `load_config`, `resolve_secret`, `build_engine`, `warehouse.open_warehouse`,
and the `[Email]` profile — rather than new machinery. It reports every check
rather than stopping at the first failure, for the same reason `validate`
does: one broken profile shouldn't hide the rest of the picture.
"""

from __future__ import annotations

import smtplib
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config import (
    EMAIL_VERIFIED_AUTH_MODES,
    ConfigError,
    ConnectorConfig,
    profile_needs_secret,
    resolve_secret,
)
from etl_craft.db import ConnectionError_, build_engine, is_sqlite_url, resolve_sqlite_path
from etl_craft.dialects.engine_dialects import for_jdbc_url
from etl_craft.warehouse import (
    READ_ONLY_WAIT_SECONDS,
    is_in_memory,
    is_single_writer,
    open_warehouse,
    warehouse_dialect,
)

SMTP_PROBE_TIMEOUT_SECONDS = 10.0


@dataclass(frozen=True)
class CheckResult:
    """One thing `doctor` looked at, and what it found."""

    name: str
    ok: bool
    detail: str
    # [ADDITION, 2026-09-24] Worth reading, not a failure: doctor still exits 0.
    warning: bool = False

    @property
    def marker(self) -> str:
        """Return the status marker this result renders with."""
        if not self.ok:
            return "FAIL"
        return "WARN" if self.warning else "OK  "


def _secret_check(config: ConnectorConfig, label: str, profile: object) -> CheckResult:
    var_name = profile.secret_var  # type: ignore[attr-defined]
    try:
        resolve_secret(config, profile)  # type: ignore[arg-type]
    except ConfigError as exc:
        return CheckResult(f"{label} secret", False, f"{exc} (expected in {var_name})")
    return CheckResult(f"{label} secret", True, f"resolved from {var_name}")


def _settings_check(config: ConnectorConfig) -> list[CheckResult]:
    """Name every value used as written that reads like a variable name.

    [ADDITION, 2026-09-24] A setting is a variable's value when the secrets
    source defines that variable, and the text as written otherwise. That makes
    a missing or misspelled variable invisible -- `user: ENGINE_USER` quietly
    becomes the user "ENGINE_USER" -- so each such value is reported here.
    """
    from_variables = sum(1 for source in config.settings if source.variable)
    results = [
        CheckResult(
            "Settings",
            True,
            f"{from_variables} from variables, "
            f"{len(config.settings) - from_variables} used as written",
        )
    ]
    for source in config.settings:
        if source.looks_like_a_missing_variable:
            results.append(
                CheckResult(
                    "Setting used as written",
                    True,
                    f"{source.where} is {source.written!r}: no variable by that name is set in "
                    f"the {config.source.type} source, so the text itself is the value",
                    warning=True,
                )
            )
    return results


def _auth_check(
    label: str, auth_mode: str, verified: frozenset[str], target: str
) -> list[CheckResult]:
    """Say so when an auth mode is implemented but has never run against a live service.

    [ADDITION, 2026-09-24] Per explicit instruction: "say these can be used but
    success is not guaranteed".
    """
    if auth_mode in verified:
        return []
    return [
        CheckResult(
            f"{label} auth",
            True,
            f"auth_mode {auth_mode} on {target} follows the vendor's documentation but is "
            "untested against a live service here: it can be used, success is not guaranteed",
            warning=True,
        )
    ]


def _engine_db_check(config: ConnectorConfig) -> CheckResult:
    engine = None
    try:
        engine = build_engine(config)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except (ConfigError, SQLAlchemyError) as exc:
        return CheckResult("Engine DB connection", False, str(exc))
    finally:
        # Same reason setup disposes its engine: a probe that leaves pooled
        # connections open blocks anything trying to drop the database.
        if engine is not None:
            engine.dispose()
    if is_sqlite_url(config.postgres.active.jdbc_url):
        path = resolve_sqlite_path(config.postgres.active.jdbc_url, config.config_path)
        return CheckResult("Engine DB connection", True, f"SQLite file {path}")
    return CheckResult("Engine DB connection", True, f"connected as {config.postgres.active.user}")


def _engine_db_kind_check(config: ConnectorConfig) -> list[CheckResult]:
    """Say plainly what a SQLite Engine DB can and cannot do.

    [ADDITION, 2026-09-24] SQLite is the default Engine DB and PostgreSQL the
    recommended production one. The difference matters most in one case:
    remote orchestration, where tasks may run on other machines that cannot
    open this file at all.
    """
    if not is_sqlite_url(config.postgres.active.jdbc_url):
        return []
    detail = (
        "SQLite Engine DB: good for local development and single-machine deployments. "
        "Engine DB writes are serialized; use PostgreSQL for production."
    )
    if config.mode == "orchestrator":
        detail += (
            " Mode is remote: every orchestrator worker must run on this machine, because "
            "a worker elsewhere cannot open this file -- use PostgreSQL otherwise."
        )
    return [CheckResult("Engine DB kind", True, detail)]


def _warehouse_check(config: ConnectorConfig) -> list[CheckResult]:
    if config.warehouse is None:
        return [
            CheckResult(
                "Warehouse",
                True,
                "no [Warehouse] section configured (only needed for SQL/BUSINESS_RULES tasks)",
            )
        ]
    profile = config.warehouse.active
    results = [_secret_check(config, "Warehouse", profile)] if profile_needs_secret(profile) else []
    try:
        dialect = warehouse_dialect(config)
    except (ConfigError, ConnectionError_) as exc:
        results.append(CheckResult("Warehouse", False, str(exc)))
        return results
    results.extend(
        _auth_check(
            "Warehouse", profile.auth_mode, dialect.verified_auth_modes, dialect.display_name
        )
    )

    # [ADDITION, 2026-09-21, E2-63] Refuse an in-memory warehouse here, where
    # it is cheap to notice. Every task runs in its own process and builds its
    # own warehouse engine, so an in-memory warehouse is empty at the start of
    # every task -- nothing errors, targets simply are not there, and the
    # pipeline fails somewhere far from the cause. Failing at doctor/setup
    # beats failing at 3am.
    if is_in_memory(config):
        results.append(
            CheckResult(
                "Warehouse",
                False,
                "an in-memory DuckDB warehouse (jdbc:duckdb: with no path) cannot hold data "
                "between tasks — each task runs in its own process, so every task would start "
                "against an empty database. Use jdbc:duckdb:<path>.",
            )
        )
        return results

    # [ADDITION, 2026-09-21, E2-61] Report the constraint, so someone reading
    # doctor's output learns it here rather than from a task failing under a
    # parallel wave. A single-writer warehouse admits one writing OS process
    # at a time, so the engine serializes warehouse access across processes.
    if is_single_writer(config):
        results.append(
            CheckResult(
                "Warehouse concurrency",
                True,
                "single-writer warehouse: warehouse access is serialized across processes, so "
                "tasks in a parallel wave queue rather than run concurrently. Covers "
                "HANDLER=PYTHON ingestion scripts too (E2-81) — the lock is held around "
                "the script, even though the script opens its own connection",
            )
        )

    # Take the same queueing route every other warehouse caller takes, so doctor
    # works while a task is running instead of erroring on the file lock —
    # which is exactly when someone reaches for it. The Engine DB engine is
    # what holds that lock; if it cannot be built, fall through unserialized
    # rather than failing a check that is about the warehouse.
    engine_db: Engine | None = None
    try:
        engine_db = build_engine(config)
    except (ConfigError, SQLAlchemyError):
        engine_db = None
    try:
        with (
            open_warehouse(
                config, engine_db, wait_seconds=READ_ONLY_WAIT_SECONDS
            ) as warehouse_engine,
            warehouse_engine.connect() as conn,
        ):
            conn.execute(text("SELECT 1"))
    except (ConfigError, SQLAlchemyError, NotImplementedError) as exc:
        results.append(CheckResult("Warehouse connection", False, str(exc)))
        return results
    finally:
        if engine_db is not None:
            engine_db.dispose()
    results.append(CheckResult("Warehouse connection", True, "connected"))
    # [DEVIATION, 2026-09-22, E2-72] The Iceberg-catalog check lived here and
    # moved to `validate`. It has to know every active task's TABLE_FORMAT
    # override, and doctor reads no CFG_ rows -- so from here it got the
    # question wrong in both directions: skipping the check for a task that
    # asked for Iceberg against a native default, and failing it for a catalog
    # nothing needed. `validate` already reads task parameters and already
    # talks to the warehouse, which is where a cross-database config check
    # belongs. One home, not two.
    return results


def _email_check(config: ConnectorConfig) -> list[CheckResult]:
    if config.email is None:
        return [
            CheckResult(
                "Email relay",
                True,
                "no [Email] section configured (only needed for EMAIL_ALERT tasks)",
            )
        ]
    profile = config.email.active
    results: list[CheckResult] = []
    if profile_needs_secret(profile):
        results.append(_secret_check(config, "Email", profile))
    results.extend(
        _auth_check("Email", profile.auth_mode, EMAIL_VERIFIED_AUTH_MODES, "the SMTP relay")
    )
    try:
        with smtplib.SMTP(profile.host, profile.port, timeout=SMTP_PROBE_TIMEOUT_SECONDS) as server:
            server.noop()
    except (smtplib.SMTPException, OSError) as exc:
        results.append(CheckResult("Email relay", False, f"{profile.host}:{profile.port}: {exc}"))
        return results
    # Deliberately no login attempt: a NOOP proves reachability without
    # burning an auth attempt against a relay that may rate-limit or lock out.
    results.append(CheckResult("Email relay", True, f"reachable at {profile.host}:{profile.port}"))
    return results


def run_checks(config: ConnectorConfig) -> list[CheckResult]:
    """Run every configuration check and return all results, failures included."""
    results: list[CheckResult] = [
        CheckResult("Execution mode", True, config.mode),
        CheckResult(
            "Secret source",
            True,
            f"{config.source.type}" + (f" ({config.source.path})" if config.source.path else ""),
        ),
    ]
    results.extend(_settings_check(config))
    engine_profile = config.postgres.active
    if profile_needs_secret(engine_profile):
        results.append(_secret_check(config, "Engine DB", engine_profile))
    engine_dialect = for_jdbc_url(engine_profile.jdbc_url)
    results.extend(
        _auth_check(
            "Engine DB",
            engine_profile.auth_mode,
            engine_dialect.verified_auth_modes,
            f"a {engine_dialect.name} Engine DB",
        )
    )
    results.append(_engine_db_check(config))
    results.extend(_engine_db_kind_check(config))
    results.extend(_warehouse_check(config))
    results.extend(_email_check(config))
    return results
