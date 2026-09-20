"""`etl-craft doctor` — check a configuration end to end before anything depends on it.

[ADDITION, 2026-09-20, E2-16] `configure` writes a profile whose secret is
looked up as `ETL_CRAFT_{SECTION}_{PROFILE}_SECRET` and never mentions that
name anywhere, so the flow was: run `configure`, answer every prompt, then
have the next command fail with `secret 'ETL_CRAFT_POSTGRES_DEV_SECRET' not
found`. There was also no way to verify a configuration at all short of
running a real pipeline.

This is deliberately a read-only report built entirely from code that already
exists — `load_config`, `resolve_secret`, `build_engine`, `build_data_engine`,
and the `[Email]` profile — rather than new machinery. It reports every check
rather than stopping at the first failure, for the same reason `validate`
does: one broken profile shouldn't hide the rest of the picture.
"""

from __future__ import annotations

import smtplib
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config import ConfigError, ConnectorConfig, resolve_secret
from etl_craft.db import build_engine
from etl_craft.warehouse import build_data_engine

SMTP_PROBE_TIMEOUT_SECONDS = 10.0


@dataclass(frozen=True)
class CheckResult:
    """One thing `doctor` looked at, and what it found."""

    name: str
    ok: bool
    detail: str

    @property
    def marker(self) -> str:
        """Return the single-character status marker this result renders with."""
        return "OK  " if self.ok else "FAIL"


def _secret_check(config: ConnectorConfig, label: str, profile: object) -> CheckResult:
    var_name = profile.secret_var  # type: ignore[attr-defined]
    try:
        resolve_secret(config, profile)  # type: ignore[arg-type]
    except ConfigError as exc:
        return CheckResult(f"{label} secret", False, f"{exc} (expected in {var_name})")
    return CheckResult(f"{label} secret", True, f"resolved from {var_name}")


def _engine_db_check(config: ConnectorConfig) -> CheckResult:
    try:
        engine = build_engine(config)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except (ConfigError, SQLAlchemyError) as exc:
        return CheckResult("Engine DB connection", False, str(exc))
    return CheckResult("Engine DB connection", True, f"connected as {config.postgres.active.user}")


def _warehouse_check(config: ConnectorConfig) -> list[CheckResult]:
    if config.warehouse is None:
        return [
            CheckResult(
                "Data DB",
                True,
                "no [Warehouse] section configured (only needed for SQL/BUSINESS_RULES tasks)",
            )
        ]
    results = [_secret_check(config, "Data DB", config.warehouse.active)]
    try:
        data_engine = build_data_engine(config)
        try:
            with data_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        finally:
            data_engine.dispose()
    except (ConfigError, SQLAlchemyError, NotImplementedError) as exc:
        results.append(CheckResult("Data DB connection", False, str(exc)))
        return results
    results.append(CheckResult("Data DB connection", True, "connected"))
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
    if profile.auth_mode == "password":
        results.append(_secret_check(config, "Email", profile))
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
    results = [
        CheckResult("Execution mode", True, config.mode),
        CheckResult(
            "Secret source",
            True,
            f"{config.source.type}" + (f" ({config.source.path})" if config.source.path else ""),
        ),
        _secret_check(config, "Engine DB", config.postgres.active),
        _engine_db_check(config),
    ]
    results.extend(_warehouse_check(config))
    results.extend(_email_check(config))
    return results
