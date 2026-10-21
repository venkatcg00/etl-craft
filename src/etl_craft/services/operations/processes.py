"""Admission checks for the long-running controller and stateless API roles."""

from dataclasses import replace
from importlib.util import find_spec

from etl_craft import __version__
from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.core.errors import ConfigurationError, MigrationError
from etl_craft.dialects.engine import for_engine
from etl_craft.engine.migrations import pending_migrations
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.models import Health
from etl_craft.services.operations.requests import NoInput


def controller(ctx: OperationContext, request: NoInput) -> None:
    """Refuse to supervise with an Engine DB that does not match this package."""
    check_package_schema(ctx)


def ui(ctx: OperationContext, request: NoInput) -> None:
    """Admit a stateless API copy only on a matching PostgreSQL Engine DB."""
    if find_spec("fastapi") is None or find_spec("uvicorn") is None:
        raise ConfigurationError(
            "etl-craft ui dependencies are missing; expected the ui extra; "
            "install `etl-craft[ui]` on this host"
        )
    if not for_engine(ctx.engine).supports_api_replicas:
        raise ConfigurationError(
            f"{ctx.config.config_path}: Engine.Jdbc_url={ctx.config.engine.jdbc_url!r}; "
            "etl-craft ui requires PostgreSQL; use `etl-craft controller` with the ui extra "
            "for SQLite, or configure a PostgreSQL Engine DB"
        )
    check_package_schema(ctx)


def check_package_schema(ctx: OperationContext) -> None:
    """Require the existing migration ledger to match this package before using it."""
    try:
        pending = pending_migrations(ctx.engine, project_default=ctx.config.migrations_dir)
    except MigrationError as error:
        raise MigrationError(
            f"Engine DB does not match etl-craft package version {__version__}: {error}; "
            "install the matching package on every role and run `etl-craft migrate`"
        ) from error
    engine = [name for name in pending if name.startswith("engine/")]
    if engine:
        raise MigrationError(
            f"Engine DB is missing migrations {engine}; expected etl-craft package version "
            f"{__version__}; run `etl-craft migrate` before starting controller or ui"
        )


def probe(ctx: OperationContext) -> Health:
    """Read the same health action under the explicit system actor for an unsigned probe."""
    from etl_craft.services.operations.actions import invoke

    result = invoke(
        replace(ctx, actor=SYSTEM_ACTOR, role=ApiRole.VIEWER), ActionName.HEALTH, NoInput()
    )
    assert isinstance(result, Health)
    return result


def readiness(ctx: OperationContext) -> Health:
    """Check the Engine DB afresh, without reusing an outage-retrying connection pool."""
    from sqlalchemy.pool import NullPool

    from etl_craft.engine.connection import engine_db

    engine = engine_db(replace(ctx.config, sizing=None), poolclass=NullPool)
    try:
        check_package_schema(replace(ctx, engine=engine))
        return probe(ctx)
    finally:
        engine.dispose()
