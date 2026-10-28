"""Admission checks for the long-running controller and stateless API roles."""

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from importlib.util import find_spec

from sqlalchemy.engine import Connection

from etl_craft import __version__
from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.core.enums import ActionName, ApiRole, ConnectionUse, ProcessRole
from etl_craft.core.errors import ConfigurationError, MigrationError
from etl_craft.dialects.engine import for_engine
from etl_craft.dialects.engine.guards import require_guards
from etl_craft.engine.migrations import pending_migrations
from etl_craft.engine.notifications import check_notifications
from etl_craft.engine.process_reservations import (
    Allocation,
    connection_slot,
    reserve_process,
    task_capacity,
)
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.models import Health
from etl_craft.services.operations.requests import NoInput


def controller(ctx: OperationContext, request: NoInput) -> None:
    """Refuse to supervise with an Engine DB that does not match this package."""
    check_package_schema(ctx)
    check_notifications(ctx.config, ctx.engine)


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
    check_notifications(ctx.config, ctx.engine)


def check_package_schema(ctx: OperationContext, connection: Connection | None = None) -> None:
    """Require the existing migration ledger to match this package before using it."""
    try:
        pending = pending_migrations(
            ctx.engine, project_default=ctx.config.migrations_dir, connection=connection
        )
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

    if connection is not None:
        require_guards(connection)
    else:
        with ctx.engine.connect() as conn:
            require_guards(conn)


def probe(ctx: OperationContext) -> Health:
    """Read the same health action under the explicit system actor for an unsigned probe."""
    from etl_craft.services.operations.actions import invoke

    result = invoke(
        replace(ctx, actor=SYSTEM_ACTOR, role=ApiRole.VIEWER), ActionName.HEALTH, NoInput()
    )
    assert isinstance(result, Health)
    return result


def readiness(ctx: OperationContext) -> Health:
    """Probe promptly on one dedicated session without creating another Engine."""
    with connection_slot(ctx.engine, ConnectionUse.HEALTH) as available:
        if not available:
            raise ConfigurationError(
                "readiness connection is busy; expected a free probe session; retry another UI copy"
            )
        with for_engine(ctx.engine).session_connection(ctx.engine, ctx.config) as conn:
            check_package_schema(ctx, connection=conn)
        return probe(ctx)


@contextmanager
def connection_process(ctx: OperationContext, role: ProcessRole) -> Iterator[OperationContext]:
    """Reserve process capacity until its requests and local children have stopped."""
    if not for_engine(ctx.engine).supports_api_replicas:
        yield ctx
        return
    if role == ProcessRole.RUN and (capacity := task_capacity(ctx.engine)) is not None:
        yield replace(ctx, child=replace(ctx.child, cancel=ctx.child.cancel or capacity[1]))
        return
    from etl_craft.engine.connection_budget import footprint

    budget = footprint(ctx.config, role)
    cancel = ctx.child.cancel or threading.Event()
    updated = replace(ctx, shutdown=ctx.shutdown or cancel, child=replace(ctx.child, cancel=cancel))
    with (
        acting_as(ctx.actor),
        reserve_process(
            ctx.engine,
            ctx.config,
            Allocation(role, budget, cancel, 60 + ctx.config.limits.engine_db_outage_seconds),
        ),
    ):
        yield updated
