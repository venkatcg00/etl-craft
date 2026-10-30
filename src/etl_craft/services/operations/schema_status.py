"""Refuse current-schema authentication before an installation is upgraded."""

from sqlalchemy.engine import Connection

from etl_craft import __version__
from etl_craft.core.errors import MigrationError
from etl_craft.dialects.engine import for_engine
from etl_craft.engine.migrations import pending_migrations
from etl_craft.engine.queries import statement
from etl_craft.services.credentials import database_key
from etl_craft.services.operations.context import OperationContext


def require_current_schema(ctx: OperationContext, connection: Connection | None = None) -> None:
    """Name applied and required Engine migrations before reading current account columns."""
    try:
        pending = pending_migrations(
            ctx.engine, project_default=ctx.config.migrations_dir, connection=connection
        )
    except MigrationError as error:
        raise MigrationError(
            f"Engine DB does not match etl-craft package version {__version__}: {error}; "
            "install the matching package and run `etl-craft migrate`"
        ) from error
    if not any(name.startswith("engine/") for name in pending):
        return
    with ctx.engine.connect() as conn:
        applied = conn.execute(statement(conn, "about_schema")).scalar_one_or_none()
    required = max(path.stem for path in for_engine(ctx.engine).migrations_dir().glob("*.sql"))
    key = database_key(ctx.engine, ctx.config.engine.schema)
    raise MigrationError(
        f"Engine DB {key} is at migration {applied or 'none'}, this etl-craft needs {required}; "
        "run `etl-craft migrate` as an admin"
    )
