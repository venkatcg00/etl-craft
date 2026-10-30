"""Public version information without host identities or connection details."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from importlib.metadata import metadata
from typing import ClassVar

from etl_craft import __version__
from etl_craft.core.errors import MetadataError
from etl_craft.engine.connection import read_snapshot
from etl_craft.engine.queries import statement
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.requests import NoInput


@dataclass(frozen=True)
class ControllerVersion:
    """A recent heartbeat identity, without its host or PID."""

    controller_id: int
    version: str


@dataclass(frozen=True)
class About:
    """Installed software and applied schema versions."""

    SCHEMA: ClassVar[str] = "etl-craft/about/1"
    package_version: str
    schema_version: str | None
    server_version: str
    controllers: tuple[ControllerVersion, ...]
    license: str
    docs_url: str


def read_about(ctx: OperationContext, request: NoInput) -> About:
    """Read applied migrations and live controller heartbeat versions in one snapshot."""
    with read_snapshot(ctx.engine) as conn:
        schema = conn.execute(statement(conn, "about_schema")).scalar_one_or_none()
        controllers = tuple(
            ControllerVersion(**row._mapping)
            for row in conn.execute(
                statement(conn, "about_controllers"),
                {"since": datetime.now(UTC) - timedelta(seconds=60)},
            )
        )
    package = metadata("etl-craft")
    license_name = package["License-Expression"] or package["License"]
    if not license_name:
        raise MetadataError(
            "package license is missing; expected installed license metadata; reinstall etl-craft"
        )
    release_line = ".".join(__version__.split(".")[:2])
    return About(
        __version__,
        schema,
        __version__,
        controllers,
        license_name,
        f"https://venkatcg00.github.io/etl-craft/{release_line}/",
    )
