"""What every command that reads craft-connector.yml does first."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, fields
from typing import TYPE_CHECKING, Literal, TypeVar

from sqlalchemy.engine import Engine

from etl_craft.cli.authentication import command_identity
from etl_craft.cli.client import ApiClient
from etl_craft.cli.output import Output
from etl_craft.config import ConnectorConfig, load_config, resolve_config_path
from etl_craft.config.sizing import start_role
from etl_craft.core.actor import current_actor
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.core.errors import UsageError
from etl_craft.engine.connection import check_reachable, engine_db
from etl_craft.execution.attempt_types import ChildOptions
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import ACTIONS, invoke
from etl_craft.services.operations.identity import authenticated_context

if TYPE_CHECKING:
    from _typeshed import DataclassInstance

logger = logging.getLogger(__name__)

R = TypeVar("R", bound="DataclassInstance")


@dataclass(frozen=True)
class Command:
    """One subcommand: its name, its one-line help, its options and what it does.

    ``configure`` adds the command's own options to its parser. ``run`` receives the parsed
    arguments, including the global ``config``, ``log_level`` and ``log_format``, and returns
    the exit code. It reports failures by raising an ``EtlCraftError``, which the command line
    turns into an ``error:`` line and that error's exit code.
    """

    name: str
    help: str
    run: Callable[[argparse.Namespace, Output], int]
    configure: Callable[[argparse.ArgumentParser], None] | None = None
    actions: tuple[ActionName, ...] = ()
    """The actions the command calls, whichever its options select."""


def load_command_config(args: argparse.Namespace) -> ConnectorConfig:
    """Find and load craft-connector.yml: ``--config``, ``$ETL_CRAFT_CONFIG``, or a search."""
    scoped = command_identity.get()
    if scoped is not None:
        return scoped.config
    path = resolve_config_path(args.config)
    logger.debug("reading %s", path)
    return load_config(path)


def connect_engine_db(config: ConnectorConfig) -> Engine:
    """Build the Engine DB engine and check it, and its schema, can be used."""
    engine = engine_db(config)
    try:
        check_reachable(engine, config.engine.schema)
    except BaseException:
        engine.dispose()
        raise
    return engine


def configure_run_selector(parser: argparse.ArgumentParser) -> None:
    """Add mutually exclusive identities for a pipeline's run."""
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--run-id", type=int, help="The exact pipeline run id.")
    selection.add_argument("--run-key", help="The exact run key within this pipeline.")


def configure_tags(parser: argparse.ArgumentParser) -> None:
    """Offer the shared repeatable AND filter."""
    parser.add_argument(
        "--tag",
        action="append",
        default=[],
        help="Carry every given tag (repeatable; exact match).",
    )


def configure_output(parser: argparse.ArgumentParser) -> None:
    """Offer one schema-versioned operation document instead of text output."""
    parser.add_argument(
        "--format",
        dest="output_format",
        choices=("text", "json"),
        default="text",
        help="Result format (default: text).",
    )


@contextmanager
def command_context(
    args: argparse.Namespace, *, role: Literal["run", "controller", "ui"] | None = None
) -> Iterator[OperationContext]:
    """Load command resources and dispose them after the service operation.

    ``role`` marks a process that runs tasks or serves: its allocation is measured and, with a
    ``Deployment.Profile``, its slots, threads and Engine DB pool are sized from it.
    """
    config = load_command_config(args)
    if role is not None:
        config = start_role(config, server=role in ("controller", "ui"))
    engine = connect_engine_db(config)
    try:
        context = OperationContext(
            engine,
            config,
            current_actor(),
            ChildOptions(log_level=args.log_level, log_format=args.log_format),
            role=(
                ApiRole(scoped.identity.role)
                if (scoped := command_identity.get()) and scoped.identity
                else None
            ),
            provisioning=scoped is not None and scoped.provisioning,
        )
        yield (
            authenticated_context(context, scoped.identity)
            if scoped is not None and scoped.identity is not None
            else context
        )
    finally:
        engine.dispose()


def request_from(args: argparse.Namespace, cls: type[R], **given: object) -> R:
    """Build an action's request from the options whose names are its fields, and ``given``."""
    values = {f.name: getattr(args, f.name) for f in fields(cls) if hasattr(args, f.name)}
    return cls(**{**values, **given})


def call(
    args: argparse.Namespace,
    name: ActionName,
    request: object,
    *,
    role: Literal["run", "controller", "ui"] | None = None,
) -> object:
    """Run one action: through the API with ``--api``, else on this project."""
    if args.api:
        return ApiClient(args.api).invoke(name, request)
    with command_context(args, role=role) as ctx:
        return invoke(ctx, name, request)


def on_host(args: argparse.Namespace, name: ActionName) -> None:
    """Refuse ``--api`` for an action that runs only on the controller's host."""
    if args.api:
        remedy = ACTIONS[name].remedy
        raise UsageError(f"`{name}` runs on the controller's host only: {remedy}")
