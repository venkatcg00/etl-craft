"""Resolve a local command's credential once, before dispatching its handler."""

import argparse
import os
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.core.enums import ApiRole
from etl_craft.core.errors import ConfigurationError
from etl_craft.dialects.engine import for_engine
from etl_craft.engine.connection import check_reachable, engine_db
from etl_craft.services.credentials import database_key, read_session
from etl_craft.services.operations.accounts import LOCAL_PROVISIONING, local_provisioning
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.permissions import authorize
from etl_craft.services.operations.tokens import TokenView, authenticate


@dataclass(frozen=True)
class CommandIdentity:
    """The exact configuration and verified identity used throughout one dispatch."""

    config: ConnectorConfig
    identity: TokenView | None


command_identity: ContextVar[CommandIdentity | None] = ContextVar("command_identity", default=None)
_SIGN_IN = frozenset({"login", "logout"})


@contextmanager
def access_context(args: argparse.Namespace) -> Iterator[None]:
    """Admit sign-in and local provisioning explicitly; all other commands require proof."""
    from etl_craft.cli.commands.common import load_command_config

    config = load_command_config(args)
    engine = engine_db(config)
    try:
        check_reachable(engine, config.engine.schema)
        if args.command not in LOCAL_PROVISIONING and not for_engine(engine).existing_tables(
            engine, ("cfg_accounts",)
        ):
            raise ConfigurationError(
                "Engine DB has no CFG_ACCOUNTS; expected the account schema; "
                "run `etl-craft setup` locally before signing in"
            )
        with acting_as(SYSTEM_ACTOR):
            identity = None
            if args.command not in _SIGN_IN and not local_provisioning(engine, args.command):
                token = os.environ.get("ETL_CRAFT_TOKEN")
                if token is None:
                    token = read_session(database_key(engine, config.engine.schema))
                ctx = OperationContext(engine, config, SYSTEM_ACTOR)
                identity = authenticate(ctx, token) if token is not None else None
                if identity is None:
                    raise ConfigurationError(
                        f"no signed-in account for {database_key(engine, config.engine.schema)}; "
                        "run `etl-craft login`, or set ETL_CRAFT_TOKEN for a service account"
                    )
                authorize(
                    OperationContext(engine, config, identity.actor, role=ApiRole(identity.role)),
                    args.command,
                    vars(args),
                )
            scoped = command_identity.set(CommandIdentity(config, identity))
            try:
                with acting_as(identity.actor if identity else SYSTEM_ACTOR):
                    yield
            finally:
                command_identity.reset(scoped)
    finally:
        engine.dispose()
