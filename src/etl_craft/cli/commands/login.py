"""Local password and device-flow sign-in, sign-out and account identity."""

import argparse
from dataclasses import dataclass, replace
from typing import ClassVar

from etl_craft.cli.authentication import command_identity
from etl_craft.cli.commands.common import Command, command_context, configure_output
from etl_craft.cli.output import Output
from etl_craft.cli.prompts import interactive, password
from etl_craft.core.actor import acting_as
from etl_craft.core.errors import EtlCraftError, UsageError
from etl_craft.services import credentials, oidc
from etl_craft.services.operations import sessions, tokens
from etl_craft.services.operations.context import operation


@dataclass(frozen=True)
class Identity:
    """The signed-in account and its current deployment role."""

    SCHEMA: ClassVar[str] = "etl-craft/identity/1"
    name: str
    role: str


def _login(args: argparse.Namespace, out: Output) -> int:
    interactive()
    with command_context(args) as ctx:
        key = credentials.database_key(ctx.engine, ctx.config.engine.schema)
        if ctx.config.auth.oidc_issuer and input("Sign in with SSO? [y/N]: ").lower() == "y":
            try:
                subject = oidc.device_subject(ctx.config.auth, out.line)
            except EtlCraftError:
                with operation(ctx, "login", {"method": "SSO"}):
                    raise
            created = sessions.sign_in_subject(ctx, subject)
        else:
            created = sessions.sign_in(ctx, input("Account name: "), password())
        try:
            credentials.store_session(key, created.token)
        except BaseException:
            with acting_as(created.metadata.actor):
                sessions.sign_out(replace(ctx, actor=created.metadata.actor), created.token)
            raise
        out.line(f"Signed in as {created.metadata.name} ({created.metadata.role})")
    return 0


def _logout(args: argparse.Namespace, out: Output) -> int:
    with command_context(args) as ctx:
        key = credentials.database_key(ctx.engine, ctx.config.engine.schema)
        token = credentials.read_session(key)
        if token is not None:
            identity = tokens.authenticate(ctx, token)
            with acting_as(identity.actor if identity else ctx.actor):
                sessions.sign_out(
                    replace(ctx, actor=identity.actor if identity else ctx.actor), token
                )
            credentials.store_session(key, None)
    out.line("Signed out")
    return 0


def _whoami(args: argparse.Namespace, out: Output) -> int:
    scoped = command_identity.get()
    if scoped is None or scoped.identity is None:
        raise UsageError("no signed-in account; run `etl-craft login`")
    identity = Identity(scoped.identity.name, scoped.identity.role)
    out.result(
        identity, args.output_format, text=lambda: out.line(f"{identity.name} {identity.role}")
    )
    return 0


COMMANDS = (
    Command("login", "Sign in on this server with a password or SSO.", _login),
    Command("logout", "Revoke and remove this person's stored session.", _logout),
    Command("whoami", "Print the signed-in account and role.", _whoami, configure_output),
)
