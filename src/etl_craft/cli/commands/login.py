"""Local password and device-flow sign-in, sign-out and account identity."""

import argparse
import time
from collections.abc import Callable

from etl_craft.cli.commands.common import Command, command_context, configure_output
from etl_craft.cli.output import Output
from etl_craft.cli.prompts import interactive, password
from etl_craft.cli.remote import ServerClient
from etl_craft.core import interrupts
from etl_craft.core.errors import ConfigurationError
from etl_craft.services import credentials
from etl_craft.services.operations import sessions


def _login(args: argparse.Namespace, out: Output) -> int:
    interactive()
    with command_context(args) as ctx:
        key = credentials.database_key(ctx.engine, ctx.config.engine.schema)
        if ctx.config.auth.oidc_issuer and input("Sign in with SSO? [y/N]: ").lower() == "y":
            session = sso_session(
                lambda: sessions.start_sso(ctx),
                lambda start: sessions.poll_sso(ctx, start.device_code, start.nonce),
                out,
            )
        else:
            session = sessions.sign_in(ctx, input("Account name: "), password())
        try:
            credentials.store_session(key, session.access_token)
        except BaseException:
            sessions.sign_out(ctx, session.access_token)
            raise
        out.line(f"Signed in as {session.metadata.name} ({session.metadata.role})")
    return 0


def sso_session(
    start: Callable[[], sessions.SsoStart],
    poll: Callable[[sessions.SsoStart], sessions.SsoPoll],
    out: Output,
) -> sessions.Session:
    """Show the issuer's code, then poll until the person approves or the code expires."""
    begun = start()
    out.line(f"Open {begun.verification_uri} and enter {begun.user_code}")
    deadline = time.monotonic() + begun.expires_in
    interval = begun.interval
    while time.monotonic() + interval < deadline:
        interrupts.sleep(interval)
        polled = poll(begun)
        if polled.session is not None:
            return polled.session
        interval += polled.interval
    raise ConfigurationError("OIDC device code expired; run `etl-craft login` again")


def _logout(args: argparse.Namespace, out: Output) -> int:
    with command_context(args) as ctx:
        key = credentials.database_key(ctx.engine, ctx.config.engine.schema)
        token = credentials.read_session(key)
        if token is not None:
            sessions.sign_out(ctx, token)
            credentials.store_session(key, None)
    out.line("Signed out")
    return 0


def _whoami(args: argparse.Namespace, out: Output) -> int:
    with command_context(args) as ctx:
        identity = sessions.whoami(ctx)
    out.result(
        identity, args.output_format, text=lambda: out.line(f"{identity.name} {identity.role}")
    )
    return 0


def _login_remote(args: argparse.Namespace, out: Output, server: ServerClient) -> int:
    interactive()
    methods = server.call("GET", "/api/v1/login/methods", sessions.SignInMethods, signed_in=False)
    if methods.sso and input("Sign in with SSO? [y/N]: ").lower() == "y":
        session = sso_session(
            lambda: server.call(
                "POST", "/api/v1/login/sso", sessions.SsoStart, body={}, signed_in=False
            ),
            lambda start: server.call(
                "POST",
                "/api/v1/login/sso/poll",
                sessions.SsoPoll,
                body={"device_code": start.device_code, "nonce": start.nonce},
                signed_in=False,
            ),
            out,
        )
    else:
        session = server.call(
            "POST",
            "/api/v1/login/password",
            sessions.Session,
            body={"name": input("Account name: "), "password": password()},
            signed_in=False,
        )
    try:
        server.store(session)
    except BaseException:
        if session.refresh_token is not None:
            server.send("POST", "/api/v1/login/revoke", {"token": session.refresh_token})
        raise
    out.line(f"Signed in to {server.url} as {session.metadata.name} ({session.metadata.role})")
    return 0


def _logout_remote(args: argparse.Namespace, out: Output, server: ServerClient) -> int:
    refresh = server.forget()
    if refresh is not None:
        server.send("POST", "/api/v1/login/revoke", {"token": refresh})
    out.line(f"Signed out of {server.url}")
    return 0


def _whoami_remote(args: argparse.Namespace, out: Output, server: ServerClient) -> int:
    identity = server.call("GET", "/api/v1/whoami", sessions.Identity)
    out.result(
        identity, args.output_format, text=lambda: out.line(f"{identity.name} {identity.role}")
    )
    return 0


COMMANDS = (
    Command(
        "login",
        "Sign in on this project's Engine DB, or on a server with --server.",
        _login,
        remote=_login_remote,
    ),
    Command(
        "logout",
        "Revoke and remove this person's stored session.",
        _logout,
        remote=_logout_remote,
    ),
    Command(
        "whoami",
        "Print the signed-in account and role.",
        _whoami,
        configure_output,
        remote=_whoami_remote,
    ),
)
