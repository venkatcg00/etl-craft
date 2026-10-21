"""Password and device-flow sign-in, sign-out and account identity, locally or on a controller."""

import argparse
import time
from collections.abc import Callable

from etl_craft.cli.client import ApiClient
from etl_craft.cli.commands.common import Command, call, command_context, configure_output
from etl_craft.cli.output import Output
from etl_craft.cli.prompts import interactive, password
from etl_craft.core import interrupts
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ConfigurationError
from etl_craft.services import credentials
from etl_craft.services.operations import sessions
from etl_craft.services.operations.requests import NoInput


def _login(args: argparse.Namespace, out: Output) -> int:
    interactive()
    methods = call(args, ActionName.LOGIN_METHODS, NoInput())
    assert isinstance(methods, sessions.SignInMethods)
    if methods.sso and input("Sign in with SSO? [y/N]: ").lower() == "y":
        session = sso_session(
            lambda: call(args, ActionName.LOGIN_SSO, NoInput()),
            lambda start: call(
                args,
                ActionName.LOGIN_SSO_POLL,
                sessions.SsoPollInput(device_code=start.device_code, nonce=start.nonce),
            ),
            out,
        )
    else:
        signed = call(
            args,
            ActionName.LOGIN,
            sessions.SignIn(name=input("Account name: "), password=password()),
        )
        assert isinstance(signed, sessions.Session)
        session = signed
    try:
        where = _store(args, session)
    except BaseException:
        token = session.refresh_token or session.access_token
        call(args, ActionName.LOGOUT, sessions.TokenInput(token=token))
        raise
    out.line(f"Signed in{where} as {session.metadata.name} ({session.metadata.role})")
    return 0


def _store(args: argparse.Namespace, session: sessions.Session) -> str:
    """Keep the session where later commands find it; return where, for the message."""
    if args.api:
        client = ApiClient(args.api)
        client.store(session)
        return f" to {client.url}"
    with command_context(args) as ctx:
        key = credentials.database_key(ctx.engine, ctx.config.engine.schema)
    credentials.store_session(key, session.access_token)
    return ""


def sso_session(
    start: Callable[[], object],
    poll: Callable[[sessions.SsoStart], object],
    out: Output,
) -> sessions.Session:
    """Show the issuer's code, then poll until the person approves or the code expires."""
    begun = start()
    assert isinstance(begun, sessions.SsoStart)
    out.line(f"Open {begun.verification_uri} and enter {begun.user_code}")
    deadline = time.monotonic() + begun.expires_in
    interval = begun.interval
    while time.monotonic() + interval < deadline:
        interrupts.sleep(interval)
        polled = poll(begun)
        assert isinstance(polled, sessions.SsoPoll)
        if polled.session is not None:
            return polled.session
        interval += polled.interval
    raise ConfigurationError("OIDC device code expired; run `etl-craft login` again")


def _logout(args: argparse.Namespace, out: Output) -> int:
    if args.api:
        client = ApiClient(args.api)
        token = client.forget()
        where = f" of {client.url}"
    else:
        with command_context(args) as ctx:
            key = credentials.database_key(ctx.engine, ctx.config.engine.schema)
        token = credentials.read_session(key)
        credentials.store_session(key, None)
        where = ""
    if token is not None:
        call(args, ActionName.LOGOUT, sessions.TokenInput(token=token))
    out.line(f"Signed out{where}")
    return 0


def _whoami(args: argparse.Namespace, out: Output) -> int:
    identity = call(args, ActionName.WHOAMI, NoInput())
    assert isinstance(identity, sessions.Identity)
    out.result(
        identity, args.output_format, text=lambda: out.line(f"{identity.name} {identity.role}")
    )
    return 0


COMMANDS = (
    Command(
        "login",
        "Sign in on this project's Engine DB, or through the API with --api.",
        _login,
        actions=(
            ActionName.LOGIN_METHODS,
            ActionName.LOGIN,
            ActionName.LOGIN_SSO,
            ActionName.LOGIN_SSO_POLL,
            ActionName.LOGOUT,
        ),
    ),
    Command(
        "logout",
        "Revoke and remove this person's stored session.",
        _logout,
        actions=(ActionName.LOGOUT,),
    ),
    Command(
        "whoami",
        "Print the signed-in account and role.",
        _whoami,
        configure_output,
        actions=(ActionName.WHOAMI,),
    ),
)
