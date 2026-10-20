"""Local administration of bearer tokens."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import Command, call, configure_output, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.requests import NoInput
from etl_craft.services.operations.tokens import CreatedToken, TokenCreate, TokenRevoke, TokensView


def _configure(parser: argparse.ArgumentParser) -> None:
    verbs = parser.add_subparsers(dest="token_verb", required=True)
    create = verbs.add_parser("create", help="Create a token and print its value once.")
    create.add_argument("--name", required=True)
    create.add_argument("--role", type=ApiRole, choices=tuple(ApiRole), required=True)
    create.add_argument("--expires", help="Lifetime in days, such as 90d.")
    configure_output(create)
    revoke = verbs.add_parser("revoke", help="Revoke a token immediately.")
    revoke.add_argument("--token-id", type=int, required=True)
    configure_output(revoke)
    configure_output(verbs.add_parser("list", help="List safe token metadata."))


def _run(args: argparse.Namespace, out: Output) -> int:
    if args.token_verb == "create":
        done = call(args, ActionName.TOKEN_CREATE, request_from(args, TokenCreate))
        assert isinstance(done, CreatedToken)
        if args.output_format == "text":
            out.line(done.token)
        else:
            out.document(done)
        return ExitCode.SUCCESS
    listed = (
        call(args, ActionName.TOKEN_REVOKE, request_from(args, TokenRevoke))
        if args.token_verb == "revoke"
        else call(args, ActionName.TOKEN_LIST, NoInput())
    )
    assert isinstance(listed, TokensView)
    if args.output_format == "json":
        out.document(listed)
    else:
        for token in listed.tokens:
            out.line(
                f"{token.token_id} {token.name} {token.role} "
                f"expires={token.expires_at} revoked={token.revoked_at}"
            )
    return ExitCode.SUCCESS


COMMAND = Command(
    "token",
    "Create, list or revoke API tokens (admin only).",
    run=_run,
    configure=_configure,
    actions=(ActionName.TOKEN_CREATE, ActionName.TOKEN_REVOKE, ActionName.TOKEN_LIST),
)
