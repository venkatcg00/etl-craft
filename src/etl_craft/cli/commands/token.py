"""Local administration of bearer tokens."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import Command, command_context, configure_output
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.tokens import ROLES, create_token, list_tokens, revoke_token


def _configure(parser: argparse.ArgumentParser) -> None:
    verbs = parser.add_subparsers(dest="token_verb", required=True)
    create = verbs.add_parser("create", help="create a token and print its value once")
    create.add_argument("--name", required=True)
    create.add_argument("--role", choices=ROLES, required=True)
    create.add_argument("--expires", help="lifetime in days, such as 90d")
    configure_output(create)
    revoke = verbs.add_parser("revoke", help="revoke a token immediately")
    revoke.add_argument("--token-id", type=int, required=True)
    configure_output(revoke)
    configure_output(verbs.add_parser("list", help="list safe token metadata"))


def _run(args: argparse.Namespace, out: Output) -> int:
    with command_context(args) as ctx:
        if args.token_verb == "create":
            done = create_token(ctx, args.name, args.role, args.expires)
            if args.output_format == "text":
                out.line(done.token)
            else:
                out.document(done)
        else:
            listed = (
                revoke_token(ctx, args.token_id)
                if args.token_verb == "revoke"
                else list_tokens(ctx)
            )
            if args.output_format == "json":
                out.document(listed)
            else:
                for token in listed.tokens:
                    out.line(
                        f"{token.token_id} {token.name} {token.role} "
                        f"expires={token.expires_at} revoked={token.revoked_at}"
                    )
    return ExitCode.SUCCESS


COMMAND = Command("token", "create, list or revoke API tokens", run=_run, configure=_configure)
