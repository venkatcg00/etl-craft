"""Administrator controls for registered users and service accounts."""

import argparse

from etl_craft.cli.commands.common import Command, command_context, configure_output
from etl_craft.cli.output import Output
from etl_craft.cli.prompts import password
from etl_craft.core.enums import AccountKind, ApiRole, YesNo
from etl_craft.services.operations import accounts


def _configure(parser: argparse.ArgumentParser) -> None:
    verbs = parser.add_subparsers(dest="account_verb", required=True)
    add = verbs.add_parser("add", help="register a user or service account")
    add.add_argument("--name", required=True)
    add.add_argument("--kind", choices=tuple(AccountKind), default=AccountKind.USER)
    add.add_argument("--role", choices=tuple(ApiRole), required=True)
    add.add_argument("--email")
    add.add_argument("--oidc-subject")
    configure_output(add)
    configure_output(
        verbs.add_parser("list", help="list account identities, never password hashes")
    )
    for verb in ("set-role", "disable", "reset-password"):
        sub = verbs.add_parser(verb)
        sub.add_argument("--name", required=True)
        if verb == "set-role":
            sub.add_argument("--role", choices=tuple(ApiRole), required=True)
        configure_output(sub)


def _run(args: argparse.Namespace, out: Output) -> int:
    with command_context(args) as ctx:
        if args.account_verb == "add":
            account = accounts.Account(
                0,
                args.name,
                AccountKind(args.kind),
                ApiRole(args.role),
                args.email,
                args.oidc_subject,
                YesNo.YES,
            )
            secret = (
                password(confirm=True)
                if account.kind == AccountKind.USER and not account.oidc_subject
                else None
            )
            accounts.add_account(ctx, account, secret)
        elif args.account_verb == "set-role":
            accounts.set_role(ctx, args.name, ApiRole(args.role))
        elif args.account_verb == "disable":
            accounts.disable(ctx, args.name)
        elif args.account_verb == "reset-password":
            accounts.reset_password(ctx, args.name, password(confirm=True))
        done = accounts.list_accounts(ctx)
        out.result(
            done,
            args.output_format,
            text=lambda: out.rows(
                (a.name, a.kind, a.role, a.active_flag, a.email) for a in done.accounts
            ),
        )
    return 0


COMMAND = Command("account", "Manage registered accounts (admin only).", _run, _configure)
