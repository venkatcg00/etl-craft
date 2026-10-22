"""Administrator controls for registered users and service accounts."""

import argparse

from etl_craft.cli.commands.common import Command, call, configure_output, request_from
from etl_craft.cli.output import Output
from etl_craft.cli.prompts import password
from etl_craft.core.enums import AccountKind, ActionName, ApiRole
from etl_craft.services.operations import accounts
from etl_craft.services.operations.requests import NoInput


def _configure(parser: argparse.ArgumentParser) -> None:
    verbs = parser.add_subparsers(dest="account_verb", required=True)
    add = verbs.add_parser("add", help="Register a user or service account.")
    add.add_argument("--name", required=True)
    add.add_argument(
        "--kind", type=AccountKind, choices=tuple(AccountKind), default=AccountKind.USER
    )
    add.add_argument(
        "--role",
        type=ApiRole,
        choices=tuple(role for role in ApiRole if role != ApiRole.AGENT),
        required=True,
    )
    add.add_argument("--email")
    add.add_argument("--oidc-subject")
    configure_output(add)
    configure_output(
        verbs.add_parser("list", help="List account identities, never password hashes.")
    )
    for verb in ("set-role", "disable", "reset-password"):
        sub = verbs.add_parser(verb)
        sub.add_argument("--name", required=True)
        if verb == "set-role":
            sub.add_argument(
                "--role",
                type=ApiRole,
                choices=tuple(role for role in ApiRole if role != ApiRole.AGENT),
                required=True,
            )
        configure_output(sub)


def _run(args: argparse.Namespace, out: Output) -> int:
    if args.account_verb == "add":
        user = args.kind == AccountKind.USER and not args.oidc_subject
        request: object = request_from(
            args, accounts.AccountAdd, password=password(confirm=True) if user else None
        )
        done = call(args, ActionName.ACCOUNT_ADD, request)
    elif args.account_verb == "set-role":
        done = call(args, ActionName.ACCOUNT_SET_ROLE, request_from(args, accounts.AccountRole))
    elif args.account_verb == "disable":
        done = call(args, ActionName.ACCOUNT_DISABLE, request_from(args, accounts.AccountName))
    elif args.account_verb == "reset-password":
        request = request_from(args, accounts.AccountPassword, password=password(confirm=True))
        done = call(args, ActionName.ACCOUNT_RESET_PASSWORD, request)
    else:
        done = call(args, ActionName.ACCOUNT_LIST, NoInput())
    assert isinstance(done, accounts.Accounts)
    out.result(
        done,
        args.output_format,
        text=lambda: out.rows(
            (a.name, a.kind, a.role, a.active_flag, a.email) for a in done.accounts
        ),
    )
    return 0


COMMAND = Command(
    "account",
    "Manage registered accounts (admin only).",
    _run,
    _configure,
    actions=(
        ActionName.ACCOUNT_ADD,
        ActionName.ACCOUNT_SET_ROLE,
        ActionName.ACCOUNT_DISABLE,
        ActionName.ACCOUNT_RESET_PASSWORD,
        ActionName.ACCOUNT_LIST,
    ),
)
