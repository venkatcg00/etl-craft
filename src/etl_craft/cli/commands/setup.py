"""``etl-craft setup``: check the configuration, then create or migrate the Engine DB."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import Command, command_context, load_command_config
from etl_craft.cli.commands.doctor import report
from etl_craft.cli.output import Output
from etl_craft.cli.prompts import interactive, password
from etl_craft.core.enums import AccountKind, ApiRole, YesNo
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations import accounts
from etl_craft.services.operations.maintenance import grants, setup_engine


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--oidc-subject", help="provider subject for the first or recovery admin")
    parser.add_argument(
        "--print-grants",
        action="store_true",
        help="print PostgreSQL role grants without executing them",
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    config = load_command_config(args)
    if args.print_grants:
        out.line(grants(config))
        return ExitCode.SUCCESS
    result = setup_engine(config)
    if report(out, result.checks):
        out.line("setup: nothing changed; fix the failed check(s) and run setup again")
        return ExitCode.FAILURE
    if result.created_tables:
        out.line("setup: created the Engine DB tables")
    for version in result.applied_migrations:
        out.line(f"setup: applied {version}")
    if not result.created_tables and not result.applied_migrations:
        out.line("setup: the Engine DB is already up to date")
    with command_context(args) as ctx:
        if not accounts.has_admin(ctx.engine):
            interactive()
            name = input("First or recovery admin account name: ")
            secret = None if args.oidc_subject else password(confirm=True)
            admin = accounts.Account(
                0, name, AccountKind.USER, ApiRole.ADMIN, None, args.oidc_subject, YesNo.YES
            )
            accounts.add_account(ctx, admin, secret, recovery=True)
            out.line(f"setup: created admin {name}; run `etl-craft login`")
    return ExitCode.SUCCESS


COMMAND = Command(
    name="setup",
    help="Check the configuration, then create or bring up to date the Engine DB tables.",
    configure=_configure,
    run=_run,
)
