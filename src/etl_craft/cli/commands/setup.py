"""``etl-craft setup``: check the configuration, then create or migrate the Engine DB."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import Command, call, command_context
from etl_craft.cli.commands.doctor import report
from etl_craft.cli.output import Output
from etl_craft.cli.prompts import interactive, password
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations import accounts
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.maintenance import Grants, SetupResult
from etl_craft.services.operations.requests import NoInput


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--oidc-subject", help="Provider subject for the first or recovery admin.")
    parser.add_argument(
        "--print-grants",
        action="store_true",
        help="Print PostgreSQL role grants without executing them.",
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    if args.print_grants:
        grants = call(args, ActionName.SETUP_GRANTS, NoInput())
        assert isinstance(grants, Grants)
        out.line(grants.sql)
        return ExitCode.SUCCESS
    result = call(args, ActionName.SETUP, NoInput())
    assert isinstance(result, SetupResult)
    if report(out, result.checks):
        out.line("setup: nothing changed; fix the failed check(s) and run setup again")
        return ExitCode.FAILURE
    if result.created_tables:
        out.line(
            "setup: recommended Retention days: Run_history_days 90, Attempt_log_days 30, "
            "Consumption_days 90, Interventions_days 365, Config_history_days 365; "
            "add the section to team-owned craft-connector.yml, then review purge --dry-run"
        )
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
            admin = accounts.AccountAdd(
                name=name,
                role=ApiRole.ADMIN,
                oidc_subject=args.oidc_subject,
                password=secret,
                recovery=True,
            )
            invoke(ctx, ActionName.ACCOUNT_ADD, admin)
            out.line(f"setup: created admin {name}; run `etl-craft login`")
    return ExitCode.SUCCESS


COMMAND = Command(
    name="setup",
    help="Check the configuration, then create or bring up to date the Engine DB tables.",
    configure=_configure,
    run=_run,
    actions=(ActionName.SETUP, ActionName.SETUP_GRANTS, ActionName.ACCOUNT_ADD),
)
