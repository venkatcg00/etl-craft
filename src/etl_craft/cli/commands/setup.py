"""``etl-craft setup``: check the configuration, then create or migrate the Engine DB."""

from __future__ import annotations

import argparse
import sys

from etl_craft.cli.commands.common import Command, call, command_context
from etl_craft.cli.commands.doctor import report
from etl_craft.cli.output import Output
from etl_craft.cli.prompts import interactive, password
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.core.errors import ExitCode, UsageError
from etl_craft.services.operations import accounts
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.maintenance import Grants, SetupResult
from etl_craft.services.operations.requests import NoInput


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--admin-name", help="Name for unattended first admin creation.")
    parser.add_argument(
        "--password-stdin", action="store_true", help="Read one password line from a pipe."
    )
    parser.add_argument("--oidc-subject", help="Provider subject for the first or recovery admin.")
    parser.add_argument(
        "--print-grants",
        action="store_true",
        help="Print PostgreSQL role grants without executing them.",
    )


def _check_bootstrap_flags(args: argparse.Namespace) -> None:
    if args.password_stdin:
        if sys.stdin.isatty() or not args.admin_name or args.oidc_subject:
            raise UsageError(
                "setup --password-stdin requires piped stdin, --admin-name N "
                "and no --oidc-subject; "
                "pipe a secret from your secret manager instead of typing it visibly"
            )
    elif args.admin_name:
        raise UsageError("setup --admin-name requires --password-stdin; pipe the admin password")


def _run(args: argparse.Namespace, out: Output) -> int:
    _check_bootstrap_flags(args)
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
        out.line(
            "setup: recommended tables.csv days: REJECT_RETENTION_DAYS 30; "
            "SNAPSHOT_RETENTION_DAYS 7 and ORPHAN_RETENTION_DAYS 7 on supported Iceberg "
            "maintenance dialects; empty cells retain forever. Review purge --dry-run"
        )
        out.line("setup: created the Engine DB tables")
    for version in result.applied_migrations:
        out.line(f"setup: applied {version}")
    if not result.created_tables and not result.applied_migrations:
        out.line("setup: the Engine DB is already up to date")
    _bootstrap(args, out)
    return ExitCode.SUCCESS


def _bootstrap(args: argparse.Namespace, out: Output) -> None:
    with command_context(args) as ctx:
        if accounts.has_admin(ctx.engine):
            if args.password_stdin:
                raise UsageError(
                    "setup: an active admin already exists; expected no active admin for "
                    "--password-stdin; use etl-craft account add or account reset-password"
                )
        else:
            if args.password_stdin:
                secret = sys.stdin.readline(1026).removesuffix("\n").removesuffix("\r")
                name = args.admin_name
            else:
                if not sys.stdin.isatty():
                    raise UsageError(
                        "setup: no active admin and stdin is not a terminal; expected first admin "
                        "credentials; use --admin-name N --password-stdin with a piped secret"
                    )
                interactive()
                name = input("First or recovery admin account name: ")
                secret = None if args.oidc_subject else password(confirm=True)
            _create_admin(args, ctx, name, secret, out)


def _create_admin(
    args: argparse.Namespace,
    ctx: OperationContext,
    name: str,
    secret: str | None,
    out: Output,
) -> None:
    for attempt in range(3):
        try:
            invoke(
                ctx,
                ActionName.ACCOUNT_ADD,
                accounts.AccountAdd(
                    name=name,
                    role=ApiRole.ADMIN,
                    oidc_subject=args.oidc_subject,
                    password=secret,
                    recovery=True,
                ),
            )
            out.line(f"setup: created admin {name}; run `etl-craft login`")
            return
        except UsageError:
            if (
                args.password_stdin
                or args.oidc_subject
                or secret is None
                or 12 <= len(secret) <= 1024
                or attempt == 2
            ):
                raise
            out.line("Password length must be 12 to 1024 characters; try again.")
            secret = password(confirm=True)


COMMAND = Command(
    name="setup",
    help="Check the configuration, then create or bring up to date the Engine DB tables.",
    configure=_configure,
    run=_run,
    actions=(ActionName.SETUP, ActionName.SETUP_GRANTS, ActionName.ACCOUNT_ADD),
)
