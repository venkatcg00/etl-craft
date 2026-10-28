"""Thin project and pool administration interfaces."""

import argparse

from etl_craft.cli.commands.common import Command, call, configure_output, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName, EnvironmentType, Governance
from etl_craft.core.errors import UsageError
from etl_craft.services.operations import admin_metadata as operations

PROJECT_VERBS = {
    "add": (ActionName.PROJECT_ADD, operations.ProjectFields),
    "set": (ActionName.PROJECT_SET, operations.ProjectFields),
    "list": (ActionName.PROJECT_LIST, operations.ProjectIdentity),
    "disable": (ActionName.PROJECT_DISABLE, operations.ProjectIdentity),
}
POOL_VERBS = {
    "add": (ActionName.POOL_ADD, operations.PoolFields),
    "set": (ActionName.POOL_SET, operations.PoolFields),
    "list": (ActionName.POOL_LIST, operations.PoolIdentity),
    "disable": (ActionName.POOL_DISABLE, operations.PoolIdentity),
}


def configure_project(parser: argparse.ArgumentParser) -> None:
    """Use global --project as identity; allow fields only on add and set."""
    verbs = parser.add_subparsers(dest="admin_verb", required=True)
    for verb in PROJECT_VERBS:
        sub = verbs.add_parser(verb)
        sub.add_argument("--project", default=argparse.SUPPRESS)
        sub.add_argument("--reason", required=True)
        if verb in ("add", "set"):
            sub.add_argument("--name")
            sub.add_argument("--timezone")
            sub.add_argument("--default-pool")
            sub.add_argument("--max-running-tasks", type=int)
            sub.add_argument(
                "--environment-type", type=EnvironmentType, choices=tuple(EnvironmentType)
            )
            sub.add_argument("--environment-name")
            sub.add_argument("--governance", type=Governance, choices=tuple(Governance))
        configure_output(sub)


def configure_pool(parser: argparse.ArgumentParser) -> None:
    """Pool identity and description follow the existing group command pattern."""
    verbs = parser.add_subparsers(dest="admin_verb", required=True)
    for verb in POOL_VERBS:
        sub = verbs.add_parser(verb)
        sub.add_argument("--project", default=argparse.SUPPRESS)
        sub.add_argument("--reason", required=True)
        sub.add_argument("--name", required=verb != "list")
        if verb in ("add", "set"):
            sub.add_argument("--description")
        configure_output(sub)


def run_project(args: argparse.Namespace, out: Output) -> int:
    """Never use the configuration's fallback as a mutation identity."""
    if args.admin_verb != "list" and not args.project:
        raise UsageError(
            "--project=None: expected an explicit project identity; supply "
            "--project CODE on the command line"
        )
    action, request = PROJECT_VERBS[args.admin_verb]
    done = call(args, action, request_from(args, request))
    assert isinstance(done, operations.Projects)
    out.result(
        done,
        args.output_format,
        text=lambda: out.rows(
            (
                p.project,
                p.name,
                p.timezone,
                p.default_pool,
                p.max_running_tasks,
                p.active_flag,
                p.environment_type,
                p.environment_name,
                p.governance,
                "shared warehouse" if p.shared_warehouse else "",
            )
            for p in done.projects
        ),
    )
    if args.output_format != "json":
        for note in done.warehouse_notes:
            out.line(note)
    return 0


def run_pool(args: argparse.Namespace, out: Output) -> int:
    """Delegate all validation and attribution to the operation."""
    action, request = POOL_VERBS[args.admin_verb]
    done = call(args, action, request_from(args, request))
    assert isinstance(done, operations.Pools)
    out.result(
        done,
        args.output_format,
        text=lambda: out.rows((p.name, p.description, p.active_flag) for p in done.pools),
    )
    return 0


PROJECT = Command(
    "project",
    "Manage projects (admin only).",
    run_project,
    configure_project,
    actions=tuple(a for a, _ in PROJECT_VERBS.values()),
)
POOL = Command(
    "pool",
    "Manage pools (admin only).",
    run_pool,
    configure_pool,
    actions=tuple(a for a, _ in POOL_VERBS.values()),
)
