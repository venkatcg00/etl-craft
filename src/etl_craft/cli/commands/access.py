"""Admin access commands; bulk input bytes cross the API, never a client path."""

import argparse
from pathlib import Path

from etl_craft.cli.commands.common import (
    Command,
    call,
    command_project,
    configure_output,
    request_from,
)
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import UsageError
from etl_craft.services.operations.access import AccessFilter, AccessView, AccessWrite

VERBS = {
    "grant": ActionName.ACCESS_GRANT,
    "revoke": ActionName.ACCESS_REVOKE,
    "list": ActionName.ACCESS_LIST,
}


def configure(parser: argparse.ArgumentParser) -> None:
    """Keep project selection on the shared global flag."""
    verbs = parser.add_subparsers(dest="access_verb", required=True)
    for name in VERBS:
        sub = verbs.add_parser(name)
        sub.add_argument("--project", default=argparse.SUPPRESS)
        principal = sub.add_mutually_exclusive_group()
        principal.add_argument("--group")
        principal.add_argument("--role")
        principal.add_argument("--account")
        target = sub.add_mutually_exclusive_group()
        target.add_argument("--pipeline")
        target.add_argument("--tag")
        if name != "list":
            sub.add_argument("--permission")
            sub.add_argument("--reason", required=True)
        if name == "grant":
            sub.add_argument("--file", type=Path)
            sub.add_argument("--dry-run", action="store_true")
        configure_output(sub)


def run(args: argparse.Namespace, out: Output) -> int:
    """Read a bulk file once, then let the service validate and diff the submitted content."""
    if getattr(args, "file", None) is None:
        args.project = command_project(args)
    if not args.project and getattr(args, "file", None) is None:
        raise UsageError(
            "access: project is missing; expected an explicit scope; supply --project P"
        )
    content = None
    if getattr(args, "file", None) is not None:
        try:
            content = args.file.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as error:
            raise UsageError(
                f"{args.file}: unreadable access input; expected UTF-8 CSV; fix the file: {error}"
            ) from error
    request = (
        request_from(args, AccessFilter)
        if args.access_verb == "list"
        else request_from(args, AccessWrite, content=content)
    )
    done = call(args, VERBS[args.access_verb], request)
    assert isinstance(done, AccessView)
    out.result(
        done,
        args.output_format,
        text=lambda: render(out, done),
    )
    return 0


def render(out: Output, done: AccessView) -> None:
    """Show retained grants and the caller's effective pipeline permissions."""
    out.rows(
        [
            (
                d.line,
                d.change,
                d.grant.project,
                d.grant.group or d.grant.account or d.grant.role,
                d.grant.pipeline or d.grant.tag,
                d.previous_permission,
                d.grant.permission,
            )
            for d in done.changes
        ]
        if done.changes
        else [
            (
                g.project,
                g.group or g.account or g.role,
                g.pipeline or g.tag or "project",
                g.permission,
                g.active_flag,
            )
            for g in done.grants
        ]
    )
    if done.project_permission is not None:
        out.rows([("caller project", done.project_permission, ", ".join(done.project_sources))])
    out.rows(
        ("caller effective", code, level, ", ".join(sources))
        for code, level, sources in done.effective
    )


COMMAND = Command(
    "access", "Manage pipeline permissions.", run, configure, actions=tuple(VERBS.values())
)
