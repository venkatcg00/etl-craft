"""``etl-craft docs-version``: record a new version of each task's changed documentation."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import Command, call
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.documents import DocsVersions
from etl_craft.services.operations.requests import NoInput


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--check", action="store_true", help="Report what would change, and record nothing."
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    name = ActionName.DOCS_VERSION_CHECK if args.check else ActionName.DOCS_VERSION
    done = call(args, name, NoInput())
    assert isinstance(done, DocsVersions)
    versions = done.versions
    if not versions:
        out.empty("no task has a DOCUMENTATION parameter")
        return ExitCode.SUCCESS
    out.rows([("PIPELINE_CODE", "TASK_CODE", "VERSION", "CHANGED")])
    out.rows(
        (v.pipeline_code, v.task_code, v.version, "yes" if v.changed else "no") for v in versions
    )
    return ExitCode.SUCCESS


COMMAND = Command(
    name="docs-version",
    help="Record a new version of each task's DOCUMENTATION that changed.",
    configure=_configure,
    run=_run,
    actions=(ActionName.DOCS_VERSION, ActionName.DOCS_VERSION_CHECK),
)
