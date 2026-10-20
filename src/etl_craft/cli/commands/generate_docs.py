"""``etl-craft generate-docs``: write the catalog site, with its lineage graphs and search."""

from __future__ import annotations

import argparse
from pathlib import Path

from etl_craft.cli.commands.common import Command, call, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations import documents
from etl_craft.services.operations.documents import DEFAULT_FOLDER

"""Where the site goes, in the project directory, unless ``--output`` or ``Docs_site.Output``
names another folder."""


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--output",
        type=Path,
        metavar="DIR",
        help=f"The folder to write (default: Docs_site.Output, else {DEFAULT_FOLDER}/ in the "
        "project directory).",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Write nothing and exit 1 when any SQL task's columns cannot be traced.",
    )
    parser.add_argument(
        "--with-warehouse",
        action="store_true",
        help="Compare documented columns with the warehouse and add deployed types and comments.",
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    built = call(args, ActionName.GENERATE_DOCS, request_from(args, documents.DocsInput))
    assert isinstance(built, documents.DocsBuild)
    for task in built.untraced:
        out.line(f"column lineage unavailable: {task.task}: {task.error}")
    if built.folder is None:
        out.line(f"generate-docs: nothing written; {len(built.untraced)} task(s) not traced")
        return ExitCode.FAILURE
    for finding in built.warehouse_findings:
        out.line(f"warehouse comparison: {finding}")
    out.line(f"generate-docs: wrote {built.pages} page(s) to {built.folder}")
    return ExitCode.SUCCESS


COMMAND = Command(
    name="generate-docs",
    help="Write the catalog site: every pipeline, task and table, lineage graphs and search.",
    configure=_configure,
    run=_run,
    actions=(ActionName.GENERATE_DOCS,),
)
