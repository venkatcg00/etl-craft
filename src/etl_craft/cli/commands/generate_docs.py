"""``etl-craft generate-docs``: write the catalog site, with its lineage graphs and search."""

from __future__ import annotations

import argparse
from pathlib import Path

from etl_craft.cli.commands.common import Command, command_context
from etl_craft.cli.output import Output
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
        help=f"the folder to write (default: Docs_site.Output, else {DEFAULT_FOLDER}/ in the "
        "project directory)",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="write nothing and exit 1 when any SQL task's columns cannot be traced",
    )
    parser.add_argument(
        "--with-warehouse",
        action="store_true",
        help="compare documented columns with the warehouse and add deployed types and comments",
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    with command_context(args) as ctx:
        built = documents.generate_docs(
            ctx, with_warehouse=args.with_warehouse, output=args.output, strict=args.strict
        )
    for task in built.catalog.untraced:
        out.line(f"column lineage unavailable: {task.label}: {task.lineage_error}")
    if built.site is None:
        out.line(
            f"generate-docs: nothing written; {len(built.catalog.untraced)} task(s) not traced"
        )
        return ExitCode.FAILURE
    for table in built.catalog.tables.values():
        for finding in table.warehouse_findings:
            out.line(f"warehouse comparison: {table.name}: {finding}")
    out.line(f"generate-docs: wrote {built.site.pages} page(s) to {built.site.folder}")
    return ExitCode.SUCCESS


COMMAND = Command(
    name="generate-docs",
    help="Write the catalog site: every pipeline, task and table, lineage graphs and search.",
    configure=_configure,
    run=_run,
)
