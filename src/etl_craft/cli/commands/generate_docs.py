"""``etl-craft generate-docs``: write the catalog site, with its lineage graphs and search."""

from __future__ import annotations

import argparse
from pathlib import Path

from etl_craft.cli.commands import Command
from etl_craft.cli.commands.common import connect_engine_db, load_command_config
from etl_craft.cli.output import Output
from etl_craft.core.errors import ExitCode
from etl_craft.services.catalog import build_catalog
from etl_craft.services.catalog_site import write_site

DEFAULT_FOLDER = "catalog"
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
        help="add column types and comments from the warehouse",
    )


def _run(args: argparse.Namespace, out: Output) -> int:
    config = load_command_config(args)
    engine = connect_engine_db(config)
    try:
        catalog = build_catalog(engine, config, with_warehouse=args.with_warehouse)
    finally:
        engine.dispose()
    for task in catalog.untraced:
        out.line(f"column lineage unavailable: {task.label}: {task.lineage_error}")
    if args.strict and catalog.untraced:
        out.line(f"generate-docs: nothing written; {len(catalog.untraced)} task(s) not traced")
        return ExitCode.FAILURE
    folder = args.output or config.docs_site.output or config.project_dir / DEFAULT_FOLDER
    site = write_site(catalog, config, folder)
    out.line(f"generate-docs: wrote {site.pages} page(s) to {site.folder}")
    return ExitCode.SUCCESS


COMMAND = Command(
    name="generate-docs",
    help="Write the catalog site: every pipeline, task and table, lineage graphs and search.",
    configure=_configure,
    run=_run,
)
