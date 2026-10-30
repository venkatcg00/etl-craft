"""Version information and generated catalog reads share the browser's operations."""

import argparse
from collections.abc import Iterable
from pathlib import Path

from etl_craft.cli.commands.common import Command, call, configure_output
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.services.operations.catalog_pages import CatalogInput, CatalogPage
from etl_craft.services.operations.requests import NoInput


def _about_options(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    parser.add_argument("--json", dest="output_format", action="store_const", const="json")


def _about(args: argparse.Namespace, out: Output) -> int:
    result = call(args, ActionName.ABOUT, NoInput())
    out.result(result, args.output_format, text=lambda: out.document(result))
    return 0


def _catalog_options(parser: argparse.ArgumentParser) -> None:
    verbs = parser.add_subparsers(dest="catalog_verb", required=True)
    page = verbs.add_parser("page", help="Read an existing generated page without rebuilding it.")
    page.add_argument("--path", default="index.html")
    page.add_argument("--output", type=Path)


def _catalog(args: argparse.Namespace, out: Output) -> int:
    page = call(args, ActionName.CATALOG_PAGE, CatalogInput(path=args.path))
    assert isinstance(page, Iterable)
    try:
        if args.output is None:
            out.chunks(page, binary=True)
        else:
            with args.output.open("wb") as target:
                for chunk in page:
                    target.write(chunk)
    finally:
        if isinstance(page, CatalogPage):
            page.close()
    return 0


ABOUT = Command(
    "about",
    "Installed package, schema and live-controller versions.",
    _about,
    _about_options,
    actions=(ActionName.ABOUT,),
)
CATALOG = Command(
    "catalog",
    "Read the selected project's existing generated catalog.",
    _catalog,
    _catalog_options,
    actions=(ActionName.CATALOG_PAGE,),
)
