"""``etl-craft search``: find pipelines, tasks, tables and runs from one text."""

import argparse

from etl_craft.cli.commands.common import Command, call, configure_output, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.search import SearchInput, SearchView


def configure(parser: argparse.ArgumentParser) -> None:
    """Take the text and an optional limit."""
    configure_output(parser)
    parser.add_argument("q", metavar="TEXT", help="Part of a name, or an exact run id or key.")
    parser.add_argument("--limit", type=int, default=20, help="Most hits to show (1 to 50).")


def run(args: argparse.Namespace, out: Output) -> int:
    """Render the same hits the API and the UI search box return."""
    result = call(args, ActionName.SEARCH, request_from(args, SearchInput))
    assert isinstance(result, SearchView)

    def render_text() -> None:
        if not result.hits:
            out.empty(f"nothing visible matches {result.q!r}")
            return
        out.rows([("KIND", "NAME", "PIPELINE", "RUN_ID", "DETAIL")])
        out.rows(
            (h.kind, h.label, h.pipeline_code or "", h.run_id or "", h.detail) for h in result.hits
        )
        if result.truncated:
            out.line(f"more matched; showing the first {len(result.hits)}; use --limit")

    out.result(result, args.output_format, text=render_text)
    return ExitCode.SUCCESS


COMMAND = Command(
    "search",
    "Find visible pipelines, tasks, tables and runs.",
    run,
    configure,
    actions=(ActionName.SEARCH,),
)
