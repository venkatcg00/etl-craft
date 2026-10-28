"""Keep per-item execution under the existing run verb."""

import argparse

from etl_craft.cli.commands.common import call, request_from
from etl_craft.cli.commands.items import render
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName, ItemAction
from etl_craft.core.errors import ExitCode, UsageError
from etl_craft.services.operations.items import ItemControl, ItemView


def configure(parser: argparse.ArgumentParser) -> None:
    """Select an item or the failed items for an explicit task rerun."""
    selector = parser.add_mutually_exclusive_group()
    selector.add_argument("--item-key", help="With --rerun: rerun only this Python work item.")
    selector.add_argument(
        "--all-failed",
        action="store_true",
        help="With --rerun: rerun only failed Python work items.",
    )
    parser.add_argument(
        "--reason", default="", help="Why an override, rerun, skip or backfill is needed; recorded."
    )


def selected(args: argparse.Namespace) -> bool:
    """Recognize the item selection before building a whole-task run request."""
    return args.item_key is not None or args.all_failed


def execute(args: argparse.Namespace, out: Output) -> int:
    """Parse item scope, then call the same operation as the HTTP action."""
    if not args.rerun or args.task_code is None or args.run_id is None:
        raise UsageError(
            "item rerun: expected --rerun, --task-code and --run-id; select the exact task run"
        )
    incompatible = (
        "force",
        "standalone",
        "ignore_dependencies",
        "init_only",
        "skip",
        "with_downstream",
        "run_date",
        "backfill",
        "recover",
        "restart",
        "allow_paused",
        "controls_file",
    )
    found = [name for name in incompatible if getattr(args, name, None)]
    if found:
        raise UsageError(
            f"item rerun: incompatible options={found}; expected only item selection "
            "and rerun; remove the other execution options"
        )
    request = request_from(args, ItemControl, action=ItemAction.RERUN)
    done = call(args, ActionName.ITEM_CONTROL, request, role="run")
    assert isinstance(done, ItemView)
    out.result(done, args.output_format, text=lambda: render(out, done))
    return ExitCode.FAILURE if done.progress.failed else ExitCode.SUCCESS
