"""Inspect and control recorded work items inside one Python task."""

import argparse

from etl_craft.cli.commands.common import Command, call, configure_output, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName, ItemAction, ItemStatus
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.items import ItemControl, ItemInput, ItemView


def configure(parser: argparse.ArgumentParser) -> None:
    """Keep every item operation scoped to an exact task and run."""
    subs = parser.add_subparsers(dest="item_command", required=True)
    for name in ("list", "mark", "cancel"):
        sub = subs.add_parser(name)
        configure_output(sub)
        sub.add_argument("--pipeline-code", required=True)
        sub.add_argument("--task-code", required=True)
        sub.add_argument("--run-id", type=int, required=True)
        if name == "list":
            continue
        selector = sub.add_mutually_exclusive_group(required=True)
        selector.add_argument("--item-key")
        selector.add_argument("--all-failed", action="store_true")
        sub.add_argument("--reason", default="")
        if name == "mark":
            sub.add_argument(
                "--status",
                choices=[ItemStatus.SUCCESS, ItemStatus.FAILED, ItemStatus.SKIPPED],
                required=True,
            )
            sub.add_argument("--rows", type=int, default=0)


def run(args: argparse.Namespace, out: Output) -> int:
    """Call the same inspection or control action row as the API."""
    if args.item_command == "list":
        result = call(args, ActionName.ITEMS, request_from(args, ItemInput))
    else:
        values: dict[str, object] = {"action": ItemAction(args.item_command.upper())}
        if getattr(args, "status", None) is not None:
            values["status"] = ItemStatus(args.status)
        result = call(args, ActionName.ITEM_CONTROL, request_from(args, ItemControl, **values))
    assert isinstance(result, ItemView)
    out.result(result, args.output_format, text=lambda: render(out, result))
    return ExitCode.SUCCESS


COMMAND = Command(
    "items",
    "Inspect, mark or cancel Python work items.",
    run,
    configure,
    (ActionName.ITEMS, ActionName.ITEM_CONTROL),
)


def render(out: Output, view: ItemView) -> None:
    """Show the same persisted progress and identities as the JSON response."""
    out.line(str(view.progress))
    out.rows(
        [("Key", "Status", "Item attempt", "Rows", "Started", "Ended", "Error", "Task attempt")]
    )
    out.rows(
        (r.key, r.status, r.number, r.row_count, r.started, r.ended, r.error, r.attempt_id)
        for r in view.records
    )
