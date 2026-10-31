"""``etl-craft holds``: list the holds still in force on queued and running runs."""

import argparse

from etl_craft.cli.commands.common import Command, call, configure_output
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.holds import Holds
from etl_craft.services.operations.requests import NoInput


def run(args: argparse.Namespace, out: Output) -> int:
    """Render the holds the browser's Held and paused page lists."""
    result = call(args, ActionName.HOLDS, NoInput())
    assert isinstance(result, Holds)

    def render_text() -> None:
        if not result.holds:
            out.empty("no run or task is held")
            return
        out.rows([("PIPELINE", "RUN_ID", "TASK", "HELD_BY", "HELD_AT", "REASON")])
        out.rows(
            (
                h.pipeline_code,
                h.pipeline_run_id,
                h.task_code or "(whole run)",
                h.requested_by,
                h.requested_at.isoformat(),
                h.reason or "",
            )
            for h in result.holds
        )

    out.result(result, args.output_format, text=render_text)
    return ExitCode.SUCCESS


COMMAND = Command(
    "holds",
    "List held runs and tasks that have not been released.",
    run,
    configure_output,
    actions=(ActionName.HOLDS,),
)
