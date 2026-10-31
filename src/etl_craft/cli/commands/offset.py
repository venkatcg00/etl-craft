"""``etl-craft offset``: show what a task keeps between runs and what is stored now."""

import argparse

from etl_craft.cli.commands.common import Command, call, configure_output, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.offsets import TaskInput, TaskOffset


def configure(parser: argparse.ArgumentParser) -> None:
    """Take the pipeline and task."""
    configure_output(parser)
    parser.add_argument("--pipeline-code", required=True, help="The task's pipeline.")
    parser.add_argument("--task-code", required=True, help="The task.")


def run(args: argparse.Namespace, out: Output) -> int:
    """Render the stored offset or watermark the UI's Offsets panel shows."""
    result = call(args, ActionName.OFFSET, request_from(args, TaskInput))
    assert isinstance(result, TaskOffset)

    def render_text() -> None:
        where = f"{result.pipeline_code}.{result.task_code}"
        if result.kind is None:
            out.line(f"{where} keeps no offset or watermark between runs")
        elif result.offset_type is None:
            out.line(f"{where}: no {result.kind.lower()} stored yet; the first run starts it")
        else:
            out.line(f"{where} {result.kind}: {result.offset_type} {result.offset_value}")
            if result.pending_task_run_id is not None:
                out.line(
                    f"pending window up to {result.pending_upper_bound} "
                    f"held by task run {result.pending_task_run_id}"
                )

    out.result(result, args.output_format, text=render_text)
    return ExitCode.SUCCESS


COMMAND = Command(
    "offset",
    "Show a task's stored offset or watermark.",
    run,
    configure,
    actions=(ActionName.OFFSET,),
)
