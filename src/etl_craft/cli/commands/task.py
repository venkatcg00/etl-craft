"""``etl-craft task``: show one task's handler and parameters."""

import argparse

from etl_craft.cli.commands.common import Command, call, configure_output, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.offsets import TaskInput
from etl_craft.services.operations.tasks import TaskDefinition


def configure(parser: argparse.ArgumentParser) -> None:
    """Take the pipeline and task."""
    configure_output(parser)
    parser.add_argument("--pipeline-code", required=True, help="The task's pipeline.")
    parser.add_argument("--task-code", required=True, help="The task.")


def run(args: argparse.Namespace, out: Output) -> int:
    """Render the definition the browser's task page shows."""
    result = call(args, ActionName.TASK, request_from(args, TaskInput))
    assert isinstance(result, TaskDefinition)

    def render_text() -> None:
        out.line(f"{result.pipeline_code}.{result.task_code}: {result.handler}")
        out.rows([("PARAMETER", "VALUE")])
        out.rows(sorted(result.parameters.items()))

    out.result(result, args.output_format, text=render_text)
    return ExitCode.SUCCESS


COMMAND = Command(
    "task", "Show a task's handler and parameters.", run, configure, actions=(ActionName.TASK,)
)
