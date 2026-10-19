"""``etl-craft steps``: a pipeline's active tasks and their parameters."""

from __future__ import annotations

import argparse

from etl_craft.cli.commands.common import (
    Command,
    call,
    configure_output,
    configure_run_selector,
    request_from,
)
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName, RunCondition
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.models import StepsView
from etl_craft.services.operations.requests import RunSelection


def _configure(parser: argparse.ArgumentParser) -> None:
    configure_output(parser)
    configure_run_selector(parser)
    parser.add_argument("--pipeline-code", required=True, help="The pipeline to show.")


def _run(args: argparse.Namespace, out: Output) -> int:
    done = call(args, ActionName.STEPS, request_from(args, RunSelection))
    assert isinstance(done, StepsView)

    def render_text() -> None:
        steps = done.steps
        if not steps:
            out.empty("no active tasks")
            return
        out.rows([("TASK_CODE", "STATUS", "HANDLER", "TASK_TYPE", "RUN_CONDITION", "PARAMETERS")])
        out.rows(
            (
                s.task_code,
                s.status or "NOT-RUN",
                s.handler,
                s.task_type,
                f"N={s.run_condition_count}"
                if s.run_condition == RunCondition.N
                else s.run_condition or "ALL",
                ", ".join(f"{name}={value}" for name, value in s.parameters.items()),
            )
            for s in steps
        )
        return

    out.result(done, args.output_format, text=render_text)
    return ExitCode.SUCCESS


COMMAND = Command(
    name="steps",
    help="List a pipeline's active tasks and their parameters.",
    configure=_configure,
    run=_run,
    actions=(ActionName.STEPS,),
)
