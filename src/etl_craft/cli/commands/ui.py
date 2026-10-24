"""Serve a stateless API copy without starting a controller."""

import argparse
from dataclasses import replace

from etl_craft.cli.commands.common import Command, command_context, on_host
from etl_craft.cli.lifecycle import shutdown_event
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName, ProcessRole
from etl_craft.core.errors import ExitCode
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.processes import connection_process
from etl_craft.services.operations.requests import NoInput


def _run(args: argparse.Namespace, out: Output) -> int:
    on_host(args, ActionName.UI)
    with shutdown_event() as stop, command_context(args, role="ui") as ctx:
        invoke(ctx, ActionName.UI, NoInput())
        ctx = replace(ctx, shutdown=stop, child=replace(ctx.child, cancel=stop))
        with connection_process(ctx, ProcessRole.UI) as ctx:
            from etl_craft.api.serve import serve_http

            serve_http(ctx, stop, controller=False)
    return ExitCode.SUCCESS


COMMAND = Command(
    "ui", "Serve a stateless API copy against PostgreSQL.", _run, actions=(ActionName.UI,)
)
