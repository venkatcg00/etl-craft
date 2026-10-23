"""Start an API-only agent or send an operator command to a registered agent."""

import argparse
import math
from dataclasses import replace
from pathlib import Path

from etl_craft.cli.client import ApiClient
from etl_craft.cli.commands.common import Command, call, configure_output
from etl_craft.cli.lifecycle import shutdown_event
from etl_craft.cli.output import Output
from etl_craft.config import load_config, resolve_config_path
from etl_craft.config.sizing import ceilings
from etl_craft.core.agent_resources import DeclaredCapacity
from etl_craft.core.enums import ActionName, AgentCommand
from etl_craft.core.errors import ExitCode, UsageError
from etl_craft.services.operations.agent_models import AgentControl
from etl_craft.services.operations.agents import AgentsView
from etl_craft.services.operations.requests import NoInput


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("operation", choices=("drain", "resume", "stop"), nargs="?")
    parser.add_argument("--agent-id", type=int)
    parser.add_argument("--project", type=Path, help="This agent's project directory.")
    parser.add_argument("--pool", default="default")
    parser.add_argument("--python-slots", type=int)
    parser.add_argument("--warehouse-slots", type=int)
    parser.add_argument("--cpu", type=int, help="Lower measured CPU capacity to this ceiling.")
    parser.add_argument(
        "--memory-mb", type=int, help="Lower measured memory capacity to this MB ceiling."
    )
    parser.add_argument(
        "--declared-capacity",
        action="store_true",
        help="Front an external quota; requires both --cpu and --memory-mb.",
    )
    parser.add_argument("--drain-seconds", type=float, default=300)
    configure_output(parser)


def _run(args: argparse.Namespace, out: Output) -> int:
    if not args.api:
        raise UsageError(
            "agent requires --api or ETL_CRAFT_API; agents never connect to the Engine DB"
        )
    if args.operation is not None:
        if args.agent_id is None:
            raise UsageError("agent command requires --agent-id; select a registered agent")
        done = ApiClient(args.api).invoke(
            ActionName.AGENT_CONTROL,
            AgentControl(agent_id=args.agent_id, command=AgentCommand(args.operation.upper())),
        )
        out.result(
            done,
            args.output_format,
            text=lambda: out.rows((("Agent", args.agent_id, args.operation),)),
        )
        return ExitCode.SUCCESS
    if args.project is not None and args.config is not None:
        raise UsageError("choose --project or --config, not both")
    path = resolve_config_path(
        args.config if args.project is None else args.project / "craft-connector.yml"
    )
    config = load_config(path, agent=True)
    selected = ceilings(config)
    selected = replace(
        selected,
        python_slots=args.python_slots if args.python_slots is not None else selected.python_slots,
        warehouse_slots=args.warehouse_slots
        if args.warehouse_slots is not None
        else selected.warehouse_slots,
    )
    if (
        not math.isfinite(args.drain_seconds)
        or args.drain_seconds < 0
        or any(
            value is not None and value < 0
            for value in (
                selected.max_parallel_tasks,
                selected.python_slots,
                selected.warehouse_slots,
            )
        )
    ):
        raise UsageError("agent slots and drain seconds must be nonnegative; set a valid capacity")
    from etl_craft.agent.loop import AgentLoop, AgentOptions

    with shutdown_event() as stop:
        AgentLoop(
            config,
            AgentOptions(
                args.api,
                args.pool,
                selected,
                args.drain_seconds,
                DeclaredCapacity(args.cpu, args.memory_mb, args.declared_capacity),
            ),
            stop,
        ).serve()
    return ExitCode.SUCCESS


def _list(args: argparse.Namespace, out: Output) -> int:
    done = call(args, ActionName.AGENTS, NoInput())
    assert isinstance(done, AgentsView)
    out.result(done, args.output_format, text=lambda: out.agents(done))
    return ExitCode.SUCCESS


COMMAND = Command(
    "agent",
    "Run an API-only task agent, or drain, resume or stop one.",
    _run,
    _configure,
    actions=(ActionName.AGENT_REGISTER, ActionName.AGENT_CONTROL),
)
LIST = Command(
    "agents",
    "List registered agent capacity, attempts and commands.",
    _list,
    configure_output,
    actions=(ActionName.AGENTS,),
)
