"""Parse run control selections and render their shared service results."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from etl_craft.cli.commands.common import call, request_from
from etl_craft.cli.output import Output
from etl_craft.core.enums import ActionName, ControlAction
from etl_craft.core.errors import EtlCraftError, ExitCode, UsageError
from etl_craft.engine.controls import ControlView
from etl_craft.services.operations.controls import BulkControlView
from etl_craft.services.operations.requests import BulkControlRequest, ControlRequest


def configure(parser: argparse.ArgumentParser) -> None:
    """Keep execution controls under the existing run verb."""
    action = parser.add_mutually_exclusive_group()
    action.add_argument(
        "--recover",
        action="store_true",
        help="Prepare unfinished work in this run; keep SUCCESS/SKIPPED and checkpoints.",
    )
    action.add_argument(
        "--restart",
        action="store_true",
        help="Prepare a new run retaining current checkpoints; source history is kept.",
    )
    action.add_argument(
        "--allow-paused",
        action="store_true",
        help="Permit the exact selected run while its automatic schedule stays paused.",
    )
    action.add_argument(
        "--controls-file",
        type=Path,
        help="JSON list of explicit scoped controls with per-object request keys; prepare only.",
    )
    parser.add_argument(
        "--request-key", help="Idempotency key for recovery/restart/one-run permission."
    )


def selected(args: argparse.Namespace) -> bool:
    """Whether this request prepares durable controls rather than directly driving tasks."""
    return bool(args.recover or args.restart or args.allow_paused or args.controls_file)


def execute(args: argparse.Namespace, out: Output) -> int:
    """Submit a recovery, a restart, a one-run permission or a selection of controls."""
    if any(
        (
            args.standalone,
            args.init_only,
            args.force,
            args.ignore_dependencies,
            args.rerun,
            args.with_downstream,
            args.skip,
            args.run_date,
            args.backfill,
        )
    ):
        raise UsageError(
            "recovery/restart/control selection prepares durable contexts; omit execution flags, "
            "then run the returned execution_run_id explicitly"
        )
    if args.controls_file is not None:
        return _bulk(args, out)
    if args.pipeline_code is None:
        raise UsageError("recovery/restart/one-run permission needs `pipeline_code`")
    if args.run_id is None and args.run_key is None and not args.allow_paused:
        raise UsageError(
            "recovery/restart/one-run permission needs an explicit `run_id` or `run_key`"
        )
    action = (
        ControlAction.RECOVER
        if args.recover
        else ControlAction.RESTART
        if args.restart
        else ControlAction.RUN_PAUSED
    )
    control = call(args, ActionName.CONTROL, request_from(args, ControlRequest, action=action))
    out.result(control, args.output_format)
    return ExitCode.SUCCESS


def read_selection(path: Path) -> tuple[ControlRequest, ...]:
    """Refuse malformed batch input before performing any selected action."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise UsageError(
            f"control selection {path}: {error}; supply a UTF-8 JSON list of control objects"
        ) from error
    if not isinstance(data, list):
        raise UsageError(f"control selection {path}: expected a JSON list of control objects")
    requests = []
    allowed = {"pipeline_code", "task_code", "run_id", "run_key", "action", "reason", "request_key"}
    for index, row in enumerate(data):
        if not isinstance(row, dict) or set(row) - allowed:
            raise UsageError(f"control selection {path}[{index}]: expected only {sorted(allowed)}")
        if row.get("run_id") is not None and type(row["run_id"]) is not int:
            raise UsageError(f"control selection {path}[{index}]: run_id must be a whole number")
        try:
            action = ControlAction(str(row.get("action")))
            requests.append(ControlRequest(**{**row, "action": action}))
        except (EtlCraftError, TypeError, ValueError) as error:
            raise UsageError(f"control selection {path}[{index}]: {error}") from error
    return tuple(requests)


def _bulk(args: argparse.Namespace, out: Output) -> int:
    """Submit a fully parsed selection through the ordinary bulk operation."""
    if any(
        v is not None
        for v in (
            args.pipeline_code,
            args.task_code,
            args.run_id,
            args.run_key,
            args.reason,
            args.request_key,
        )
    ):
        raise UsageError(
            "`controls_file` contains its own selection, reasons and keys; omit other selectors"
        )
    request = BulkControlRequest(requests=read_selection(args.controls_file))
    done = call(args, ActionName.CONTROL_BULK, request)
    assert isinstance(done, BulkControlView)
    out.result(done, args.output_format, text=lambda: render_bulk(out, done))
    return ExitCode.FAILURE if any(o.error for o in done.outcomes) else ExitCode.SUCCESS


def render_bulk(out: Output, done: BulkControlView) -> None:
    """Show each selected object's accepted identity or refusal in ordinary text output."""
    out.rows(
        (
            o.pipeline_code,
            o.task_code or "all",
            o.action,
            o.request_key,
            f"control_id={o.control.control_id} {o.control.status}"
            if isinstance(o.control, ControlView)
            else o.control.message
            if o.control is not None
            else o.error,
        )
        for o in done.outcomes
    )
    out.line(done.message)
