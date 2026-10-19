"""Parse run control selections and render their shared service results."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from etl_craft.cli.commands.common import command_context
from etl_craft.cli.output import Output
from etl_craft.cli.remote import ServerClient, api_path
from etl_craft.core.enums import ControlAction
from etl_craft.core.errors import ExitCode, UsageError
from etl_craft.engine.controls import ControlView
from etl_craft.engine.runlog import RunSelector
from etl_craft.services.operations.bulk_controls import BulkControlView, request_bulk_controls
from etl_craft.services.operations.controls import ControlRequest, request_control


def configure(parser: argparse.ArgumentParser) -> None:
    """Keep execution controls under the existing run verb."""
    action = parser.add_mutually_exclusive_group()
    action.add_argument(
        "--recover",
        action="store_true",
        help="prepare unfinished work in this run; keep SUCCESS/SKIPPED and checkpoints",
    )
    action.add_argument(
        "--restart",
        action="store_true",
        help="prepare a new run retaining current checkpoints; source history is kept",
    )
    action.add_argument(
        "--allow-paused",
        action="store_true",
        help="permit the exact selected run while its automatic schedule stays paused",
    )
    action.add_argument(
        "--controls-file",
        type=Path,
        help="JSON list of explicit scoped controls with per-object request keys; prepare only",
    )
    parser.add_argument(
        "--request-key", help="idempotency key for recovery/restart/one-run permission"
    )


def selected(args: argparse.Namespace) -> bool:
    """Whether this request prepares durable controls rather than directly driving tasks."""
    return bool(args.recover or args.restart or args.allow_paused or args.controls_file)


def execute(args: argparse.Namespace, out: Output, server: ServerClient | None = None) -> int:
    """Submit through the shared operation locally or through its HTTP transport."""
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
        return _bulk(args, out, server)
    if args.pipeline_code is None or not args.reason:
        raise UsageError(
            "recovery/restart/one-run permission needs --pipeline_code and a nonblank --reason"
        )
    if args.run_id is None and args.run_key is None and not args.allow_paused:
        raise UsageError(
            "recovery/restart/one-run permission needs an explicit --run-id or --run-key"
        )
    action = (
        ControlAction.RECOVER
        if args.recover
        else ControlAction.RESTART
        if args.restart
        else ControlAction.RUN_PAUSED
    )
    request = ControlRequest(
        args.pipeline_code,
        action,
        args.reason,
        RunSelector(args.run_id, args.run_key),
        args.task_code,
        args.request_key,
    )
    if server is None:
        with command_context(args) as ctx:
            control = request_control(ctx, request)
    else:
        control = server.call(
            "POST",
            api_path("pipelines", args.pipeline_code, "controls"),
            ControlView,
            body=_body(request),
        )
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
        try:
            action = ControlAction(str(row.get("action")))
        except (TypeError, ValueError) as error:
            raise UsageError(
                f"control selection {path}[{index}]: action={row.get('action')!r}; "
                "use a supported control action"
            ) from error
        if not all(
            isinstance(row.get(name), str) and row[name].strip()
            for name in ("pipeline_code", "reason", "request_key")
        ):
            raise UsageError(
                f"control selection {path}[{index}]: pipeline_code, reason and request_key "
                "must be nonblank strings"
            )
        if row.get("task_code") is not None and not isinstance(row["task_code"], str):
            raise UsageError(f"control selection {path}[{index}]: task_code must be a string")
        if (
            row.get("run_id") is not None and (type(row["run_id"]) is not int or row["run_id"] < 1)
        ) or (row.get("run_key") is not None and not isinstance(row["run_key"], str)):
            raise UsageError(
                f"control selection {path}[{index}]: use a positive run_id or nonblank run_key"
            )
        requests.append(
            ControlRequest(
                row["pipeline_code"],
                action,
                row["reason"],
                RunSelector(row.get("run_id"), row.get("run_key")),
                row.get("task_code"),
                row["request_key"],
            )
        )
    return tuple(requests)


def _body(request: ControlRequest) -> dict[str, object]:
    return {
        "action": request.action,
        "reason": request.reason,
        "run_id": request.selector.run_id,
        "run_key": request.selector.run_key,
        "task_code": request.task_code,
        "request_key": request.request_key,
    }


def _bulk(args: argparse.Namespace, out: Output, server: ServerClient | None) -> int:
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
            "--controls-file contains its own selection/reasons/keys; omit other selectors"
        )
    requests = read_selection(args.controls_file)
    if server is None:
        with command_context(args) as ctx:
            done = request_bulk_controls(ctx, requests)
    else:
        done = server.call(
            "POST",
            api_path("controls", "bulk"),
            BulkControlView,
            body={"requests": [_body(r) | {"pipeline_code": r.pipeline_code} for r in requests]},
        )
    out.result(done, args.output_format, text=lambda: _render_bulk(out, done))
    return ExitCode.FAILURE if any(o.error for o in done.outcomes) else ExitCode.SUCCESS


def _render_bulk(out: Output, done: BulkControlView) -> None:
    """Show each selected object's accepted identity or refusal in ordinary text output."""
    out.rows(
        (
            o.pipeline_code,
            o.task_code or "all",
            o.action,
            o.request_key,
            f"control_id={o.control.control_id} {o.control.status}" if o.control else o.error,
        )
        for o in done.outcomes
    )
    out.line(done.message)
