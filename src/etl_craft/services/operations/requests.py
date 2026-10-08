"""Validated execution requests, independent of argument parsing."""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import date
from typing import Any

from etl_craft.core.errors import UsageError
from etl_craft.engine.runlog import ACTIVE_RUN, RunSelector


@dataclass(frozen=True)
class RunRequest:
    """One pipeline, task, lifecycle step or backfill request."""

    pipeline_code: str
    selector: RunSelector = ACTIVE_RUN
    task_code: str | None = None
    init_only: bool = False
    finalize_only: bool = False
    force: bool = False
    ignore_dependencies: bool = False
    rerun: bool = False
    with_downstream: bool = False
    skip: bool = False
    run_date: date | None = None
    backfill: tuple[date, date] | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        """Refuse incompatible requests before any resources are opened."""
        if self.task_code is not None and not self.task_code.strip():
            raise UsageError("--task_code must not be empty; supply an active task code")
        if sum(bool(v) for v in (self.task_code, self.init_only, self.finalize_only)) > 1:
            raise UsageError("choose one of --task_code, --init-only or --finalize-only")
        if self.force and (self.init_only or self.finalize_only):
            raise UsageError(
                "--force runs tasks; it does not apply to --init-only or --finalize-only"
            )
        if (self.ignore_dependencies or self.rerun) and not self.task_code:
            raise UsageError(
                "--ignore-dependencies and --rerun apply to one task: pass --task_code"
            )
        if self.ignore_dependencies and self.rerun:
            raise UsageError("--rerun already runs the task without checking its dependencies")
        if self.force and (self.ignore_dependencies or self.rerun):
            option = "--rerun" if self.rerun else "--ignore-dependencies"
            raise UsageError(f"{option} and --force are different overrides: choose one")
        if self.with_downstream and not self.rerun:
            raise UsageError("--with-downstream goes with --rerun")
        if self.skip and (self.task_code or self.init_only or self.finalize_only or self.force):
            raise UsageError("--skip records a whole run SKIPPED; it takes only --reason")
        if self.reason and not (
            self.ignore_dependencies or self.rerun or self.skip or self.backfill
        ):
            raise UsageError(
                "--reason goes with --ignore-dependencies, --rerun, --skip or --backfill"
            )
        if self.backfill and (
            self.task_code or self.init_only or self.finalize_only or self.force or self.skip
        ):
            raise UsageError(
                "--backfill runs the whole pipeline once per date; it takes only --reason"
            )
        if (self.selector.run_id is not None or self.selector.run_key is not None) and (
            self.backfill or self.skip
        ):
            raise UsageError("--backfill and --skip create new runs; do not pass a run selector")
        if self.run_date and (self.skip or self.backfill):
            raise UsageError("--run-date cannot be combined with --skip or --backfill")

    def arguments(self) -> dict[str, Any]:
        """Return audit arguments without configuration objects or secrets."""
        result = {f.name: getattr(self, f.name) for f in fields(self)}
        result.pop("selector")
        return {
            "run_id": self.selector.run_id,
            "run_key": self.selector.run_key,
            **result,
        }
