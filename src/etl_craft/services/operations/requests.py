"""Each action's request, validated before any resource is opened.

Messages name request fields in backticks (`task_code`); the CLI shows each as its flag and the
HTTP API returns it as it is.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, ClassVar

from etl_craft.core.enums import (
    DEFINITION_CONTROLS,
    MARKABLE_STATUSES,
    ControlAction,
    LineageDirection,
    RunStatus,
)
from etl_craft.core.errors import UsageError
from etl_craft.core.tags import TAG_RULE, valid_tag
from etl_craft.engine.runlog import RunSelector

if TYPE_CHECKING:
    from _typeshed import DataclassInstance

MAX_REASON = 4096
"""The longest reason an intervention records."""


def arguments(request: DataclassInstance) -> dict[str, Any]:
    """Return a request's fields for its audit row; the audit masks secret names."""
    return {f.name: getattr(request, f.name) for f in fields(request)}


def require_text(name: str, value: str | None, *, limit: int | None = None) -> None:
    """Refuse a missing or blank text field, or one longer than ``limit``."""
    if value is None or not value.strip():
        raise UsageError(f"`{name}` is required and must not be blank")
    if limit is not None and len(value) > limit:
        raise UsageError(f"`{name}` has {len(value)} characters; give at most {limit}")


def optional_text(name: str, value: str | None) -> None:
    """Refuse a given text field that is blank."""
    if value is not None and not value.strip():
        raise UsageError(f"`{name}` must not be blank when given")


@dataclass(frozen=True, kw_only=True)
class Request:
    """Every action's request: keyword fields, validated when built.

    The HTTP API refuses fields a request does not define; pydantic reads this setting.
    """

    __pydantic_config__: ClassVar[dict[str, str]] = {"extra": "forbid"}


@dataclass(frozen=True, kw_only=True)
class NoInput(Request):
    """An action that takes no input."""


@dataclass(frozen=True, kw_only=True)
class TagFilter(Request):
    """Active pipelines carrying every requested tag; an unknown tag matches nothing."""

    tag: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Refuse malformed filters at every interface boundary."""
        if not isinstance(self.tag, (tuple, list)):
            raise UsageError(
                f"`tag`={self.tag!r}; expected a list of tags; give a sequence of exact tags"
            )
        object.__setattr__(self, "tag", tuple(self.tag))
        for value in self.tag:
            if not valid_tag(value):
                raise UsageError(
                    f"`tag`={value!r}; expected {TAG_RULE}; replace or remove this filter"
                )


@dataclass(frozen=True, kw_only=True)
class RunIdentity(Request):
    """Shared exact run identity for read selections and execution requests."""

    run_id: int | None = None
    run_key: str | None = None

    def __post_init__(self) -> None:
        """Refuse conflicting or malformed identities."""
        RunSelector(self.run_id, self.run_key)

    @property
    def selector(self) -> RunSelector:
        """Select the exact run, or the single non-terminal whole-pipeline run."""
        return RunSelector(self.run_id, self.run_key)


@dataclass(frozen=True, kw_only=True)
class RunFilter(TagFilter, RunIdentity):
    """One selected run, or non-terminal runs across the matching active pipelines."""

    pipeline_code: str | None = None

    def __post_init__(self) -> None:
        """Keep explicit run identities scoped to their pipeline."""
        super().__post_init__()
        RunIdentity.__post_init__(self)
        optional_text("pipeline_code", self.pipeline_code)
        if self.pipeline_code is None and (self.run_id is not None or self.run_key is not None):
            raise UsageError(
                "`run_id` and `run_key` need `pipeline_code`; select its exact pipeline"
            )


@dataclass(frozen=True, kw_only=True)
class PipelineInput(Request):
    """One pipeline, by code."""

    pipeline_code: str

    def __post_init__(self) -> None:
        """Refuse a blank code before looking it up."""
        require_text("pipeline_code", self.pipeline_code)


@dataclass(frozen=True, kw_only=True)
class RunSelection(RunIdentity):
    """One pipeline's selected run; execution requests never accept a tag selection."""

    pipeline_code: str

    def __post_init__(self) -> None:
        """Refuse a blank code as well as invalid run identities."""
        super().__post_init__()
        require_text("pipeline_code", self.pipeline_code)


@dataclass(frozen=True, kw_only=True)
class TaskSelection(RunSelection):
    """One task of a pipeline under its selected run."""

    task_code: str

    def __post_init__(self) -> None:
        """Refuse a blank task code as well."""
        super().__post_init__()
        require_text("task_code", self.task_code)


@dataclass(frozen=True, kw_only=True)
class CapabilitiesInput(RunSelection):
    """The selected run, or one task of it, whose controls to inspect."""

    task_code: str | None = None

    def __post_init__(self) -> None:
        """Refuse a blank task code as well."""
        super().__post_init__()
        optional_text("task_code", self.task_code)


@dataclass(frozen=True, kw_only=True)
class RunId(Request):
    """One pipeline run, by id."""

    run_id: int

    def __post_init__(self) -> None:
        """Refuse an id that cannot exist."""
        if self.run_id < 1:
            raise UsageError(f"`run_id` is {self.run_id}; give a positive run id")


@dataclass(frozen=True, kw_only=True)
class AttemptId(Request):
    """One task attempt, by id."""

    attempt_id: int

    def __post_init__(self) -> None:
        """Refuse an id that cannot exist."""
        if self.attempt_id < 1:
            raise UsageError(f"`attempt_id` is {self.attempt_id}; give a positive attempt id")


@dataclass(frozen=True, kw_only=True)
class LogInput(AttemptId):
    """An attempt's log, from a byte offset."""

    offset: int = 0

    def __post_init__(self) -> None:
        """Refuse a negative offset as well."""
        super().__post_init__()
        if self.offset < 0:
            raise UsageError(f"`offset` is {self.offset}; give a nonnegative byte offset")


@dataclass(frozen=True, kw_only=True)
class ControlId(Request):
    """One recorded control request, by id."""

    control_id: int

    def __post_init__(self) -> None:
        """Refuse an id that cannot exist."""
        if self.control_id < 1:
            raise UsageError(f"`control_id` is {self.control_id}; give a positive request id")


@dataclass(frozen=True, kw_only=True)
class HistoryInput(RunFilter):
    """Which history to read: a task's or the pipeline's, for one run or a page of runs.

    ``before`` is an exclusive run-id cursor; ``all`` lists runs instead of selecting one.
    """

    task_code: str | None = None
    all: bool = False
    limit: int = 20
    before: int | None = None

    def __post_init__(self) -> None:
        """Bound the page before reading it."""
        super().__post_init__()
        optional_text("task_code", self.task_code)
        if self.task_code is not None and self.pipeline_code is None:
            raise UsageError("`task_code` needs `pipeline_code`; choose its pipeline")
        if not 1 <= self.limit <= 100:
            raise UsageError(f"`limit` is {self.limit}; give 1 to 100")
        if self.before is not None and self.before < 1:
            raise UsageError(f"`before` is {self.before}; give a positive run id")
        if self.all and (self.run_id is not None or self.run_key is not None):
            raise UsageError("`all` lists runs; omit `run_id` and `run_key`")


@dataclass(frozen=True, kw_only=True)
class AuditInput(TagFilter):
    """Recorded requests, optionally for one pipeline and since a moment."""

    pipeline_code: str | None = None
    since: datetime | None = None

    def __post_init__(self) -> None:
        """Refuse a blank code."""
        super().__post_init__()
        optional_text("pipeline_code", self.pipeline_code)


@dataclass(frozen=True, kw_only=True)
class ValidateInput(TagFilter):
    """Every active pipeline's metadata, or one pipeline's."""

    pipeline_code: str | None = None

    def __post_init__(self) -> None:
        """Refuse a blank code."""
        super().__post_init__()
        optional_text("pipeline_code", self.pipeline_code)


@dataclass(frozen=True, kw_only=True)
class LineageInput(Request):
    """Which stored or freshly traced lineage to read."""

    table: str | None = None
    column: str | None = None
    direction: LineageDirection = LineageDirection.BOTH
    depth: int | None = None
    as_of: str | None = None
    version: int | None = None

    def __post_init__(self) -> None:
        """Reject incompatible selectors before reading the Engine DB."""
        if self.column and not self.table:
            raise UsageError("`column` needs `table`")
        if self.depth is not None and self.depth < 1:
            raise UsageError(f"`depth` must be 1 or more, got {self.depth}")
        if self.as_of is not None and self.version is not None:
            raise UsageError("`as_of` and `version` are mutually exclusive; choose one")
        if self.version is not None and (self.table is None or self.version < 1):
            raise UsageError(
                f"`version` needs `table` and a positive version number; got table "
                f"{self.table!r}, version {self.version}; choose a table and version"
            )

    @property
    def upstream(self) -> bool:
        """Whether the trace follows sources."""
        return self.direction != LineageDirection.DOWNSTREAM

    @property
    def downstream(self) -> bool:
        """Whether the trace follows consumers."""
        return self.direction != LineageDirection.UPSTREAM


@dataclass(frozen=True, kw_only=True)
class LineageRefreshInput(LineageInput):
    """Work every task's lineage out again, then trace it."""

    def __post_init__(self) -> None:
        """Refuse history: a refresh traces the current lineage."""
        super().__post_init__()
        if self.as_of is not None or self.version is not None:
            raise UsageError("a lineage refresh cannot read history; omit `as_of` and `version`")


@dataclass(frozen=True, kw_only=True)
class RunRequest(RunSelection):
    """One pipeline, task, lifecycle step or backfill request.

    ``reason`` is recorded with every request; overrides, reruns, skips and backfills need one.
    """

    task_code: str | None = None
    standalone: bool = False
    init_only: bool = False
    force: bool = False
    ignore_dependencies: bool = False
    rerun: bool = False
    with_downstream: bool = False
    skip: bool = False
    run_date: date | None = None
    backfill_from: date | None = None
    backfill_to: date | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        """Refuse incompatible requests before any resources are opened."""
        super().__post_init__()
        optional_text("task_code", self.task_code)
        optional_text("reason", self.reason)
        if self.reason is not None and len(self.reason) > MAX_REASON:
            raise UsageError(f"`reason` has {len(self.reason)} characters; give at most 4096")
        if self.standalone and (not self.task_code or self.force or self.rerun):
            raise UsageError("`standalone` requires `task_code`, without `force` or `rerun`")
        if self.task_code and self.init_only:
            raise UsageError("choose one of `task_code` or `init_only`")
        if self.force and self.init_only:
            raise UsageError("`force` runs tasks; it does not apply to `init_only`")
        if (self.ignore_dependencies or self.rerun) and not self.task_code:
            raise UsageError(
                "`ignore_dependencies` and `rerun` apply to one task: give `task_code`"
            )
        if self.ignore_dependencies and self.rerun:
            raise UsageError("`rerun` already runs the task without checking its dependencies")
        if self.force and (self.ignore_dependencies or self.rerun):
            option = "rerun" if self.rerun else "ignore_dependencies"
            raise UsageError(f"`{option}` and `force` are different overrides: choose one")
        if self.with_downstream and not (self.rerun or self.standalone):
            raise UsageError("`with_downstream` goes with `rerun` or `standalone`")
        if self.skip and (self.task_code or self.init_only or self.force):
            raise UsageError("`skip` records a whole run SKIPPED; it takes only `reason`")
        self._check_backfill()

    def _check_backfill(self) -> None:
        if (self.backfill_from is None) != (self.backfill_to is None):
            raise UsageError("a backfill needs both `backfill_from` and `backfill_to`")
        if self.backfill and (self.task_code or self.init_only or self.force or self.skip):
            raise UsageError("a backfill runs the whole pipeline once per date; it takes `reason`")
        if (self.run_id is not None or self.run_key is not None) and (self.backfill or self.skip):
            raise UsageError("a backfill and `skip` create new runs; omit `run_id` and `run_key`")
        if self.run_date and (self.skip or self.backfill):
            raise UsageError("`run_date` cannot be combined with `skip` or a backfill")

    @property
    def backfill(self) -> tuple[date, date] | None:
        """The inclusive date range of a backfill request."""
        if self.backfill_from is None or self.backfill_to is None:
            return None
        return self.backfill_from, self.backfill_to


@dataclass(frozen=True, kw_only=True)
class MarkRequest(RunSelection):
    """A run, task or new stand-in run's status, or a task's watermark or offset reset."""

    reason: str
    task_code: str | None = None
    status: RunStatus | None = None
    watermark: str | None = None
    offset: str | None = None
    rows: int | None = None
    stale: bool = False
    new_run: bool = False

    def __post_init__(self) -> None:
        """Refuse a mark that names two outcomes or none."""
        super().__post_init__()
        require_text("reason", self.reason, limit=MAX_REASON)
        optional_text("task_code", self.task_code)
        if sum(value is not None for value in (self.status, self.watermark, self.offset)) != 1:
            raise UsageError("mark needs exactly one of `status`, `watermark` or `offset`")
        if self.status is not None and self.status not in MARKABLE_STATUSES:
            raise UsageError(
                f"`status` is {self.status!r}; choose one of {', '.join(MARKABLE_STATUSES)}"
            )
        if self.rows is not None and self.rows < 0:
            raise UsageError(f"`rows` is {self.rows}; give zero or more")
        for name in ("watermark", "offset"):
            if getattr(self, name) is not None and (
                not self.task_code or self.rows is not None or self.stale or self.new_run
            ):
                raise UsageError(
                    f"`{name}` needs `task_code`, without `status`, `rows`, `stale` or `new_run`"
                )
        if self.stale and (not self.task_code or self.new_run):
            raise UsageError("`stale` marks one task: give `task_code`, without `new_run`")
        if self.new_run and (self.run_id is not None or self.run_key is not None):
            raise UsageError("`new_run` creates a stand-in run; omit `run_id` and `run_key`")


@dataclass(frozen=True, kw_only=True)
class ReconcileInput(Request):
    """Fence expired owners, optionally within one pipeline or task."""

    pipeline_code: str | None = None
    task_code: str | None = None

    def __post_init__(self) -> None:
        """Refuse a task without the pipeline it belongs to."""
        optional_text("pipeline_code", self.pipeline_code)
        optional_text("task_code", self.task_code)
        if self.task_code and self.pipeline_code is None:
            raise UsageError("`task_code` needs `pipeline_code`")


@dataclass(frozen=True, kw_only=True)
class ControlRequest(RunSelection):
    """One exact run or task scope, or a definition pause; repeated keys keep their identity."""

    action: ControlAction
    reason: str
    task_code: str | None = None
    request_key: str | None = None

    def __post_init__(self) -> None:
        """Validate the action boundary before changing any admission or attempt state."""
        super().__post_init__()
        require_text("reason", self.reason, limit=MAX_REASON)
        optional_text("task_code", self.task_code)
        if self.request_key is not None:
            require_text("request_key", self.request_key, limit=128)


@dataclass(frozen=True, kw_only=True)
class BulkControlRequest(Request):
    """A bounded explicit selection whose objects succeed or fail independently."""

    requests: tuple[ControlRequest, ...]

    def __post_init__(self) -> None:
        """Every object names its run and key, so no object falls back to an active run."""
        if not 1 <= len(self.requests) <= 100:
            raise UsageError(
                f"`requests` has {len(self.requests)} objects; give 1 to 100 explicit objects"
            )
        for request in self.requests:
            if request.action in DEFINITION_CONTROLS:
                if (
                    request.run_id is not None
                    or request.run_key is not None
                    or request.task_code is not None
                ):
                    raise UsageError(
                        "definition controls name only `pipeline_code`; "
                        "omit run and task identities"
                    )
                continue
            if request.request_key is None or (request.run_id is None and request.run_key is None):
                raise UsageError(
                    "every object in `requests` needs `run_id` or `run_key` and `request_key`; "
                    "implicit active-run selection is unsafe across a batch"
                )
        if len({r.request_key for r in self.requests if r.request_key is not None}) != sum(
            r.request_key is not None for r in self.requests
        ):
            raise UsageError("`request_key` values repeat; give each selected object a unique key")
