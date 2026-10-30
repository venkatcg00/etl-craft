"""Server-sent change notices for an open monitor page.

Every few seconds the stream re-reads the documents the page shows, as the signed-in caller, and
sends ``changed`` when they differ from the last read or ``checked`` when they do not. The page
then fetches itself again; the stream carries no data of its own.
"""

import asyncio
import hashlib
import json
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass, replace
from datetime import UTC, datetime

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response, StreamingResponse
from starlette.concurrency import run_in_threadpool

from etl_craft.api import browser_sessions as browser
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import EtlCraftError, UsageError
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.requests import (
    HistoryInput,
    PipelineInput,
    RunId,
    RunSelection,
    TagFilter,
)
from etl_craft.services.operations.serialization import encode

logger = logging.getLogger(__name__)
CHECK_SECONDS = 10
NEWEST_RUNS = 25


@dataclass(frozen=True)
class Scope:
    """The documents one page shows: the list, one pipeline, or one run of it."""

    project: str
    pipeline_code: str | None
    run_id: int | None

    def reads(self) -> tuple[tuple[ActionName, object], ...]:
        """Return the read actions whose documents the page renders."""
        code = self.pipeline_code
        if code is None:
            return ((ActionName.PIPELINES, TagFilter()),)
        if self.run_id is None:
            return (
                (ActionName.PIPELINE, PipelineInput(pipeline_code=code)),
                (ActionName.HISTORY, HistoryInput(pipeline_code=code, all=True, limit=NEWEST_RUNS)),
            )
        return (
            (ActionName.RUN_DETAIL, RunId(run_id=self.run_id)),
            (ActionName.STEPS, RunSelection(pipeline_code=code, run_id=self.run_id)),
        )


def add_live_route(app: FastAPI, ctx: OperationContext) -> None:
    """Mount the change stream used by the pipeline list, pipeline and run pages."""

    @app.get("/ui/projects/{project}/live", include_in_schema=False)
    async def live(request: Request, project: str) -> Response:
        scope = _scope(request, project)
        first = await run_in_threadpool(_fingerprint, ctx, request, scope)
        return StreamingResponse(
            _events(ctx, request, scope, first),
            media_type="text/event-stream",
            headers={"X-Accel-Buffering": "no"},
        )


def _scope(request: Request, project: str) -> Scope:
    code = request.query_params.get("pipeline") or None
    run = request.query_params.get("run")
    if run is not None and (code is None or not run.isdigit() or int(run) < 1):
        raise UsageError(
            f"run={run!r}; expected a positive run id with its pipeline; open the run page again"
        )
    return Scope(project, code, None if run is None else int(run))


def _fingerprint(ctx: OperationContext, request: Request, scope: Scope) -> str:
    """Re-authenticate and re-read as the caller, so revoked access ends the stream."""
    caller = replace(browser.cookie_context(ctx, request), project=scope.project, project_id=None)
    documents = [encode(invoke(caller, name, value)) for name, value in scope.reads()]
    return hashlib.sha256(json.dumps(documents, sort_keys=True).encode()).hexdigest()


async def _events(
    ctx: OperationContext, request: Request, scope: Scope, last: str
) -> AsyncIterator[str]:
    yield f"retry: 5000\nevent: checked\ndata: {datetime.now(UTC).isoformat()}\n\n"
    while True:
        for _ in range(CHECK_SECONDS):
            await asyncio.sleep(1)
            stopping = ctx.shutdown is not None and ctx.shutdown.is_set()
            if stopping or await request.is_disconnected():
                return
        try:
            current = await run_in_threadpool(_fingerprint, ctx, request, scope)
        except (HTTPException, EtlCraftError) as error:
            # The stream is the boundary: the page shows why live updates stopped.
            logger.info("live updates ended for %s: %s", scope, error)
            detail = error.detail if isinstance(error, HTTPException) else str(error)
            yield f"event: ended\ndata: {' '.join(str(detail).split())}; reload the page\n\n"
            return
        event = "checked" if current == last else "changed"
        last = current
        yield f"event: {event}\ndata: {datetime.now(UTC).isoformat()}\n\n"
