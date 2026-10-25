"""One authorized purge operation for local, HTTP and scheduled requests."""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import ClassVar
from zoneinfo import ZoneInfo

from etl_craft.core.enums import ActionName, ActionOutcome
from etl_craft.core.errors import EtlCraftError, ExitCode
from etl_craft.engine.audit import ActionRequest
from etl_craft.engine.locks import EngineLock, advisory_key
from etl_craft.engine.queries import statement
from etl_craft.services.archives import archive_session
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.requests import Request
from etl_craft.services.retention import purge_history
from etl_craft.services.warehouse_retention import auxiliary_session


@dataclass(frozen=True, kw_only=True)
class PurgeInput(Request):
    """A read-only eligibility snapshot, or committed history/file cleanup."""

    dry_run: bool = False
    archive_to: str | None = None


@dataclass(frozen=True)
class PurgeView:
    """Rows currently removable or actually removed, by history table."""

    SCHEMA: ClassVar[str] = "etl-craft/purge/1"
    dry_run: bool
    counts: dict[str, int]


def purge(ctx: OperationContext, request: PurgeInput) -> PurgeView:
    """Serialize purges for one project without holding execution or target locks."""
    lock = EngineLock(
        f"retention project {ctx.project_id}", advisory_key(f"retention:{ctx.project_id}")
    )
    completion = ActionRequest(
        ActionName.PURGE,
        {"dry_run": request.dry_run, "archive_to": request.archive_to},
        datetime.now(UTC),
        actor=ctx.actor,
        engine=ctx.engine,
        project_id=ctx.project_id,
        outcome=ActionOutcome.FAILURE,
        exit_code=ExitCode.UNEXPECTED,
    )
    try:
        with (
            lock.hold(ctx.engine),
            archive_session(
                ctx.engine,
                ctx.config,
                request.archive_to,
                dry_run=request.dry_run,
            ) as archive,
            auxiliary_session(ctx.engine, ctx.config, archive) as auxiliary,
        ):
            now = datetime.now(UTC)
            result = PurgeView(
                request.dry_run,
                purge_history(
                    ctx.engine,
                    ctx.config,
                    now,
                    dry_run=request.dry_run,
                    archive=archive,
                ),
            )
            if auxiliary is not None:
                result.counts.update(auxiliary.run(now, dry_run=request.dry_run))
            if not request.dry_run:
                with ctx.engine.begin() as conn:
                    conn.execute(
                        statement(conn, "retention_tick_save"),
                        {
                            "date": now.astimezone(ZoneInfo(ctx.config.timezone)).date(),
                        },
                    )
            completion.outcome, completion.exit_code = ActionOutcome.SUCCESS, ExitCode.SUCCESS
            completion.arguments["counts"] = result.counts
            return result
    except EtlCraftError as error:
        completion.exit_code = error.exit_code
        raise
    finally:
        completion.record()
