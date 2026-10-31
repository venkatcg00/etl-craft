"""Stream stored upload bytes only to the attempt credential that owns their run."""

from collections.abc import Iterator
from dataclasses import dataclass

from etl_craft.core.enums import ActionName, AgentStatus, ApiRole
from etl_craft.core.errors import PermissionDeniedError, StaleTransitionError, UsageError
from etl_craft.engine.agent_state import fence
from etl_craft.engine.queries import statement
from etl_craft.engine.task_store import TaskIdentity
from etl_craft.engine.upload_files import stream_upload
from etl_craft.services.operations.agent_models import AgentRenew, AgentRequest
from etl_craft.services.operations.agents import check_agent
from etl_craft.services.operations.context import OperationContext


@dataclass(frozen=True, kw_only=True)
class AgentUpload(AgentRequest):
    """Identify the exact leased attempt and one of its run's stored files."""

    attempt_id: int
    upload_id: int

    def __post_init__(self) -> None:
        """Bound both identities before database lookup."""
        if not 0 < self.attempt_id < 2**63 or not 0 < self.upload_id < 2**63:
            raise UsageError(
                f"upload {self.upload_id}, attempt {self.attempt_id}: expected positive "
                "64-bit identities; use the recorded upload and claimed attempt ids"
            )


def required_role(ctx: OperationContext, request: object) -> ApiRole:
    """Refuse every credential except this upload reader's own attempt token."""
    assert isinstance(request, AgentUpload)
    if ctx.role != ApiRole.AGENT or ctx.credential_attempt_id != request.attempt_id:
        raise PermissionDeniedError(
            f"upload {request.upload_id}, attempt {request.attempt_id}, run unknown: "
            "credential is not this attempt's token; expected an attempt token with a "
            "live lease; use the credential issued when the attempt was claimed"
        )
    return ApiRole.AGENT


def content(ctx: OperationContext, request: AgentUpload) -> Iterator[bytes]:
    """Require an attempt token, then use the same upload read as local execution."""
    required_role(ctx, request)
    owner = str(check_agent(ctx, request))
    with ctx.engine.begin() as conn:
        work = conn.execute(statement(conn, "agent_work"), {"attempt_id": request.attempt_id}).one()
        try:
            task_run_id = fence(conn, request.attempt_id, owner)
        except StaleTransitionError as error:
            raise PermissionDeniedError(
                f"upload {request.upload_id}, attempt {request.attempt_id}, "
                f"run {work.pipeline_run_id}: lease is expired or ownership changed; "
                "expected the live owning attempt; reconcile before retrying"
            ) from error
    identity = TaskIdentity(
        int(work.pipeline_id),
        int(work.task_id),
        int(work.pipeline_run_id),
        task_run_id,
        request.attempt_id,
        owner,
    )
    return stream_upload(ctx.engine, ctx.config, identity, request.upload_id)


def attempt_request(ctx: OperationContext, name: ActionName, request: object) -> bool:
    """Admit only the exact attempt operations permitted by its credential."""
    if name == ActionName.AGENT_UPLOAD_CONTENT:
        return True
    if name in (ActionName.AGENT_REPORT, ActionName.AGENT_LOG):
        return getattr(request, "attempt_id", None) == ctx.credential_attempt_id
    return (
        name == ActionName.AGENT_RENEW
        and isinstance(request, AgentRenew)
        and request.attempts == (ctx.credential_attempt_id,)
        and not request.acknowledged_commands
        and request.status == AgentStatus.ACTIVE
    )
