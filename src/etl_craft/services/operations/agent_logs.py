"""Ordered shared log chunks, fenced and committed in the Engine DB until external storage."""

from etl_craft.core.errors import UsageError
from etl_craft.core.text import mask_arguments, sha256_hex
from etl_craft.engine.agent_state import fence
from etl_craft.engine.queries import statement
from etl_craft.services.operations.agent_models import AgentLog, AgentReply
from etl_craft.services.operations.agents import check_agent
from etl_craft.services.operations.context import OperationContext


def upload(ctx: OperationContext, request: AgentLog) -> AgentReply:
    """Acknowledge an identical retry; never replace a committed chunk or leave a sequence gap."""
    agent_id = check_agent(ctx, request)
    content = str(mask_arguments({"message": request.content})["message"])
    data = content.encode()
    if len(data) > 65536:
        raise UsageError(
            f"attempt {request.attempt_id}: masked log chunk exceeds 65536 bytes; "
            "send a smaller chunk"
        )
    digest = sha256_hex(data)
    with ctx.engine.begin() as conn:
        fence(conn, request.attempt_id, str(agent_id))
        previous = conn.execute(
            statement(conn, "agent_chunk"),
            {
                "attempt_id": request.attempt_id,
                "sequence": request.sequence,
            },
        ).one_or_none()
        if previous is not None:
            if previous.content_sha256 != digest:
                raise UsageError(
                    f"attempt {request.attempt_id}, sequence {request.sequence}: hash differs; "
                    "resend the original chunk"
                )
            return AgentReply()
        last = conn.execute(
            statement(conn, "agent_chunk_last"), {"attempt_id": request.attempt_id}
        ).one_or_none()
        expected = 0 if last is None else last.sequence_number + 1
        if request.sequence != expected:
            raise UsageError(
                f"attempt {request.attempt_id}, sequence {request.sequence}: expected {expected}; "
                "upload chunks in order"
            )
        offset = 0 if last is None else last.byte_offset + last.byte_length
        conn.execute(
            statement(conn, "agent_chunk_insert"),
            {
                "attempt_id": request.attempt_id,
                "sequence": request.sequence,
                "hash": digest,
                "content": content,
                "offset": offset,
                "length": len(data),
                "now": conn.execute(statement(conn, "agent_now")).scalar_one(),
            },
        )
    return AgentReply()
