"""Bind a proven credential's retained claims to its operation context."""

from dataclasses import replace

from etl_craft.core.enums import ApiRole, TokenKind
from etl_craft.engine.queries import statement
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.tokens import TokenView


def authenticated_context(ctx: OperationContext, identity: TokenView) -> OperationContext:
    """Use the current account role and the groups proven by this session's issuer."""
    owner_token_id = identity.token_id
    if identity.token_kind == TokenKind.ATTEMPT:
        with ctx.engine.connect() as conn:
            owner_token_id = int(
                conn.execute(
                    statement(conn, "attempt_credential_owner"), {"attempt_id": identity.attempt_id}
                ).scalar_one()
            )
    return replace(
        ctx,
        actor=identity.actor,
        role=ApiRole(identity.role),
        credential_token_id=owner_token_id,
        credential_attempt_id=identity.attempt_id,
    )
