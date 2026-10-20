"""Bind a proven credential's retained claims to its operation context."""

from dataclasses import replace

from etl_craft.core.enums import ApiRole
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.tokens import TokenView


def authenticated_context(ctx: OperationContext, identity: TokenView) -> OperationContext:
    """Use the current account role and the groups proven by this session's issuer."""
    return replace(
        ctx,
        actor=identity.actor,
        role=ApiRole(identity.role),
        credential_token_id=identity.token_id,
    )
