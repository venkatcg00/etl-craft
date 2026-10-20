"""Validated retained SSO group identities, without token values or public token fields."""

import json

from sqlalchemy.engine import Connection

from etl_craft.core.errors import MetadataError
from etl_craft.engine.queries import statement


def decode_groups(raw: str, object_name: str) -> tuple[int, ...]:
    """Refuse corrupt stored claims rather than changing the admitted identity."""
    try:
        value = json.loads(raw)
    except ValueError as error:
        raise MetadataError(
            f"{object_name}: SSO_GROUP_IDS={raw!r}; expected a JSON list of "
            "positive group ids; restore the audited claim value"
        ) from error
    if not isinstance(value, list) or any(type(i) is not int or i < 1 for i in value):
        raise MetadataError(
            f"{object_name}: SSO_GROUP_IDS={raw!r}; expected a JSON list of "
            "positive group ids; restore the audited claim value"
        )
    return tuple(value)


def token_groups(conn: Connection, token_id: int) -> tuple[int, ...]:
    """Read claims only after the credential has been authenticated."""
    raw = conn.execute(statement(conn, "token_group_claims"), {"token_id": token_id}).scalar_one()
    return decode_groups(raw, f"token_id={token_id}")
