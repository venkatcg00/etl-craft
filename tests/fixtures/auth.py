"""Explicit credentials for projects exercised through the real command boundary."""

import secrets

from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.core.enums import AccountKind, ApiRole
from etl_craft.core.text import sha256_hex
from etl_craft.engine.queries import statement


def service_token(engine, name="test-admin"):
    """Provision fixture credentials with the local Engine DB writer, outside action assertions."""
    with acting_as(SYSTEM_ACTOR), engine.begin() as conn:
        account = conn.execute(
            statement(conn, "account_add"),
            {
                "name": name,
                "kind": AccountKind.SERVICE,
                "role": ApiRole.ADMIN,
                "email": None,
                "password_hash": None,
                "oidc_subject": None,
                "actor": SYSTEM_ACTOR.name,
            },
        ).scalar_one()
        token = secrets.token_urlsafe(32)
        from datetime import UTC, datetime

        conn.execute(
            statement(conn, "api_token_create"),
            {
                "name": name,
                "account_id": account,
                "actor": SYSTEM_ACTOR.name,
                "digest": sha256_hex(token.encode()),
                "now": datetime.now(UTC),
                "expires_at": None,
            },
        )
        return token
