"""Trino over an Iceberg catalog.

On Trino the table format is a property of the *catalog* in the connection,
not of the statement: a table created in an Iceberg catalog is Iceberg by
construction, so CREATE TABLE takes no format clause (adding one is a syntax
error). ``validate`` checks the catalog really is Iceberg (E2-69). Every
difference below was found by running the action vocabulary against a real
Trino/Iceberg stack, not by reading documentation.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from etl_craft.dialects.warehouse_dialects.base import Presented, WarehouseDialect

if TYPE_CHECKING:
    from etl_craft.config import ConnectionProfile


class TrinoIcebergWarehouse(WarehouseDialect):
    """Trino, Iceberg catalog."""

    key = "trino_iceberg"
    display_name = "Trino"
    sqlalchemy_name = "trino"
    table_format = "iceberg"
    # No temporary tables at all ("mismatched input" on CREATE TEMPORARY TABLE).
    # The stage is uniquely named per task run and dropped on every path, so
    # an ordinary table behaves the same.
    temporary_tables = False
    # `UPDATE tbl t SET` and `DELETE FROM tbl t` are syntax errors; the target
    # is qualified by its own table name instead (E2-65).
    mutation_alias = False
    qualified_rename = True
    # Iceberg has no identity columns, sequences or constraints.
    surrogate_key = "computed"
    enforces_primary_keys = False
    # [ADDITION, 2026-09-24] Everything the Trino client offers. Each
    # credential goes in the URL query the trino dialect's own
    # create_connect_args reads (access_token -> JWTAuthentication, cert/key ->
    # CertificateAuthentication, externalAuthentication -> OAuth2
    # Authentication), so no Trino class is imported here. That URL is built
    # inside the connection creator and is never the Engine's logged one.
    auth_fields: Mapping[str, tuple[str, ...]] = {
        "none": (),
        "password": ("user", "secret"),
        "token": ("secret",),
        "oauth": ("client_id", "secret", "token_url"),
        # The cluster's OAuth 2.0 browser redirect: interactive only.
        "sso": (),
        # A client certificate; Trino's client takes no key passphrase.
        "key_file": ("key_file", "cert_file"),
    }
    verified_auth_modes = frozenset({"none"})
    bearer_needs_user = False

    def present(
        self, profile: ConnectionProfile, secret: str, parts: Mapping[str, Any]
    ) -> Presented:
        """Hand the credential to the Trino client through the query it reads."""
        user = profile.user or None
        mode = profile.auth_mode
        if mode == "token":
            return Presented(username=user, query={"access_token": secret})
        if mode == "oauth":
            token = self.oauth_token(profile, secret, parts)
            return Presented(username=user, query={"access_token": token})
        if mode == "sso":
            return Presented(username=user, query={"externalAuthentication": "true"})
        if mode == "key_file":
            return Presented(
                username=user,
                query={
                    "cert": str(profile.extra["cert_file"]),
                    "key": str(profile.extra["key_file"]),
                },
            )
        return super().present(profile, secret, parts)

    def hash_expression(self, values: list[str]) -> str:
        """Hex-encode md5(), which takes and returns varbinary on Trino."""
        return f"lower(to_hex(md5(to_utf8({self._hash_input(values)}))))"
