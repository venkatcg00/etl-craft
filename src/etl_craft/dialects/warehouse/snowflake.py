"""Snowflake warehouse, ordinary tables."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from etl_craft.config.auth import warehouse_by_key
from etl_craft.core.enums import AuthMode
from etl_craft.dialects.warehouse.base import Presented, SurrogateKey, WarehouseDialect

if TYPE_CHECKING:
    from etl_craft.config import ConnectionProfile
    from etl_craft.config.targets import WarehouseUrl


class SnowflakeWarehouse(WarehouseDialect):
    """Snowflake, ordinary tables."""

    spec = warehouse_by_key("snowflake")
    surrogate_key: SurrogateKey = "computed"
    enforces_primary_keys = False
    key_file_connect_args = ("private_key_file", "private_key_file_pwd")

    def present(self, profile: ConnectionProfile, secret: str, url: WarehouseUrl) -> Presented:
        """Name the connector's own authenticator for oauth, sso and sts."""
        user = profile.user or None
        mode = profile.auth_mode
        if mode == AuthMode.OAUTH:
            connect_args: dict[str, Any] = {
                "authenticator": "OAUTH_CLIENT_CREDENTIALS",
                "oauth_client_id": str(profile.extra["client_id"]),
                "oauth_client_secret": secret,
                "oauth_token_request_url": str(profile.extra["token_url"]),
            }
            if profile.extra.get("scope"):
                connect_args["oauth_scope"] = str(profile.extra["scope"])
            return Presented(username=user, connect_args=connect_args)
        if mode == AuthMode.SSO:
            return Presented(username=user, connect_args={"authenticator": "externalbrowser"})
        if mode == AuthMode.STS:
            return Presented(
                username=user,
                connect_args={
                    "authenticator": "WORKLOAD_IDENTITY",
                    "workload_identity_provider": "AWS",
                },
            )
        return super().present(profile, secret, url)
