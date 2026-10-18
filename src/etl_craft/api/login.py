"""Sign-in routes: the same session operations the CLI and the UI call."""

from typing import Any

from fastapi import FastAPI, Response
from pydantic import BaseModel, ConfigDict, Field

from etl_craft.services.operations import OperationContext, sessions, to_json


class Password(BaseModel):
    """An account name and its password, sent once over HTTPS."""

    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=1, max_length=4096)


class SsoPoll(BaseModel):
    """The device code and nonce returned by the SSO start route."""

    model_config = ConfigDict(extra="forbid")
    device_code: str = Field(min_length=1, max_length=65536)
    nonce: str = Field(min_length=1, max_length=256)


class Token(BaseModel):
    """A refresh token to renew access with, or a session token to revoke."""

    model_config = ConfigDict(extra="forbid")
    token: str = Field(min_length=1, max_length=256)


def add_login_routes(app: FastAPI, ctx: OperationContext) -> None:
    """Add the routes a person signs in, renews and signs out with; none needs a bearer token."""

    @app.get("/api/v1/login/methods")
    def methods() -> dict[str, Any]:
        return to_json(sessions.sign_in_methods(ctx))

    @app.post("/api/v1/login/password")
    def password(body: Password) -> dict[str, Any]:
        return to_json(sessions.sign_in(ctx, body.name, body.password, refreshable=True))

    @app.post("/api/v1/login/sso")
    def sso() -> dict[str, Any]:
        return to_json(sessions.start_sso(ctx))

    @app.post("/api/v1/login/sso/poll")
    def sso_poll(body: SsoPoll) -> dict[str, Any]:
        return to_json(sessions.poll_sso(ctx, body.device_code, body.nonce, refreshable=True))

    @app.post("/api/v1/login/refresh")
    def refresh(body: Token) -> dict[str, Any]:
        return to_json(sessions.refresh(ctx, body.token))

    @app.post("/api/v1/login/revoke", status_code=204)
    def revoke(body: Token) -> Response:
        sessions.sign_out(ctx, body.token)
        return Response(status_code=204)
