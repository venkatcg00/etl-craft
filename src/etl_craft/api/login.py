"""Sign-in routes: the same session actions the CLI and the UI call, with no bearer token."""

from fastapi import FastAPI

from etl_craft.api.routes import add_action_routes
from etl_craft.core.enums import ActionName
from etl_craft.services.operations import OperationContext

SIGN_IN = frozenset(
    {
        ActionName.LOGIN_METHODS,
        ActionName.LOGIN,
        ActionName.LOGIN_SSO,
        ActionName.LOGIN_SSO_POLL,
        ActionName.REFRESH,
        ActionName.LOGOUT,
    }
)
"""The actions a person signs in, renews and signs out with."""


def add_login_routes(app: FastAPI, ctx: OperationContext) -> None:
    """Add the routes a person signs in, renews and signs out with."""
    add_action_routes(app, ctx, SIGN_IN)
