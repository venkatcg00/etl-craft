"""An account's own password and validated personal display settings."""

import json
from dataclasses import asdict, dataclass
from typing import ClassVar
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from etl_craft.core.actor import ActorKind
from etl_craft.core.enums import AccountKind, ActionName, Theme, TimeDisplay, YesNo
from etl_craft.core.errors import MetadataError, PermissionDeniedError, UsageError
from etl_craft.core.text import is_metadata_code
from etl_craft.engine import locks
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.accounts import find_account
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.requests import Request
from etl_craft.services.passwords import DECOY, hash_password, verify_password


@dataclass(frozen=True, kw_only=True)
class Settings(Request):
    """Only display preferences used by the current UI."""

    SCHEMA: ClassVar[str] = "etl-craft/settings/1"
    theme: Theme = Theme.SYSTEM
    timezone: str = "UTC"
    default_project: str | None = None
    page_size: int = 25
    time_display: TimeDisplay = TimeDisplay.RELATIVE

    def __post_init__(self) -> None:
        """Refuse invalid preferences or unbounded requests before database work."""
        if self.theme not in tuple(Theme) or self.time_display not in tuple(TimeDisplay):
            raise UsageError("settings: invalid theme or time display; choose a listed option")
        try:
            ZoneInfo(self.timezone)
        except (ValueError, ZoneInfoNotFoundError) as error:
            raise UsageError(
                f"timezone={self.timezone!r}; expected an IANA timezone; choose UTC or an IANA name"
            ) from error
        if not 1 <= self.page_size <= 100:
            raise UsageError(
                f"page_size={self.page_size}; expected 1 to 100; choose a smaller page"
            )
        if self.default_project is not None and not is_metadata_code(self.default_project):
            raise UsageError(
                f"default_project={self.default_project!r}; expected a valid project code; "
                "choose an accessible project or leave it empty"
            )


@dataclass(frozen=True, kw_only=True)
class SettingsPatch(Request):
    """Only supplied preferences change; an empty default project clears it."""

    theme: Theme | None = None
    timezone: str | None = None
    default_project: str | None = None
    page_size: int | None = None
    time_display: TimeDisplay | None = None


@dataclass(frozen=True)
class PasswordChanged:
    """Acknowledgement after replacement and session revocation commit."""

    SCHEMA: ClassVar[str] = "etl-craft/password-changed/1"
    message: str = "Password changed. Sign in again with your new password."


@dataclass(frozen=True, kw_only=True)
class ChangePassword(Request):
    """Proof of the current local password and its replacement."""

    current_password: str
    password: str


def settings_from(value: str) -> Settings:
    """Refuse malformed stored preferences instead of silently dropping fields."""
    try:
        fields = json.loads(value)
        if not isinstance(fields, dict):
            raise UsageError("expected a JSON object")
        if "theme" in fields:
            fields["theme"] = Theme(fields["theme"])
        if "time_display" in fields:
            fields["time_display"] = TimeDisplay(fields["time_display"])
        return Settings(**fields)
    except (TypeError, ValueError, UsageError) as error:
        raise MetadataError(
            f"UI_SETTINGS={value!r}: expected validated personal settings; "
            "replace all five preferences with `etl-craft account settings --theme system "
            "--timezone UTC --default-project '' --page-size 25 --time-display relative`"
        ) from error


def save_settings(ctx: OperationContext, request: SettingsPatch) -> Settings:
    """Change only the requesting user's settings, never another account's."""
    if request.default_project:
        from dataclasses import replace

        from etl_craft.services.operations.permissions import project_not_found, project_permission

        with ctx.engine.connect() as conn:
            project = conn.execute(
                statement(conn, "project_by_code"), {"code": request.default_project}
            ).one_or_none()
            if (
                project is None
                or project.active_flag != YesNo.YES
                or project_permission(
                    conn,
                    replace(ctx, project=request.default_project, project_id=project.project_id),
                )
                is None
            ):
                raise project_not_found(request.default_project)
    with locks.ACCOUNTS.hold(ctx.engine), ctx.engine.begin() as conn:
        row = find_account(conn, ctx.actor.name)
        if row is None or row.kind != AccountKind.USER:
            raise UsageError("personal settings require an active USER account; sign in as a user")
        supplied = {key: value for key, value in asdict(request).items() if value is not None}
        values = (
            {}
            if len(supplied) == len(asdict(Settings()))
            else asdict(settings_from(row.ui_settings))
        )
        values.update(supplied)
        if values["default_project"] == "":
            values["default_project"] = None
        settings = Settings(**values)
        found = conn.execute(
            statement(conn, "account_settings"),
            {"name": ctx.actor.name, "settings": json.dumps(asdict(settings))},
        ).scalar_one_or_none()
        if found is None:
            raise UsageError("personal settings require an active USER account; sign in as a user")
    return settings


def change_password(ctx: OperationContext, request: ChangePassword) -> PasswordChanged:
    """Check outside the account lock; replace and revoke sessions together under it."""
    with ctx.engine.connect() as conn:
        row = find_account(conn, ctx.actor.name)
    local = (
        row is not None
        and row.kind == AccountKind.USER
        and row.oidc_subject is None
        and row.active_flag == YesNo.YES
    )
    valid = verify_password(
        request.current_password, row.password_hash if local and row is not None else DECOY
    )
    if not local or not valid:
        raise UsageError("password change refused; expected your current local password; retry it")
    assert row is not None
    password_hash = hash_password(request.password)
    with locks.ACCOUNTS.hold(ctx.engine), ctx.engine.begin() as conn:
        current = find_account(conn, ctx.actor.name)
        if current != row:
            raise UsageError("account changed during password verification; sign in and retry")
        conn.execute(
            statement(conn, "account_change_password"),
            {"name": ctx.actor.name, "password_hash": password_hash},
        )
        from datetime import UTC, datetime

        conn.execute(
            statement(conn, "account_revoke"),
            {"account_id": row.account_id, "now": datetime.now(UTC)},
        )
    return PasswordChanged()


def require_password_change(ctx: OperationContext, action: ActionName) -> None:
    """Temporary credentials authorize only identity, renewal, sign-out and password change."""
    if ctx.upgrade_signin and action == ActionName.MIGRATE:
        return
    if ctx.actor.kind != ActorKind.USER or action in {
        ActionName.LOGIN_METHODS,
        ActionName.LOGIN,
        ActionName.LOGIN_SSO,
        ActionName.LOGIN_SSO_POLL,
        ActionName.REFRESH,
        ActionName.LOGOUT,
        ActionName.WHOAMI,
        ActionName.PASSWORD_CHANGE,
    }:
        return
    with ctx.engine.connect() as conn:
        row = find_account(conn, ctx.actor.name)
    if row is not None and row.must_change_password == YesNo.YES:
        raise PermissionDeniedError(
            f"account {ctx.actor.name!r}: temporary password requires replacement; expected a "
            "personal password; run `etl-craft account change-password` or use the password page"
        )
