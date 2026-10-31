"""Closed values for project environments, governance and pipeline access.

Split from ``core.enums`` by concern; the same rules apply to every closed set here.
"""

from __future__ import annotations

from enum import StrEnum


class AccessIntent(StrEnum):
    """Bulk access command intent."""

    GRANT = "GRANT"
    REVOKE = "REVOKE"


class AccessChange(StrEnum):
    """The actual change relative to current access rows."""

    ADDED = "added"
    CHANGED_PERMISSION = "changed permission"
    REVOKED = "revoked"
    UNCHANGED = "unchanged"


class Permission(StrEnum):
    """Increasing permissions on a pipeline or tag."""

    CAN_VIEW = "CAN_VIEW"
    CAN_UPLOAD = "CAN_UPLOAD"
    CAN_RUN = "CAN_RUN"
    CAN_MANAGE = "CAN_MANAGE"


class EnvironmentType(StrEnum):
    """Administrator-selected purpose of a project."""

    DEV = "DEV"
    SIT = "SIT"
    UAT = "UAT"
    PROD = "PROD"
    SANDBOX = "SANDBOX"
    CUSTOM = "CUSTOM"


class Governance(StrEnum):
    """Project intervention policy; authorization is independent."""

    STRICT = "STRICT"
    RELAXED = "RELAXED"


class ActorKind(StrEnum):
    """The kind of account responsible for an action."""

    USER = "USER"
    SERVICE = "SERVICE"
    SYSTEM = "SYSTEM"


class TokenKind(StrEnum):
    """What a stored credential is for: automation, a request, or renewing a user's access."""

    AGENT = "AGENT"
    ATTEMPT = "ATTEMPT"
    SERVICE = "SERVICE"
    ACCESS = "ACCESS"
    REFRESH = "REFRESH"


class AccountKind(StrEnum):
    """Whether a registered account represents a person or an automated caller."""

    USER = "USER"
    SERVICE = "SERVICE"


class ApiRole(StrEnum):
    """A deployment account role, each including the ones before it."""

    AGENT = "agent"
    VIEWER = "viewer"
    OPERATOR = "operator"
    DEVELOPER = "developer"
    ADMIN = "admin"


class ConnectionUse(StrEnum):
    """Separately bounded Engine DB sessions in one serving process."""

    TASK = "TASK"
    PROBE = "PROBE"
    HEALTH = "HEALTH"


class ManagedProvider(StrEnum):
    """Platforms that mint short-lived PostgreSQL database credentials."""

    NONE = "NONE"
    SNOWFLAKE = "SNOWFLAKE"
    DATABRICKS = "DATABRICKS"


class ProcessRole(StrEnum):
    """Processes that reserve Engine DB capacity; API-only agents reserve none."""

    CONTROLLER = "CONTROLLER"
    UI = "UI"
    RUN = "RUN"
