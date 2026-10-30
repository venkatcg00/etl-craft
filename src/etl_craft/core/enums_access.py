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
