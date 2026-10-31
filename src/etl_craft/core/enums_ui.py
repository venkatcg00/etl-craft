"""Closed values for the browser UI: personal display settings, search and announcements.

Split from ``core.enums`` by concern; the same rules apply to every closed set here.
"""

from __future__ import annotations

from enum import StrEnum


class Theme(StrEnum):
    """System-selected appearance or an explicit override."""

    SYSTEM = "system"
    LIGHT = "light"
    DARK = "dark"


class TimeDisplay(StrEnum):
    """Display an instant relatively or as an absolute zoned time."""

    RELATIVE = "relative"
    ABSOLUTE = "absolute"


class SearchKind(StrEnum):
    """The kinds of object one search box finds."""

    PIPELINE = "PIPELINE"
    TASK = "TASK"
    TABLE = "TABLE"
    RUN = "RUN"


class AnnouncementLevel(StrEnum):
    """How urgent an administrator's announcement banner is."""

    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
