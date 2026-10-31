"""Administrator announcements: a banner on every page from its start until its end."""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import ClassVar

from etl_craft.core.actor import current_actor
from etl_craft.core.enums_ui import AnnouncementLevel
from etl_craft.core.errors import ResourceNotFoundError, UsageError
from etl_craft.core.governance import reason_text
from etl_craft.core.time import as_utc
from etl_craft.engine.queries import statement
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.requests import Request

MAX_MESSAGE = 500
LISTED = 200


@dataclass(frozen=True, kw_only=True)
class AnnouncementAdd(Request):
    """A message, its level, when it shows and why it was posted."""

    message: str
    reason: str
    level: AnnouncementLevel = AnnouncementLevel.INFO
    starts_at: datetime | None = None
    ends_at: datetime | None = None

    def __post_init__(self) -> None:
        """Refuse an empty or long message, an unknown level and a window that ends first."""
        object.__setattr__(self, "reason", reason_text(self.reason))
        if not self.reason.strip():
            raise UsageError("announce add: `reason` is empty; say why the announcement is posted")
        if not self.message.strip() or len(self.message) > MAX_MESSAGE:
            raise UsageError(
                f"announce add: `message` has {len(self.message.strip())} characters; "
                f"expected 1 to {MAX_MESSAGE}; write a shorter message"
            )
        if self.level not in tuple(AnnouncementLevel):
            raise UsageError(
                f"announce add: `level`={self.level!r}; expected INFO, WARNING or CRITICAL"
            )
        for name in ("starts_at", "ends_at"):
            value = getattr(self, name)
            if value is not None and value.utcoffset() is None:
                raise UsageError(
                    f"announce add: `{name}`={value.isoformat()!r}; expected ISO-8601 with a "
                    "timezone offset, for example 2026-11-02T08:00:00+00:00"
                )
        start = self.starts_at or datetime.now(UTC)
        if self.ends_at is not None and self.ends_at <= start:
            raise UsageError(
                f"announce add: `ends_at`={self.ends_at.isoformat()} is not after the start "
                f"{start.isoformat()}; give an end after the start"
            )


@dataclass(frozen=True, kw_only=True)
class AnnouncementEnd(Request):
    """The announcement to take down now, and why."""

    announcement_id: int
    reason: str

    def __post_init__(self) -> None:
        """Require a reason, as for every administrator change."""
        object.__setattr__(self, "reason", reason_text(self.reason))
        if not self.reason.strip():
            raise UsageError("announce end: `reason` is empty; say why it is taken down")


@dataclass(frozen=True, kw_only=True)
class AnnouncementFilter(Request):
    """Only the announcements showing now, or ``all`` of the newest ones."""

    all: bool = False


@dataclass(frozen=True)
class Announcement:
    """One announcement and who posted or ended it."""

    announcement_id: int
    level: AnnouncementLevel
    message: str
    starts_at: datetime
    ends_at: datetime | None
    reason: str
    created_by: str
    created_at: datetime
    ended_by: str | None
    ended_at: datetime | None


@dataclass(frozen=True)
class Announcements:
    """Announcements newest first."""

    SCHEMA: ClassVar[str] = "etl-craft/announcements/1"
    announcements: tuple[Announcement, ...]


def _when(value: object) -> datetime | None:
    if value is None:
        return None
    return as_utc(value if isinstance(value, datetime) else datetime.fromisoformat(str(value)))


def read(ctx: OperationContext, request: AnnouncementFilter) -> Announcements:
    """Read the announcements showing now, or the newest of every one."""
    params = {"all": 1 if request.all else 0, "now": datetime.now(UTC), "limit": LISTED}
    with ctx.engine.connect() as conn:
        rows = conn.execute(statement(conn, "announcements"), params).all()
    return Announcements(
        tuple(
            Announcement(
                row.announcement_id,
                AnnouncementLevel(row.level),
                row.message,
                as_utc(row.starts_at),
                _when(row.ends_at),
                row.reason,
                row.created_by,
                as_utc(row.created_at),
                row.ended_by,
                _when(row.ended_at),
            )
            for row in rows
        )
    )


def add_announcement(ctx: OperationContext, request: AnnouncementAdd) -> Announcements:
    """Post an announcement; it shows on every page from its start."""
    actor = current_actor()
    now = datetime.now(UTC)
    with ctx.engine.begin() as conn:
        conn.execute(
            statement(conn, "announcement_add"),
            {
                "level": request.level,
                "message": request.message.strip(),
                "starts_at": as_utc(request.starts_at) if request.starts_at else now,
                "ends_at": as_utc(request.ends_at) if request.ends_at else None,
                "reason": request.reason,
                "actor": actor.name,
                "actor_kind": actor.kind,
                "now": now,
            },
        ).one()
    return read(ctx, AnnouncementFilter(all=True))


def end_announcement(ctx: OperationContext, request: AnnouncementEnd) -> Announcements:
    """Take an announcement down now; one already ended or unknown is refused."""
    actor = current_actor()
    with ctx.engine.begin() as conn:
        ended = conn.execute(
            statement(conn, "announcement_end"),
            {
                "announcement_id": request.announcement_id,
                "now": datetime.now(UTC),
                "actor": actor.name,
                "actor_kind": actor.kind,
            },
        ).rowcount
    if not ended:
        raise ResourceNotFoundError(
            f"announcement {request.announcement_id}: not found or already ended; "
            "list them with `etl-craft announce list --all`"
        )
    return read(ctx, AnnouncementFilter(all=True))
