"""UTC instants read from database timestamps."""

from datetime import UTC, datetime


def as_utc(value: object) -> datetime:
    """Read a datetime or ISO timestamp as an aware UTC instant."""
    result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    return result.replace(tzinfo=UTC) if result.tzinfo is None else result.astimezone(UTC)


def elapsed_seconds(start: datetime | None, end: datetime | None) -> float | None:
    """Seconds from ``start`` to ``end`` as UTC instants; ``None`` when either is missing.

    An ``end`` before ``start``, from clock skew between hosts, reads as zero.
    """
    if start is None or end is None:
        return None
    return max((as_utc(end) - as_utc(start)).total_seconds(), 0.0)
