"""UTC instants read from database timestamps."""

from datetime import UTC, datetime


def as_utc(value: object) -> datetime:
    """Read a datetime or ISO timestamp as an aware UTC instant."""
    result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    return result.replace(tzinfo=UTC) if result.tzinfo is None else result.astimezone(UTC)
