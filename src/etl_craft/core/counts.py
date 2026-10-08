"""The row counts recorded for one task attempt."""

from dataclasses import dataclass, fields


@dataclass(frozen=True)
class Counts:
    """Reported row counts; an absent write count stays unknown."""

    source_count: int | None = None
    target_count: int | None = None
    insert_count: int | None = None
    update_count: int | None = None
    delete_count: int | None = None
    rows_written: int | None = None

    def __post_init__(self) -> None:
        """Derive writes when insert, update or delete counts were reported."""
        writes = (self.insert_count, self.update_count, self.delete_count)
        if self.rows_written is None and any(count is not None for count in writes):
            object.__setattr__(self, "rows_written", sum(count or 0 for count in writes))

    def parameters(self) -> dict[str, int | None]:
        """Return the six audit columns, including when a handler extends this record."""
        return {field.name: getattr(self, field.name) for field in fields(Counts)}


NO_COUNTS = Counts()
