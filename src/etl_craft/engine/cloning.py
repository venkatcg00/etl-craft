"""Durable per-mirror baselines and exact-version dirty-key acknowledgements."""

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.engine import Connection, Engine

from etl_craft.core.time import as_utc
from etl_craft.engine import locks, resource_locks
from etl_craft.engine.queries import statement


@dataclass(frozen=True)
class CloneState:
    """One resolved destination/table; no credentials are persisted."""

    mirror_id: str
    table_name: str
    holder: resource_locks.Holder | None = None
    signature: str = ""

    def fence(self, conn: Connection) -> None:
        """Hold live ownership through checkpoint publication when called by a clone."""
        if self.holder is not None:
            resource_locks.renew(conn, locks.CLONE.key, self.holder)

    def parameters(self) -> dict[str, object]:
        """Return the exact mirror/table identity for every state query."""
        return {"mirror_id": self.mirror_id, "table_name": self.table_name}

    def register(self, engine: Engine) -> bool:
        """Register before reading a full baseline so concurrent changes stay pending."""
        with engine.begin() as conn:
            self.fence(conn)
            conn.execute(
                statement(conn, "clone_register"),
                {**self.parameters(), "signature": self.signature},
            )
            return bool(
                conn.execute(statement(conn, "clone_state"), self.parameters()).one().initialized
            )

    def finished_since(self, engine: Engine, requested: datetime) -> bool:
        """Coalesce a request covered by a clone that completed while it waited."""
        with engine.connect() as conn:
            row = conn.execute(statement(conn, "clone_state"), self.parameters()).first()
            return (
                row is not None and row.copied_at is not None and as_utc(row.copied_at) >= requested
            )

    def pending(self, engine: Engine, after: str, limit: int) -> list[tuple[str, str]]:
        """Read a bounded page; later commits remain pending even below the page cursor."""
        with engine.connect() as conn:
            rows = conn.execute(
                statement(conn, "clone_pending"),
                {**self.parameters(), "after": after, "limit": limit},
            )
            return [(row.row_key, row.version) for row in rows]

    def acknowledge(self, engine: Engine, changes: list[tuple[str, str]]) -> None:
        """Release only versions whose mirror write committed; concurrent replacements remain."""
        if not changes:
            return
        with engine.begin() as conn:
            self.fence(conn)
            conn.execute(
                statement(conn, "clone_acknowledge"),
                [
                    {**self.parameters(), "row_key": key, "version": version}
                    for key, version in changes
                ],
            )

    def complete(self, engine: Engine) -> None:
        """Publish the baseline only after its warehouse commit."""
        with engine.begin() as conn:
            self.fence(conn)
            conn.execute(
                statement(conn, "clone_complete"), {**self.parameters(), "now": datetime.now(UTC)}
            )
