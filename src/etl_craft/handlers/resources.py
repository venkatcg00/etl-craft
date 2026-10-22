"""Join durable Engine DB ownership with warehouse-session termination."""

from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from uuid import uuid4

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.engine import resource_locks
from etl_craft.warehouse.fencing import confirm_stopped


@dataclass(frozen=True)
class LocalLocker:
    """Confirm prior warehouse work ended before a durable lock can pass to its successor."""

    engine: Engine
    config: ConnectorConfig
    identity: str = field(default_factory=lambda: uuid4().hex)

    @property
    def session_tag(self) -> str:
        """Use the enclosing attempt or standalone action identity."""
        return resource_locks.session_tag(self.identity)

    def confirm(self, holder_id: str) -> None:
        """Stop a stale holder's tagged warehouse statements before taking over its lock."""
        confirm_stopped(self.config, resource_locks.holder_tag(holder_id))

    def hold(self, key: int, name: str, wait_seconds: float) -> AbstractContextManager[None]:
        """Keep Engine DB ownership while warehouse work is confirmed gone."""
        owner = resource_locks.Owner(self.engine, self.config, self.confirm, self.identity)
        return resource_locks.hold(owner, key, name, wait_seconds)
