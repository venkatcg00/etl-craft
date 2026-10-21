"""Resource ownership used by warehouse writers through local or remote task services."""

from contextlib import AbstractContextManager
from typing import Protocol

from etl_craft.core.text import sha256_hex


def attempt_holder(attempt_id: int, namespace: str) -> str:
    """Keep independently numbered deployments distinct within warehouse session tags."""
    return f"attempt:{attempt_id}:{sha256_hex(namespace.encode())[:24]}"


class ResourceLocker(Protocol):
    """Acquire one named resource without exposing an Engine DB connection to its writer."""

    @property
    def session_tag(self) -> str:
        """Identify all warehouse sessions opened under this holder."""
        ...

    def hold(self, key: int, name: str, wait_seconds: float) -> AbstractContextManager[None]:
        """Hold a bounded, renewable lease until the resource operation finishes."""
        ...
