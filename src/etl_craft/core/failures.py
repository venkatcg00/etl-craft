"""Failure evidence shared by dialect classification and attempt audit."""

from dataclasses import dataclass

from etl_craft.core.enums import FailureClass


@dataclass(frozen=True)
class Failure:
    """A class and the original exception identity, without driver dependencies."""

    kind: FailureClass
    source: str
    code: str | None = None


def retry_reason(kind: str, source: str | None, code: str | None, policy: str | None) -> str | None:
    """Describe stored evidence without consulting current configuration."""
    if source is None:
        return None
    return f"{kind}: {source}{' ' + code if code else ''}; RETRY_ON={policy or 'unrecorded'}"
