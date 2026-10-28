"""Scoped intervention reasons, independent of roles and audit attribution."""

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from etl_craft.core.enums import EnvironmentType, Governance
from etl_craft.core.errors import UsageError


@dataclass(frozen=True)
class ProjectPolicy:
    """The selected project's administrator-managed intervention policy."""

    project: str = "(selected project)"
    environment: str = EnvironmentType.CUSTOM
    governance: str = Governance.STRICT


_policy: ContextVar[ProjectPolicy | None] = ContextVar("etl_craft_policy", default=None)


@contextmanager
def using_policy(policy: ProjectPolicy) -> Iterator[None]:
    """Restore the caller's policy after a nested operation or refusal."""
    token = _policy.set(policy)
    try:
        yield
    finally:
        _policy.reset(token)


def check_reason(reason: str | None) -> str:
    """Validate length everywhere and require an explanation in STRICT projects."""
    policy = _policy.get() or ProjectPolicy()
    reason = reason_text(reason)
    if (not reason or not reason.strip()) and policy.governance == Governance.STRICT:
        raise UsageError(
            f"project {policy.project!r}, environment {policy.environment}, governance STRICT: "
            f"--reason={reason!r}; expected a non-empty reason; supply --reason REASON"
        )
    return reason or ""


def reason_text(reason: str | None) -> str:
    """Normalize omitted text without deciding policy during input parsing."""
    if reason is not None and len(reason) > 4096:
        raise UsageError(
            f"--reason length={len(reason)}; expected at most 4096 characters; shorten it"
        )
    return reason or ""
