"""The agent protocol: every exchange is bound to a token, version and exact attempt."""

from dataclasses import dataclass
from typing import ClassVar

from etl_craft.core.agent_resources import AgentResources
from etl_craft.core.counts import Counts
from etl_craft.core.enums import AgentCommand, AgentReport, AgentStatus, AttemptStatus, SlotKind
from etl_craft.core.errors import UsageError
from etl_craft.core.failures import Failure
from etl_craft.core.items import ItemRecord
from etl_craft.engine.repository.offsets import StoredOffset
from etl_craft.engine.task_store import RuleEnding, TargetPublication
from etl_craft.services.operations.requests import Request, require_text


@dataclass(frozen=True, kw_only=True)
class AgentRequest(Request):
    """Every agent request proves the running package version."""

    version: str


@dataclass(frozen=True, kw_only=True)
class AgentRegister(AgentRequest):
    """Advertise one measured task host under its token's identity."""

    pool: str
    resources: AgentResources

    def __post_init__(self) -> None:
        """Keep pool names bounded and usable in notification routing."""
        require_text("pool", self.pool, limit=128)


@dataclass(frozen=True, kw_only=True)
class AgentClaim(AgentRequest):
    """Wait for an eligible attempt while accepting commands and cancellations."""

    free_kinds: tuple[SlotKind, ...]


@dataclass(frozen=True, kw_only=True)
class AgentRenew(AgentRequest):
    """Renew only this process's known attempts and acknowledge applied commands."""

    attempts: tuple[int, ...]
    resources: AgentResources
    acknowledged_commands: tuple[int, ...] = ()
    status: AgentStatus = AgentStatus.ACTIVE


@dataclass(frozen=True, kw_only=True)
class AgentTask(AgentRequest):
    """Report one bounded operation against this token's exact owned attempt."""

    attempt_id: int
    operation: AgentReport
    item: ItemRecord | None = None
    item_keys: tuple[str, ...] = ()
    target: str | None = None
    publication: TargetPublication | None = None
    watermark_from: str | None = None
    watermark_to: str | None = None
    rule_id: int | None = None
    rule_ending: RuleEnding | None = None
    lock_key: int | None = None
    lock_wait_seconds: float = 30
    confirmed_holder: str | None = None
    status: AttemptStatus | None = None
    counts: Counts | None = None
    offset: StoredOffset | None = None
    error_message: str | None = None
    task_log: str | None = None
    exit_code: int | None = None
    pid: int | None = None
    process_start: str | None = None
    failure: Failure | None = None
    queries_ended: bool = False


@dataclass(frozen=True, kw_only=True)
class AgentLog(AgentRequest):
    """One ordered, retry-safe log chunk; its content is never in action arguments."""

    attempt_id: int
    sequence: int
    content: str

    def __post_init__(self) -> None:
        """Bound each upload before opening a file or database transaction."""
        if self.sequence < 0 or len(self.content.encode()) > 65536:
            raise UsageError("log sequence must be nonnegative and content at most 65536 bytes")


@dataclass(frozen=True, kw_only=True)
class AgentControl(Request):
    """An operator's durable command to one registered agent."""

    agent_id: int
    command: AgentCommand


@dataclass(frozen=True)
class AgentReply:
    """A versioned protocol result, including an empty long-poll answer."""

    SCHEMA: ClassVar[str] = "etl-craft/agent-reply/1"
    data: object = None
