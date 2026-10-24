"""Read-only dependency diagnostics from an authenticated agent interpreter."""

from dataclasses import dataclass
from typing import ClassVar

from etl_craft.core.errors import ConfigurationError, UsageError
from etl_craft.engine.bundles import Bundle
from etl_craft.engine.project_files import script_imports
from etl_craft.engine.queries import statement
from etl_craft.services.operations.agent_models import AgentRequest
from etl_craft.services.operations.agents import check_agent
from etl_craft.services.operations.context import OperationContext


@dataclass(frozen=True, kw_only=True)
class AgentDoctor(AgentRequest):
    """Discovery returns import names; the next call reports this interpreter's observations."""

    bundle_id: str | None = None
    missing: tuple[str, ...] | None = None


@dataclass(frozen=True)
class AgentImports:
    """None means not inspected yet; an empty missing tuple means every import was found."""

    SCHEMA: ClassVar[str] = "etl-craft/agent-imports/1"
    bundle_id: str
    imports: tuple[str, ...]
    missing: tuple[str, ...] | None


def inspect(ctx: OperationContext, request: AgentDoctor) -> AgentImports:
    """Authorize the agent, read the exact bundle, and judge bounded module observations."""
    check_agent(ctx, request)
    with ctx.engine.connect() as conn:
        identity = (
            request.bundle_id or conn.execute(statement(conn, "bundle_latest")).scalar_one_or_none()
        )
        if identity is None:
            raise ConfigurationError(
                "agent imports: no published bundle; expected a project "
                "publication; run `etl-craft bundle publish` before checking agents"
            )
        bundle = Bundle.load(conn, identity)
    imports = script_imports(bundle.materialize(ctx.config))
    if request.missing is not None and (
        request.bundle_id is None or not set(request.missing) <= set(imports)
    ):
        raise UsageError(
            f"agent import observations={request.missing!r}: not from bundle "
            f"{identity}; expected only its returned import names; repeat "
            f"discovery and inspect those names"
        )
    return AgentImports(identity, imports, request.missing)
