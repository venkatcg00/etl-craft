"""Action metadata shared by dispatch and HTTP document generation."""

from collections.abc import Callable
from dataclasses import dataclass

from etl_craft.core.enums import ActionName, ActionScope, ApiRole, Audit, HttpMethod, Placement
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.requests import Request


@dataclass(frozen=True)
class Action:
    """One action and every rule an interface needs about it.

    ``call`` performs the operation or validates a process startup. It is ``None`` when an
    interface owns the complete process inside ``admitted``. ``result`` is the document type,
    or ``None`` for no content; ``streams`` marks
    an action that returns bytes. ``escalate`` raises the role from what the request would
    change, as ``migrate`` does for packaged migrations. ``remedy`` says how to run a
    ``CONTROLLER_HOST`` action, for an interface that refuses it elsewhere.
    """

    name: ActionName
    call: Callable[..., object] | None
    request: type[Request]
    result: object
    role: ApiRole | None
    method: HttpMethod | None = None
    path: str | None = None
    audit: Audit = Audit.NONE
    placement: Placement = Placement.ANYWHERE
    remedy: str | None = None
    provisioning: bool = False
    streams: bool = False
    scope: ActionScope = ActionScope.DEPLOYMENT
    escalate: Callable[[OperationContext, object], ApiRole] | None = None

    def required_role(self, ctx: OperationContext, request: object) -> ApiRole | None:
        """Return the role this request needs."""
        return self.role if self.escalate is None else self.escalate(ctx, request)
