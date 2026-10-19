"""Call any action the way every interface does: build its request, then ``invoke`` it."""

from etl_craft.core.enums import ActionName
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import ACTIONS, invoke


def act(ctx: OperationContext, action: ActionName, /, **fields: object) -> object:
    """Invoke ``action`` with a request built from ``fields``, checked and recorded as usual."""
    return invoke(ctx, action, ACTIONS[action].request(**fields))
