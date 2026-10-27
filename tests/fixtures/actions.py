"""Call any action the way every interface does: build its request, then ``invoke`` it."""

import enum
import types
import typing
from dataclasses import MISSING, fields, is_dataclass

from etl_craft.core.enums import ActionName, ApiRole, RunStatus
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import ACTIONS, invoke
from fixtures.agents import resources


def act(ctx: OperationContext, action: ActionName, /, **fields: object) -> object:
    """Invoke ``action`` with a request built from ``fields``, checked and recorded as usual."""
    return invoke(ctx, action, ACTIONS[action].request(**fields))


def sample(hint):
    """Return a value of ``hint`` that its request type accepts."""
    origin = typing.get_origin(hint)
    if origin in (typing.Union, types.UnionType):
        return sample(next(arg for arg in typing.get_args(hint) if arg is not type(None)))
    if origin is tuple:
        return (sample(typing.get_args(hint)[0]),)
    if origin is dict:
        return {"id": 1}
    if isinstance(hint, type) and issubclass(hint, enum.Enum):
        return next(iter(hint))
    if is_dataclass(hint):
        return build(hint)
    return {str: "x", int: 1, bool: False}[hint]


# Requests whose checks need more than one value of each required field's type.
GIVEN = {
    "PublishBundle": {"files": {"craft-connector.yml": "eA=="}},
    "ItemControl": {"item_key": "x"},
    "MarkRequest": {"status": RunStatus.SUCCESS},
    "ControlRequest": {"run_id": 1, "request_key": "k"},
    "AccountAdd": {"password": "secret", "role": ApiRole.ADMIN},
    "AccountRole": {"role": ApiRole.ADMIN},
    "TokenCreate": {"role": ApiRole.VIEWER},
    "AgentRegister": {"resources": resources()},
    "AgentRenew": {"resources": resources()},
}


def build(cls):
    hints = typing.get_type_hints(cls)
    required = {
        f.name: sample(hints[f.name])
        for f in fields(cls)
        if f.default is MISSING
        and f.default_factory is MISSING
        and f.name not in GIVEN.get(cls.__name__, {})
    }
    return cls(**{**required, **GIVEN.get(cls.__name__, {})})
