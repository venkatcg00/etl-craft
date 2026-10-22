"""The caller's Engine DB, configuration and audit identity."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import Actor
from etl_craft.core.enums import ApiRole, HttpScheme, Transport
from etl_craft.execution.attempt_types import ChildOptions


@dataclass(frozen=True)
class OperationContext:
    """Resources owned by the caller; operations never dispose the Engine DB.

    ``actor`` scopes requests and dispatched work; automatic transitions retain their system
    actor. ``transport`` says how the request arrived, so an operation can refuse an option that
    works only on the controller's host. ``provisioning`` is local Engine DB setup before any
    admin exists: the actions the table marks as provisioning skip only their role check.
    """

    engine: Engine
    config: ConnectorConfig
    actor: Actor
    child: ChildOptions = field(default_factory=ChildOptions)
    role: ApiRole | None = None
    transport: Transport = Transport.LOCAL
    provisioning: bool = False
    sso_group_ids: tuple[int, ...] = ()
    credential_token_id: int | None = None
    credential_digest: str | None = None
    client_address: str | None = None
    client_scheme: HttpScheme | None = None
    shutdown: threading.Event | None = None
