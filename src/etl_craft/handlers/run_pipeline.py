"""Validated parameters for a pipeline trigger; admission belongs to operations."""

import math
from collections.abc import Mapping
from dataclasses import dataclass

from etl_craft.core.enums import TriggerMode, enum_value
from etl_craft.core.errors import MetadataError
from etl_craft.core.text import is_metadata_code

PARAMETERS = frozenset({"TRIGGER_PIPELINE_CODE", "TRIGGER_MODE", "TRIGGER_TIMEOUT_MINUTES"})


@dataclass(frozen=True)
class Trigger:
    """The exact child definition and controller wait policy."""

    pipeline_code: str
    mode: TriggerMode
    timeout_minutes: float | None


def read_trigger(params: Mapping[str, str]) -> Trigger:
    """Refuse malformed values before admitting a child or a wait."""
    for name in sorted(PARAMETERS & params.keys()):
        if not isinstance(params[name], str):
            raise MetadataError(
                f"{name}={params[name]!r}; expected a text parameter value; "
                "set the documented value in CFG_TASK_PARAMETERS"
            )
    code = params.get("TRIGGER_PIPELINE_CODE", "")
    if not is_metadata_code(code):
        raise MetadataError(
            f"TRIGGER_PIPELINE_CODE={code!r}; expected an active pipeline code; "
            "set TRIGGER_PIPELINE_CODE to the child's pipeline code"
        )
    mode = enum_value(
        TriggerMode,
        params.get("TRIGGER_MODE", TriggerMode.WAIT),
        MetadataError(
            f"TRIGGER_MODE={params.get('TRIGGER_MODE')!r}; expected WAIT or DETACH; "
            "set TRIGGER_MODE to one of these values"
        ),
    )
    raw = params.get("TRIGGER_TIMEOUT_MINUTES", "NONE")
    if mode == TriggerMode.DETACH and "TRIGGER_TIMEOUT_MINUTES" in params:
        raise MetadataError(
            f"TRIGGER_TIMEOUT_MINUTES={raw!r} with TRIGGER_MODE=DETACH; expected no timeout; "
            "remove TRIGGER_TIMEOUT_MINUTES or use WAIT"
        )
    timeout = None
    if raw != "NONE":
        try:
            timeout = float(raw)
        except ValueError:
            timeout = 0
        if not math.isfinite(timeout) or timeout <= 0:
            raise MetadataError(
                f"TRIGGER_TIMEOUT_MINUTES={raw!r}; expected NONE or a finite positive number; "
                "set a positive duration or NONE"
            )
    return Trigger(code, mode, timeout)
