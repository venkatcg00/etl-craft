"""The Python task's required contract, shared by validation and execution."""

from collections.abc import Mapping

from etl_craft.core.enums import ScriptKind, enum_value
from etl_craft.core.errors import HandlerError, MetadataError


def script_kind(params: Mapping[str, str], task: str) -> ScriptKind:
    """Refuse an absent or unsupported kind before running user code."""
    value = params.get("SCRIPT_KIND")
    return enum_value(
        ScriptKind,
        value or "",
        MetadataError(
            f"{task}: SCRIPT_KIND={value!r}; expected INGESTION or UTILITY; "
            "set SCRIPT_KIND in task_parameters.csv"
        ),
    )


def reported_count(value: object, kind: ScriptKind, name: str) -> int | None:
    """Distinguish unmeasured utilities from measured zero, including inside fan-out."""
    if kind == ScriptKind.UTILITY and value is None:
        return None
    if type(value) is not int or value < 0:
        raise HandlerError(
            f"{name} returned row_count={value!r}; it must be a whole number, 0 or more; "
            f"SCRIPT_KIND={kind}; return row_count"
        )
    return value
