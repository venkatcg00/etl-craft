"""One JSON representation for CLI and Python operation results."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from datetime import date
from enum import Enum
from typing import Any, cast

from etl_craft.core.errors import UsageError


def to_json(obj: object) -> dict[str, Any]:
    """Return a fresh JSON document; dates use ISO 8601 and enums use their values."""
    if (
        isinstance(obj, type)
        or not is_dataclass(obj)
        or not isinstance(getattr(obj, "SCHEMA", None), str)
    ):
        raise UsageError(
            f"cannot serialize {type(obj).__name__} as an operation document; "
            "pass a result returned by services.operations"
        )
    return cast(dict[str, Any], _encode(obj))


def _encode(obj: object) -> object:
    if isinstance(obj, Enum):
        return _encode(obj.value)
    if isinstance(obj, date):
        return obj.isoformat()
    if is_dataclass(obj) and not isinstance(obj, type):
        result = {f.name: _encode(getattr(obj, f.name)) for f in fields(obj)}
        schema = getattr(obj, "SCHEMA", None)
        return {"schema": schema, **result} if schema is not None else result
    if isinstance(obj, Mapping):
        if any(not isinstance(key, str) for key in obj):
            raise UsageError("JSON document keys must be strings; use named fields")
        return {key: _encode(value) for key, value in obj.items()}
    if isinstance(obj, (tuple, list)):
        return [_encode(value) for value in obj]
    if isinstance(obj, float) and not math.isfinite(obj):
        raise UsageError("JSON numbers must be finite; use a finite number or None")
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    raise UsageError(
        f"cannot serialize {type(obj).__name__} in an operation document; "
        "use dataclasses, dates, enums and JSON values"
    )
