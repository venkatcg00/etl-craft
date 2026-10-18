"""One JSON representation for CLI and Python operation results."""

from __future__ import annotations

import math
import types
from collections.abc import Mapping
from dataclasses import MISSING, fields, is_dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any, TypeVar, Union, cast, get_args, get_origin, get_type_hints

from etl_craft.core.errors import ServerError, UsageError

T = TypeVar("T")


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


def from_json(cls: type[T], document: object) -> T:
    """Rebuild the operation result ``to_json`` wrote, so every interface renders it the same way.

    Unknown fields are ignored, so a newer server can add fields an older client does not read.
    """
    result = _decode(cls, document, cls.__name__)
    if not isinstance(result, cls):
        raise _unexpected(cls.__name__, document, f"a {cls.__name__} document")
    return result


def _decode(hint: object, value: object, where: str) -> object:
    origin = get_origin(hint)
    if origin in (Union, types.UnionType):
        return _decode_union(get_args(hint), value, where)
    if origin in (tuple, list, dict):
        return _decode_collection(origin, get_args(hint), value, where)
    if isinstance(hint, type) and is_dataclass(hint):
        return _decode_dataclass(hint, value, where)
    return _decode_value(hint, value, where)


def _decode_collection(
    origin: object, items: tuple[object, ...], value: object, where: str
) -> object:
    if origin is dict:
        if not isinstance(value, dict):
            raise _unexpected(where, value, "an object")
        return {key: _decode(items[1], v, f"{where}.{key}") for key, v in value.items()}
    if not isinstance(value, list):
        raise _unexpected(where, value, "a list")
    decoded = [_decode(items[0], v, f"{where}[{i}]") for i, v in enumerate(value)]
    return tuple(decoded) if origin is tuple else decoded


def _decode_value(hint: object, value: object, where: str) -> object:
    if hint is Any:
        return value
    if isinstance(hint, type) and issubclass(hint, Enum):
        try:
            return hint(value)
        except ValueError as error:
            raise _unexpected(where, value, f"one of {[m.value for m in hint]}") from error
    if hint is datetime or hint is date:
        try:
            return (datetime if hint is datetime else date).fromisoformat(str(value))
        except ValueError as error:
            raise _unexpected(where, value, "an ISO 8601 text") from error
    if hint is float and isinstance(value, int) and not isinstance(value, bool):
        return float(value)
    if isinstance(hint, type) and isinstance(value, hint):
        return value
    raise _unexpected(where, value, getattr(hint, "__name__", str(hint)))


def _decode_union(options: tuple[object, ...], value: object, where: str) -> object:
    if value is None and type(None) in options:
        return None
    schema = value.get("schema") if isinstance(value, dict) else None
    named = [o for o in options if schema is not None and getattr(o, "SCHEMA", None) == schema]
    failures = []
    for option in named or [o for o in options if o is not type(None)]:
        try:
            return _decode(option, value, where)
        except ServerError as error:
            failures.append(str(error))
    raise ServerError(f"{where}: no form of {options} matches; " + "; ".join(failures))


def _decode_dataclass(cls: type, value: object, where: str) -> object:
    if not isinstance(value, dict):
        raise _unexpected(where, value, f"a {cls.__name__} object")
    schema = getattr(cls, "SCHEMA", None)
    if schema is not None and value.get("schema", schema) != schema:
        raise _unexpected(f"{where}.schema", value.get("schema"), schema)
    hints = get_type_hints(cls)
    values = {}
    for field in fields(cls):
        if not field.init:
            continue
        if field.name in value:
            values[field.name] = _decode(
                hints[field.name], value[field.name], f"{where}.{field.name}"
            )
        elif field.default is MISSING and field.default_factory is MISSING:
            raise ServerError(
                f"{where}.{field.name}: missing; the server and this etl-craft differ"
            )
    return cls(**values)


def _unexpected(where: str, value: object, expected: str) -> ServerError:
    return ServerError(
        f"{where}={value!r}; expected {expected}; "
        "check that the server runs the same etl-craft version as this installation"
    )
