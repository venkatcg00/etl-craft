"""Reconstruct safe API refusals for both task and command clients."""

import json
from collections.abc import Iterator

from etl_craft.core.errors import EtlCraftError, ServerError, UsageError


def response_error(status: int, body: bytes, url: str) -> EtlCraftError:
    """Rebuild the API's error class from its exit code, so local and remote exits match."""
    try:
        document = json.loads(body)
    except ValueError:
        document = None
    if isinstance(document, dict) and isinstance(document.get("message"), str):
        code = document.get("exit_code")
        classes = {int(cls.exit_code): cls for cls in _subclasses(EtlCraftError)}
        cls = classes.get(code, ServerError) if isinstance(code, int) else ServerError
        return cls(document["message"])
    if isinstance(document, dict) and "detail" in document:
        return UsageError(f"{url} refused the request (HTTP {status}): {document['detail']}")
    return ServerError(f"{url} answered HTTP {status} without an etl-craft error; check the URL")


def _subclasses(cls: type[EtlCraftError]) -> Iterator[type[EtlCraftError]]:
    for sub in cls.__subclasses__():
        yield sub
        yield from _subclasses(sub)
