"""Classify the original error across handler and script exception boundaries."""

from dataclasses import replace

from sqlalchemy.exc import DBAPIError

from etl_craft.config import ConnectorConfig
from etl_craft.core.enums import FailureClass
from etl_craft.core.errors import EtlCraftError, PermanentError, TransientError
from etl_craft.core.failures import Failure
from etl_craft.dialects.failures import classify
from etl_craft.warehouse.connection import warehouse_dialect


def classify_failure(
    error: BaseException, config: ConnectorConfig, *, script: bool = False
) -> Failure:
    """Preserve deliberate failures; otherwise classify the cause rather than its wrapper."""
    seen: set[int] = set()
    while id(error) not in seen:
        seen.add(id(error))
        if isinstance(error, PermanentError) or (
            isinstance(error, EtlCraftError) and not error.retryable
        ):
            return Failure(FailureClass.PERMANENT, type(error).__name__)
        if isinstance(error, TransientError):
            return Failure(FailureClass.TRANSIENT, type(error).__name__)
        if isinstance(error, DBAPIError) or error.__cause__ is None:
            break
        error = error.__cause__
    if isinstance(error, EtlCraftError):
        return Failure(FailureClass.TRANSIENT, type(error).__name__)
    failure = (
        warehouse_dialect(config).classify_failure(error)
        if config.warehouse is not None
        else classify(error)
    )
    return replace(failure, kind=FailureClass.UNKNOWN) if script else failure
