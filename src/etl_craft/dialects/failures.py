"""DB-API failure evidence, without importing optional drivers."""

from __future__ import annotations

from sqlalchemy.exc import DBAPIError

from etl_craft.core.enums import FailureClass
from etl_craft.core.failures import Failure


def driver_error(error: BaseException) -> BaseException:
    """Unwrap SQLAlchemy while retaining the driver's exception identity."""
    return error.orig if isinstance(error, DBAPIError) and error.orig is not None else error


def sqlstate(error: BaseException) -> str | None:
    """Read standard PostgreSQL and DB-API SQLSTATE attributes."""
    value = getattr(error, "sqlstate", None) or getattr(error, "pgcode", None)
    return str(value) if value and len(str(value)) == 5 else None


def classify(error: BaseException) -> Failure:
    """Prefer specific SQLSTATE evidence over broad DB-API exception classes."""
    error = driver_error(error)
    state = sqlstate(error)
    code = state
    if code is None:
        raw = getattr(error, "errno", None)
        if raw is None or raw == -1:
            raw = getattr(error, "error_code", None)
        code = None if raw is None else str(raw)
    kind = state_class(state)
    if kind == FailureClass.UNKNOWN:
        names = {base.__name__ for base in type(error).__mro__}
        if names & {"ProgrammingError", "DataError", "IntegrityError", "NotSupportedError"}:
            kind = FailureClass.PERMANENT
        elif names & {"OperationalError", "InterfaceError"}:
            kind = FailureClass.TRANSIENT
    return Failure(kind, type(error).__name__, None if code is None else str(code))


def state_class(state: str | None) -> FailureClass:
    """Classify SQLSTATE codes shared by warehouse drivers."""
    if state is None:
        return FailureClass.UNKNOWN
    if state[:2] in {"08", "40", "53"} or state in {
        "57P01",
        "57P02",
        "57P03",
        "55P03",
        "57014",
        "HYT00",
        "HYT01",
    }:
        return FailureClass.TRANSIENT
    if state[:2] in {"42", "22", "23", "28", "0A"}:
        return FailureClass.PERMANENT
    return FailureClass.UNKNOWN
