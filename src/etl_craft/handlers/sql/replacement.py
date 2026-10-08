"""Recover rows on non-transactional overwrites and retain original objects during promotion."""

import contextlib
import logging
from collections.abc import Iterator

from etl_craft.core.errors import SqlGuardError
from etl_craft.core.faults import fault_point
from etl_craft.handlers.sql.session import Session

logger = logging.getLogger(__name__)


def cleanup(session: Session, name: str) -> None:
    """Cleanup cannot turn a committed replacement into a failed action."""
    if session.dialect.replace_strategy == "transactional":
        session.drop(name)
        return
    try:
        session.drop(name)
    except Exception:
        logger.warning("replacement succeeded but cleanup failed; remove %s manually", name)


def promote(session: Session, candidate: str, *, existing: bool) -> None:
    """Keep the original object until promotion succeeds; restore its definition on failure."""
    keep = f"{session.target}__etl_keep_{session.token}"
    retained = False
    try:
        fault_point("sql.replace.before_publish")
        if existing:
            session.rename(session.target, keep)
            retained = True
        fault_point("sql.replace.after_clear")
        session.rename(candidate, session.target)
        fault_point("sql.replace.after_publish")
    except Exception as error:
        try:
            if retained:
                session.drop(session.target)
                session.rename(keep, session.target)
            elif not existing:
                session.drop(session.target)
        except Exception as restore_error:
            raise SqlGuardError(
                f"{session.target}: replacement failed and restoration failed; "
                f"the original table is retained at {keep}. Restore it before retrying"
            ) from restore_error
        raise error
    if retained:
        cleanup(session, keep)


@contextlib.contextmanager
def recover_overwrite(session: Session) -> Iterator[None]:
    """Keep a durable row copy; restore into the untouched definition when a later write fails."""
    keep = f"{session.target}__etl_keep_{session.token}"
    columns = ", ".join(name for name, _ in session.target_columns())
    session.dialect.backup_table(session.conn, keep, session.target)
    try:
        yield
    except Exception as error:
        try:
            session.run(f"TRUNCATE TABLE {session.target}", step="clear failed overwrite")
            session.run(
                f"INSERT INTO {session.target} ({columns}) SELECT {columns} FROM {keep}",
                step="restore original rows",
            )
        except Exception as restore_error:
            raise SqlGuardError(
                f"{session.target}: overwrite failed and restoration failed; "
                f"original rows are retained at {keep}. Restore them before retrying"
            ) from restore_error
        cleanup(session, keep)
        raise error
    cleanup(session, keep)
