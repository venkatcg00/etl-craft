"""The URL the catalog site is published at, in ``AUD_DOCS_PUBLICATION``."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy.engine import Connection

from etl_craft.engine.queries import statement


@dataclass(frozen=True)
class Publication:
    """A URL the site was published at, and when first and last."""

    docs_publication_id: int
    url: str
    first_published: datetime
    last_published: datetime


def fetch_publication(conn: Connection) -> Publication | None:
    """Return the URL the site was last published at, or ``None`` if it never was."""
    row = conn.execute(statement(conn, "latest_docs_publication")).first()
    if row is None:
        return None
    return Publication(
        int(row.docs_publication_id), row.published_url, row.first_published, row.last_published
    )


def record_publication(conn: Connection, url: str) -> None:
    """Record ``url``, or note that the site was published at it again."""
    current = fetch_publication(conn)
    if current is not None and current.url == url:
        conn.execute(
            statement(conn, "touch_docs_publication"),
            {"docs_publication_id": current.docs_publication_id},
        )
        return
    conn.execute(statement(conn, "insert_docs_publication"), {"published_url": url})
