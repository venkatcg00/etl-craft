"""Cancel tagged SQL-connector statements with the authenticated session's REST identity."""

import json
import time
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request
from uuid import UUID

from sqlalchemy.engine import Connection

from etl_craft.core.enums import DatabricksQueryStatus
from etl_craft.core.errors import ConfigurationError, SqlGuardError
from etl_craft.core.http_client import open_request

ENDED = {
    DatabricksQueryStatus.CANCELED,
    DatabricksQueryStatus.FAILED,
    DatabricksQueryStatus.FINISHED,
}


def request(conn: Connection, path: str, *, method: str = "GET") -> dict[str, object]:
    """Reuse the connector's authentication, including refresh, without forwarding redirects."""
    driver = conn.connection.dbapi_connection
    session = getattr(driver, "session", None)
    provider = getattr(session, "auth_provider", None)
    if provider is None:
        raise ConfigurationError(
            "Databricks query cancellation: connector has no authenticated session provider; "
            "expected a connector exposing session authentication; upgrade databricks-sql-connector"
        )
    headers: dict[str, str] = {}
    provider.add_headers(headers)
    url = f"https://{conn.engine.url.host}/api/2.0/sql/{path}"
    try:
        with open_request(Request(url, headers=headers, method=method), timeout=5) as response:
            result = json.load(response)
    except (URLError, OSError, ValueError) as error:
        raise SqlGuardError(
            f"Databricks query cancellation {path.split('?')[0]}: {type(error).__name__}; "
            "expected an authenticated query-control response; "
            "check warehouse permissions and retry"
        ) from error
    if not isinstance(result, dict):
        raise SqlGuardError(
            "Databricks query cancellation: invalid response; expected a JSON object; "
            "check the workspace query-control API before retrying"
        )
    return result


def active(conn: Connection, tag: str) -> list[str]:
    """Read every page, preserving unknown or unfinished queries as active work."""
    page = ""
    found: list[str] = []
    deadline = time.monotonic() + 5
    for _ in range(100):
        parameters: dict[str, str | list[str]] = {
            "max_results": "999",
            "include_metrics": "false",
            "filter_by.statuses": [state for state in DatabricksQueryStatus if state not in ENDED],
        }
        if time.monotonic() >= deadline:
            break
        if page:
            parameters["page_token"] = page
        data = request(conn, "history/queries?" + urlencode(parameters, doseq=True))
        rows = data.get("res", [] if data.get("has_next_page") is False else None)
        if not isinstance(rows, list):
            raise SqlGuardError(
                f"Databricks holder {tag}: history omitted its query list; expected complete "
                "query history; restore query-history access before retrying cancellation"
            )
        found.extend(identity for row in rows if (identity := _tagged_query(row, tag)) is not None)
        if not data.get("has_next_page"):
            return found
        next_page = str(data.get("next_page_token") or "")
        if not next_page or next_page == page:
            break
        page = next_page
    raise SqlGuardError(
        f"Databricks holder {tag}: incomplete query history; expected every page within "
        "100 requests or five seconds; stop the queries in the warehouse and retry"
    )


def _tagged_query(row: object, tag: str) -> str | None:
    if not isinstance(row, dict):
        raise SqlGuardError(f"Databricks holder {tag}: invalid query record; repair API access")
    tags = row.get("query_tags")
    tags = [] if tags is None else tags
    if not isinstance(tags, list) or any(not isinstance(value, dict) for value in tags):
        raise SqlGuardError(
            f"Databricks holder {tag}: invalid query tags; expected a list of tag objects; "
            "repair query-history access before retrying"
        )
    if any(
        isinstance(value, dict)
        and value.get("key") == "etl_craft_holder"
        and value.get("value") == tag
        for value in tags
    ):
        try:
            state = DatabricksQueryStatus(str(row.get("status")))
        except ValueError as error:
            raise SqlGuardError(
                f"Databricks holder {tag}: query state {row.get('status')!r}; "
                "expected a documented Query History state; inspect the query and retry"
            ) from error
        if state in ENDED:
            return None
        identity = str(row.get("query_id", ""))
        try:
            UUID(identity)
        except ValueError as error:
            raise SqlGuardError(
                f"Databricks holder {tag}: invalid statement identity {identity!r}; "
                "expected a UUID; repair query-history access before retrying"
            ) from error
        return identity
    return None
