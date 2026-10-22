"""The agent's outbound protocol client, authenticated only by its agent token."""

import json
import os
from urllib.error import HTTPError, URLError
from urllib.request import Request

from etl_craft.config.remote import server_url
from etl_craft.core.api_errors import response_error
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ServerError, UsageError
from etl_craft.core.http_client import open_request
from etl_craft.services.operations.actions import ACTIONS
from etl_craft.services.operations.agent_models import AgentReply, AgentRequest
from etl_craft.services.operations.serialization import encode, from_json


class AgentClient:
    """Use the same action table and error documents as every other interface."""

    def __init__(self, url: str) -> None:
        """Require a token explicitly; an agent never reads a person's stored session."""
        self.url = server_url(url)
        self.token = os.environ.get("ETL_CRAFT_TOKEN")
        if not self.token:
            raise UsageError(
                "agent requires ETL_CRAFT_TOKEN; issue a distinct "
                "`token create --role agent` credential"
            )

    def call(self, name: ActionName, request: AgentRequest, *, timeout: float = 5) -> AgentReply:
        """Send one bounded exchange without opening an Engine DB connection."""
        action = ACTIONS[name]
        assert action.path is not None and action.method is not None
        message = Request(
            self.url + action.path,
            method=action.method,
            data=json.dumps(encode(request)).encode(),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.token}",
            },
        )
        try:
            with open_request(message, timeout) as response:
                body = response.read(16 * 1024 * 1024 + 1)
        except HTTPError as error:
            raise response_error(error.code, error.read(65536), self.url) from error
        except (URLError, OSError) as error:
            raise ServerError(
                f"{self.url}: agent API unavailable ({type(error).__name__}); "
                "restore the API or network"
            ) from error
        if len(body) > 16 * 1024 * 1024:
            raise ServerError(f"{self.url}: agent response exceeds 16 MiB; check the API")
        try:
            return from_json(AgentReply, json.loads(body))
        except ValueError as error:
            raise ServerError(
                f"{self.url}: agent response is not JSON; check the API address"
            ) from error
