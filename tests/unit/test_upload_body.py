"""HTTP byte limits apply before parsing, regardless of Content-Length."""

import asyncio

import pytest
from starlette.requests import Request

from etl_craft.api.routes import read_body
from etl_craft.core.errors import UsageError
from etl_craft.services.operations.agent_models import AgentLog
from etl_craft.services.operations.bundles import PublishBundle

pytestmark = pytest.mark.unit


def request(chunks):
    observed = []
    pending = iter(chunks)

    async def receive():
        chunk = next(pending)
        observed.append(chunk)
        return {"type": "http.request", "body": chunk, "more_body": True}

    return Request(
        {
            "type": "http",
            "path": "/upload",
            "headers": [],
            "scheme": "https",
            "server": ("example.test", 443),
        },
        receive,
    ), observed


@pytest.mark.parametrize("model", [PublishBundle, AgentLog])
def test_upload_limits_stop_streaming_before_json_decoding(model, monkeypatch):
    monkeypatch.setattr(model, "MAX_BODY_BYTES", 8)
    incoming, observed = request([b"      ", b"   ", b"must not be read"])
    with pytest.raises(UsageError) as caught:
        asyncio.run(read_body(incoming, model.MAX_BODY_BYTES))
    for part in ("/upload", "9 bytes", "at most 8", "smaller request"):
        assert part in str(caught.value)
    assert observed == [b"      ", b"   "]


@pytest.mark.parametrize("content,expected", [(b"", {}), (b'{"key":1}', {"key": 1})])
def test_empty_and_bounded_json_bodies_remain_json_objects(content, expected):
    async def receive():
        return {"type": "http.request", "body": content, "more_body": False}

    incoming = Request({"type": "http", "path": "/upload"}, receive)
    assert asyncio.run(read_body(incoming, 9)) == expected
