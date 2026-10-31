"""Administrator announcements show on every page from their start until their end or an end."""

from datetime import UTC, datetime, timedelta

import pytest

from etl_craft.core.actor import Actor, ActorKind
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.core.enums_ui import AnnouncementLevel
from etl_craft.core.errors import PermissionDeniedError, ResourceNotFoundError, UsageError
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.identity import authenticated_context
from fixtures.actions import act

ADMIN = Actor("admin", ActorKind.USER)


@pytest.fixture
def ctx(engine_db):
    return OperationContext(engine_db.engine, engine_db.config, ADMIN, role=ApiRole.ADMIN)


def showing(ctx):
    return [a.message for a in act(ctx, ActionName.ANNOUNCE_LIST).announcements]


def test_announcements_show_inside_their_window_until_ended(ctx):
    now = datetime.now(UTC)
    act(ctx, ActionName.ANNOUNCE_ADD, message="Upgrade at 18:00", reason="upgrade")
    act(
        ctx,
        ActionName.ANNOUNCE_ADD,
        message="Later",
        reason="later",
        level=AnnouncementLevel.WARNING,
        starts_at=now + timedelta(hours=1),
    )
    act(
        ctx,
        ActionName.ANNOUNCE_ADD,
        message="Over",
        reason="past",
        starts_at=now - timedelta(hours=2),
        ends_at=now - timedelta(hours=1),
    )
    assert showing(ctx) == ["Upgrade at 18:00"]
    everything = act(ctx, ActionName.ANNOUNCE_LIST, all=True).announcements
    assert [a.message for a in everything] == ["Over", "Later", "Upgrade at 18:00"]
    first = everything[-1]
    assert (first.created_by, first.reason, first.level) == ("admin", "upgrade", "INFO")
    ended = act(ctx, ActionName.ANNOUNCE_END, announcement_id=first.announcement_id, reason="done")
    assert (
        next(a for a in ended.announcements if a.message == "Upgrade at 18:00").ended_by == "admin"
    )
    assert showing(ctx) == []
    with pytest.raises(ResourceNotFoundError, match="already ended"):
        act(ctx, ActionName.ANNOUNCE_END, announcement_id=first.announcement_id, reason="again")


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ({"message": "", "reason": "x"}, "expected 1 to 500"),
        ({"message": "x" * 501, "reason": "x"}, "expected 1 to 500"),
        ({"message": "ok", "reason": " "}, "`reason` is empty"),
        ({"message": "ok", "reason": "x", "level": "LOUD"}, "INFO, WARNING or CRITICAL"),
        (
            {"message": "ok", "reason": "x", "ends_at": datetime(2020, 1, 1, tzinfo=UTC)},
            "is not after the start",
        ),
        ({"message": "ok", "reason": "x", "starts_at": datetime(2030, 1, 1)}, "timezone offset"),
    ],
)
def test_bad_announcements_are_refused_with_the_remedy(ctx, fields, message):
    with pytest.raises(UsageError, match=message):
        act(ctx, ActionName.ANNOUNCE_ADD, **fields)


def test_only_administrators_post_or_end_and_everyone_reads(ctx):
    token = act(ctx, ActionName.TOKEN_CREATE, name="reader", role=ApiRole.VIEWER)
    reader = authenticated_context(ctx, token.metadata)
    act(ctx, ActionName.ANNOUNCE_ADD, message="Hello", reason="welcome")
    assert showing(reader) == ["Hello"]
    with pytest.raises(PermissionDeniedError):
        act(reader, ActionName.ANNOUNCE_ADD, message="Mine", reason="x")


def test_cli_posts_lists_and_ends(metadata_project, monkeypatch, capsys):
    import json

    from etl_craft.cli import main

    p = metadata_project
    admin = OperationContext(p.engine, p.config, ADMIN, role=ApiRole.ADMIN)
    token = act(admin, ActionName.TOKEN_CREATE, name="cli", role=ApiRole.ADMIN).token
    monkeypatch.chdir(p.root)
    monkeypatch.setenv("ETL_CRAFT_TOKEN", token)
    args = ["announce", "add", "--message", "Maintenance", "--level", "WARNING"]
    assert main([*args, "--reason", "window", "--format", "json"]) == 0
    posted = json.loads(capsys.readouterr().out)["announcements"][0]
    assert (posted["message"], posted["level"]) == ("Maintenance", "WARNING")
    assert main(["announce", "list"]) == 0
    assert "Maintenance" in capsys.readouterr().out
    end = ["announce", "end", "--announcement-id", str(posted["announcement_id"])]
    assert main([*end, "--reason", "over"]) == 0
    capsys.readouterr()
    assert main(["announce", "list"]) == 0
    assert "no announcements" in capsys.readouterr().out
