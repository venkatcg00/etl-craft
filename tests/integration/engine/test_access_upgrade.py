"""Released metadata retains grant identities, history and enforcement after conversion."""

import shutil

import pytest
from sqlalchemy import text

from etl_craft.core.actor import SYSTEM_ACTOR, Actor, ActorKind
from etl_craft.core.enums import ApiRole, Permission
from etl_craft.dialects.engine.base import EngineDialect
from etl_craft.engine import migrations
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.permissions import pipeline_access
from fixtures.metadata import add_pipeline
from fixtures.released_schema import SCHEMAS, install


@pytest.mark.parametrize("release", sorted(p.name for p in SCHEMAS.iterdir() if p.is_dir()))
def test_released_grants_and_history_convert(empty_engine_db, release, tmp_path, monkeypatch):
    db = empty_engine_db
    install(db, release)
    old = tmp_path / "previous"
    old.mkdir()
    for file in db.dialect.migrations_dir().glob("*.sql"):
        if file.name < "0063":
            shutil.copyfile(file, old / file.name)
    with monkeypatch.context() as patch:
        patch.setattr(EngineDialect, "migrations_dir", lambda self: old)
        migrations.apply_pending_migrations(db.engine)
    with db.engine.begin() as conn:
        for code, role in (("VIEW", "viewer"), ("RUN", "operator")):
            add_pipeline(conn, code)
            conn.execute(text("INSERT INTO CFG_GROUPS (GROUP_NAME) VALUES (:name)"), {"name": code})
            conn.execute(
                text(
                    "INSERT INTO CFG_PIPELINE_GRANTS (GROUP_ID,PIPELINE_CODE,ROLE) "
                    "SELECT GROUP_ID,:code,:role FROM CFG_GROUPS WHERE GROUP_NAME=:group_name"
                ),
                {"code": code, "role": role, "group_name": code},
            )
        conn.execute(
            text(
                "INSERT INTO CFG_ACCOUNTS (ACCOUNT_NAME,ACCOUNT_KIND,ROLE) "
                "VALUES ('legacy-member','SERVICE','viewer')"
            )
        )
        conn.execute(
            text(
                "INSERT INTO CFG_GROUP_MEMBERS (GROUP_ID,ACCOUNT_ID) "
                "SELECT g.GROUP_ID,a.ACCOUNT_ID FROM CFG_GROUPS g CROSS JOIN CFG_ACCOUNTS a "
                "WHERE g.GROUP_NAME IN ('VIEW','RUN') AND a.ACCOUNT_NAME='legacy-member'"
            )
        )
        before = conn.execute(
            text(
                "SELECT CHANGE_ID AS id,PIPELINE_GRANT_ID AS grant_id,ROLE AS "
                "role,ACTOR AS actor FROM AUD_CFG_PIPELINE_GRANTS_HISTORY ORDER "
                "BY CHANGE_ID"
            )
        ).all()
    migrations.apply_pending_migrations(db.engine)
    with db.engine.connect() as conn:
        after = conn.execute(
            text(
                "SELECT CHANGE_ID AS id,PIPELINE_GRANT_ID AS grant_id,PERMISSION "
                "AS permission,ACTOR AS actor FROM AUD_CFG_PIPELINE_GRANTS_HISTORY "
                "ORDER BY CHANGE_ID"
            )
        ).all()
        assert len(after) == len(before) == 2
        for original, converted in zip(before, after, strict=True):
            assert converted.id == original.id and converted.grant_id == original.grant_id
            assert converted.actor == original.actor
            assert converted.permission == (
                Permission.CAN_VIEW if original.role == "viewer" else Permission.CAN_RUN
            )
        member = OperationContext(
            db.engine, db.config, Actor("legacy-member", ActorKind.SERVICE), role=ApiRole.VIEWER
        )
        assert pipeline_access(conn, member).levels["VIEW"] == Permission.CAN_VIEW
        assert pipeline_access(conn, member).levels["RUN"] == Permission.CAN_RUN
        # With no group membership both restricted pipelines still refuse the deployment viewer.
        ctx = OperationContext(db.engine, db.config, SYSTEM_ACTOR, role=ApiRole.VIEWER)
        assert not pipeline_access(conn, ctx).permits("VIEW")
        assert not pipeline_access(conn, ctx).permits("RUN")
