"""Released metadata retains grant identities, history and enforcement after conversion."""

import json
import shutil

import pytest
from sqlalchemy import text

from etl_craft.core.actor import SYSTEM_ACTOR, Actor, ActorKind
from etl_craft.core.enums import ApiRole
from etl_craft.core.enums_access import Permission
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
    through_a = tmp_path / "part-a"
    through_a.mkdir()
    for file in db.dialect.migrations_dir().glob("*.sql"):
        if file.name < "0064":
            shutil.copyfile(file, through_a / file.name)
    with monkeypatch.context() as patch:
        patch.setattr(EngineDialect, "migrations_dir", lambda self: through_a)
        migrations.apply_pending_migrations(db.engine)
    expected, credentials, second = seed_project_isolation(db)
    from etl_craft.dialects.engine import for_engine

    with db.engine.connect() as conn:
        historical = for_engine(conn).table_rows(
            conn, "AUD_CFG_PIPELINE_GRANTS_HISTORY", order_by="CHANGE_ID"
        )
    migrations.apply_pending_migrations(db.engine)
    check_preserved_accounts(db, expected, credentials, second)
    with db.engine.connect() as conn:
        retained = for_engine(conn).table_rows(
            conn, "AUD_CFG_PIPELINE_GRANTS_HISTORY", order_by="CHANGE_ID"
        )
        assert retained[: len(historical)] == historical
        after = conn.execute(
            text(
                "SELECT CHANGE_ID AS id,PIPELINE_GRANT_ID AS grant_id,PERMISSION "
                "AS permission,ACTOR AS actor FROM AUD_CFG_PIPELINE_GRANTS_HISTORY "
                "WHERE PIPELINE_GRANT_ID IN (:first,:second) ORDER BY CHANGE_ID"
            ),
            {"first": before[0].grant_id, "second": before[1].grant_id},
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


def seed_project_isolation(db):
    """An independent old-policy oracle, with every deployment role and principal source."""
    from datetime import UTC, datetime

    from etl_craft.core.enums import TokenKind
    from etl_craft.core.text import sha256_hex
    from etl_craft.engine.queries import statement

    expected = {
        "legacy-member": (ApiRole.VIEWER, ("CAN_VIEW", "CAN_RUN", None, "CAN_RUN", "CAN_VIEW")),
        "outside": (ApiRole.VIEWER, (None, None, None, None, "CAN_VIEW")),
        "operator": (ApiRole.OPERATOR, (None, None, "CAN_VIEW", None, "CAN_RUN")),
        "developer": (ApiRole.DEVELOPER, (None, None, "CAN_VIEW", None, "CAN_RUN")),
        "admin": (ApiRole.ADMIN, ("CAN_MANAGE",) * 5),
        "direct": (ApiRole.VIEWER, ("CAN_MANAGE", None, None, None, "CAN_VIEW")),
        "sso": (ApiRole.VIEWER, ("CAN_VIEW", "CAN_RUN", None, "CAN_RUN", "CAN_VIEW")),
    }
    tokens = {}
    with db.engine.begin() as conn:
        for name, (role, _) in expected.items():
            if name != "legacy-member":
                conn.execute(
                    text(
                        "INSERT INTO CFG_ACCOUNTS (ACCOUNT_NAME,ACCOUNT_KIND,ROLE) VALUES "
                        "(:name,'SERVICE',:role)"
                    ),
                    {"name": name, "role": role},
                )
        for code in ("ROLE", "TAGGED", "OPEN"):
            add_pipeline(conn, code)
        conn.execute(
            text("UPDATE CFG_PIPELINES SET PIPELINE_PARAMETERS=:tags WHERE PIPELINE_CODE='TAGGED'"),
            {"tags": '{"TAGS":["classified"]}'},
        )
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINE_GRANTS (GRANTEE_ROLE,PIPELINE_CODE,PERMISSION) "
                "VALUES ('operator','ROLE','CAN_VIEW')"
            )
        )
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINE_GRANTS (ACCOUNT_ID,PIPELINE_CODE,PERMISSION) SELECT "
                "ACCOUNT_ID,'VIEW','CAN_MANAGE' FROM CFG_ACCOUNTS WHERE ACCOUNT_NAME='direct'"
            )
        )
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINE_GRANTS (GROUP_ID,TAG,PERMISSION) SELECT "
                "GROUP_ID,'classified','CAN_RUN' FROM CFG_GROUPS WHERE GROUP_NAME='RUN'"
            )
        )
        groups = (
            conn.execute(text("SELECT GROUP_ID AS group_id FROM CFG_GROUPS ORDER BY GROUP_ID"))
            .scalars()
            .all()
        )
        for name in expected:
            token = "preservation-" + name
            account = conn.execute(statement(conn, "account_find"), {"name": name}).one()
            conn.execute(
                text(
                    "INSERT INTO CFG_API_TOKENS (NAME,TOKEN_SHA256,ACCOUNT_ID,CREATED_"
                    "BY,CREATE_DATE,EXPIRES_AT,TOKEN_KIND,REFRESH_TOKEN_ID,SSO_GROUP_I"
                    "DS) VALUES (:name,:digest,:account_id,:actor,:now,:expires_at,:ki"
                    "nd,:refresh_token_id,:sso_group_ids)"
                ),
                {
                    "name": name,
                    "account_id": account.account_id,
                    "actor": SYSTEM_ACTOR.name,
                    "digest": sha256_hex(token.encode()),
                    "now": datetime.now(UTC),
                    "expires_at": None,
                    "kind": TokenKind.ACCESS if name == "sso" else TokenKind.SERVICE,
                    "refresh_token_id": None,
                    "sso_group_ids": json.dumps(groups if name == "sso" else []),
                },
            )
            tokens[name] = token
        second = conn.execute(
            text(
                "INSERT INTO CFG_PROJECTS (PROJECT_CODE,PROJECT_NAME,DEFAULT_POOL) VALUES "
                "('other','Other','default') RETURNING PROJECT_ID AS project_id"
            )
        ).scalar_one()
        add_pipeline(conn, "OPEN", project_id=second)
    return expected, tokens, second


def check_preserved_accounts(db, expected, credentials, second):
    from dataclasses import replace

    from etl_craft.core.enums import ActionName
    from etl_craft.services.operations import tokens
    from etl_craft.services.operations.identity import authenticated_context
    from fixtures.actions import act

    for name, (role, old_levels) in expected.items():
        base = OperationContext(db.engine, db.config, SYSTEM_ACTOR)
        identity = tokens.authenticate(base, credentials[name])
        assert identity is not None and identity.role == role
        ctx = authenticated_context(base, identity)
        with db.engine.connect() as conn:
            from etl_craft.engine.repository.group_claims import token_groups

            ctx = replace(
                ctx,
                sso_group_ids=token_groups(conn, identity.token_id),
                project="default",
                project_id=1,
            )
            levels = pipeline_access(conn, ctx).levels
            assert (
                tuple(levels[code] for code in ("VIEW", "RUN", "ROLE", "TAGGED", "OPEN"))
                == old_levels
            )
            other = pipeline_access(conn, replace(ctx, project="other", project_id=second))
            assert other.levels["OPEN"] == old_levels[-1]
        codes = {row.pipeline_code for row in act(ctx, ActionName.PIPELINES).pipelines}
        assert ("VIEW" in codes) == (old_levels[0] is not None)
    with db.engine.connect() as conn:
        projects = (
            conn.execute(
                text(
                    "SELECT PERMISSION AS permission FROM CFG_PIPELINE_GRANTS WHERE "
                    "PIPELINE_CODE IS NULL AND TAG IS NULL"
                )
            )
            .scalars()
            .all()
        )
        assert projects and set(projects) == {Permission.CAN_VIEW, Permission.CAN_RUN}

        seeded = conn.execute(
            text(
                "SELECT COUNT(*) AS value FROM AUD_CFG_PIPELINE_GRANTS_HISTORY "
                "WHERE PIPELINE_CODE IS NULL AND TAG IS NULL AND OPERATION='INSERT'"
            )
        ).scalar_one()
        assert seeded == len(projects)
