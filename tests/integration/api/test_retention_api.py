"""The retention API enforces the same project and administrator boundary as the CLI."""

from dataclasses import replace

from fastapi.testclient import TestClient
from sqlalchemy import text

from etl_craft.api.app import create_app
from etl_craft.config.model import Retention
from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.tokens import TokenCreate


def test_http_purge_requires_admin_and_returns_the_shared_snapshot(engine_db):
    ctx = OperationContext(
        engine_db.engine,
        replace(engine_db.config, retention=Retention(run_history_days=90)),
        SYSTEM_ACTOR,
        role=ApiRole.ADMIN,
    )
    admin = invoke(ctx, ActionName.TOKEN_CREATE, TokenCreate(name="admin", role=ApiRole.ADMIN))
    developer = invoke(
        ctx, ActionName.TOKEN_CREATE, TokenCreate(name="developer", role=ApiRole.DEVELOPER)
    )
    with TestClient(create_app(ctx)) as client:
        response = client.post(
            "/api/v1/projects/default/purge",
            json={"dry_run": True},
            headers={"Authorization": f"Bearer {developer.token}"},
        )
        assert response.status_code == 403, response.text
        response = client.post(
            "/api/v1/projects/default/purge",
            json={"dry_run": True},
            headers={"Authorization": f"Bearer {admin.token}"},
        )
        assert response.status_code == 200, response.text
        assert response.json()["dry_run"] is True
        assert "counts" in response.json()
    with ctx.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT COUNT(*) AS count FROM AUD_RETENTION_TICKS")).scalar_one()
            == 0
        )
