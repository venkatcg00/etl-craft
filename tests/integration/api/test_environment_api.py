"""The API and CLI use the same administrator fields and intervention policy."""

import pytest
from fastapi.testclient import TestClient

from etl_craft.api.app import create_app
from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ActionName, ApiRole, ControlAction
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import ACTIONS, action_path
from fixtures.actions import act
from fixtures.metadata import add_pipeline


@pytest.mark.parametrize("policy", ("STRICT", "RELAXED"))
def test_api_governance_and_admin_environment_fields(engine_db, policy):
    ctx = OperationContext(engine_db.engine, engine_db.config, SYSTEM_ACTOR, role=ApiRole.ADMIN)
    admin = act(ctx, ActionName.TOKEN_CREATE, name="owner", role=ApiRole.ADMIN)
    viewer = act(ctx, ActionName.TOKEN_CREATE, name="manager", role=ApiRole.VIEWER)
    act(
        ctx,
        ActionName.ACCESS_GRANT,
        project="default",
        account="manager",
        permission="CAN_MANAGE",
        reason="delegate",
    )
    with ctx.engine.begin() as conn:
        add_pipeline(conn, "P")
    client = TestClient(create_app(ctx))
    headers = {"Authorization": "Bearer " + admin.token}
    fields = {
        "project": "default",
        "environment_type": "DEV",
        "environment_name": "test",
        "governance": policy,
    }
    path = action_path(ACTIONS[ActionName.PROJECT_SET])
    assert (
        client.post(
            path,
            json={**fields, "reason": "policy"},
            headers={"Authorization": "Bearer " + viewer.token},
        ).status_code
        == 403
    )
    assert client.post(path, json=fields, headers=headers).status_code >= 400
    response = client.post(path, json={**fields, "reason": "policy"}, headers=headers)
    assert response.status_code == 200, response.text
    refused = client.post(
        path,
        json={**fields, "environment_type": "PROD", "governance": "RELAXED", "reason": "test"},
        headers=headers,
    )
    assert refused.status_code >= 400 and "PROD requires STRICT" in refused.text
    control_path = action_path(ACTIONS[ActionName.CONTROL]).format(project="default")
    for action in (ControlAction.PAUSE, ControlAction.RESUME):
        response = client.post(
            control_path,
            json={"pipeline_code": "P", "action": action, "reason": ""},
            headers=headers,
        )
        if policy == "STRICT":
            assert (
                response.status_code >= 400
                and "STRICT" in response.text
                and "--reason" in response.text
            )
        else:
            assert response.status_code == 200, response.text
