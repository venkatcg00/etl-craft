"""Catalog header actions, project Refresh all, and a task's definition and Data panel."""

import re

import pytest
from fastapi.testclient import TestClient

from etl_craft.api.app import create_app
from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.services.operations import OperationContext
from fixtures.actions import act
from fixtures.metadata import add_pipeline, add_task

PASSWORD = "correct horse battery"
OWNER = Actor("owner", ActorKind.USER)
TASK = "/ui/projects/default/pipelines/SALES/tasks"


@pytest.fixture
def site(metadata_project):
    from etl_craft.engine.migrations import mark_packaged_migrations_applied

    p = metadata_project
    with p.engine.begin() as conn:
        mark_packaged_migrations_applied(conn)
    ctx = OperationContext(p.engine, p.config, OWNER, role=ApiRole.ADMIN)
    for name, role in (
        ("owner", ApiRole.ADMIN),
        ("dev", ApiRole.DEVELOPER),
        ("alice", ApiRole.VIEWER),
    ):
        act(ctx, ActionName.ACCOUNT_ADD, name=name, role=role, password=PASSWORD)
    with acting_as(OWNER), p.engine.begin() as conn:
        pipeline = add_pipeline(conn, "SALES")
        add_task(
            conn,
            pipeline,
            "CUSTOMERS",
            "SQL",
            SQL_ACTION="SCD2_MERGE",
            TARGET_OBJECT="dm.customers",
            SELECT_QUERY="SELECT customer_id, region, name FROM ods.customers",
            MERGE_KEY="customer_id|region",
            MERGE_COMPARE_COLUMNS="name",
        )
        add_task(conn, pipeline, "NOTIFY", "EMAIL_ALERT")
    client = TestClient(
        create_app(ctx), base_url="https://testserver", raise_server_exceptions=False
    )
    return ctx, client


def sign_in(client, name):
    client.cookies.clear()
    page = client.get("/signin")
    csrf = re.search(r'name="csrf-token" content="([^"]+)"', page.text).group(1)
    client.post(
        "/api/v1/login/password",
        headers={"HX-Request": "true", "X-CSRF-Token": csrf, "Origin": "https://testserver"},
        json={"name": name, "password": PASSWORD, "_return_to": "/ui"},
    )


def test_task_read_returns_the_definition_and_parsed_merge_columns(site):
    ctx, _ = site
    task = act(ctx, ActionName.TASK, pipeline_code="SALES", task_code="CUSTOMERS")
    assert (task.handler, task.merge_key, task.merge_columns) == (
        "SQL",
        ("customer_id", "region"),
        ("name",),
    )
    assert task.parameters["SQL_ACTION"] == "SCD2_MERGE"
    other = act(ctx, ActionName.TASK, pipeline_code="SALES", task_code="NOTIFY")
    assert (other.merge_key, other.merge_columns) == ((), ())


def test_a_merge_task_page_offers_lookup_to_viewers_and_edits_to_admins(site):
    _, client = site
    sign_in(client, "alice")
    page = client.get(f"{TASK}/CUSTOMERS").text
    assert "<code>SCD2_MERGE</code>" in page
    assert 'name="key.customer_id"' in page and 'name="key.region"' in page
    assert "/api/v1/projects/default/values/provenance" in page
    assert 'name="values.name"' not in page and "/values/unlock" not in page
    sign_in(client, "owner")
    admin = client.get(f"{TASK}/CUSTOMERS").text
    assert 'name="values.name"' in admin and "/api/v1/projects/default/values/unlock" in admin
    assert ">Data<" not in client.get(f"{TASK}/NOTIFY").text


@pytest.mark.parametrize(
    ("name", "visible", "hidden"),
    [
        ("alice", ["/validation"], ["/docs", "/lineage/refresh", "/docs/versions"]),
        ("dev", ["/validation", "/docs", "/lineage/refresh"], ["/docs/versions"]),
        ("owner", ["/validation", "/docs", "/lineage/refresh", "/docs/versions"], []),
    ],
)
def test_catalog_header_actions_follow_the_role(site, name, visible, hidden):
    _, client = site
    sign_in(client, name)
    page = client.get("/ui/projects/default/catalog").text
    for route in visible:
        assert f'data-url="/api/v1/projects/default{route}"' in page
    for route in hidden:
        assert f'data-url="/api/v1/projects/default{route}"' not in page


def test_refresh_all_is_offered_to_developers_on_the_pipeline_list(site):
    _, client = site
    sign_in(client, "alice")
    assert "Refresh all" not in client.get("/ui/projects/default/pipelines").text
    sign_in(client, "dev")
    page = client.get("/ui/projects/default/pipelines").text
    assert 'data-post="/api/v1/projects/default/pipelines/refresh"' in page
    assert "data-body='{}'" in page


def test_before_the_first_build_the_catalog_page_says_so_and_offers_rebuild(site):
    _, client = site
    sign_in(client, "dev")
    page = client.get("/ui/projects/default/catalog")
    assert page.status_code == 200
    assert "catalog is not generated" in page.text and "Rebuild catalog" in page.text
    assert "catalog-frame" not in page.text
