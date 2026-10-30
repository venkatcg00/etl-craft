"""The Workflow Monitor in a real browser: select through the views, then act on the selection."""

import pytest
from playwright.sync_api import expect, sync_playwright
from sqlalchemy import text

from etl_craft.core.actor import Actor, ActorKind, acting_as
from fixtures.metadata import add_dependency, add_pipeline, add_task, finish_run, start_run

pytestmark = pytest.mark.browser
PASSWORD = "correct horse battery"


@pytest.fixture
def run_site(site):
    ctx, url = site
    with acting_as(Actor("owner", ActorKind.USER)), ctx.engine.begin() as conn:
        pipeline = add_pipeline(conn, "OPS")
        extract = add_task(conn, pipeline, "EXTRACT")
        load = add_task(conn, pipeline, "LOAD")
        add_dependency(conn, pipeline, load, extract)
        finish_run(conn, start_run(conn, pipeline))
        active = start_run(conn, pipeline)
    return ctx, url, active


@pytest.mark.parametrize("browser_name", ["chromium", "firefox"])
def test_select_a_task_in_the_graph_and_hold_it_from_the_dialog(run_site, browser_name):
    ctx, url, active = run_site
    with sync_playwright() as pw:
        browser = getattr(pw, browser_name).launch()
        page = browser.new_context(ignore_https_errors=True).new_page()
        page.goto(url + "/signin?return_to=%2Fui%2Fprojects%2Fdefault%2Fpipelines")
        page.get_by_label("Account name", exact=True).fill("owner")
        page.get_by_label("Password", exact=True).fill(PASSWORD)
        page.get_by_role("button", name="Sign in", exact=True).click()
        page.get_by_role("link", name="OPS", exact=True).click()
        page.get_by_role("link", name=str(active), exact=True).click()
        expect(page.get_by_text(f"Selected: the whole run {active}")).to_be_visible()
        page.get_by_role("link", name="LOAD, QUEUED").or_(
            page.get_by_role("link", name="LOAD, not started")
        ).first.click()
        expect(page.get_by_role("status").filter(has_text="task LOAD")).to_be_visible()
        page.get_by_role("button", name="Hold", exact=True).click()
        dialog = page.get_by_role("dialog")
        expect(dialog.get_by_text(f"task LOAD in run {active}")).to_be_visible()
        dialog.get_by_label("Reason (recorded with the request)").fill("check the load")
        dialog.get_by_role("button", name="Request", exact=True).click()
        expect(page.locator("#result")).to_contain_text("HOLD")
        expect(page.get_by_role("button", name="Hold", exact=True)).to_be_focused()
        page.go_back()
        expect(page.get_by_text(f"Selected: the whole run {active}")).to_be_visible()
        browser.close()
    with ctx.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT ACTION AS action, TASK_ID AS task_id, REASON AS reason "
                "FROM AUD_CONTROL_REQUESTS WHERE PIPELINE_RUN_ID=:run"
            ),
            {"run": active},
        ).all()
    assert [(row.action, row.reason) for row in rows] == [("HOLD", "check the load")]


def test_a_refused_control_keeps_the_dialog_open_with_the_servers_reason(run_site):
    _, url, active = run_site
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_context(ignore_https_errors=True).new_page()
        page.goto(url + "/signin?return_to=%2Fui%2Fprojects%2Fdefault%2Fpipelines%2FOPS")
        page.get_by_label("Account name", exact=True).fill("owner")
        page.get_by_label("Password", exact=True).fill(PASSWORD)
        page.get_by_role("button", name="Sign in", exact=True).click()
        page.get_by_role("button", name="Pause", exact=True).click()
        dialog = page.get_by_role("dialog")
        dialog.get_by_role("button", name="Request", exact=True).click()
        expect(page.locator("#control-error")).to_contain_text("reason")
        dialog.get_by_label("Reason (recorded with the request)").fill("maintenance")
        dialog.get_by_role("button", name="Request", exact=True).click()
        expect(page.locator("#result")).to_contain_text("PAUSE")
        page.goto(url + f"/ui/projects/default/pipelines/OPS/runs/{active}?task=LOAD")
        page.evaluate("document.querySelector('#control-dialog').dataset.run = '999999'")
        page.get_by_role("button", name="Hold", exact=True).click()
        dialog.get_by_label("Reason (recorded with the request)").fill("stale page")
        dialog.get_by_role("button", name="Request", exact=True).click()
        error = page.locator("#control-error")
        expect(error).to_be_visible()
        expect(page.get_by_role("dialog")).to_be_visible()
        browser.close()
