"""The Uploads page in a real browser: choose a pipeline, upload a file, see its receipt."""

import pytest
from playwright.sync_api import expect, sync_playwright
from sqlalchemy import text

from etl_craft.core.actor import Actor, ActorKind, acting_as
from fixtures.metadata import add_pipeline, add_task

pytestmark = pytest.mark.browser
PASSWORD = "correct horse battery"


def test_a_steward_uploads_a_file_and_sees_it_listed(site, tmp_path):
    ctx, url = site
    with acting_as(Actor("owner", ActorKind.USER)), ctx.engine.begin() as conn:
        pipeline = add_pipeline(conn, "CORRECTIONS")
        add_task(
            conn,
            pipeline,
            "check",
            "FILE_VALIDATION",
            SCRIPT_FILE="validation_scripts/check.py",
            FILE_FORMAT="CSV",
        )
        conn.execute(text("UPDATE CFG_PIPELINES SET STEWARDABLE_FLAG='Y'"))
    data = tmp_path / "corrections.csv"
    data.write_text("id|name\n1|first\n")
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_context(ignore_https_errors=True).new_page()
        page.goto(url + "/signin?return_to=%2Fui%2Fprojects%2Fdefault%2Fuploads")
        page.get_by_label("Account name", exact=True).fill("owner")
        page.get_by_label("Password", exact=True).fill(PASSWORD)
        page.get_by_role("button", name="Sign in", exact=True).click()
        page.wait_for_url("**/uploads")
        page.wait_for_load_state("load")
        page.get_by_label("File", exact=True).set_input_files(str(data))
        page.get_by_role("button", name="Upload", exact=True).click()
        expect(page.locator("#upload-result")).to_contain_text("received")
        page.get_by_role("link", name="Reload to see its state").click()
        expect(page.get_by_role("cell", name="corrections.csv")).to_be_visible()
        browser.close()
