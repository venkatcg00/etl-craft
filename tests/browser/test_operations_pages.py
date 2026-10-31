"""Operations pages in a real browser: preview a purge, then purge after confirming."""

import pytest
from playwright.sync_api import expect, sync_playwright

pytestmark = pytest.mark.browser
PASSWORD = "correct horse battery"


def test_an_admin_previews_then_confirms_a_purge(site):
    _, url = site
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_context(ignore_https_errors=True).new_page()
        page.goto(url + "/signin?return_to=%2Fui%2Fprojects%2Fdefault%2Fmaintenance")
        page.get_by_label("Account name", exact=True).fill("owner")
        page.get_by_label("Password", exact=True).fill(PASSWORD)
        page.get_by_role("button", name="Sign in", exact=True).click()
        page.wait_for_url("**/maintenance")
        page.wait_for_load_state("load")
        page.get_by_role("button", name="Preview purge", exact=True).click()
        result = page.locator("#admin-result")
        expect(result).to_contain_text("Preview only: nothing changed.")
        expect(result.locator("pre")).to_contain_text('"dry_run": true')
        page.once("dialog", lambda dialog: dialog.dismiss())
        page.get_by_role("button", name="Purge", exact=True).click()
        expect(result).to_contain_text("Preview only: nothing changed.")
        page.once("dialog", lambda dialog: dialog.accept())
        page.get_by_role("button", name="Purge", exact=True).click()
        expect(result).to_contain_text("Request recorded.")
        expect(result.locator("pre")).to_contain_text('"dry_run": false')
        browser.close()
