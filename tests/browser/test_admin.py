"""The administration page in a real browser: post an announcement and create a token once."""

import pytest
from playwright.sync_api import expect, sync_playwright

pytestmark = pytest.mark.browser
PASSWORD = "correct horse battery"


def test_an_admin_posts_an_announcement_ends_it_and_creates_a_token(site):
    _, url = site
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_context(ignore_https_errors=True).new_page()
        page.goto(url + "/signin?return_to=%2Fui%2Fadmin%3Ftab%3Dannouncements")
        page.get_by_label("Account name", exact=True).fill("owner")
        page.get_by_label("Password", exact=True).fill(PASSWORD)
        page.get_by_role("button", name="Sign in", exact=True).click()
        page.wait_for_url("**/ui/admin?tab=announcements")
        page.wait_for_load_state("load")
        page.get_by_label("Message").fill("Upgrade tonight")
        page.get_by_label("Level").select_option("WARNING")
        page.get_by_label("Reason (recorded)").fill("upgrade")
        page.get_by_role("button", name="Post", exact=True).click()
        expect(page.locator("#admin-result")).to_contain_text("Request recorded.")
        page.reload()
        expect(page.locator(".announcement")).to_contain_text("Upgrade tonight")
        page.get_by_role("button", name="End", exact=True).click()
        dialog = page.get_by_role("dialog")
        dialog.get_by_label("Reason (recorded with the request)").fill("done")
        dialog.get_by_role("button", name="Request", exact=True).click()
        expect(page.locator("#result")).to_contain_text("End")
        page.reload()
        expect(page.locator(".announcement")).to_have_count(0)
        page.get_by_role("link", name="Tokens", exact=True).click()
        page.wait_for_url("**tab=tokens")
        page.wait_for_load_state("load")
        page.get_by_label("Name", exact=True).fill("loader")
        page.get_by_role("button", name="Create token", exact=True).click()
        expect(page.locator("#created-token")).not_to_be_empty()
        page.get_by_role("link", name="Accounts", exact=True).click()
        page.wait_for_url("**tab=accounts")
        page.wait_for_load_state("load")
        page.get_by_role("button", name="Disable", exact=True).last.click()
        expect(dialog.get_by_label("Reason (recorded with the request)")).to_be_hidden()
        browser.close()
