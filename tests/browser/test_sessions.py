"""Real browser navigation, account flows and both accessible themes over HTTPS."""

import pytest
from playwright.sync_api import expect, sync_playwright

from etl_craft.core.actor import acting_as
from etl_craft.core.enums import ActionName, ApiRole
from fixtures.actions import act

pytestmark = pytest.mark.browser
PASSWORD = "correct horse battery"


def sign_in(page, url, password=PASSWORD):
    page.goto(url + "/signin?return_to=%2Fui%2Fabout")
    page.get_by_label("Account name", exact=True).fill("alice")
    page.get_by_label("Password", exact=True).fill(password)
    page.get_by_role("button", name="Sign in", exact=True).click()


@pytest.mark.parametrize("browser_name", ["chromium", "firefox"])
def test_sessions_settings_themes_and_keyboard(site, browser_name):
    ctx, url = site
    with sync_playwright() as pw:
        browser = getattr(pw, browser_name).launch()
        context = browser.new_context(
            ignore_https_errors=True, viewport={"width": 1280, "height": 900}
        )
        page = context.new_page()
        sign_in(page, url)
        expect(page).to_have_url(url + "/ui/about")
        expect(page.get_by_role("heading", name="About etl-craft")).to_be_visible()
        tab = context.new_page()
        tab.goto(url + "/ui/profile/settings")
        tab.get_by_label("Theme", exact=True).select_option("dark")
        tab.get_by_label("Timezone (IANA name)").fill("Asia/Kolkata")
        tab.get_by_label("Page size").fill("40")
        tab.get_by_role("button", name="Save settings").click()
        expect(tab.get_by_text("Personal settings saved.")).to_be_visible()
        page.reload()
        expect(page.locator("html")).to_have_attribute("data-theme", "dark")
        for theme in ("light", "dark"):
            page.evaluate("theme => document.documentElement.dataset.theme=theme", theme)
            contrast = page.evaluate("""() => {
                const rgb=s=>s.match(/[\\d.]+/g).slice(0,3).map(Number)
                    .map(x=>{x/=255;return x<=.04045?x/12.92:((x+.055)/1.055)**2.4});
                const lum=c=>{const [r,g,b]=rgb(c);return .2126*r+.7152*g+.0722*b;};
                const results=[];
                for(const selector of
                    ['body','.panel','a','.environment','footer','button','input']) {
                    const el=document.querySelector(selector);if(!el)continue;
                    let parent=el,bg=getComputedStyle(parent).backgroundColor;
                    while(bg==='rgba(0, 0, 0, 0)'&&parent.parentElement){
                        parent=parent.parentElement;bg=getComputedStyle(parent).backgroundColor;}
                    const a=lum(getComputedStyle(el).color),b=lum(bg);
                    results.push([selector,(Math.max(a,b)+.05)/(Math.min(a,b)+.05)]);
                } return results;
            }""")
            assert all(ratio >= 4.5 for _, ratio in contrast), (theme, contrast)
        page.keyboard.press("?")
        expect(page.get_by_role("dialog")).to_be_visible()
        page.keyboard.press("Escape")
        expect(page.get_by_role("dialog")).not_to_be_visible()
        page.goto(url + "/ui")
        page.keyboard.press("Tab")
        expect(page.get_by_role("link", name="Skip to content")).to_be_focused()
        page.keyboard.press("Enter")
        expect(page.locator("#main")).to_be_focused()
        page.set_viewport_size({"width": 640, "height": 450})
        assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
        cookies = context.cookies()
        assert all(
            c["httpOnly"] and c["secure"] and c["sameSite"] == "Lax"
            for c in cookies
            if c["name"].startswith("__Host-etl-craft")
        )
        assert page.evaluate(
            "Object.keys(localStorage).every(k=>!k.toLowerCase().includes('token'))"
        )
        page.get_by_text("alice · Profile", exact=True).click()
        page.get_by_role("button", name="Sign out", exact=True).click()
        expect(page).to_have_url(url + "/signin")
        expect(tab).to_have_url(url + "/signin?return_to=%2Fui%2Fprofile%2Fsettings")
        for _ in range(ctx.config.auth.lockout_after):
            sign_in(page, url, "wrong")
            expect(
                page.get_by_text(
                    "sign-in refused; account name or password is incorrect.", exact=True
                )
            ).to_be_visible()
        sign_in(page, url)
        expect(
            page.get_by_text("Account locked, contact your administrator", exact=False)
        ).to_be_visible()
        act(ctx, ActionName.ACCOUNT_UNLOCK, name="alice")
        act(ctx, ActionName.ACCOUNT_RESET_PASSWORD, name="alice", password="temporary password")
        sign_in(page, url, "temporary password")
        expect(page.get_by_role("heading", name="Change password", exact=True)).to_be_visible()
        page.get_by_label("Current password", exact=True).fill("temporary password")
        page.get_by_label("New password", exact=True).fill("replacement password")
        page.get_by_label("Confirm new password", exact=True).fill("replacement password")
        page.get_by_role("button", name="Change password and sign out").click()
        expect(page).to_have_url(url + "/signin?return_to=%2Fui%2Fabout")
        sign_in(page, url, "replacement password")
        expect(page).to_have_url(url + "/ui/about")
        context.close()
        browser.close()


def test_shared_mark_visuals_in_both_themes_and_sizes(site, tmp_path):
    import os
    from pathlib import Path

    _, url = site
    output = Path(os.environ.get("ETL_CRAFT_UI_SCREENSHOTS", str(tmp_path / "screenshots")))
    output.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        for theme in ("light", "dark"):
            context = browser.new_context(ignore_https_errors=True, color_scheme=theme)
            page = context.new_page()
            for width, size in ((1280, "desktop"), (375, "mobile")):
                page.set_viewport_size({"width": width, "height": 900})
                page.goto(url + "/signin")
                page.evaluate("theme=>document.documentElement.dataset.theme=theme", theme)
                expect(page.locator(".signin-mark svg")).to_be_visible()
                expect(page.get_by_role("link", name="etl-craft home", exact=True)).to_be_visible()
                assert page.locator(".signin-mark svg").bounding_box()["width"] == 56
                page.screenshot(path=str(output / f"signin-{theme}-{size}.png"), full_page=True)
                sign_in(page, url)
                expect(page).to_have_url(url + "/ui/about")
                page.evaluate("theme=>document.documentElement.dataset.theme=theme", theme)
                page.locator(".topbar").screenshot(path=str(output / f"topbar-{theme}-{size}.png"))
                context.clear_cookies()
            for size in (16, 32, 64):
                page.set_content(
                    f'<img alt="etl-craft favicon at {size} pixels" width="{size}" '
                    f'height="{size}" src="{url}/static/shared/favicon.svg">'
                )
                expect(page.locator("img")).to_be_visible()
                page.locator("img").screenshot(
                    path=str(output / f"favicon-{size}-{theme}.png"), omit_background=True
                )
            context.close()
        browser.close()


def test_browser_empty_error_loading_csrf_and_expiry_states(site, monkeypatch):
    from etl_craft.api import ui_pages

    ctx, url = site
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        context = browser.new_context(ignore_https_errors=True)
        page = context.new_page()
        page.clock.install()
        sign_in(page, url)
        expect(page).to_have_url(url + "/ui/about")
        denied = context.request.post(url + "/api/v1/account/settings", data={"theme": "dark"})
        assert denied.status == 403
        page.goto(url + "/ui/profile/settings")
        pending = []
        page.route("**/api/v1/account/settings", lambda route: pending.append(route))
        page.get_by_role("button", name="Save settings").click()
        expect(page.locator("#loading")).to_be_visible()
        pending[0].continue_()
        expect(page.get_by_text("Personal settings saved.")).to_be_visible()
        page.unroute("**/api/v1/account/settings")
        page.goto(url + "/ui/missing")
        expect(page.get_by_role("heading", name="Not found or no access")).to_be_visible()
        original = ui_pages.invoke

        def failing(caller, name, request):
            if name == ActionName.ABOUT:
                raise OSError("private server details")
            return original(caller, name, request)

        with monkeypatch.context() as patch:
            patch.setattr(ui_pages, "invoke", failing)
            page.goto(url + "/ui/about")
            expect(page.get_by_role("heading", name="Server error")).to_be_visible()
            expect(page.get_by_role("button", name="Copy details")).to_be_visible()
            assert "private server details" not in page.content()
        with acting_as(ctx.actor), ctx.engine.begin() as conn:
            from sqlalchemy import text

            conn.execute(text("UPDATE CFG_PIPELINE_GRANTS SET ACTIVE_FLAG='N'"))
        page.goto(url + "/ui")
        expect(page.get_by_role("heading", name="No visible projects")).to_be_visible()
        delay = page.evaluate("Date.parse(document.body.dataset.expires)-Date.now()-120000+1")
        page.clock.fast_forward(max(1, int(delay)))
        expect(page.locator("#session-warning")).to_be_visible()
        page.clock.fast_forward(120000)
        expect(page).to_have_url(url + "/signin?return_to=%2Fui")
        context.close()
        browser.close()


def test_keyboard_only_signin_and_settings_across_browser_engines(site):
    _, url = site
    with sync_playwright() as pw:
        chromium = pw.chromium.launch()
        context = chromium.new_context(ignore_https_errors=True)
        page = context.new_page()
        page.goto(url + "/signin")
        for _ in range(4):
            page.keyboard.press("Tab")
        expect(page.get_by_label("Account name", exact=True)).to_be_focused()
        page.keyboard.type("alice")
        page.keyboard.press("Tab")
        page.keyboard.type(PASSWORD)
        page.keyboard.press("Tab")
        page.keyboard.press("Enter")
        expect(page).to_have_url(url + "/ui")
        page.goto(url + "/ui/profile/settings")
        page.get_by_label("Theme", exact=True).select_option("dark")
        page.get_by_role("button", name="Save settings").click()
        expect(page.get_by_text("Personal settings saved.")).to_be_visible()
        context.close()
        chromium.close()
        firefox = pw.firefox.launch()
        context = firefox.new_context(ignore_https_errors=True)
        page = context.new_page()
        sign_in(page, url)
        expect(page).to_have_url(url + "/ui/about")
        expect(page.locator("html")).to_have_attribute("data-theme", "dark")
        context.close()
        firefox.close()


def test_catalog_and_shell_share_theme_and_session(site):
    from importlib.resources import files

    ctx, url = site
    folder = ctx.config.host_dir / "catalog"
    folder.mkdir()
    folder.with_name("." + folder.name + ".publish.lock").touch()
    (folder / ".etl-craft-catalog").write_text('{"project":"default"}')
    button = files("etl_craft.core").joinpath("static/theme_button.html").read_text()
    (folder / "index.html").write_text(
        '<html lang="en"><head><title>Project catalog</title></head><body>'
        + button
        + '<script src="/static/shared/theme.js"></script></body></html>'
    )
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        context = browser.new_context(ignore_https_errors=True)
        page = context.new_page()
        sign_in(page, url)
        expect(page).to_have_url(url + "/ui/about")
        page.goto(url + "/ui/projects/default/catalog")
        frame = page.frame_locator("#catalog-frame")
        frame.get_by_role("button", name="Theme: system.", exact=False).click()
        expect(page.locator("html")).to_have_attribute("data-theme", "light")
        expect(page.get_by_role("button", name="Theme: light.", exact=False)).to_be_visible()
        page.get_by_role("button", name="Theme: light.", exact=False).click()
        expect(frame.locator("html")).to_have_attribute("data-theme", "dark")
        context.close()
        browser.close()


@pytest.mark.parametrize("site", ["sso"], indirect=True)
def test_sso_approval_preserves_poll_interval_and_adds_slowdown(site, monkeypatch):
    import time

    from etl_craft.services import oidc

    ctx, url = site
    act(ctx, ActionName.ACCOUNT_ADD, name="sso", role=ApiRole.VIEWER, oidc_subject="subject")
    monkeypatch.setattr(
        oidc,
        "start_device",
        lambda auth: oidc.DeviceGrant(
            "APPROVE", "https://issuer.example/approve", "device", "nonce", 2, 60
        ),
    )
    observed = []
    results = iter((0, 5, oidc.IdentityClaims("subject")))

    def poll(*args):
        observed.append(time.monotonic())
        return next(results)

    monkeypatch.setattr(oidc, "poll_device", poll)
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        context = browser.new_context(ignore_https_errors=True)
        page = context.new_page()
        page.goto(url + "/signin?return_to=%2Fui%2Fabout")
        page.wait_for_load_state("networkidle")
        page.get_by_role("button", name="Sign in with SSO", exact=True).click()
        expect(page.get_by_role("heading", name="Approve SSO sign-in")).to_be_visible()
        expect(page).to_have_url(url + "/ui/about", timeout=30000)
        assert observed[1] - observed[0] >= 1.9
        assert observed[2] - observed[1] >= 6.9
        expect(page.get_by_text("sso · Profile", exact=True)).to_be_visible()
        page.get_by_text("sso · Profile", exact=True).click()
        expect(page.get_by_role("link", name="Change password", exact=True)).to_have_count(0)
        context.close()
        browser.close()
